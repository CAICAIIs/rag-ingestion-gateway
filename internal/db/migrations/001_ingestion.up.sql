-- Enable btree_gist for EXCLUDE constraint on active tasks
CREATE EXTENSION IF NOT EXISTS btree_gist;

-- ============================================================
-- ingestion_tasks: core FSM table for pipeline task tracking
-- ============================================================
CREATE TABLE IF NOT EXISTS ingestion_tasks (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    paper_id        TEXT NOT NULL,
    source_url      TEXT,
    pdf_object_key  TEXT,
    callback_url    TEXT,
    state           TEXT NOT NULL DEFAULT 'pending',
    status          TEXT NOT NULL DEFAULT 'queued',
    error_message   TEXT,
    retry_count     INT NOT NULL DEFAULT 0,
    max_retries     INT NOT NULL DEFAULT 3,
    worker_id       TEXT,
    last_heartbeat  TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Dispatcher polling: fetch pending/active tasks efficiently
CREATE INDEX idx_ingestion_tasks_active_state
    ON ingestion_tasks (state)
    WHERE state NOT IN ('completed', 'failed', 'canceled');

-- Duplicate checking by paper_id + state
CREATE INDEX idx_ingestion_tasks_paper_state
    ON ingestion_tasks (paper_id, state);

-- Prevent duplicate active tasks for the same paper
EXCLUDE USING btree (paper_id WITH =)
    WHERE (status IN ('queued', 'processing'));

-- Zombie detection: find stale heartbeats on active tasks
CREATE INDEX idx_ingestion_tasks_zombie
    ON ingestion_tasks (last_heartbeat)
    WHERE state NOT IN ('completed', 'failed', 'canceled');

-- Auto-update updated_at on row modification
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_ingestion_tasks_updated_at
    BEFORE UPDATE ON ingestion_tasks
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================
-- ingestion_state_history: audit trail for FSM transitions
-- ============================================================
CREATE TABLE IF NOT EXISTS ingestion_state_history (
    id          BIGSERIAL PRIMARY KEY,
    task_id     UUID NOT NULL REFERENCES ingestion_tasks(id) ON DELETE CASCADE,
    from_state  TEXT,
    to_state    TEXT NOT NULL,
    changed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    reason      TEXT
);

CREATE INDEX idx_state_history_task_time
    ON ingestion_state_history (task_id, changed_at);

-- ============================================================
-- ingestion_artifacts: pipeline output references
-- ============================================================
CREATE TABLE IF NOT EXISTS ingestion_artifacts (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id     UUID NOT NULL REFERENCES ingestion_tasks(id) ON DELETE CASCADE,
    kind        TEXT NOT NULL,   -- 'chunk', 'embedding', 'qdrant_point'
    ref_key     TEXT NOT NULL,   -- object key, point ID, etc.
    metadata    JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_artifacts_task_kind
    ON ingestion_artifacts (task_id, kind);
