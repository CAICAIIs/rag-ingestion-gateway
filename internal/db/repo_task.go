package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrStateConflict     = errors.New("state conflict: task not in expected state")
	ErrTaskNotFound      = errors.New("task not found")
	ErrTaskNotCancelable = errors.New("task cannot be canceled in current state")
)

// stateToStatus maps FSM states to collapsed status values.
var stateToStatus = map[string]string{
	"pending":     "queued",
	"downloading": "processing",
	"chunking":    "processing",
	"embedding":   "processing",
	"indexing":    "processing",
	"completed":   "completed",
	"failed":      "failed",
	"canceled":    "canceled",
}

// Task represents a row in ingestion_tasks.
type Task struct {
	ID            uuid.UUID
	PaperID       string
	SourceURL     string
	PDFObjectKey  string
	CallbackURL   string
	State         string
	Status        string
	ErrorMessage  *string
	RetryCount    int
	MaxRetries    int
	WorkerID      *string
	LastHeartbeat *time.Time
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// TaskRepo provides database operations for ingestion tasks using pgx.
type TaskRepo struct {
	pool *pgxpool.Pool
}

// NewTaskRepo creates a new TaskRepo.
func NewTaskRepo(pool *pgxpool.Pool) *TaskRepo {
	return &TaskRepo{pool: pool}
}

// CreateTask inserts a new ingestion task and its initial state history record.
func (r *TaskRepo) CreateTask(ctx context.Context, paperID, sourceURL, pdfObjectKey, callbackURL string, maxRetries int) (uuid.UUID, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return uuid.Nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	var id uuid.UUID
	err = tx.QueryRow(ctx,
		`INSERT INTO ingestion_tasks (paper_id, source_url, pdf_object_key, callback_url, max_retries)
		 VALUES ($1, $2, $3, $4, $5)
		 RETURNING id`,
		paperID, sourceURL, pdfObjectKey, callbackURL, maxRetries,
	).Scan(&id)
	if err != nil {
		return uuid.Nil, fmt.Errorf("insert task: %w", err)
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO ingestion_state_history (task_id, from_state, to_state, reason)
		 VALUES ($1, NULL, 'pending', 'task created')`,
		id,
	)
	if err != nil {
		return uuid.Nil, fmt.Errorf("insert initial history: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, fmt.Errorf("commit: %w", err)
	}
	return id, nil
}

// GetTask retrieves a task by ID.
func (r *TaskRepo) GetTask(ctx context.Context, taskID uuid.UUID) (*Task, error) {
	t := &Task{}
	err := r.pool.QueryRow(ctx,
		`SELECT id, paper_id, source_url, pdf_object_key, callback_url,
		        state, status, error_message, retry_count, max_retries,
		        worker_id, last_heartbeat, created_at, updated_at
		 FROM ingestion_tasks WHERE id = $1`,
		taskID,
	).Scan(
		&t.ID, &t.PaperID, &t.SourceURL, &t.PDFObjectKey, &t.CallbackURL,
		&t.State, &t.Status, &t.ErrorMessage, &t.RetryCount, &t.MaxRetries,
		&t.WorkerID, &t.LastHeartbeat, &t.CreatedAt, &t.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrTaskNotFound
		}
		return nil, fmt.Errorf("get task: %w", err)
	}
	return t, nil
}

// TransitionState atomically transitions a task from one state to another.
func (r *TaskRepo) TransitionState(ctx context.Context, taskID uuid.UUID, fromState, toState, reason string) error {
	status, ok := stateToStatus[toState]
	if !ok {
		return fmt.Errorf("unknown target state: %s", toState)
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx,
		`UPDATE ingestion_tasks SET state = $1, status = $2
		 WHERE id = $3 AND state = $4`,
		toState, status, taskID, fromState,
	)
	if err != nil {
		return fmt.Errorf("update state: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrStateConflict
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO ingestion_state_history (task_id, from_state, to_state, reason)
		 VALUES ($1, $2, $3, $4)`,
		taskID, fromState, toState, reason,
	)
	if err != nil {
		return fmt.Errorf("insert history: %w", err)
	}

	return tx.Commit(ctx)
}

// ClaimTask assigns a worker to a pending task.
func (r *TaskRepo) ClaimTask(ctx context.Context, taskID uuid.UUID, workerID string) error {
	tag, err := r.pool.Exec(ctx,
		`UPDATE ingestion_tasks SET worker_id = $1, last_heartbeat = now()
		 WHERE id = $2 AND state = 'pending'`,
		workerID, taskID,
	)
	if err != nil {
		return fmt.Errorf("claim task: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrStateConflict
	}
	return nil
}

// Heartbeat updates the last_heartbeat timestamp for a task.
func (r *TaskRepo) Heartbeat(ctx context.Context, taskID uuid.UUID) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE ingestion_tasks SET last_heartbeat = now() WHERE id = $1`,
		taskID,
	)
	if err != nil {
		return fmt.Errorf("heartbeat: %w", err)
	}
	return nil
}

// FetchPendingTasks returns up to limit tasks in pending state, ordered by creation time.
func (r *TaskRepo) FetchPendingTasks(ctx context.Context, limit int) ([]Task, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, paper_id, source_url, pdf_object_key, callback_url,
		        state, status, error_message, retry_count, max_retries,
		        worker_id, last_heartbeat, created_at, updated_at
		 FROM ingestion_tasks
		 WHERE state = 'pending'
		 ORDER BY created_at ASC
		 LIMIT $1`,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("fetch pending: %w", err)
	}
	defer rows.Close()

	return scanTasks(rows)
}

// FetchZombieTasks returns tasks with stale heartbeats that are in active processing states.
func (r *TaskRepo) FetchZombieTasks(ctx context.Context, timeout time.Duration) ([]Task, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, paper_id, source_url, pdf_object_key, callback_url,
		        state, status, error_message, retry_count, max_retries,
		        worker_id, last_heartbeat, created_at, updated_at
		 FROM ingestion_tasks
		 WHERE state NOT IN ('pending', 'completed', 'failed', 'canceled')
		   AND last_heartbeat < now() - $1::interval`,
		timeout.String(),
	)
	if err != nil {
		return nil, fmt.Errorf("fetch zombies: %w", err)
	}
	defer rows.Close()

	return scanTasks(rows)
}

// SetError marks a task as failed with an error message.
func (r *TaskRepo) SetError(ctx context.Context, taskID uuid.UUID, errMsg string) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	var fromState string
	err = tx.QueryRow(ctx,
		`UPDATE ingestion_tasks SET state = 'failed', status = 'failed', error_message = $1
		 WHERE id = $2
		 RETURNING (SELECT state FROM ingestion_tasks WHERE id = $2)`,
		errMsg, taskID,
	).Scan(&fromState)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrTaskNotFound
		}
		return fmt.Errorf("set error: %w", err)
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO ingestion_state_history (task_id, from_state, to_state, reason)
		 VALUES ($1, $2, 'failed', $3)`,
		taskID, fromState, errMsg,
	)
	if err != nil {
		return fmt.Errorf("insert history: %w", err)
	}

	return tx.Commit(ctx)
}

// CancelTask cancels a task if it's in a cancelable state.
func (r *TaskRepo) CancelTask(ctx context.Context, taskID uuid.UUID) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	var fromState string
	err = tx.QueryRow(ctx,
		`UPDATE ingestion_tasks SET state = 'canceled', status = 'canceled'
		 WHERE id = $1 AND state NOT IN ('completed', 'failed', 'canceled')
		 RETURNING (SELECT state FROM ingestion_tasks WHERE id = $1)`,
		taskID,
	).Scan(&fromState)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrTaskNotCancelable
		}
		return fmt.Errorf("cancel task: %w", err)
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO ingestion_state_history (task_id, from_state, to_state, reason)
		 VALUES ($1, $2, 'canceled', 'user requested cancellation')`,
		taskID, fromState,
	)
	if err != nil {
		return fmt.Errorf("insert history: %w", err)
	}

	return tx.Commit(ctx)
}

// IncrementRetry bumps the retry count and returns the new value.
func (r *TaskRepo) IncrementRetry(ctx context.Context, taskID uuid.UUID) (int, error) {
	var count int
	err := r.pool.QueryRow(ctx,
		`UPDATE ingestion_tasks SET retry_count = retry_count + 1
		 WHERE id = $1
		 RETURNING retry_count`,
		taskID,
	).Scan(&count)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, ErrTaskNotFound
		}
		return 0, fmt.Errorf("increment retry: %w", err)
	}
	return count, nil
}

// UpdatePDFObjectKey sets the pdf_object_key after a successful download.
func (r *TaskRepo) UpdatePDFObjectKey(ctx context.Context, taskID uuid.UUID, key string) error {
	tag, err := r.pool.Exec(ctx,
		`UPDATE ingestion_tasks SET pdf_object_key = $1 WHERE id = $2`,
		key, taskID,
	)
	if err != nil {
		return fmt.Errorf("update pdf_object_key: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrTaskNotFound
	}
	return nil
}

// SaveArtifact records a pipeline output artifact for a task.
func (r *TaskRepo) SaveArtifact(ctx context.Context, taskID uuid.UUID, kind, refKey string, metadata []byte) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO ingestion_artifacts (task_id, kind, ref_key, metadata)
		 VALUES ($1, $2, $3, $4)`,
		taskID, kind, refKey, metadata,
	)
	if err != nil {
		return fmt.Errorf("save artifact: %w", err)
	}
	return nil
}

// FindCompletedByPaperID returns the task ID of a completed task for the given paper_id,
// or uuid.Nil if none exists. Used for deduplication in batch ingest.
func (r *TaskRepo) FindCompletedByPaperID(ctx context.Context, paperID string) (uuid.UUID, error) {
	var id uuid.UUID
	err := r.pool.QueryRow(ctx,
		`SELECT id FROM ingestion_tasks
		 WHERE paper_id = $1 AND state = 'completed'
		 ORDER BY created_at DESC LIMIT 1`,
		paperID,
	).Scan(&id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return uuid.Nil, nil
		}
		return uuid.Nil, fmt.Errorf("find completed by paper_id: %w", err)
	}
	return id, nil
}

// scanTasks scans multiple rows into a Task slice.
func scanTasks(rows pgx.Rows) ([]Task, error) {
	var tasks []Task
	for rows.Next() {
		var t Task
		err := rows.Scan(
			&t.ID, &t.PaperID, &t.SourceURL, &t.PDFObjectKey, &t.CallbackURL,
			&t.State, &t.Status, &t.ErrorMessage, &t.RetryCount, &t.MaxRetries,
			&t.WorkerID, &t.LastHeartbeat, &t.CreatedAt, &t.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan task: %w", err)
		}
		tasks = append(tasks, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration: %w", err)
	}
	return tasks, nil
}
