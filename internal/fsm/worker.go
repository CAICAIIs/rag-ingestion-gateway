package fsm

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/CAICAIIs/rag-ingestion-gateway/internal/db"
	"github.com/CAICAIIs/rag-ingestion-gateway/internal/pipeline"
)

type Worker struct {
	id       string
	repo     *db.TaskRepo
	pipeline *pipeline.Pipeline
	taskCh   <-chan db.Task
	hbInt    time.Duration
	logger   *slog.Logger
}

func NewWorker(repo *db.TaskRepo, p *pipeline.Pipeline, taskCh <-chan db.Task, hbInterval time.Duration, logger *slog.Logger) *Worker {
	return &Worker{
		id:       fmt.Sprintf("worker-%s", uuid.New().String()[:8]),
		repo:     repo,
		pipeline: p,
		taskCh:   taskCh,
		hbInt:    hbInterval,
		logger:   logger,
	}
}

func (w *Worker) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-w.taskCh:
			w.process(ctx, task)
		}
	}
}

func (w *Worker) process(ctx context.Context, task db.Task) {
	log := w.logger.With("task_id", task.ID, "paper_id", task.PaperID, "worker", w.id)

	if err := w.repo.ClaimTask(ctx, task.ID, w.id); err != nil {
		log.Debug("claim failed, another worker took it", "error", err)
		return
	}

	hbCtx, hbCancel := context.WithCancel(ctx)
	defer hbCancel()
	go w.heartbeat(hbCtx, task.ID)

	if err := w.runPipeline(ctx, log, task); err != nil {
		log.Error("pipeline failed", "error", err)
		w.handleFailure(ctx, task, err)
		return
	}

	if err := w.repo.TransitionState(ctx, task.ID, StateIndexing, StateCompleted, "pipeline finished"); err != nil {
		log.Error("failed to mark completed", "error", err)
	}
}

func (w *Worker) runPipeline(ctx context.Context, log *slog.Logger, task db.Task) error {
	// If no PDF object key, need to download first
	startState := StateChunking
	if task.PDFObjectKey == "" {
		startState = StateDownloading
	}

	steps := []struct {
		from string
		to   string
		fn   func(context.Context, db.Task) error
	}{
		{StatePending, StateDownloading, w.pipeline.Download},
		{StateDownloading, StateChunking, nil},
		{StatePending, StateChunking, nil},
		{StateChunking, StateEmbedding, w.pipeline.Chunk},
		{StateEmbedding, StateIndexing, w.pipeline.Embed},
	}

	currentState := StatePending
	for _, step := range steps {
		if step.from != currentState {
			continue
		}
		if !CanTransition(currentState, step.to) {
			continue
		}

		// Skip download if we already have a PDF
		if step.to == StateDownloading && startState != StateDownloading {
			currentState = StateChunking
			continue
		}

		if err := w.repo.TransitionState(ctx, task.ID, currentState, step.to, ""); err != nil {
			return fmt.Errorf("transition %s->%s: %w", currentState, step.to, err)
		}
		currentState = step.to

		if step.fn != nil {
			if err := step.fn(ctx, task); err != nil {
				return fmt.Errorf("step %s: %w", step.to, err)
			}
		}
	}

	return nil
}

func (w *Worker) handleFailure(ctx context.Context, task db.Task, pipeErr error) {
	retryCount, err := w.repo.IncrementRetry(ctx, task.ID)
	if err != nil {
		w.logger.Error("increment retry failed", "task_id", task.ID, "error", err)
	}

	if retryCount < task.MaxRetries {
		// Reset to pending for retry
		_ = w.repo.TransitionState(ctx, task.ID, task.State, StatePending, fmt.Sprintf("retry %d: %s", retryCount, pipeErr))
		return
	}

	_ = w.repo.SetError(ctx, task.ID, pipeErr.Error())
}

func (w *Worker) heartbeat(ctx context.Context, taskID uuid.UUID) {
	ticker := time.NewTicker(w.hbInt)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.repo.Heartbeat(ctx, taskID); err != nil {
				w.logger.Warn("heartbeat failed", "task_id", taskID, "error", err)
			}
		}
	}
}
