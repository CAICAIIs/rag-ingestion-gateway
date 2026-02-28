package fsm

import (
	"context"
	"log/slog"
	"time"

	"github.com/CAICAIIs/rag-ingestion-gateway/internal/db"
)

type Dispatcher struct {
	repo     *db.TaskRepo
	taskCh   chan db.Task
	interval time.Duration
	logger   *slog.Logger
}

func NewDispatcher(repo *db.TaskRepo, taskCh chan db.Task, interval time.Duration, logger *slog.Logger) *Dispatcher {
	return &Dispatcher{
		repo:     repo,
		taskCh:   taskCh,
		interval: interval,
		logger:   logger,
	}
}

func (d *Dispatcher) Run(ctx context.Context) {
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.dispatch(ctx)
		}
	}
}

func (d *Dispatcher) dispatch(ctx context.Context) {
	tasks, err := d.repo.FetchPendingTasks(ctx, cap(d.taskCh))
	if err != nil {
		d.logger.Error("dispatcher: fetch pending failed", "error", err)
		return
	}

	for _, task := range tasks {
		select {
		case d.taskCh <- task:
			d.logger.Debug("dispatcher: dispatched task", "task_id", task.ID, "paper_id", task.PaperID)
		default:
			d.logger.Debug("dispatcher: channel full, skipping remaining")
			return
		}
	}
}
