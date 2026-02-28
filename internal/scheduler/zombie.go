package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/CAICAIIs/rag-ingestion-gateway/internal/db"
	"github.com/CAICAIIs/rag-ingestion-gateway/internal/fsm"
)

type ZombieRecovery struct {
	repo     *db.TaskRepo
	interval time.Duration
	timeout  time.Duration
	logger   *slog.Logger
}

func NewZombieRecovery(repo *db.TaskRepo, interval, timeout time.Duration, logger *slog.Logger) *ZombieRecovery {
	return &ZombieRecovery{
		repo:     repo,
		interval: interval,
		timeout:  timeout,
		logger:   logger,
	}
}

func (z *ZombieRecovery) Run(ctx context.Context) {
	ticker := time.NewTicker(z.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			z.recover(ctx)
		}
	}
}

func (z *ZombieRecovery) recover(ctx context.Context) {
	zombies, err := z.repo.FetchZombieTasks(ctx, z.timeout)
	if err != nil {
		z.logger.Error("zombie scan failed", "error", err)
		return
	}

	for _, task := range zombies {
		retryCount, err := z.repo.IncrementRetry(ctx, task.ID)
		if err != nil {
			z.logger.Error("zombie retry increment failed", "task_id", task.ID, "error", err)
			continue
		}

		if retryCount >= task.MaxRetries {
			_ = z.repo.SetError(ctx, task.ID, "zombie: exceeded max retries after heartbeat timeout")
			z.logger.Warn("zombie task exhausted retries", "task_id", task.ID)
			continue
		}

		err = z.repo.TransitionState(ctx, task.ID, task.State, fsm.StatePending, "zombie recovery: heartbeat timeout")
		if err != nil {
			z.logger.Error("zombie reset failed", "task_id", task.ID, "error", err)
			continue
		}

		z.logger.Info("recovered zombie task", "task_id", task.ID, "retry", retryCount)
	}
}
