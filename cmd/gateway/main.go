package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/CAICAIIs/rag-ingestion-gateway/internal/api"
	"github.com/CAICAIIs/rag-ingestion-gateway/internal/clients"
	"github.com/CAICAIIs/rag-ingestion-gateway/internal/config"
	"github.com/CAICAIIs/rag-ingestion-gateway/internal/db"
	"github.com/CAICAIIs/rag-ingestion-gateway/internal/fsm"
	"github.com/CAICAIIs/rag-ingestion-gateway/internal/pipeline"
	"github.com/CAICAIIs/rag-ingestion-gateway/internal/scheduler"
	"github.com/CAICAIIs/rag-ingestion-gateway/internal/storage"
	"github.com/CAICAIIs/rag-ingestion-gateway/pkg/logger"
)

func main() {
	cfg := config.Load()
	log := logger.New(os.Getenv("LOG_LEVEL"))

	if err := run(cfg, log); err != nil {
		log.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run(cfg *config.Config, log *slog.Logger) error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	pool, err := pgxpool.New(ctx, cfg.PostgresDSN())
	if err != nil {
		return fmt.Errorf("postgres pool: %w", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("postgres ping: %w", err)
	}
	log.Info("connected to postgres")

	store, err := storage.NewClient(cfg.MinIOEndpoint, cfg.MinIOAccessKey, cfg.MinIOSecretKey, cfg.MinIOSecure, log)
	if err != nil {
		return fmt.Errorf("minio client: %w", err)
	}
	if err := store.EnsureBucket(ctx, cfg.BucketRaw); err != nil {
		return fmt.Errorf("ensure bucket: %w", err)
	}

	redisClient := clients.NewRedisClient(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	defer redisClient.Close()
	if err := redisClient.Ping(ctx); err != nil {
		return fmt.Errorf("redis ping: %w", err)
	}
	log.Info("connected to redis")

	qdrantClient, err := clients.NewQdrantClient(cfg.QdrantHost, cfg.QdrantGRPCPort, cfg.QdrantCollectionName)
	if err != nil {
		return fmt.Errorf("qdrant client: %w", err)
	}
	defer qdrantClient.Close()
	log.Info("connected to qdrant")

	repo := db.NewTaskRepo(pool)

	chunker, err := pipeline.NewChunker(cfg.TiktokenModel, cfg.ChunkSizeTokens, cfg.ChunkOverlapTokens)
	if err != nil {
		return fmt.Errorf("chunker init: %w", err)
	}

	embedder := pipeline.NewEmbedder(
		cfg.EmbeddingAPIKey, cfg.EmbeddingBaseURL, cfg.EmbeddingModel,
		cfg.EmbeddingAPIType, cfg.EmbeddingDimensions, cfg.EmbeddingBatchSize,
	)

	pipe := pipeline.NewPipeline(store, repo, chunker, embedder, qdrantClient, cfg.BucketRaw, cfg.PDFDownloadTimeout, log)

	taskCh := make(chan db.Task, cfg.WorkerCount*2)

	dispatcher := fsm.NewDispatcher(repo, taskCh, cfg.DispatchInterval, log)
	go dispatcher.Run(ctx)

	for i := 0; i < cfg.WorkerCount; i++ {
		w := fsm.NewWorker(repo, pipe, taskCh, cfg.HeartbeatInterval, log)
		go w.Run(ctx)
	}
	log.Info("started worker pool", "count", cfg.WorkerCount)

	zombie := scheduler.NewZombieRecovery(repo, cfg.ZombieCheckInterval, cfg.ZombieTimeout, log)
	go zombie.Run(ctx)

	router := api.NewRouter(repo, store, cfg.BucketRaw, cfg.MaxUploadBytes(), log)

	srv := &http.Server{
		Addr:         cfg.HTTPAddr,
		Handler:      router,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info("HTTP server starting", "addr", cfg.HTTPAddr)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if err != http.ErrServerClosed {
			return fmt.Errorf("http server: %w", err)
		}
	case <-ctx.Done():
		log.Info("shutting down...")
		shutCtx, shutCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer shutCancel()
		if err := srv.Shutdown(shutCtx); err != nil {
			return fmt.Errorf("http shutdown: %w", err)
		}
	}

	log.Info("gateway stopped")
	return nil
}
