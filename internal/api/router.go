package api

import (
	"log/slog"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/CAICAIIs/rag-ingestion-gateway/internal/db"
	"github.com/CAICAIIs/rag-ingestion-gateway/internal/storage"
)

func NewRouter(repo *db.TaskRepo, store *storage.Client, bucket string, maxUploadBytes int64, logger *slog.Logger) *chi.Mux {
	h := NewHandler(repo, store, bucket, maxUploadBytes, logger)

	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	r.Use(RequestID)
	r.Use(AccessLog(logger))

	r.Route("/api/v1", func(r chi.Router) {
		r.With(MaxBodySize(maxUploadBytes)).Post("/ingest", h.HandleIngest)
		r.Post("/ingest/batch", h.HandleBatchIngest)
		r.Get("/tasks/{taskID}", h.HandleGetTask)
		r.Delete("/tasks/{taskID}", h.HandleCancelTask)
		r.Get("/health", h.HandleHealth)
	})

	return r
}
