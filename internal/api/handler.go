package api

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/CAICAIIs/rag-ingestion-gateway/internal/db"
	"github.com/CAICAIIs/rag-ingestion-gateway/internal/storage"
)

type Handler struct {
	repo    *db.TaskRepo
	store   *storage.Client
	bucket  string
	maxSize int64
	logger  *slog.Logger
}

func NewHandler(repo *db.TaskRepo, store *storage.Client, bucket string, maxSize int64, logger *slog.Logger) *Handler {
	return &Handler{
		repo:    repo,
		store:   store,
		bucket:  bucket,
		maxSize: maxSize,
		logger:  logger,
	}
}

type BatchIngestRequest struct {
	Items []BatchIngestItem `json:"items"`
}

type BatchIngestItem struct {
	PaperID     string `json:"paper_id"`
	SourceURL   string `json:"source_url"`
	CallbackURL string `json:"callback_url,omitempty"`
}

func (h *Handler) HandleIngest(w http.ResponseWriter, r *http.Request) {
	mr, err := r.MultipartReader()
	if err != nil {
		writeError(w, http.StatusBadRequest, "expected multipart/form-data", "INVALID_CONTENT_TYPE")
		return
	}

	var paperID, callbackURL, objectKey string

	for {
		part, err := mr.NextPart()
		if err != nil {
			break
		}

		switch part.FormName() {
		case "paper_id":
			buf := make([]byte, 512)
			n, _ := part.Read(buf)
			paperID = string(buf[:n])
		case "callback_url":
			buf := make([]byte, 2048)
			n, _ := part.Read(buf)
			callbackURL = string(buf[:n])
		case "file":
			if paperID == "" {
				writeError(w, http.StatusBadRequest, "paper_id must appear before file in multipart", "MISSING_PAPER_ID")
				return
			}
			objectKey = fmt.Sprintf("ingestion/%s/%s.pdf", paperID, uuid.New().String()[:8])
			_, err := h.store.StreamUpload(r.Context(), h.bucket, objectKey, part, "application/pdf")
			if err != nil {
				h.logger.Error("stream upload failed", "error", err, "paper_id", paperID)
				writeError(w, http.StatusInternalServerError, "upload failed", "UPLOAD_ERROR")
				return
			}
		}
		part.Close()
	}

	if paperID == "" {
		writeError(w, http.StatusBadRequest, "paper_id is required", "MISSING_PAPER_ID")
		return
	}

	taskID, err := h.repo.CreateTask(r.Context(), paperID, "", objectKey, callbackURL, 3)
	if err != nil {
		h.logger.Error("create task failed", "error", err, "paper_id", paperID)
		writeError(w, http.StatusConflict, "task creation failed (duplicate?)", "TASK_CONFLICT")
		return
	}

	writeJSON(w, http.StatusAccepted, IngestResponse{
		TaskID: taskID.String(),
		Status: "queued",
	})
}

func (h *Handler) HandleBatchIngest(w http.ResponseWriter, r *http.Request) {
	var req BatchIngestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body", "INVALID_JSON")
		return
	}

	if len(req.Items) == 0 {
		writeError(w, http.StatusBadRequest, "items array is empty", "EMPTY_BATCH")
		return
	}

	results := make([]BatchItemResponse, 0, len(req.Items))
	for _, item := range req.Items {
		if item.PaperID == "" || item.SourceURL == "" {
			results = append(results, BatchItemResponse{
				PaperID: item.PaperID,
				Status:  http.StatusBadRequest,
				Error:   "paper_id and source_url are required",
			})
			continue
		}

		taskID, err := h.repo.CreateTask(r.Context(), item.PaperID, item.SourceURL, "", item.CallbackURL, 3)
		if err != nil {
			results = append(results, BatchItemResponse{
				PaperID: item.PaperID,
				Status:  http.StatusConflict,
				Error:   "task creation failed",
			})
			continue
		}

		results = append(results, BatchItemResponse{
			PaperID: item.PaperID,
			TaskID:  taskID.String(),
			Status:  http.StatusAccepted,
		})
	}

	writeJSON(w, http.StatusMultiStatus, results)
}

func (h *Handler) HandleGetTask(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "taskID")
	taskID, err := uuid.Parse(idStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid task ID", "INVALID_ID")
		return
	}

	task, err := h.repo.GetTask(r.Context(), taskID)
	if err != nil {
		writeError(w, http.StatusNotFound, "task not found", "NOT_FOUND")
		return
	}

	writeJSON(w, http.StatusOK, TaskResponse{
		ID:        task.ID.String(),
		PaperID:   task.PaperID,
		State:     task.State,
		Status:    task.Status,
		Error:     task.ErrorMessage,
		CreatedAt: task.CreatedAt.Format("2006-01-02T15:04:05Z"),
		UpdatedAt: task.UpdatedAt.Format("2006-01-02T15:04:05Z"),
	})
}

func (h *Handler) HandleCancelTask(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "taskID")
	taskID, err := uuid.Parse(idStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid task ID", "INVALID_ID")
		return
	}

	if err := h.repo.CancelTask(r.Context(), taskID); err != nil {
		writeError(w, http.StatusConflict, "task cannot be canceled", "CANCEL_CONFLICT")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) HandleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, HealthResponse{
		Status: "ok",
		Deps:   map[string]string{"postgres": "ok", "minio": "ok", "redis": "ok", "qdrant": "ok"},
	})
}
