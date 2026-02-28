package api

import (
	"encoding/json"
	"net/http"
)

type ErrorResponse struct {
	Error   string `json:"error"`
	Code    string `json:"code,omitempty"`
	TraceID string `json:"trace_id,omitempty"`
}

type TaskResponse struct {
	ID        string  `json:"id"`
	PaperID   string  `json:"paper_id"`
	State     string  `json:"state"`
	Status    string  `json:"status"`
	Error     *string `json:"error,omitempty"`
	CreatedAt string  `json:"created_at"`
	UpdatedAt string  `json:"updated_at"`
}

type IngestResponse struct {
	TaskID string `json:"task_id"`
	Status string `json:"status"`
}

type BatchItemResponse struct {
	PaperID string `json:"paper_id"`
	TaskID  string `json:"task_id,omitempty"`
	Status  int    `json:"status"`
	Error   string `json:"error,omitempty"`
}

type HealthResponse struct {
	Status string            `json:"status"`
	Deps   map[string]string `json:"dependencies"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg, code string) {
	writeJSON(w, status, ErrorResponse{Error: msg, Code: code})
}
