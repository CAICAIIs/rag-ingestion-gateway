package pipeline

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/CAICAIIs/rag-ingestion-gateway/internal/clients"
	"github.com/CAICAIIs/rag-ingestion-gateway/internal/db"
	"github.com/CAICAIIs/rag-ingestion-gateway/internal/storage"
)

type Pipeline struct {
	store      *storage.Client
	repo       *db.TaskRepo
	chunker    *Chunker
	embedder   *Embedder
	qdrant     *clients.QdrantClient
	bucket     string
	httpClient *http.Client
	logger     *slog.Logger
}

func NewPipeline(
	store *storage.Client,
	repo *db.TaskRepo,
	chunker *Chunker,
	embedder *Embedder,
	qdrant *clients.QdrantClient,
	bucket string,
	downloadTimeout time.Duration,
	logger *slog.Logger,
) *Pipeline {
	return &Pipeline{
		store:    store,
		repo:     repo,
		chunker:  chunker,
		embedder: embedder,
		qdrant:   qdrant,
		bucket:   bucket,
		httpClient: &http.Client{
			Timeout: downloadTimeout,
		},
		logger: logger,
	}
}

func (p *Pipeline) Download(ctx context.Context, task db.Task) error {
	if task.SourceURL == "" {
		return fmt.Errorf("no source URL for task %s", task.ID)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, task.SourceURL, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("download PDF: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download returned status %d", resp.StatusCode)
	}

	objectKey := fmt.Sprintf("ingestion/%s/%s.pdf", task.PaperID, uuid.New().String()[:8])
	_, err = p.store.StreamUpload(ctx, p.bucket, objectKey, resp.Body, "application/pdf")
	if err != nil {
		return fmt.Errorf("upload to minio: %w", err)
	}

	p.logger.Info("downloaded PDF", "task_id", task.ID, "object_key", objectKey)
	return nil
}

func (p *Pipeline) Chunk(ctx context.Context, task db.Task) error {
	if task.PDFObjectKey == "" {
		return fmt.Errorf("no PDF object key for task %s", task.ID)
	}

	obj, err := p.store.GetObject(ctx, p.bucket, task.PDFObjectKey)
	if err != nil {
		return fmt.Errorf("get PDF from minio: %w", err)
	}
	defer obj.Close()

	pdfBytes, err := io.ReadAll(obj)
	if err != nil {
		return fmt.Errorf("read PDF: %w", err)
	}

	text := extractTextFromPDF(pdfBytes)
	chunks := p.chunker.Split(text)

	for i, chunk := range chunks {
		refKey := fmt.Sprintf("%s/chunk_%04d", task.PDFObjectKey, i)
		metadata := []byte(fmt.Sprintf(`{"index":%d,"tokens":%d}`, i, chunk.TokenCount))
		if err := p.repo.SaveArtifact(ctx, task.ID, "chunk", refKey, metadata); err != nil {
			return fmt.Errorf("save chunk artifact: %w", err)
		}
	}

	p.logger.Info("chunked PDF", "task_id", task.ID, "chunks", len(chunks))
	return nil
}

func (p *Pipeline) Embed(ctx context.Context, task db.Task) error {
	obj, err := p.store.GetObject(ctx, p.bucket, task.PDFObjectKey)
	if err != nil {
		return fmt.Errorf("get PDF from minio: %w", err)
	}
	defer obj.Close()

	pdfBytes, err := io.ReadAll(obj)
	if err != nil {
		return fmt.Errorf("read PDF: %w", err)
	}

	text := extractTextFromPDF(pdfBytes)
	chunks := p.chunker.Split(text)

	texts := make([]string, len(chunks))
	for i, c := range chunks {
		texts[i] = c.Text
	}

	embeddings, err := p.embedder.EmbedBatch(ctx, texts)
	if err != nil {
		return fmt.Errorf("embed batch: %w", err)
	}

	payloads := make([]clients.ChunkPayload, len(chunks))
	for i, c := range chunks {
		payloads[i] = clients.ChunkPayload{Text: c.Text, Index: c.Index, TokenCount: c.TokenCount}
	}

	if err := p.qdrant.UpsertPoints(ctx, task.PaperID, payloads, embeddings); err != nil {
		return fmt.Errorf("upsert to qdrant: %w", err)
	}

	for i := range embeddings {
		refKey := fmt.Sprintf("qdrant:%s:chunk_%04d", task.PaperID, i)
		if err := p.repo.SaveArtifact(ctx, task.ID, "qdrant_point", refKey, nil); err != nil {
			return fmt.Errorf("save qdrant artifact: %w", err)
		}
	}

	p.logger.Info("embedded and indexed", "task_id", task.ID, "vectors", len(embeddings))
	return nil
}

func extractTextFromPDF(data []byte) string {
	// Placeholder: in production, use pdfcpu or call Python fallback.
	// For now, treat the bytes as raw text (will be replaced with real extraction).
	return string(data)
}
