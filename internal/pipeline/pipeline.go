package pipeline

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/ledongthuc/pdf"

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

	if err := p.repo.UpdatePDFObjectKey(ctx, task.ID, objectKey); err != nil {
		return fmt.Errorf("persist pdf_object_key: %w", err)
	}

	p.logger.Info("downloaded PDF", "task_id", task.ID, "object_key", objectKey)
	return nil
}

func (p *Pipeline) Chunk(ctx context.Context, task db.Task) error {
	p.logger.Info("chunk: start", "task_id", task.ID, "pdf_key", task.PDFObjectKey)
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
	p.logger.Info("chunk: read PDF", "task_id", task.ID, "bytes", len(pdfBytes))

	text, err := extractTextFromPDF(pdfBytes)
	if err != nil {
		return fmt.Errorf("extract text from PDF: %w", err)
	}
	p.logger.Info("chunk: extracted text", "task_id", task.ID, "chars", len(text))

	chunks := p.chunker.Split(text)
	p.logger.Info("chunk: split done", "task_id", task.ID, "chunks", len(chunks))

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
	p.logger.Info("embed: start", "task_id", task.ID, "pdf_key", task.PDFObjectKey)
	obj, err := p.store.GetObject(ctx, p.bucket, task.PDFObjectKey)
	if err != nil {
		return fmt.Errorf("get PDF from minio: %w", err)
	}
	defer obj.Close()

	pdfBytes, err := io.ReadAll(obj)
	if err != nil {
		return fmt.Errorf("read PDF: %w", err)
	}

	text, err := extractTextFromPDF(pdfBytes)
	if err != nil {
		return fmt.Errorf("extract text from PDF: %w", err)
	}
	chunks := p.chunker.Split(text)
	p.logger.Info("embed: chunks ready", "task_id", task.ID, "chunks", len(chunks))

	texts := make([]string, len(chunks))
	for i, c := range chunks {
		texts[i] = c.Text
	}

	p.logger.Info("embed: calling embedding API", "task_id", task.ID, "texts", len(texts))
	embeddings, err := p.embedder.EmbedBatch(ctx, texts)
	if err != nil {
		return fmt.Errorf("embed batch: %w", err)
	}
	p.logger.Info("embed: got embeddings", "task_id", task.ID, "vectors", len(embeddings))

	payloads := make([]clients.ChunkPayload, len(chunks))
	for i, c := range chunks {
		payloads[i] = clients.ChunkPayload{Text: c.Text, Index: c.Index, TokenCount: c.TokenCount}
	}

	p.logger.Info("embed: upserting to qdrant", "task_id", task.ID, "points", len(payloads))
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

func extractTextFromPDF(data []byte) (string, error) {
	r, err := pdf.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return "", fmt.Errorf("open PDF: %w", err)
	}
	var buf strings.Builder
	for i := 1; i <= r.NumPage(); i++ {
		p := r.Page(i)
		if p.V.IsNull() {
			continue
		}
		text, err := p.GetPlainText(nil)
		if err != nil {
			continue
		}
		buf.WriteString(text)
		buf.WriteString("\n")
	}
	result := sanitizeUTF8(buf.String())
	if len(strings.TrimSpace(result)) == 0 {
		return "", fmt.Errorf("no text extracted from PDF (%d pages)", r.NumPage())
	}
	return result, nil
}

func sanitizeUTF8(s string) string {
	if utf8.ValidString(s) {
		return s
	}
	var buf strings.Builder
	buf.Grow(len(s))
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size <= 1 {
			i++
			continue
		}
		buf.WriteRune(r)
		i += size
	}
	return buf.String()
}
