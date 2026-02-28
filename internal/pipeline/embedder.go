package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/CAICAIIs/rag-ingestion-gateway/internal/backoff"
)

type Embedder struct {
	apiKey    string
	baseURL   string
	model     string
	dims      int
	batchSize int
	http      *http.Client
}

func NewEmbedder(apiKey, baseURL, model string, dims, batchSize int) *Embedder {
	return &Embedder{
		apiKey:    apiKey,
		baseURL:   baseURL,
		model:     model,
		dims:      dims,
		batchSize: batchSize,
		http:      &http.Client{},
	}
}

type embeddingRequest struct {
	Input []string `json:"input"`
	Model string   `json:"model"`
}

type embeddingResponse struct {
	Data []struct {
		Embedding []float32 `json:"embedding"`
	} `json:"data"`
}

func (e *Embedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	allEmbeddings := make([][]float32, len(texts))

	for i := 0; i < len(texts); i += e.batchSize {
		end := i + e.batchSize
		if end > len(texts) {
			end = len(texts)
		}
		batch := texts[i:end]

		embeddings, err := backoff.RetryWithJitter(ctx, 3, func() ([][]float32, error) {
			return e.callAPI(ctx, batch)
		})
		if err != nil {
			return nil, fmt.Errorf("embed batch [%d:%d]: %w", i, end, err)
		}

		copy(allEmbeddings[i:end], embeddings)
	}

	return allEmbeddings, nil
}

func (e *Embedder) callAPI(ctx context.Context, texts []string) ([][]float32, error) {
	reqBody := embeddingRequest{Input: texts, Model: e.model}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	url := fmt.Sprintf("%s/embeddings", e.baseURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", e.apiKey))

	resp, err := e.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("embedding API call: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("embedding API returned status %d", resp.StatusCode)
	}

	var result embeddingResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	embeddings := make([][]float32, len(result.Data))
	for i, d := range result.Data {
		embeddings[i] = d.Embedding
	}
	return embeddings, nil
}
