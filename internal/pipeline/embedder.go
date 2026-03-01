package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/CAICAIIs/rag-ingestion-gateway/internal/backoff"
)

type Embedder struct {
	apiKey    string
	baseURL   string
	model     string
	apiType   string
	dims      int
	batchSize int
	http      *http.Client
}

func NewEmbedder(apiKey, baseURL, model, apiType string, dims, batchSize int) *Embedder {
	if apiType == "" {
		apiType = "openai"
	}
	return &Embedder{
		apiKey:    apiKey,
		baseURL:   baseURL,
		model:     model,
		apiType:   apiType,
		dims:      dims,
		batchSize: batchSize,
		http:      &http.Client{},
	}
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
	switch e.apiType {
	case "minimaxi":
		return e.callMiniMaxiAPI(ctx, texts)
	default:
		return e.callOpenAIAPI(ctx, texts)
	}
}

type openaiEmbeddingRequest struct {
	Input []string `json:"input"`
	Model string   `json:"model"`
}

type openaiEmbeddingResponse struct {
	Data []struct {
		Embedding []float32 `json:"embedding"`
	} `json:"data"`
}

func (e *Embedder) callOpenAIAPI(ctx context.Context, texts []string) ([][]float32, error) {
	reqBody := openaiEmbeddingRequest{Input: texts, Model: e.model}
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
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("embedding API returned status %d: %s", resp.StatusCode, string(respBody[:min(len(respBody), 200)]))
	}

	var result openaiEmbeddingResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	embeddings := make([][]float32, len(result.Data))
	for i, d := range result.Data {
		embeddings[i] = d.Embedding
	}
	return embeddings, nil
}

type minimaxiEmbeddingRequest struct {
	Texts []string `json:"texts"`
	Model string   `json:"model"`
	Type  string   `json:"type"`
}

type minimaxiEmbeddingResponse struct {
	Vectors  [][]float32 `json:"vectors"`
	BaseResp struct {
		StatusCode int    `json:"status_code"`
		StatusMsg  string `json:"status_msg"`
	} `json:"base_resp"`
}

func (e *Embedder) callMiniMaxiAPI(ctx context.Context, texts []string) ([][]float32, error) {
	reqBody := minimaxiEmbeddingRequest{Texts: texts, Model: e.model, Type: "db"}
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
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("embedding API returned status %d: %s", resp.StatusCode, string(respBody[:min(len(respBody), 200)]))
	}

	var result minimaxiEmbeddingResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	if result.BaseResp.StatusCode != 0 {
		return nil, fmt.Errorf("minimaxi API error %d: %s", result.BaseResp.StatusCode, result.BaseResp.StatusMsg)
	}

	if len(result.Vectors) != len(texts) {
		return nil, fmt.Errorf("expected %d vectors, got %d", len(texts), len(result.Vectors))
	}

	return result.Vectors, nil
}
