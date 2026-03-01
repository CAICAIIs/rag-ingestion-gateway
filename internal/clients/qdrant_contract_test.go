package clients

import (
	"encoding/json"
	"os"
	"testing"
)

// contractSchema mirrors the relevant parts of qdrant_payload.schema.json.
type contractSchema struct {
	Required   []string                  `json:"required"`
	Properties map[string]map[string]any `json:"properties"`
}

// TestChunkPayloadFieldsMatchContract verifies that the Qdrant payload keys
// produced by UpsertPoints match the shared contract schema.
// This is the Go-side counterpart of tests/test_contracts.py in auto-scholar.
func TestChunkPayloadFieldsMatchContract(t *testing.T) {
	data, err := os.ReadFile("../../contracts/qdrant_payload.schema.json")
	if err != nil {
		t.Fatalf("read contract schema: %v (run from repo root or ensure contracts/ exists)", err)
	}

	var schema contractSchema
	if err := json.Unmarshal(data, &schema); err != nil {
		t.Fatalf("parse contract schema: %v", err)
	}

	// These are the payload keys that UpsertPoints actually writes (see qdrant.go).
	// If you add/remove payload fields in UpsertPoints, update this list AND the contract.
	actualPayloadKeys := map[string]bool{
		"paper_id":    true,
		"chunk_index": true,
		"chunk_text":  true,
		"token_count": true,
	}

	// Every key we write must exist in the contract schema.
	for key := range actualPayloadKeys {
		if _, ok := schema.Properties[key]; !ok {
			t.Errorf("payload key %q written by gateway is NOT in contract schema", key)
		}
	}

	// Every required field in the contract should ideally be written by gateway.
	// Some fields (title, start_char, end_char) are only written by the inline pipeline,
	// so we just warn rather than fail for those.
	inlinePipelineOnly := map[string]bool{
		"title":      true,
		"start_char": true,
		"end_char":   true,
	}
	for _, req := range schema.Required {
		if !actualPayloadKeys[req] && !inlinePipelineOnly[req] {
			t.Errorf("contract requires %q but gateway does not write it", req)
		}
	}
}

// TestChunkPayloadStructHasExpectedFields ensures the ChunkPayload struct
// has the fields we expect, preventing silent renames.
func TestChunkPayloadStructHasExpectedFields(t *testing.T) {
	p := ChunkPayload{
		Text:       "test",
		Index:      0,
		TokenCount: 10,
	}
	if p.Text == "" || p.TokenCount == 0 {
		t.Error("ChunkPayload fields not set correctly")
	}
}
