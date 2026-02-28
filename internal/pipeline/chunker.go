package pipeline

import (
	"github.com/pkoukk/tiktoken-go"
)

type Chunk struct {
	Text       string
	TokenCount int
	Index      int
}

type Chunker struct {
	enc        *tiktoken.Tiktoken
	chunkSize  int
	overlapTok int
}

func NewChunker(encoding string, chunkSize, overlap int) (*Chunker, error) {
	enc, err := tiktoken.GetEncoding(encoding)
	if err != nil {
		return nil, err
	}
	return &Chunker{enc: enc, chunkSize: chunkSize, overlapTok: overlap}, nil
}

func (c *Chunker) Split(text string) []Chunk {
	tokens := c.enc.Encode(text, nil, nil)
	if len(tokens) == 0 {
		return nil
	}

	var chunks []Chunk
	idx := 0
	start := 0

	for start < len(tokens) {
		end := start + c.chunkSize
		if end > len(tokens) {
			end = len(tokens)
		}

		chunkTokens := tokens[start:end]
		chunkText := c.enc.Decode(chunkTokens)

		chunks = append(chunks, Chunk{
			Text:       chunkText,
			TokenCount: len(chunkTokens),
			Index:      idx,
		})

		idx++
		start = end - c.overlapTok
		if start <= 0 && end == len(tokens) {
			break
		}
		if start < 0 {
			start = 0
		}
	}

	return chunks
}
