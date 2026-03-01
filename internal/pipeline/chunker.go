package pipeline

import (
	"strings"
	"unicode"
)

type Chunk struct {
	Text       string
	TokenCount int
	Index      int
}

type Chunker struct {
	chunkChars  int
	overlapChar int
}

func NewChunker(_ string, chunkSizeTokens, overlapTokens int) (*Chunker, error) {
	return &Chunker{
		chunkChars:  chunkSizeTokens * 4,
		overlapChar: overlapTokens * 4,
	}, nil
}

func (c *Chunker) Split(text string) []Chunk {
	text = strings.TrimSpace(text)
	if len(text) == 0 {
		return nil
	}

	var chunks []Chunk
	idx := 0
	start := 0

	for start < len(text) {
		end := start + c.chunkChars
		if end > len(text) {
			end = len(text)
		}

		if end < len(text) {
			boundary := findSentenceBoundary(text, end)
			if boundary > start {
				end = boundary
			}
		}

		chunkText := strings.TrimSpace(text[start:end])
		if len(chunkText) > 0 {
			chunks = append(chunks, Chunk{
				Text:       chunkText,
				TokenCount: estimateTokens(chunkText),
				Index:      idx,
			})
			idx++
		}

		next := end - c.overlapChar
		if next <= start {
			next = end
		}
		start = next

		if end >= len(text) {
			break
		}
	}

	return chunks
}

func findSentenceBoundary(text string, pos int) int {
	searchStart := pos - 200
	if searchStart < 0 {
		searchStart = 0
	}
	best := 0
	for i := pos; i >= searchStart; i-- {
		ch := rune(text[i])
		if ch == '.' || ch == '!' || ch == '?' || ch == '\n' {
			if i+1 < len(text) && (text[i+1] == ' ' || text[i+1] == '\n') {
				return i + 1
			}
			if best == 0 {
				best = i + 1
			}
		}
	}
	return best
}

func estimateTokens(text string) int {
	words := 0
	inWord := false
	for _, r := range text {
		if unicode.IsSpace(r) {
			inWord = false
		} else if !inWord {
			inWord = true
			words++
		}
	}
	tokens := words * 4 / 3
	if tokens == 0 && len(text) > 0 {
		tokens = 1
	}
	return tokens
}
