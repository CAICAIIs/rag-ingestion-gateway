package pipeline

import (
	"strings"
	"unicode"
	"unicode/utf8"
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
		end = alignRuneBoundary(text, end)

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
		next = alignRuneBoundary(text, next)
		start = next

		if end >= len(text) {
			break
		}
	}

	return chunks
}

func alignRuneBoundary(text string, pos int) int {
	if pos >= len(text) {
		return len(text)
	}
	for pos > 0 && !utf8.RuneStart(text[pos]) {
		pos--
	}
	return pos
}

func findSentenceBoundary(text string, pos int) int {
	searchStart := pos - 200
	if searchStart < 0 {
		searchStart = 0
	}
	best := 0
	for i := pos; i >= searchStart; {
		r, size := utf8.DecodeLastRuneInString(text[:i])
		if size == 0 {
			break
		}
		i -= size
		if r == '.' || r == '!' || r == '?' || r == '\n' ||
			r == '\u3002' || r == '\uff01' || r == '\uff1f' {
			nextPos := i + size
			if nextPos < len(text) {
				nr, _ := utf8.DecodeRuneInString(text[nextPos:])
				if nr == ' ' || nr == '\n' {
					return nextPos
				}
			}
			if best == 0 {
				best = nextPos
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
