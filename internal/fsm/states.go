package fsm

const (
	StatePending     = "pending"
	StateDownloading = "downloading"
	StateChunking    = "chunking"
	StateEmbedding   = "embedding"
	StateIndexing    = "indexing"
	StateCompleted   = "completed"
	StateFailed      = "failed"
	StateCanceled    = "canceled"
)

var ValidTransitions = map[string][]string{
	StatePending:     {StateDownloading, StateChunking, StateFailed, StateCanceled},
	StateDownloading: {StateChunking, StateFailed, StateCanceled},
	StateChunking:    {StateEmbedding, StateFailed, StateCanceled},
	StateEmbedding:   {StateIndexing, StateFailed, StateCanceled},
	StateIndexing:    {StateCompleted, StateFailed, StateCanceled},
}

func CanTransition(from, to string) bool {
	targets, ok := ValidTransitions[from]
	if !ok {
		return false
	}
	for _, t := range targets {
		if t == to {
			return true
		}
	}
	return false
}
