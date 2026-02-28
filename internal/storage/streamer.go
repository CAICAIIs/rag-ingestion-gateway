package storage

import (
	"context"
	"io"
	"sync"
)

// bufferSize is 32KB — matches typical OS page size for optimal syscall throughput.
const bufferSize = 32 * 1024

// bufPool is a sync.Pool of 32KB byte slices to avoid GC pressure on high-concurrency uploads.
var bufPool = sync.Pool{
	New: func() any {
		buf := make([]byte, bufferSize)
		return &buf
	},
}

// StreamToMinIO streams from src reader directly to MinIO via io.Pipe,
// using pooled 32KB buffers. Peak memory: ~64KB per upload (read buf + pipe buf).
//
// It takes a callback that receives the pipe reader — the callback should call
// MinIO PutObject with that reader. This design allows the caller to control
// bucket/key/options while this function handles the zero-copy plumbing.
//
// Returns the number of bytes written and any error.
func StreamToMinIO(ctx context.Context, src io.Reader, putFn func(ctx context.Context, r io.Reader, size int64) error) (int64, error) {
	pr, pw := io.Pipe()

	// Get a pooled buffer
	bufPtr := bufPool.Get().(*[]byte)
	buf := *bufPtr

	var written int64
	var copyErr error

	// Writer goroutine: reads from src using pooled buffer, writes to pipe
	go func() {
		defer func() {
			bufPool.Put(bufPtr)
			pw.CloseWithError(copyErr)
		}()
		written, copyErr = io.CopyBuffer(pw, src, buf)
	}()

	// Reader side: putFn consumes the pipe reader (MinIO PutObject)
	// size=-1 tells MinIO to use multipart upload automatically
	err := putFn(ctx, pr, -1)

	// If putFn failed, close the reader to unblock the writer goroutine
	pr.CloseWithError(err)

	if err != nil {
		return written, err
	}
	if copyErr != nil {
		return written, copyErr
	}
	return written, nil
}
