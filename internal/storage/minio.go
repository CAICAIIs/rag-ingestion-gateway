package storage

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Client wraps MinIO operations for the gateway.
type Client struct {
	mc     *minio.Client
	logger *slog.Logger
}

// NewClient creates a new MinIO storage client.
func NewClient(endpoint, accessKey, secretKey string, secure bool, logger *slog.Logger) (*Client, error) {
	mc, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: secure,
	})
	if err != nil {
		return nil, fmt.Errorf("minio client init: %w", err)
	}
	return &Client{mc: mc, logger: logger}, nil
}

// EnsureBucket creates the bucket if it doesn't exist.
func (c *Client) EnsureBucket(ctx context.Context, bucket string) error {
	exists, err := c.mc.BucketExists(ctx, bucket)
	if err != nil {
		return fmt.Errorf("check bucket %s: %w", bucket, err)
	}
	if !exists {
		if err := c.mc.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
			return fmt.Errorf("create bucket %s: %w", bucket, err)
		}
		c.logger.Info("created bucket", "bucket", bucket)
	}
	return nil
}

// StreamUpload streams from src directly to MinIO using zero-copy streaming.
// Returns the number of bytes written.
func (c *Client) StreamUpload(ctx context.Context, bucket, key string, src io.Reader, contentType string) (int64, error) {
	putFn := func(ctx context.Context, r io.Reader, size int64) error {
		opts := minio.PutObjectOptions{
			ContentType: contentType,
		}
		_, err := c.mc.PutObject(ctx, bucket, key, r, size, opts)
		return err
	}

	n, err := StreamToMinIO(ctx, src, putFn)
	if err != nil {
		return n, fmt.Errorf("stream upload %s/%s: %w", bucket, key, err)
	}

	c.logger.Debug("uploaded object", "bucket", bucket, "key", key, "bytes", n)
	return n, nil
}

// GetObject retrieves an object from MinIO.
func (c *Client) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	obj, err := c.mc.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("get object %s/%s: %w", bucket, key, err)
	}
	return obj, nil
}

// DeleteObject removes an object from MinIO.
func (c *Client) DeleteObject(ctx context.Context, bucket, key string) error {
	err := c.mc.RemoveObject(ctx, bucket, key, minio.RemoveObjectOptions{})
	if err != nil {
		return fmt.Errorf("delete object %s/%s: %w", bucket, key, err)
	}
	return nil
}

// StatObject checks if an object exists and returns its info.
func (c *Client) StatObject(ctx context.Context, bucket, key string) (minio.ObjectInfo, error) {
	info, err := c.mc.StatObject(ctx, bucket, key, minio.StatObjectOptions{})
	if err != nil {
		return minio.ObjectInfo{}, fmt.Errorf("stat object %s/%s: %w", bucket, key, err)
	}
	return info, nil
}
