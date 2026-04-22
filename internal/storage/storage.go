package storage

import (
	"context"
	"io"
	"time"
)

// Backend is the pluggable storage interface.
// Local disk is the default; swap in S3/MinIO without changing callers.
type Backend interface {
	// Store writes r under key and returns bytes written.
	Store(ctx context.Context, key string, r io.Reader) (int64, error)
	// Open returns a read-closer for the artifact at key.
	Open(ctx context.Context, key string) (io.ReadCloser, error)
	// GetSignedURL returns a pre-signed URL for direct client download.
	// Returns ErrSignedURLUnsupported for local-disk backend (callers serve via API).
	GetSignedURL(ctx context.Context, key string, ttl time.Duration) (string, error)
	// Delete removes the stored artifact.
	Delete(ctx context.Context, key string) error
}
