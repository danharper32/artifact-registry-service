package local

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/danharper32/artifact-registry-service/internal/domain"
)

// Local is a local-disk storage backend.
type Local struct {
	baseDir string
}

func New(baseDir string) (*Local, error) {
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, fmt.Errorf("create storage dir %q: %w", baseDir, err)
	}
	return &Local{baseDir: baseDir}, nil
}

func (l *Local) Store(_ context.Context, key string, r io.Reader) (int64, error) {
	fullPath := filepath.Join(l.baseDir, filepath.FromSlash(key))
	if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
		return 0, fmt.Errorf("create dirs for %q: %w", key, err)
	}
	f, err := os.Create(fullPath)
	if err != nil {
		return 0, fmt.Errorf("create file %q: %w", fullPath, err)
	}
	defer f.Close()
	n, err := io.Copy(f, r)
	if err != nil {
		os.Remove(fullPath)
		return 0, fmt.Errorf("write artifact: %w", err)
	}
	return n, nil
}

func (l *Local) Open(_ context.Context, key string) (io.ReadCloser, error) {
	fullPath := filepath.Join(l.baseDir, filepath.FromSlash(key))
	f, err := os.Open(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, domain.ErrNotFound
		}
		return nil, fmt.Errorf("open %q: %w", fullPath, err)
	}
	return f, nil
}

// GetSignedURL is not supported for local disk — callers serve via API.
func (l *Local) GetSignedURL(_ context.Context, _ string, _ time.Duration) (string, error) {
	return "", domain.ErrSignedURLUnsupported
}

func (l *Local) Delete(_ context.Context, key string) error {
	fullPath := filepath.Join(l.baseDir, filepath.FromSlash(key))
	if err := os.Remove(fullPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete %q: %w", key, err)
	}
	return nil
}
