package storage

import (
	"fmt"
	"strings"

	"github.com/danharper32/artifact-registry-service/internal/config"
	s3backend "github.com/danharper32/artifact-registry-service/internal/storage/s3"
	"github.com/danharper32/artifact-registry-service/internal/storage/local"
)

// NewBackend constructs the storage Backend specified by cfg.StorageBackend.
// Defaults to the local disk backend when StorageBackend is empty or "local".
// Returns an error immediately (fail-fast) if S3 settings are invalid.
func NewBackend(cfg *config.Config) (Backend, error) {
	switch strings.ToLower(cfg.StorageBackend) {
	case "s3":
		return s3backend.New(s3backend.Config{
			Endpoint:        cfg.S3.Endpoint,
			Region:          cfg.S3.Region,
			Bucket:          cfg.S3.Bucket,
			AccessKeyID:     cfg.S3.AccessKeyID,
			SecretAccessKey: cfg.S3.SecretAccessKey,
			ForcePathStyle:  cfg.S3.ForcePathStyle,
			Prefix:          cfg.S3.Prefix,
		})
	case "", "local":
		return local.New(cfg.StorageDir)
	default:
		return nil, fmt.Errorf("unknown STORAGE_BACKEND %q: must be \"local\" or \"s3\"", cfg.StorageBackend)
	}
}
