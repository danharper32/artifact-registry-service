// Package s3backend implements storage.Backend for S3-compatible object storage.
// Compatible with AWS S3 and MinIO when ForcePathStyle is enabled.
package s3backend

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/danharper32/artifact-registry-service/internal/domain"
)

// Config holds the S3/MinIO connection parameters.
type Config struct {
	Endpoint        string // full URL including scheme, e.g. http://localhost:9000
	Region          string
	Bucket          string
	AccessKeyID     string
	SecretAccessKey string
	ForcePathStyle  bool // required for MinIO
	Prefix          string
}

// Backend is a storage.Backend backed by S3-compatible object storage.
type Backend struct {
	client  *s3.Client
	presign *s3.PresignClient
	bucket  string
	prefix  string
}

// New constructs a Backend from cfg. Returns an error if required fields are missing.
func New(cfg Config) (*Backend, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("S3_BUCKET is required")
	}
	if cfg.Region == "" {
		return nil, fmt.Errorf("S3_REGION is required")
	}
	if cfg.AccessKeyID == "" {
		return nil, fmt.Errorf("S3_ACCESS_KEY_ID is required")
	}
	if cfg.SecretAccessKey == "" {
		return nil, fmt.Errorf("S3_SECRET_ACCESS_KEY is required")
	}
	if cfg.Endpoint == "" {
		return nil, fmt.Errorf("S3_ENDPOINT is required")
	}

	awsCfg := aws.Config{
		Region:      cfg.Region,
		Credentials: credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.Endpoint)
		o.UsePathStyle = cfg.ForcePathStyle
	})

	return &Backend{
		client:  client,
		presign: s3.NewPresignClient(client),
		bucket:  cfg.Bucket,
		prefix:  cfg.Prefix,
	}, nil
}

// objectKey prepends the optional prefix to the canonical storage key.
// Preserves the existing key layout (type/name/version/artifact) exactly.
func (b *Backend) objectKey(key string) string {
	if b.prefix == "" {
		return key
	}
	return b.prefix + "/" + key
}

// Store buffers the reader to a temp file to obtain content length, then
// uploads via PutObject. Avoids multipart upload per spec (no optimization).
func (b *Backend) Store(ctx context.Context, key string, r io.Reader) (int64, error) {
	tmp, err := os.CreateTemp("", "ars-s3-upload-*")
	if err != nil {
		return 0, fmt.Errorf("create temp file: %w", err)
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close()

	size, err := io.Copy(tmp, r)
	if err != nil {
		return 0, fmt.Errorf("buffer artifact: %w", err)
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return 0, fmt.Errorf("seek temp file: %w", err)
	}

	_, err = b.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(b.bucket),
		Key:           aws.String(b.objectKey(key)),
		Body:          tmp,
		ContentLength: aws.Int64(size),
	})
	if err != nil {
		return 0, fmt.Errorf("s3 put object %q: %w", key, err)
	}
	return size, nil
}

// Open returns the object body for streaming. Maps NoSuchKey → ErrNotFound.
func (b *Backend) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	out, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(b.objectKey(key)),
	})
	if err != nil {
		return nil, mapNotFound(err, key)
	}
	return out.Body, nil
}

// GetSignedURL returns a pre-signed GET URL valid for ttl. Callers receive
// a 307 redirect directly to this URL (no proxy through the API server).
func (b *Backend) GetSignedURL(ctx context.Context, key string, ttl time.Duration) (string, error) {
	req, err := b.presign.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(b.objectKey(key)),
	}, func(o *s3.PresignOptions) {
		o.Expires = ttl
	})
	if err != nil {
		return "", fmt.Errorf("presign get object %q: %w", key, err)
	}
	return req.URL, nil
}

// Delete removes the object from S3. Silently succeeds if the key is absent.
func (b *Backend) Delete(ctx context.Context, key string) error {
	_, err := b.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(b.objectKey(key)),
	})
	if err != nil {
		return fmt.Errorf("s3 delete object %q: %w", key, err)
	}
	return nil
}

// mapNotFound translates S3 not-found errors to domain.ErrNotFound.
func mapNotFound(err error, key string) error {
	var nsKey *s3types.NoSuchKey
	if errors.As(err, &nsKey) {
		return domain.ErrNotFound
	}
	// Fallback: any 404 from the HTTP layer (covers non-standard S3 errors from MinIO).
	if strings.Contains(err.Error(), "404") || isHTTP404(err) {
		return domain.ErrNotFound
	}
	return fmt.Errorf("s3 get object %q: %w", key, err)
}

type httpStatusCoder interface {
	HTTPStatusCode() int
}

func isHTTP404(err error) bool {
	var sc httpStatusCoder
	if errors.As(err, &sc) {
		return sc.HTTPStatusCode() == http.StatusNotFound
	}
	return false
}
