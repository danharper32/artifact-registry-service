package s3backend_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/danharper32/artifact-registry-service/internal/config"
	"github.com/danharper32/artifact-registry-service/internal/domain"
	"github.com/danharper32/artifact-registry-service/internal/storage"
	s3backend "github.com/danharper32/artifact-registry-service/internal/storage/s3"
)

// ── mock S3 server ─────────────────────────────────────────────────────────────

type mockS3 struct {
	mu      sync.Mutex
	objects map[string][]byte
	// paths records the last path seen per method for key assertions.
	paths map[string]string
}

func newMockS3(t *testing.T) (*httptest.Server, *mockS3) {
	t.Helper()
	m := &mockS3{
		objects: make(map[string][]byte),
		paths:   make(map[string]string),
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		m.mu.Lock()
		m.paths[r.Method] = path
		m.mu.Unlock()

		switch r.Method {
		case http.MethodPut:
			body, _ := io.ReadAll(r.Body)
			m.mu.Lock()
			m.objects[path] = body
			m.mu.Unlock()
			w.Header().Set("ETag", `"mock-etag"`)
			w.WriteHeader(http.StatusOK)

		case http.MethodGet:
			m.mu.Lock()
			body, ok := m.objects[path]
			m.mu.Unlock()
			if !ok {
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprintf(w,
					`<?xml version="1.0" encoding="UTF-8"?>`+
						`<Error><Code>NoSuchKey</Code>`+
						`<Message>The specified key does not exist.</Message>`+
						`<Key>%s</Key><RequestId>test</RequestId><HostId>test</HostId></Error>`,
					path)
				return
			}
			w.WriteHeader(http.StatusOK)
			w.Write(body)

		case http.MethodDelete:
			m.mu.Lock()
			delete(m.objects, path)
			m.mu.Unlock()
			w.WriteHeader(http.StatusNoContent)

		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	return srv, m
}

func newTestBackend(t *testing.T, srv *httptest.Server, prefix string) *s3backend.Backend {
	t.Helper()
	b, err := s3backend.New(s3backend.Config{
		Endpoint:        srv.URL,
		Region:          "us-east-1",
		Bucket:          "test-bucket",
		AccessKeyID:     "test-key",
		SecretAccessKey: "test-secret",
		ForcePathStyle:  true,
		Prefix:          prefix,
	})
	if err != nil {
		t.Fatalf("s3backend.New: %v", err)
	}
	return b
}

// ── tests ──────────────────────────────────────────────────────────────────────

// TestLocalBackendUnchanged verifies the local backend still stores, opens, and
// deletes artifacts correctly after the S3 backend was added.
func TestLocalBackendUnchanged(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{
		StorageDir:     dir,
		StorageBackend: "local",
	}
	be, err := storage.NewBackend(cfg)
	if err != nil {
		t.Fatalf("NewBackend: %v", err)
	}

	ctx := context.Background()
	content := []byte("local backend test content")

	n, err := be.Store(ctx, "model/x/v1.0.0/artifact", bytes.NewReader(content))
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	if n != int64(len(content)) {
		t.Errorf("Store returned %d bytes, want %d", n, len(content))
	}

	rc, err := be.Open(ctx, "model/x/v1.0.0/artifact")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, content) {
		t.Errorf("Open returned %q, want %q", got, content)
	}

	if err := be.Delete(ctx, "model/x/v1.0.0/artifact"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, err = be.Open(ctx, "model/x/v1.0.0/artifact")
	if !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("Open after delete: got %v, want ErrNotFound", err)
	}
}

// TestS3StoreReadRoundTrip verifies that Store writes bytes to S3 and Open reads them back.
func TestS3StoreReadRoundTrip(t *testing.T) {
	srv, _ := newMockS3(t)
	defer srv.Close()

	b := newTestBackend(t, srv, "")
	ctx := context.Background()

	content := []byte("s3 round-trip payload")
	n, err := b.Store(ctx, "model/mymodel/v1.0.0/artifact", bytes.NewReader(content))
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	if n != int64(len(content)) {
		t.Errorf("Store returned %d bytes, want %d", n, len(content))
	}

	rc, err := b.Open(ctx, "model/mymodel/v1.0.0/artifact")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("round-trip: got %q, want %q", got, content)
	}
}

// TestS3OpenNotFound verifies that a missing object maps to domain.ErrNotFound.
func TestS3OpenNotFound(t *testing.T) {
	srv, _ := newMockS3(t)
	defer srv.Close()

	b := newTestBackend(t, srv, "")
	ctx := context.Background()

	_, err := b.Open(ctx, "model/nonexistent/v9.9.9/artifact")
	if !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("Open missing key: got %v, want ErrNotFound", err)
	}
}

// TestDeterministicObjectKey verifies that the storage key seen by S3 is the
// canonical path pattern (type/name/version/artifact) with optional prefix prepended.
//
// Key rule: keys are always deterministic — same inputs produce the same S3 path.
func TestDeterministicObjectKey(t *testing.T) {
	const artifactKey = "model/fraud-detector/v1.2.3/artifact"

	t.Run("no prefix", func(t *testing.T) {
		srv, m := newMockS3(t)
		defer srv.Close()

		b := newTestBackend(t, srv, "")
		_, err := b.Store(context.Background(), artifactKey, bytes.NewReader([]byte("data")))
		if err != nil {
			t.Fatalf("Store: %v", err)
		}

		m.mu.Lock()
		putPath := m.paths[http.MethodPut]
		m.mu.Unlock()

		wantPath := "/test-bucket/" + artifactKey
		if putPath != wantPath {
			t.Errorf("S3 PUT path = %q, want %q", putPath, wantPath)
		}
	})

	t.Run("with prefix", func(t *testing.T) {
		srv, m := newMockS3(t)
		defer srv.Close()

		b := newTestBackend(t, srv, "prod")
		_, err := b.Store(context.Background(), artifactKey, bytes.NewReader([]byte("data")))
		if err != nil {
			t.Fatalf("Store: %v", err)
		}

		m.mu.Lock()
		putPath := m.paths[http.MethodPut]
		m.mu.Unlock()

		wantPath := "/test-bucket/prod/" + artifactKey
		if putPath != wantPath {
			t.Errorf("S3 PUT path = %q, want %q", putPath, wantPath)
		}
	})
}

// TestS3StartupMissingConfig verifies that New() fails fast when required fields are absent.
func TestS3StartupMissingConfig(t *testing.T) {
	base := s3backend.Config{
		Endpoint:        "http://localhost:9000",
		Region:          "us-east-1",
		Bucket:          "my-bucket",
		AccessKeyID:     "key",
		SecretAccessKey: "secret",
		ForcePathStyle:  true,
	}

	cases := []struct {
		name   string
		mutate func(*s3backend.Config)
	}{
		{"missing endpoint", func(c *s3backend.Config) { c.Endpoint = "" }},
		{"missing bucket", func(c *s3backend.Config) { c.Bucket = "" }},
		{"missing region", func(c *s3backend.Config) { c.Region = "" }},
		{"missing access key", func(c *s3backend.Config) { c.AccessKeyID = "" }},
		{"missing secret key", func(c *s3backend.Config) { c.SecretAccessKey = "" }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			tc.mutate(&cfg)
			_, err := s3backend.New(cfg)
			if err == nil {
				t.Error("expected error for missing required config, got nil")
			}
		})
	}
}

// TestBackendSelectionDefault verifies that an empty or "local" STORAGE_BACKEND
// produces a local disk backend.
func TestBackendSelectionDefault(t *testing.T) {
	dir := t.TempDir()

	for _, backendVal := range []string{"", "local"} {
		t.Run(fmt.Sprintf("STORAGE_BACKEND=%q", backendVal), func(t *testing.T) {
			// Temporarily override env to simulate config.Load() behavior.
			old := os.Getenv("STORAGE_BACKEND")
			os.Setenv("STORAGE_BACKEND", backendVal)
			defer os.Setenv("STORAGE_BACKEND", old)

			cfg := &config.Config{
				StorageDir:     dir,
				StorageBackend: strings.ToLower(backendVal),
			}
			be, err := storage.NewBackend(cfg)
			if err != nil {
				t.Fatalf("NewBackend: %v", err)
			}
			// A local backend can store and open without network.
			content := []byte("selection test")
			_, err = be.Store(context.Background(), "type/name/v1.0.0/artifact", bytes.NewReader(content))
			if err != nil {
				t.Fatalf("Store on local backend: %v", err)
			}
		})
	}
}

// TestBackendSelectionS3 verifies that STORAGE_BACKEND=s3 with required credentials
// constructs an S3 backend that is usable (connects to a mock server).
func TestBackendSelectionS3(t *testing.T) {
	srv, _ := newMockS3(t)
	defer srv.Close()

	cfg := &config.Config{
		StorageBackend: "s3",
		S3: config.S3Config{
			Endpoint:        srv.URL,
			Region:          "us-east-1",
			Bucket:          "test-bucket",
			AccessKeyID:     "key",
			SecretAccessKey: "secret",
			ForcePathStyle:  true,
		},
	}
	be, err := storage.NewBackend(cfg)
	if err != nil {
		t.Fatalf("NewBackend(s3): %v", err)
	}

	// Confirm the backend is functional by doing a round-trip through the mock.
	content := []byte("s3 selection test")
	_, err = be.Store(context.Background(), "model/x/v1.0.0/artifact", bytes.NewReader(content))
	if err != nil {
		t.Fatalf("Store through s3 backend: %v", err)
	}

	rc, err := be.Open(context.Background(), "model/x/v1.0.0/artifact")
	if err != nil {
		t.Fatalf("Open through s3 backend: %v", err)
	}
	rc.Close()
}
