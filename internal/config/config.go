package config

import (
	"os"
	"strconv"
	"strings"

	"github.com/danharper32/artifact-registry-service/internal/auth"
)

// S3Config holds connection parameters for S3-compatible object storage.
type S3Config struct {
	Endpoint        string
	Region          string
	Bucket          string
	AccessKeyID     string
	SecretAccessKey string
	UseSSL          bool
	ForcePathStyle  bool
	Prefix          string
}

type Config struct {
	Port           int
	StorageDir     string
	StorageBackend string // "local" (default) or "s3"
	DatabasePath   string
	MaxUploadMB    int64
	BaseURL        string
	Auth           *auth.Store
	WebhookURLs    []string
	WebhookSecret  string
	S3             S3Config
}

func Load() *Config {
	port := 8080
	if p := os.Getenv("PORT"); p != "" {
		if v, err := strconv.Atoi(p); err == nil {
			port = v
		}
	}

	maxUploadMB := int64(500)
	if m := os.Getenv("MAX_UPLOAD_MB"); m != "" {
		if v, err := strconv.ParseInt(m, 10, 64); err == nil {
			maxUploadMB = v
		}
	}

	authDisabled := strings.EqualFold(getEnv("ARS_AUTH_DISABLED", "false"), "true")

	return &Config{
		Port:           port,
		StorageDir:     getEnv("STORAGE_DIR", "./data/artifacts"),
		StorageBackend: strings.ToLower(getEnv("STORAGE_BACKEND", "local")),
		DatabasePath:   getEnv("DATABASE_PATH", "./data/registry.db"),
		MaxUploadMB:    maxUploadMB,
		BaseURL:        getEnv("BASE_URL", "http://localhost:8080"),
		Auth:           auth.NewStore(loadKeys(), !authDisabled),
		WebhookURLs:    splitKeys(os.Getenv("WEBHOOK_URLS")),
		WebhookSecret:  os.Getenv("WEBHOOK_SECRET"),
		S3: S3Config{
			Endpoint:        os.Getenv("S3_ENDPOINT"),
			Region:          getEnv("S3_REGION", "us-east-1"),
			Bucket:          os.Getenv("S3_BUCKET"),
			AccessKeyID:     os.Getenv("S3_ACCESS_KEY_ID"),
			SecretAccessKey: os.Getenv("S3_SECRET_ACCESS_KEY"),
			UseSSL:          !strings.EqualFold(getEnv("S3_USE_SSL", "true"), "false"),
			ForcePathStyle:  strings.EqualFold(getEnv("S3_FORCE_PATH_STYLE", "false"), "true"),
			Prefix:          os.Getenv("S3_PREFIX"),
		},
	}
}

// loadKeys builds the key list from environment variables.
//
//	ARS_ADMIN_KEYS=key1,key2   — read + write + admin
//	ARS_WRITE_KEYS=key3        — read + write
//	ARS_READ_KEYS=key4,key5    — read only
func loadKeys() []auth.KeyEntry {
	var entries []auth.KeyEntry
	for _, raw := range splitKeys(os.Getenv("ARS_ADMIN_KEYS")) {
		entries = append(entries, auth.KeyEntry{Key: raw, Scopes: []string{auth.ScopeAdmin, auth.ScopeWrite, auth.ScopeRead}})
	}
	for _, raw := range splitKeys(os.Getenv("ARS_WRITE_KEYS")) {
		entries = append(entries, auth.KeyEntry{Key: raw, Scopes: []string{auth.ScopeWrite, auth.ScopeRead}})
	}
	for _, raw := range splitKeys(os.Getenv("ARS_READ_KEYS")) {
		entries = append(entries, auth.KeyEntry{Key: raw, Scopes: []string{auth.ScopeRead}})
	}
	return entries
}

func splitKeys(s string) []string {
	var out []string
	for _, k := range strings.Split(s, ",") {
		if k = strings.TrimSpace(k); k != "" {
			out = append(out, k)
		}
	}
	return out
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
