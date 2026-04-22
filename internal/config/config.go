package config

import (
	"os"
	"strconv"
)

type Config struct {
	Port         int
	StorageDir   string
	DatabasePath string
	MaxUploadMB  int64
	BaseURL      string
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

	return &Config{
		Port:         port,
		StorageDir:   getEnv("STORAGE_DIR", "./data/artifacts"),
		DatabasePath: getEnv("DATABASE_PATH", "./data/registry.db"),
		MaxUploadMB:  maxUploadMB,
		BaseURL:      getEnv("BASE_URL", "http://localhost:8080"),
	}
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
