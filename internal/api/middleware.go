package api

import (
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/danharper32/artifact-registry-service/internal/auth"
)

func requestLogger(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rw := &responseWriter{ResponseWriter: w, status: 200}
			next.ServeHTTP(rw, r)
			log.Info("request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", rw.status,
				"duration_ms", time.Since(start).Milliseconds(),
			)
		})
	}
}

// requireScope returns a middleware that enforces the given scope.
// Extracts the bearer token from Authorization: Bearer <key>.
func requireScope(store *auth.Store, scope string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := extractBearer(r)
			if !store.Allowed(key, scope) {
				if store.Enabled() && !store.Authenticated(key) {
					writeJSON(w, http.StatusUnauthorized, map[string]string{
						"error": "missing or invalid API key",
					})
					return
				}
				writeJSON(w, http.StatusForbidden, map[string]string{
					"error": "insufficient scope — required: " + scope,
				})
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func extractBearer(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if after, ok := strings.CutPrefix(h, "Bearer "); ok {
		return strings.TrimSpace(after)
	}
	return ""
}

type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}
