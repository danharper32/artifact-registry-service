package api

import (
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"

	"github.com/danharper32/artifact-registry-service/internal/auth"
	"github.com/danharper32/artifact-registry-service/internal/config"
	"github.com/danharper32/artifact-registry-service/internal/service"
	"github.com/danharper32/artifact-registry-service/internal/storage"
	"github.com/danharper32/artifact-registry-service/internal/webhooks"
)

func NewRouter(svc *service.Registry, stor storage.Backend, log *slog.Logger, cfg *config.Config, wh *webhooks.Dispatcher) http.Handler {
	h := &Handler{svc: svc, stor: stor, log: log, cfg: cfg, wh: wh}
	store := cfg.Auth

	readOnly  := requireScope(store, auth.ScopeRead)
	writeOnly := requireScope(store, auth.ScopeWrite)
	adminOnly := requireScope(store, auth.ScopeAdmin)

	r := chi.NewRouter()
	r.Use(chimiddleware.Recoverer)
	r.Use(requestLogger(log))

	r.Route("/v1", func(r chi.Router) {
		// Public — no auth required.
		r.Get("/health", h.Health)

		// Artifact endpoints.
		r.Route("/artifacts/{type}/{name}", func(r chi.Router) {
			r.With(writeOnly).Post("/", h.Upload)
			r.With(readOnly).Get("/versions", h.ListVersions)
			r.With(readOnly).Get("/latest", h.ResolveLatest)
			r.With(readOnly).Get("/{version}", h.GetVersion)
			r.With(readOnly).Get("/{version}/download", h.Download)
		})

		// Channel endpoints.
		r.Route("/channels/{channel}", func(r chi.Router) {
			r.With(adminOnly).Post("/promote/{type}/{name}/{version}", h.Promote)
			r.With(adminOnly).Post("/rollback/{type}/{name}", h.Rollback)
			r.With(readOnly).Get("/state/{type}/{name}", h.GetChannelState)
			r.With(readOnly).Get("/history/{type}/{name}", h.GetChannelHistory)
		})

		// Admin endpoints — require admin scope.
		r.Route("/admin", func(r chi.Router) {
			r.With(adminOnly).Post("/webhooks/{delivery_id}/replay", h.ReplayWebhook)
		})
	})

	return r
}
