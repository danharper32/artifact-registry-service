package api

import (
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"

	"github.com/danharper32/artifact-registry-service/internal/config"
	"github.com/danharper32/artifact-registry-service/internal/service"
	"github.com/danharper32/artifact-registry-service/internal/storage"
)

func NewRouter(svc *service.Registry, stor storage.Backend, log *slog.Logger, cfg *config.Config) http.Handler {
	h := &Handler{svc: svc, stor: stor, log: log, cfg: cfg}

	r := chi.NewRouter()
	r.Use(chimiddleware.Recoverer)
	r.Use(requestLogger(log))

	r.Route("/v1", func(r chi.Router) {
		r.Get("/health", h.Health)

		// Artifact endpoints
		r.Route("/artifacts/{type}/{name}", func(r chi.Router) {
			r.Post("/", h.Upload)
			r.Get("/versions", h.ListVersions)
			r.Get("/latest", h.ResolveLatest)
			r.Get("/{version}", h.GetVersion)
			r.Get("/{version}/download", h.Download)
		})

		// Channel endpoints
		r.Route("/channels/{channel}", func(r chi.Router) {
			r.Post("/promote/{type}/{name}/{version}", h.Promote)
			r.Post("/rollback/{type}/{name}", h.Rollback)
			r.Get("/state/{type}/{name}", h.GetChannelState)
			r.Get("/history/{type}/{name}", h.GetChannelHistory)
		})
	})

	return r
}
