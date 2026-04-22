package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/danharper32/artifact-registry-service/internal/config"
	"github.com/danharper32/artifact-registry-service/internal/domain"
	"github.com/danharper32/artifact-registry-service/internal/service"
	"github.com/danharper32/artifact-registry-service/internal/storage"
	"github.com/danharper32/artifact-registry-service/internal/webhooks"
)

type Handler struct {
	svc  *service.Registry
	stor storage.Backend
	log  *slog.Logger
	cfg  *config.Config
	wh   *webhooks.Dispatcher
}

// ── Upload ────────────────────────────────────────────────────────────────────

func (h *Handler) Upload(w http.ResponseWriter, r *http.Request) {
	artifactType := chi.URLParam(r, "type")
	name := chi.URLParam(r, "name")

	maxBytes := h.cfg.MaxUploadMB << 20
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)

	if err := r.ParseMultipartForm(32 << 20); err != nil {
		writeError(w, http.StatusBadRequest, "invalid multipart form: "+err.Error())
		return
	}

	version := r.FormValue("version")
	if version == "" {
		writeError(w, http.StatusBadRequest, "version is required")
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		writeError(w, http.StatusBadRequest, "file is required")
		return
	}
	defer file.Close()

	// Parse optional metadata JSON from form field
	meta := map[string]string{}
	if metaStr := r.FormValue("metadata"); metaStr != "" {
		if err := json.Unmarshal([]byte(metaStr), &meta); err != nil {
			writeError(w, http.StatusBadRequest, "metadata must be a JSON object of string values")
			return
		}
	}

	ct := header.Header.Get("Content-Type")
	if ct == "" {
		ct = "application/octet-stream"
	}

	artifact, err := h.svc.Upload(r.Context(), service.UploadRequest{
		Type:        artifactType,
		Name:        name,
		Version:     version,
		ContentType: ct,
		Metadata:    meta,
		File:        file,
	})
	if err != nil {
		switch {
		case errors.Is(err, domain.ErrAlreadyExists):
			writeError(w, http.StatusConflict, err.Error())
		case errors.Is(err, domain.ErrInvalidVersion):
			writeError(w, http.StatusBadRequest, err.Error())
		case errors.Is(err, domain.ErrInvalidInput):
			writeError(w, http.StatusBadRequest, err.Error())
		default:
			h.log.Error("upload failed", "err", err)
			writeError(w, http.StatusInternalServerError, "upload failed")
		}
		return
	}
	h.wh.Emit(webhooks.EventArtifactUploaded, webhooks.ArtifactUploadedData{
		ArtifactID: artifact.ID,
		Version:    artifact.Version,
		SHA256:     artifact.SHA256,
		SizeBytes:  artifact.SizeBytes,
	})
	writeJSON(w, http.StatusCreated, artifact)
}

// ── List versions ─────────────────────────────────────────────────────────────

func (h *Handler) ListVersions(w http.ResponseWriter, r *http.Request) {
	versions, err := h.svc.ListVersions(r.Context(),
		chi.URLParam(r, "type"),
		chi.URLParam(r, "name"),
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if versions == nil {
		versions = []*domain.Artifact{}
	}
	writeJSON(w, http.StatusOK, versions)
}

// ── Resolve latest for channel ────────────────────────────────────────────────

func (h *Handler) ResolveLatest(w http.ResponseWriter, r *http.Request) {
	channel := r.URL.Query().Get("channel")
	if channel == "" {
		channel = "stable"
	}

	artifact, err := h.svc.ResolveLatest(r.Context(),
		channel,
		chi.URLParam(r, "type"),
		chi.URLParam(r, "name"),
	)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			writeError(w, http.StatusNotFound, fmt.Sprintf("no version promoted to channel %q", channel))
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	resp := artifactWithDownloadURL(artifact, h.cfg.BaseURL)
	writeJSON(w, http.StatusOK, resp)
}

// ── Get specific version ──────────────────────────────────────────────────────

func (h *Handler) GetVersion(w http.ResponseWriter, r *http.Request) {
	artifact, err := h.svc.GetVersion(r.Context(),
		chi.URLParam(r, "type"),
		chi.URLParam(r, "name"),
		chi.URLParam(r, "version"),
	)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			writeError(w, http.StatusNotFound, "version not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, artifactWithDownloadURL(artifact, h.cfg.BaseURL))
}

// ── Download artifact ─────────────────────────────────────────────────────────

func (h *Handler) Download(w http.ResponseWriter, r *http.Request) {
	artifact, err := h.svc.GetVersion(r.Context(),
		chi.URLParam(r, "type"),
		chi.URLParam(r, "name"),
		chi.URLParam(r, "version"),
	)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			writeError(w, http.StatusNotFound, "version not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Try signed URL first (S3 backend). Local backend returns ErrSignedURLUnsupported.
	url, err := h.stor.GetSignedURL(r.Context(), artifact.StorageKey, 15*time.Minute)
	if err == nil {
		http.Redirect(w, r, url, http.StatusTemporaryRedirect)
		return
	}

	// Local fallback: stream directly.
	rc, err := h.stor.Open(r.Context(), artifact.StorageKey)
	if err != nil {
		writeError(w, http.StatusNotFound, "artifact file not found")
		return
	}
	defer rc.Close()

	w.Header().Set("Content-Type", artifact.ContentType)
	w.Header().Set("X-Artifact-SHA256", artifact.SHA256)
	w.Header().Set("X-Artifact-Version", artifact.Version)
	io.Copy(w, rc)
}

// ── Promote ───────────────────────────────────────────────────────────────────

func (h *Handler) Promote(w http.ResponseWriter, r *http.Request) {
	ch, err := h.svc.Promote(r.Context(),
		chi.URLParam(r, "channel"),
		chi.URLParam(r, "type"),
		chi.URLParam(r, "name"),
		chi.URLParam(r, "version"),
		r.Header.Get("X-Promoted-By"),
	)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		if errors.Is(err, domain.ErrInvalidVersion) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.wh.Emit(webhooks.EventChannelPromoted, webhooks.ChannelEventData{
		Channel:     ch.Channel,
		FromVersion: ch.PreviousVersion,
		ToVersion:   ch.Version,
	})
	writeJSON(w, http.StatusOK, ch)
}

// ── Rollback ──────────────────────────────────────────────────────────────────

func (h *Handler) Rollback(w http.ResponseWriter, r *http.Request) {
	ch, err := h.svc.Rollback(r.Context(),
		chi.URLParam(r, "channel"),
		chi.URLParam(r, "type"),
		chi.URLParam(r, "name"),
		r.Header.Get("X-Promoted-By"),
	)
	if err != nil {
		if errors.Is(err, domain.ErrNoChannelHistory) {
			writeError(w, http.StatusConflict, err.Error())
			return
		}
		if errors.Is(err, domain.ErrNotFound) {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.wh.Emit(webhooks.EventChannelRollback, webhooks.ChannelEventData{
		Channel:     ch.Channel,
		FromVersion: ch.PreviousVersion,
		ToVersion:   ch.Version,
	})
	writeJSON(w, http.StatusOK, ch)
}

// ── Channel state ─────────────────────────────────────────────────────────────

func (h *Handler) GetChannelState(w http.ResponseWriter, r *http.Request) {
	ch, err := h.svc.GetChannel(r.Context(),
		chi.URLParam(r, "channel"),
		chi.URLParam(r, "type"),
		chi.URLParam(r, "name"),
	)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			writeError(w, http.StatusNotFound, "channel not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, ch)
}

func (h *Handler) GetChannelHistory(w http.ResponseWriter, r *http.Request) {
	history, err := h.svc.GetChannelHistory(r.Context(),
		chi.URLParam(r, "channel"),
		chi.URLParam(r, "type"),
		chi.URLParam(r, "name"),
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if history == nil {
		history = []*domain.ChannelPointer{}
	}
	writeJSON(w, http.StatusOK, history)
}

// ── Health ────────────────────────────────────────────────────────────────────

func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status":  "ok",
		"service": "artifact-registry-service",
		"version": "v0.1.0",
	})
}

// ── JSON helpers ──────────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

type artifactResponse struct {
	*domain.Artifact
	DownloadURL string `json:"download_url"`
}

func artifactWithDownloadURL(a *domain.Artifact, baseURL string) artifactResponse {
	return artifactResponse{
		Artifact:    a,
		DownloadURL: fmt.Sprintf("%s/v1/artifacts/%s/%s/%s/download", baseURL, a.Type, a.Name, a.Version),
	}
}
