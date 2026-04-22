package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/danharper32/artifact-registry-service/internal/domain"
	"github.com/danharper32/artifact-registry-service/internal/repository"
	"github.com/danharper32/artifact-registry-service/internal/storage"
)

var semverRE = regexp.MustCompile(`^v?\d+\.\d+\.\d+(-[a-zA-Z0-9.]+)?(\+[a-zA-Z0-9.]+)?$`)

// Registry is the core business logic layer.
type Registry struct {
	repo    repository.Repository
	storage storage.Backend
	log     *slog.Logger
}

func New(repo repository.Repository, stor storage.Backend, log *slog.Logger) *Registry {
	return &Registry{repo: repo, storage: stor, log: log}
}

// UploadRequest carries the data for a new artifact upload.
type UploadRequest struct {
	Type        string
	Name        string
	Version     string
	ContentType string
	Metadata    map[string]string
	File        io.Reader
}

// Upload stores a new immutable artifact version.
func (r *Registry) Upload(ctx context.Context, req UploadRequest) (*domain.Artifact, error) {
	if err := validateIdentifier(req.Type); err != nil {
		return nil, fmt.Errorf("type: %w", err)
	}
	if err := validateIdentifier(req.Name); err != nil {
		return nil, fmt.Errorf("name: %w", err)
	}
	version, err := normalizeVersion(req.Version)
	if err != nil {
		return nil, err
	}
	if req.ContentType == "" {
		req.ContentType = "application/octet-stream"
	}

	storageKey := storageKey(req.Type, req.Name, version)

	// Hash while streaming to storage.
	h := sha256.New()
	tee := io.TeeReader(req.File, h)
	size, err := r.storage.Store(ctx, storageKey, tee)
	if err != nil {
		return nil, fmt.Errorf("store artifact: %w", err)
	}

	artifact := &domain.Artifact{
		ID:          newID(),
		Type:        req.Type,
		Name:        req.Name,
		Version:     version,
		SHA256:      hex.EncodeToString(h.Sum(nil)),
		SizeBytes:   size,
		ContentType: req.ContentType,
		Metadata:    req.Metadata,
		StorageKey:  storageKey,
		CreatedAt:   time.Now().UTC(),
	}

	if err := r.repo.SaveArtifact(ctx, artifact); err != nil {
		// Clean up orphaned file if DB write fails.
		_ = r.storage.Delete(ctx, storageKey)
		return nil, err
	}
	r.log.Info("artifact uploaded", "type", req.Type, "name", req.Name, "version", version, "sha256", artifact.SHA256[:16])
	return artifact, nil
}

// Promote points a channel at a specific artifact version.
func (r *Registry) Promote(ctx context.Context, channel, artifactType, name, version, promotedBy string) (*domain.ChannelPointer, error) {
	version, err := normalizeVersion(version)
	if err != nil {
		return nil, err
	}
	artifact, err := r.repo.GetArtifact(ctx, artifactType, name, version)
	if err != nil {
		return nil, fmt.Errorf("artifact %s/%s@%s: %w", artifactType, name, version, err)
	}

	var prevVersion string
	if curr, err := r.repo.GetChannel(ctx, channel, artifactType, name); err == nil {
		prevVersion = curr.Version
	}

	ch := &domain.ChannelPointer{
		Channel:         channel,
		Type:            artifactType,
		Name:            name,
		Version:         version,
		ArtifactID:      artifact.ID,
		PreviousVersion: prevVersion,
		PromotedAt:      time.Now().UTC(),
		PromotedBy:      promotedBy,
	}
	if err := r.repo.SaveChannel(ctx, ch); err != nil {
		return nil, fmt.Errorf("save channel: %w", err)
	}
	r.log.Info("channel promoted", "channel", channel, "type", artifactType, "name", name, "version", version)
	return ch, nil
}

// Rollback re-points a channel to its previous version.
func (r *Registry) Rollback(ctx context.Context, channel, artifactType, name, promotedBy string) (*domain.ChannelPointer, error) {
	history, err := r.repo.ListChannelHistory(ctx, channel, artifactType, name)
	if err != nil {
		return nil, err
	}
	// history[0] is current; history[1] is previous
	if len(history) < 2 {
		return nil, domain.ErrNoChannelHistory
	}
	prev := history[1]
	return r.Promote(ctx, channel, artifactType, name, prev.Version, promotedBy)
}

// ResolveLatest returns the current artifact for a channel.
func (r *Registry) ResolveLatest(ctx context.Context, channel, artifactType, name string) (*domain.Artifact, error) {
	ch, err := r.repo.GetChannel(ctx, channel, artifactType, name)
	if err != nil {
		return nil, fmt.Errorf("channel %q for %s/%s: %w", channel, artifactType, name, err)
	}
	return r.repo.GetArtifact(ctx, artifactType, name, ch.Version)
}

// GetVersion returns a specific artifact version.
func (r *Registry) GetVersion(ctx context.Context, artifactType, name, version string) (*domain.Artifact, error) {
	version, err := normalizeVersion(version)
	if err != nil {
		return nil, err
	}
	return r.repo.GetArtifact(ctx, artifactType, name, version)
}

// ListVersions returns all versions for an artifact, newest first.
func (r *Registry) ListVersions(ctx context.Context, artifactType, name string) ([]*domain.Artifact, error) {
	return r.repo.ListVersions(ctx, artifactType, name)
}

// GetChannel returns the current channel pointer.
func (r *Registry) GetChannel(ctx context.Context, channel, artifactType, name string) (*domain.ChannelPointer, error) {
	return r.repo.GetChannel(ctx, channel, artifactType, name)
}

// GetChannelHistory returns the full promotion history for a channel.
func (r *Registry) GetChannelHistory(ctx context.Context, channel, artifactType, name string) ([]*domain.ChannelPointer, error) {
	return r.repo.ListChannelHistory(ctx, channel, artifactType, name)
}

// ── helpers ───────────────────────────────────────────────────────────────────

func storageKey(artifactType, name, version string) string {
	return fmt.Sprintf("%s/%s/%s/artifact", artifactType, name, version)
}

func normalizeVersion(v string) (string, error) {
	v = strings.TrimSpace(v)
	if !semverRE.MatchString(v) {
		return "", domain.ErrInvalidVersion
	}
	if !strings.HasPrefix(v, "v") {
		v = "v" + v
	}
	return v, nil
}

func validateIdentifier(s string) error {
	s = strings.TrimSpace(s)
	if s == "" || len(s) > 128 {
		return domain.ErrInvalidInput
	}
	for _, c := range s {
		if !isAlphaNum(c) && c != '-' && c != '_' && c != '.' {
			return fmt.Errorf("%w: %q contains invalid character %q", domain.ErrInvalidInput, s, c)
		}
	}
	return nil
}

func isAlphaNum(c rune) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

func newID() string {
	b := make([]byte, 16)
	// Use time + counter for simplicity (crypto/rand would be overkill for local MVP)
	t := time.Now().UnixNano()
	for i := range b {
		b[i] = byte(t >> (i * 4))
	}
	return hex.EncodeToString(b)
}
