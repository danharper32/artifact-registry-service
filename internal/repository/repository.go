package repository

import (
	"context"

	"github.com/danharper32/artifact-registry-service/internal/domain"
)

// Repository is the metadata persistence interface.
type Repository interface {
	SaveArtifact(ctx context.Context, a *domain.Artifact) error
	GetArtifact(ctx context.Context, artifactType, name, version string) (*domain.Artifact, error)
	ListVersions(ctx context.Context, artifactType, name string) ([]*domain.Artifact, error)

	SaveChannel(ctx context.Context, ch *domain.ChannelPointer) error
	GetChannel(ctx context.Context, channel, artifactType, name string) (*domain.ChannelPointer, error)
	ListChannelHistory(ctx context.Context, channel, artifactType, name string) ([]*domain.ChannelPointer, error)

	Close() error
}
