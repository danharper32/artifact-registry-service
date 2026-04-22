package domain

import "time"

// Artifact is an immutable, versioned artifact record.
type Artifact struct {
	ID          string            `json:"id"`
	Type        string            `json:"type"`
	Name        string            `json:"name"`
	Version     string            `json:"version"`
	SHA256      string            `json:"sha256"`
	SizeBytes   int64             `json:"size_bytes"`
	ContentType string            `json:"content_type"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	StorageKey  string            `json:"-"`
	CreatedAt   time.Time         `json:"created_at"`
}

// ChannelPointer tracks which version is active for a named channel.
type ChannelPointer struct {
	Channel         string    `json:"channel"`
	Type            string    `json:"type"`
	Name            string    `json:"name"`
	Version         string    `json:"version"`
	ArtifactID      string    `json:"artifact_id"`
	PreviousVersion string    `json:"previous_version,omitempty"`
	PromotedAt      time.Time `json:"promoted_at"`
	PromotedBy      string    `json:"promoted_by,omitempty"`
}
