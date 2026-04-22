package webhooks

import "encoding/json"

// EventType is the string discriminator sent in every event envelope.
type EventType string

const (
	EventArtifactUploaded EventType = "artifact.uploaded"
	EventChannelPromoted  EventType = "channel.promoted"
	EventChannelRollback  EventType = "channel.rollback"
)

// Event is the full envelope sent to every configured webhook URL.
// Data is stored as raw JSON so the exact bytes are preserved through
// serialization round-trips, which is required for deterministic signing.
// Signature is computed over the JSON of this struct with Signature="";
// recipients zero the Signature field and re-marshal to verify.
type Event struct {
	EventID   string          `json:"event_id"`
	EventType EventType       `json:"event_type"`
	Timestamp string          `json:"timestamp"`
	Data      json.RawMessage `json:"data"`
	Signature string          `json:"signature"`
}

// ArtifactUploadedData is the data payload for artifact.uploaded.
type ArtifactUploadedData struct {
	ArtifactID string `json:"artifact_id"`
	Version    string `json:"version"`
	SHA256     string `json:"sha256"`
	SizeBytes  int64  `json:"size_bytes"`
}

// ChannelEventData is the data payload for channel.promoted and channel.rollback.
type ChannelEventData struct {
	Channel     string `json:"channel"`
	FromVersion string `json:"from_version"`
	ToVersion   string `json:"to_version"`
}
