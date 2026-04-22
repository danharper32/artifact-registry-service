// Package webhooks provides asynchronous, HMAC-signed webhook delivery.
//
// # Delivery guarantees
//
// Delivery is ASYNCHRONOUS. Emit() returns immediately; the HTTP POST to
// the configured endpoints happens in background goroutines. Each event is
// retried up to 5 times with exponential backoff before being marked
// dead_letter in the persistent delivery store.
//
// # Ordering guarantee: NONE
//
// Webhook delivery order is NOT guaranteed. Two events for the same artifact
// or channel may arrive out of order at the consumer. Consumers MUST treat
// webhook events as notifications, not as authoritative ordered state transitions.
// When ordering matters, consumers MUST reconcile against the service REST API
// (e.g. GET /v1/channels/{channel}/state/{type}/{name}) to obtain current state.
//
// The aggregate_key field in every event payload identifies the affected entity
// (artifact_id for artifact events; channel name for channel events) so consumers
// can route events to the correct reconciliation handler without relying on order.
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
//
// Signing contract:
//   - Data is stored as json.RawMessage to preserve exact bytes across round-trips.
//   - Signature is computed over the JSON of this struct with Signature="".
//   - Receivers verify by unmarshaling, zeroing Signature, re-marshaling, and
//     computing HMAC-SHA256 with the shared secret. Compare to X-Signature header.
//
// aggregate_key identifies the affected entity and is included in the signed
// payload so consumers can reconcile state against the REST API.
type Event struct {
	EventID      string          `json:"event_id"`
	EventType    EventType       `json:"event_type"`
	Timestamp    string          `json:"timestamp"`
	AggregateKey string          `json:"aggregate_key"`
	Data         json.RawMessage `json:"data"`
	Signature    string          `json:"signature"`
}

// ArtifactUploadedData is the data payload for artifact.uploaded.
// Contains all fields needed to reconcile artifact state without
// relying on event ordering.
type ArtifactUploadedData struct {
	ArtifactID string `json:"artifact_id"`
	Version    string `json:"version"`
	SHA256     string `json:"sha256"`
	SizeBytes  int64  `json:"size_bytes"`
}

// ChannelEventData is the shared data payload for channel.promoted and channel.rollback.
//
// Rollback semantics (explicit):
//   - from_version = version the channel pointed to immediately BEFORE the operation
//   - to_version   = version the channel points to immediately AFTER the operation
//
// These semantics hold for both promote and rollback. Rollback internally
// delegates to promote logic; the returned ChannelPointer.PreviousVersion is
// the version left behind, and ChannelPointer.Version is the new target.
// The handler maps them as: FromVersion=PreviousVersion, ToVersion=Version.
type ChannelEventData struct {
	Channel     string `json:"channel"`
	FromVersion string `json:"from_version"`
	ToVersion   string `json:"to_version"`
}
