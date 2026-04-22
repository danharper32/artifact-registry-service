package webhooks

import (
	"context"
	"time"
)

// DeliveryStatus is the lifecycle state of a single webhook delivery attempt.
type DeliveryStatus string

const (
	StatusPending    DeliveryStatus = "pending"
	StatusDelivered  DeliveryStatus = "delivered"
	StatusFailed     DeliveryStatus = "failed"    // transient failure, retries pending
	StatusDeadLetter DeliveryStatus = "dead_letter" // exhausted all retries
)

// DeliveryRecord is one delivery attempt record, persisted before dispatch.
// sequence_hint is a per-aggregate monotonic counter stored here for DLQ
// ordering; it is NOT included in the signed event payload because signing
// occurs before persistence. Consumers must not assume ordered delivery —
// see ordering guarantee documentation in api/openapi.yaml.
type DeliveryRecord struct {
	DeliveryID   string
	EventID      string
	EventType    EventType
	TargetURL    string
	AggregateKey string
	PayloadJSON  string // full signed JSON body, reused verbatim on replay
	Signature    string
	Status       DeliveryStatus
	AttemptCount int
	LastError    string
	SequenceHint int64 // monotonic per aggregate_key, assigned by store on create
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// DeliveryStore persists webhook delivery records and DLQ state.
// Implementations must be safe for concurrent calls.
type DeliveryStore interface {
	// CreateDelivery inserts a new pending delivery record. Implementations
	// must assign a monotonic SequenceHint per AggregateKey.
	CreateDelivery(ctx context.Context, d *DeliveryRecord) error

	// UpdateDelivery transitions status and records the latest attempt result.
	UpdateDelivery(ctx context.Context, deliveryID string, status DeliveryStatus, attemptCount int, lastError string) error

	// GetDelivery retrieves a single record by delivery_id.
	// Returns ErrDeliveryNotFound when absent.
	GetDelivery(ctx context.Context, deliveryID string) (*DeliveryRecord, error)

	// ListDeadLetters returns all dead_letter records ordered by created_at ASC.
	ListDeadLetters(ctx context.Context) ([]*DeliveryRecord, error)
}

// ErrDeliveryNotFound is returned by GetDelivery when no record matches.
var ErrDeliveryNotFound = errDeliveryNotFound{}

type errDeliveryNotFound struct{}

func (errDeliveryNotFound) Error() string { return "webhook delivery not found" }

// ErrNotDeadLetter is returned by Replay when the target record is not dead_letter.
var ErrNotDeadLetter = errNotDeadLetter{}

type errNotDeadLetter struct{}

func (errNotDeadLetter) Error() string {
	return "delivery is not in dead_letter state and cannot be replayed"
}
