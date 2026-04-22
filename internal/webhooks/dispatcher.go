package webhooks

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
)

// Dispatcher signs and asynchronously delivers webhook events to all configured URLs.
// When store is non-nil, each delivery is persisted and can be replayed from the
// dead-letter queue via Replay(). When store is nil, delivery is in-memory only.
type Dispatcher struct {
	urls    []string
	secret  []byte
	log     *slog.Logger
	client  *http.Client
	store   DeliveryStore
	backoff []time.Duration // injectable for tests
}

// NewDispatcher constructs a Dispatcher. Pass store=nil to disable persistence.
// When urls is empty and store is nil, Emit is a no-op.
func NewDispatcher(urls []string, secret string, log *slog.Logger, store DeliveryStore) *Dispatcher {
	return &Dispatcher{
		urls:    urls,
		secret:  []byte(secret),
		log:     log,
		client:  &http.Client{Timeout: 5 * time.Second},
		store:   store,
		backoff: defaultBackoff,
	}
}

// Emit builds, signs, and dispatches an event asynchronously.
// Returns immediately; delivery happens in background goroutines.
//
// aggregateKey identifies the affected entity (artifact_id for artifact events,
// channel name for channel events). It is included in the signed payload so
// consumers can route events without assuming delivery order.
//
// Each call to Emit produces exactly one logical event (one event_id). Retries
// for the same delivery reuse that event_id and the same signed payload verbatim.
func (d *Dispatcher) Emit(eventType EventType, aggregateKey string, data any) {
	if len(d.urls) == 0 {
		return
	}

	rawData, err := json.Marshal(data)
	if err != nil {
		d.log.Error("webhook marshal data failed", "event_type", eventType, "err", err)
		return
	}

	eventID := uuid.New().String()
	timestamp := time.Now().UTC().Format(time.RFC3339)

	e := Event{
		EventID:      eventID,
		EventType:    eventType,
		Timestamp:    timestamp,
		AggregateKey: aggregateKey,
		Data:         json.RawMessage(rawData),
		Signature:    "",
	}

	// Sign the envelope with Signature="" so receivers can verify by zeroing the field.
	unsignedBody, err := json.Marshal(e)
	if err != nil {
		d.log.Error("webhook marshal failed", "event_type", eventType, "err", err)
		return
	}
	sig := computeSignature(d.secret, unsignedBody)
	e.Signature = sig

	signedBody, err := json.Marshal(e)
	if err != nil {
		d.log.Error("webhook marshal failed", "event_type", eventType, "err", err)
		return
	}

	for _, url := range d.urls {
		url := url
		deliveryID := uuid.New().String()

		// Persist the delivery record before dispatching so the DLQ is durable.
		if d.store != nil {
			rec := &DeliveryRecord{
				DeliveryID:   deliveryID,
				EventID:      eventID,
				EventType:    eventType,
				TargetURL:    url,
				AggregateKey: aggregateKey,
				PayloadJSON:  string(signedBody),
				Signature:    sig,
				Status:       StatusPending,
				CreatedAt:    time.Now().UTC(),
			}
			if err := d.store.CreateDelivery(context.Background(), rec); err != nil {
				d.log.Error("webhook persist delivery failed", "event_type", eventType, "event_id", eventID, "err", err)
				// Continue without persistence — delivery is still attempted.
			}
		}

		go d.deliver(deliveryID, eventID, eventType, url, signedBody, sig)
	}
}

// Replay re-dispatches a dead_letter delivery using its original payload and signature.
// Returns ErrDeliveryNotFound if the delivery_id is unknown.
// Returns ErrNotDeadLetter if the record is not in dead_letter state.
func (d *Dispatcher) Replay(ctx context.Context, deliveryID string) error {
	if d.store == nil {
		return errors.New("no delivery store configured")
	}
	rec, err := d.store.GetDelivery(ctx, deliveryID)
	if err != nil {
		return err
	}
	if rec.Status != StatusDeadLetter {
		return ErrNotDeadLetter
	}
	// Reset to pending before dispatching so status is accurate during retry.
	if err := d.store.UpdateDelivery(ctx, deliveryID, StatusPending, rec.AttemptCount, ""); err != nil {
		return fmt.Errorf("reset delivery status: %w", err)
	}
	d.log.Info("webhook replay dispatched",
		"delivery_id", deliveryID,
		"event_id", rec.EventID,
		"event_type", rec.EventType,
		"url", rec.TargetURL,
	)
	go d.deliver(deliveryID, rec.EventID, rec.EventType, rec.TargetURL, []byte(rec.PayloadJSON), rec.Signature)
	return nil
}

// deliver attempts delivery with exponential backoff. Runs in its own goroutine.
// On success: marks the record delivered.
// On exhaustion: marks the record dead_letter and logs at ERROR.
func (d *Dispatcher) deliver(deliveryID string, eventID string, eventType EventType, url string, body []byte, sig string) {
	var lastErr error
	for attempt, delay := range d.backoff {
		if delay > 0 {
			time.Sleep(delay)
		}

		d.log.Info("webhook delivery",
			"event_type", eventType,
			"event_id", eventID,
			"url", url,
			"attempt", attempt+1,
		)

		if err := d.post(url, body, sig); err == nil {
			d.persistStatus(deliveryID, StatusDelivered, attempt+1, "")
			return
		} else {
			lastErr = err
			d.log.Info("webhook attempt failed",
				"event_type", eventType,
				"event_id", eventID,
				"url", url,
				"attempt", attempt+1,
				"err", err,
			)
			d.persistStatus(deliveryID, StatusFailed, attempt+1, err.Error())
		}
	}

	d.log.Error("webhook delivery failed permanently",
		"event_type", eventType,
		"event_id", eventID,
		"url", url,
		"last_error", lastErr,
	)
	d.persistStatus(deliveryID, StatusDeadLetter, len(d.backoff), lastErr.Error())
}

func (d *Dispatcher) persistStatus(deliveryID string, status DeliveryStatus, attempts int, lastErr string) {
	if d.store == nil || deliveryID == "" {
		return
	}
	if err := d.store.UpdateDelivery(context.Background(), deliveryID, status, attempts, lastErr); err != nil {
		d.log.Error("webhook persist status failed", "delivery_id", deliveryID, "status", status, "err", err)
	}
}

func (d *Dispatcher) post(url string, body []byte, sig string) error {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Signature", sig)

	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	return fmt.Errorf("non-2xx status: %d", resp.StatusCode)
}
