package webhooks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
)

// Dispatcher signs and asynchronously delivers webhook events to all configured URLs.
type Dispatcher struct {
	urls    []string
	secret  []byte
	log     *slog.Logger
	client  *http.Client
	backoff []time.Duration // injectable for tests
}

// NewDispatcher constructs a Dispatcher. When urls is empty, Emit is a no-op.
func NewDispatcher(urls []string, secret string, log *slog.Logger) *Dispatcher {
	return &Dispatcher{
		urls:    urls,
		secret:  []byte(secret),
		log:     log,
		client:  &http.Client{Timeout: 5 * time.Second},
		backoff: defaultBackoff,
	}
}

// Emit builds, signs, and dispatches an event asynchronously.
// Returns immediately; delivery happens in background goroutines.
// Emits exactly one event per call — retries reuse the same event_id.
func (d *Dispatcher) Emit(eventType EventType, data any) {
	if len(d.urls) == 0 {
		return
	}

	// Pre-marshal data to json.RawMessage so bytes are stable across round-trips.
	rawData, err := json.Marshal(data)
	if err != nil {
		d.log.Error("webhook marshal data failed", "event_type", eventType, "err", err)
		return
	}

	e := Event{
		EventID:   uuid.New().String(),
		EventType: eventType,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Data:      json.RawMessage(rawData),
		Signature: "",
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
		go d.deliver(e.EventID, eventType, url, signedBody, sig)
	}
}

// deliver attempts delivery with exponential backoff. Runs in its own goroutine.
func (d *Dispatcher) deliver(eventID string, eventType EventType, url string, body []byte, sig string) {
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
			return
		} else {
			d.log.Info("webhook attempt failed",
				"event_type", eventType,
				"event_id", eventID,
				"url", url,
				"attempt", attempt+1,
				"err", err,
			)
		}
	}

	d.log.Error("webhook delivery failed permanently",
		"event_type", eventType,
		"event_id", eventID,
		"url", url,
	)
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
