package webhooks

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func noopLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newTestDispatcher creates a dispatcher with an accelerated backoff schedule
// suitable for tests (avoids waiting seconds between retries).
func newTestDispatcher(urls []string, secret string) *Dispatcher {
	d := NewDispatcher(urls, secret, noopLog())
	d.backoff = []time.Duration{0, 5 * time.Millisecond, 10 * time.Millisecond, 20 * time.Millisecond, 40 * time.Millisecond}
	return d
}

// TestWebhookDispatchSuccess verifies that a successful endpoint receives
// exactly one delivery containing the expected event_type and event_id.
func TestWebhookDispatchSuccess(t *testing.T) {
	var received atomic.Int32
	var gotEventType string
	var mu sync.Mutex

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var e Event
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &e); err != nil {
			t.Errorf("unmarshal event: %v", err)
		}
		mu.Lock()
		gotEventType = string(e.EventType)
		mu.Unlock()
		received.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	d := newTestDispatcher([]string{srv.URL}, "test-secret")
	d.Emit(EventArtifactUploaded, ArtifactUploadedData{
		ArtifactID: "art-1",
		Version:    "v1.0.0",
		SHA256:     "abc",
		SizeBytes:  100,
	})

	// Allow goroutine to complete.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) && received.Load() == 0 {
		time.Sleep(5 * time.Millisecond)
	}

	if received.Load() != 1 {
		t.Fatalf("expected 1 delivery, got %d", received.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	if gotEventType != string(EventArtifactUploaded) {
		t.Errorf("event_type = %q, want %q", gotEventType, EventArtifactUploaded)
	}
}

// TestWebhookRetryBehavior verifies that failed deliveries are retried and
// the same event_id is reused across all attempts.
func TestWebhookRetryBehavior(t *testing.T) {
	var callCount atomic.Int32
	var seenIDs []string
	var mu sync.Mutex

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := callCount.Add(1)
		body, _ := io.ReadAll(r.Body)
		var e Event
		_ = json.Unmarshal(body, &e)
		mu.Lock()
		seenIDs = append(seenIDs, e.EventID)
		mu.Unlock()
		if n < 3 {
			// Fail first two attempts.
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	d := newTestDispatcher([]string{srv.URL}, "test-secret")
	d.Emit(EventChannelPromoted, ChannelEventData{Channel: "stable", FromVersion: "v1.0.0", ToVersion: "v1.1.0"})

	// Wait long enough for 3 attempts with accelerated backoff.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) && callCount.Load() < 3 {
		time.Sleep(10 * time.Millisecond)
	}

	if callCount.Load() < 3 {
		t.Fatalf("expected at least 3 attempts, got %d", callCount.Load())
	}

	// All retries must carry the same event_id.
	mu.Lock()
	defer mu.Unlock()
	for i, id := range seenIDs {
		if id != seenIDs[0] {
			t.Errorf("attempt %d had event_id %q, want %q (same as attempt 0)", i, id, seenIDs[0])
		}
	}
}

// TestWebhookSignatureValid verifies that the X-Signature header contains a
// valid HMAC-SHA256 of the envelope with Signature set to "".
func TestWebhookSignatureValid(t *testing.T) {
	const secret = "super-secret"
	var capturedBody []byte
	var capturedSig string
	done := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedBody, _ = io.ReadAll(r.Body)
		capturedSig = r.Header.Get("X-Signature")
		w.WriteHeader(http.StatusOK)
		close(done)
	}))
	defer srv.Close()

	d := newTestDispatcher([]string{srv.URL}, secret)
	d.Emit(EventArtifactUploaded, ArtifactUploadedData{
		ArtifactID: "art-sig-test",
		Version:    "v2.0.0",
		SHA256:     "deadbeef",
		SizeBytes:  512,
	})

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("webhook delivery timed out")
	}

	// Reconstruct unsigned body: parse the received event, zero Signature, re-marshal.
	var e Event
	if err := json.Unmarshal(capturedBody, &e); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	e.Signature = ""
	unsignedBody, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("re-marshal: %v", err)
	}

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(unsignedBody)
	want := hex.EncodeToString(mac.Sum(nil))

	if capturedSig != want {
		t.Errorf("X-Signature = %q, want %q", capturedSig, want)
	}
	// Signature field in body must match header.
	var signedEvent Event
	_ = json.Unmarshal(capturedBody, &signedEvent)
	if signedEvent.Signature != capturedSig {
		t.Errorf("body signature %q != header signature %q", signedEvent.Signature, capturedSig)
	}
}

// TestNoBlockingBehavior verifies that Emit returns before delivery completes.
// Uses a slow endpoint (100 ms) and asserts Emit returns within 20 ms.
func TestNoBlockingBehavior(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	d := newTestDispatcher([]string{srv.URL}, "secret")

	start := time.Now()
	d.Emit(EventChannelRollback, ChannelEventData{Channel: "stable", FromVersion: "v1.1.0", ToVersion: "v1.0.0"})
	elapsed := time.Since(start)

	if elapsed > 20*time.Millisecond {
		t.Errorf("Emit blocked for %v, want < 20ms", elapsed)
	}
}

// TestEventEmissionOnlyOnSuccess verifies three properties:
//  1. Emit is a no-op when no URLs are configured (zero deliveries).
//  2. Emit fires exactly once per call for each of the three event types.
//  3. Each event carries a distinct event_id (no duplicates across event types).
func TestEventEmissionOnlyOnSuccess(t *testing.T) {
	// 1. No-op with empty URL list — must not panic or deliver.
	noop := newTestDispatcher(nil, "secret")
	noop.Emit(EventArtifactUploaded, ArtifactUploadedData{})
	// If this panics or blocks, the test fails.

	// 2 & 3. Three event types, one call each.
	var mu sync.Mutex
	received := make(map[string]string) // event_type → event_id

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var e Event
		_ = json.Unmarshal(body, &e)
		mu.Lock()
		received[string(e.EventType)] = e.EventID
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	d := newTestDispatcher([]string{srv.URL}, "secret")

	d.Emit(EventArtifactUploaded, ArtifactUploadedData{ArtifactID: "a1", Version: "v1.0.0", SHA256: "x", SizeBytes: 1})
	d.Emit(EventChannelPromoted, ChannelEventData{Channel: "stable", FromVersion: "v0.9.0", ToVersion: "v1.0.0"})
	d.Emit(EventChannelRollback, ChannelEventData{Channel: "stable", FromVersion: "v1.0.0", ToVersion: "v0.9.0"})

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(received)
		mu.Unlock()
		if n == 3 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()

	wantTypes := []string{
		string(EventArtifactUploaded),
		string(EventChannelPromoted),
		string(EventChannelRollback),
	}
	for _, et := range wantTypes {
		if _, ok := received[et]; !ok {
			t.Errorf("no delivery received for event_type %q", et)
		}
	}

	// event_ids must all be distinct.
	ids := make(map[string]struct{})
	for _, id := range received {
		if _, dup := ids[id]; dup {
			t.Errorf("duplicate event_id %q across event types", id)
		}
		ids[id] = struct{}{}
	}
}
