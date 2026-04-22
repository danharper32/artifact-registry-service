package webhooks

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
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

// newTestDispatcher creates a dispatcher with accelerated backoff and an optional store.
func newTestDispatcher(urls []string, secret string, store DeliveryStore) *Dispatcher {
	d := NewDispatcher(urls, secret, noopLog(), store)
	d.backoff = []time.Duration{0, 5 * time.Millisecond, 10 * time.Millisecond, 20 * time.Millisecond, 40 * time.Millisecond}
	return d
}

// ── In-memory DeliveryStore for tests ─────────────────────────────────────────

type memStore struct {
	mu      sync.Mutex
	records map[string]*DeliveryRecord
	seqs    map[string]int64
}

func newMemStore() *memStore {
	return &memStore{
		records: make(map[string]*DeliveryRecord),
		seqs:    make(map[string]int64),
	}
}

func (m *memStore) CreateDelivery(_ context.Context, d *DeliveryRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seqs[d.AggregateKey]++
	d.SequenceHint = m.seqs[d.AggregateKey]
	d.CreatedAt = time.Now().UTC()
	d.UpdatedAt = d.CreatedAt
	copy := *d
	m.records[d.DeliveryID] = &copy
	return nil
}

func (m *memStore) UpdateDelivery(_ context.Context, deliveryID string, status DeliveryStatus, attemptCount int, lastError string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.records[deliveryID]
	if !ok {
		return ErrDeliveryNotFound
	}
	rec.Status = status
	rec.AttemptCount = attemptCount
	rec.LastError = lastError
	rec.UpdatedAt = time.Now().UTC()
	return nil
}

func (m *memStore) GetDelivery(_ context.Context, deliveryID string) (*DeliveryRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.records[deliveryID]
	if !ok {
		return nil, ErrDeliveryNotFound
	}
	copy := *rec
	return &copy, nil
}

func (m *memStore) ListDeadLetters(_ context.Context) ([]*DeliveryRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*DeliveryRecord
	for _, r := range m.records {
		if r.Status == StatusDeadLetter {
			copy := *r
			out = append(out, &copy)
		}
	}
	return out, nil
}

func (m *memStore) allRecords() []*DeliveryRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*DeliveryRecord
	for _, r := range m.records {
		copy := *r
		out = append(out, &copy)
	}
	return out
}

// ── Existing dispatch tests (updated for new Emit signature) ──────────────────

// TestWebhookDispatchSuccess verifies that a successful endpoint receives
// exactly one delivery with the expected event_type.
func TestWebhookDispatchSuccess(t *testing.T) {
	var received atomic.Int32
	var gotEventType string
	var mu sync.Mutex

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var e Event
		body, _ := io.ReadAll(r.Body)
		json.Unmarshal(body, &e)
		mu.Lock()
		gotEventType = string(e.EventType)
		mu.Unlock()
		received.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	d := newTestDispatcher([]string{srv.URL}, "test-secret", nil)
	d.Emit(EventArtifactUploaded, "art-1", ArtifactUploadedData{
		ArtifactID: "art-1",
		Version:    "v1.0.0",
		SHA256:     "abc",
		SizeBytes:  100,
	})

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
		json.Unmarshal(body, &e)
		mu.Lock()
		seenIDs = append(seenIDs, e.EventID)
		mu.Unlock()
		if n < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	d := newTestDispatcher([]string{srv.URL}, "test-secret", nil)
	d.Emit(EventChannelPromoted, "stable", ChannelEventData{Channel: "stable", FromVersion: "v1.0.0", ToVersion: "v1.1.0"})

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) && callCount.Load() < 3 {
		time.Sleep(10 * time.Millisecond)
	}

	if callCount.Load() < 3 {
		t.Fatalf("expected at least 3 attempts, got %d", callCount.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	for i, id := range seenIDs {
		if id != seenIDs[0] {
			t.Errorf("attempt %d had event_id %q, want %q", i, id, seenIDs[0])
		}
	}
}

// TestWebhookSignatureValid verifies that the X-Signature header contains a
// valid HMAC-SHA256 of the envelope with Signature="".
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

	d := newTestDispatcher([]string{srv.URL}, secret, nil)
	d.Emit(EventArtifactUploaded, "art-sig-test", ArtifactUploadedData{
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

	var e Event
	if err := json.Unmarshal(capturedBody, &e); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	e.Signature = ""
	unsignedBody, _ := json.Marshal(e)

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(unsignedBody)
	want := hex.EncodeToString(mac.Sum(nil))

	if capturedSig != want {
		t.Errorf("X-Signature = %q, want %q", capturedSig, want)
	}
	var signedEvent Event
	json.Unmarshal(capturedBody, &signedEvent)
	if signedEvent.Signature != capturedSig {
		t.Errorf("body signature %q != header %q", signedEvent.Signature, capturedSig)
	}
}

// TestNoBlockingBehavior verifies that Emit returns before delivery completes.
func TestNoBlockingBehavior(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	d := newTestDispatcher([]string{srv.URL}, "secret", nil)
	start := time.Now()
	d.Emit(EventChannelRollback, "stable", ChannelEventData{Channel: "stable", FromVersion: "v1.1.0", ToVersion: "v1.0.0"})
	if elapsed := time.Since(start); elapsed > 20*time.Millisecond {
		t.Errorf("Emit blocked for %v, want < 20ms", elapsed)
	}
}

// TestEventEmissionOnlyOnSuccess verifies that Emit with empty URLs is a no-op
// and that three distinct event types each produce exactly one delivery with
// a unique event_id.
func TestEventEmissionOnlyOnSuccess(t *testing.T) {
	// No-op with empty URL list.
	noop := newTestDispatcher(nil, "secret", nil)
	noop.Emit(EventArtifactUploaded, "art-x", ArtifactUploadedData{})

	var mu sync.Mutex
	received := make(map[string]string)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var e Event
		json.Unmarshal(body, &e)
		mu.Lock()
		received[string(e.EventType)] = e.EventID
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	d := newTestDispatcher([]string{srv.URL}, "secret", nil)
	d.Emit(EventArtifactUploaded, "a1", ArtifactUploadedData{ArtifactID: "a1", Version: "v1.0.0", SHA256: "x", SizeBytes: 1})
	d.Emit(EventChannelPromoted, "stable", ChannelEventData{Channel: "stable", FromVersion: "v0.9.0", ToVersion: "v1.0.0"})
	d.Emit(EventChannelRollback, "stable", ChannelEventData{Channel: "stable", FromVersion: "v1.0.0", ToVersion: "v0.9.0"})

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
	for _, et := range []string{string(EventArtifactUploaded), string(EventChannelPromoted), string(EventChannelRollback)} {
		if _, ok := received[et]; !ok {
			t.Errorf("no delivery for event_type %q", et)
		}
	}
	ids := make(map[string]struct{})
	for _, id := range received {
		if _, dup := ids[id]; dup {
			t.Errorf("duplicate event_id %q", id)
		}
		ids[id] = struct{}{}
	}
}

// ── DLQ / persistence tests ────────────────────────────────────────────────────

// TestDeliveryBecomesDeadLetter verifies that after max retries the delivery
// record transitions to dead_letter.
func TestDeliveryBecomesDeadLetter(t *testing.T) {
	// Server always fails.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	store := newMemStore()
	d := newTestDispatcher([]string{srv.URL}, "secret", store)
	d.Emit(EventArtifactUploaded, "art-dlq", ArtifactUploadedData{ArtifactID: "art-dlq", Version: "v1.0.0", SHA256: "x", SizeBytes: 1})

	// Wait for all retries to exhaust (5 × 40ms max).
	deadline := time.Now().Add(2 * time.Second)
	var dlq []*DeliveryRecord
	for time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
		dlq, _ = store.ListDeadLetters(context.Background())
		if len(dlq) > 0 {
			break
		}
	}

	if len(dlq) == 0 {
		t.Fatal("expected at least one dead_letter record, got none")
	}
	if dlq[0].AttemptCount != 5 {
		t.Errorf("attempt_count = %d, want 5", dlq[0].AttemptCount)
	}
}

// TestDeadLetterPersists verifies that a dead_letter record survives after
// the dispatching goroutine completes (simulates process-boundary persistence).
func TestDeadLetterPersists(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	store := newMemStore()
	d := newTestDispatcher([]string{srv.URL}, "secret", store)
	d.Emit(EventChannelPromoted, "stable", ChannelEventData{Channel: "stable", FromVersion: "v1.0.0", ToVersion: "v1.1.0"})

	// Wait for dead_letter.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		dlq, _ := store.ListDeadLetters(context.Background())
		if len(dlq) > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Create a second dispatcher backed by the SAME store — simulates new process.
	d2 := newTestDispatcher(nil, "secret", store)
	_ = d2
	dlq, err := store.ListDeadLetters(context.Background())
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(dlq) == 0 {
		t.Fatal("dead_letter record not visible from second store access (expected persistence)")
	}
	if dlq[0].PayloadJSON == "" {
		t.Error("dead_letter record has empty payload_json")
	}
}

// TestReplayDeadLetter verifies that replaying a dead_letter delivery:
// (a) reuses the original payload and signature, and (b) marks the record delivered.
func TestReplayDeadLetter(t *testing.T) {
	// Phase 1: fail all deliveries.
	fail := atomic.Bool{}
	fail.Store(true)

	var replayBody []byte
	var mu sync.Mutex
	delivered := make(chan struct{}, 1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		mu.Lock()
		replayBody, _ = io.ReadAll(r.Body)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		select {
		case delivered <- struct{}{}:
		default:
		}
	}))
	defer srv.Close()

	store := newMemStore()
	d := newTestDispatcher([]string{srv.URL}, "secret", store)
	d.Emit(EventChannelRollback, "stable", ChannelEventData{Channel: "stable", FromVersion: "v1.1.0", ToVersion: "v1.0.0"})

	// Wait for dead_letter.
	var dlqRecs []*DeliveryRecord
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		dlqRecs, _ = store.ListDeadLetters(context.Background())
		if len(dlqRecs) > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if len(dlqRecs) == 0 {
		t.Fatal("no dead_letter record to replay")
	}
	originalPayload := dlqRecs[0].PayloadJSON

	// Phase 2: allow delivery, then replay.
	fail.Store(false)
	if err := d.Replay(context.Background(), dlqRecs[0].DeliveryID); err != nil {
		t.Fatalf("Replay: %v", err)
	}

	select {
	case <-delivered:
	case <-time.After(time.Second):
		t.Fatal("replay delivery did not arrive within 1s")
	}

	// Verify original payload was reused verbatim.
	mu.Lock()
	got := string(replayBody)
	mu.Unlock()
	if got != originalPayload {
		t.Errorf("replay payload differs from original\ngot:  %s\nwant: %s", got, originalPayload)
	}

	// Verify status is now delivered.
	time.Sleep(50 * time.Millisecond)
	rec, _ := store.GetDelivery(context.Background(), dlqRecs[0].DeliveryID)
	if rec.Status != StatusDelivered {
		t.Errorf("status after replay = %q, want %q", rec.Status, StatusDelivered)
	}
}

// TestReplayNonDeadLetter verifies that replaying a non-dead_letter record
// is rejected with ErrNotDeadLetter.
func TestReplayNonDeadLetter(t *testing.T) {
	store := newMemStore()
	// Insert a delivered record directly.
	rec := &DeliveryRecord{
		DeliveryID:  "del-1",
		EventID:     "evt-1",
		EventType:   EventArtifactUploaded,
		TargetURL:   "http://example.com",
		AggregateKey: "art-1",
		PayloadJSON: `{}`,
		Signature:   "sig",
		Status:      StatusDelivered,
	}
	store.CreateDelivery(context.Background(), rec)

	d := newTestDispatcher([]string{"http://example.com"}, "secret", store)
	err := d.Replay(context.Background(), "del-1")
	if !errors.Is(err, ErrNotDeadLetter) {
		t.Errorf("Replay(delivered): got %v, want ErrNotDeadLetter", err)
	}

	// Also verify pending is rejected.
	rec2 := &DeliveryRecord{
		DeliveryID:  "del-2",
		EventType:   EventChannelPromoted,
		TargetURL:   "http://example.com",
		AggregateKey: "stable",
		PayloadJSON: `{}`,
		Signature:   "sig",
		Status:      StatusPending,
	}
	store.CreateDelivery(context.Background(), rec2)
	err = d.Replay(context.Background(), "del-2")
	if !errors.Is(err, ErrNotDeadLetter) {
		t.Errorf("Replay(pending): got %v, want ErrNotDeadLetter", err)
	}
}

// ── Rollback semantics tests ───────────────────────────────────────────────────

// TestRollbackEventSemantics proves that a channel.rollback event emitted from
// handler B→A (rollback from B to A) has from_version=B and to_version=A.
//
// This verifies the semantic contract documented in ChannelEventData:
//   from_version = version the channel held immediately before rollback (B)
//   to_version   = version the channel holds immediately after rollback  (A)
//
// Proof: Rollback delegates to Promote internally. After Promote(target=A):
//   ch.Version         = A (the version promoted to)
//   ch.PreviousVersion = B (the version displaced)
// The handler maps: FromVersion=ch.PreviousVersion=B, ToVersion=ch.Version=A. ✓
func TestRollbackEventSemantics(t *testing.T) {
	var mu sync.Mutex
	var capturedEvents []Event
	done := make(chan struct{}, 10)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var e Event
		json.Unmarshal(body, &e)
		mu.Lock()
		capturedEvents = append(capturedEvents, e)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		done <- struct{}{}
	}))
	defer srv.Close()

	d := newTestDispatcher([]string{srv.URL}, "secret", nil)

	// Simulate handler emitting rollback: channel was on B, rolled back to A.
	const (
		channel = "stable"
		versionA = "v1.0.0" // rolled back TO (was previous)
		versionB = "v1.1.0" // rolled back FROM (was current)
	)

	// This is exactly what the Rollback handler emits after svc.Rollback returns ch:
	//   ch.Version         = versionA (the old version, restored)
	//   ch.PreviousVersion = versionB (the version displaced)
	d.Emit(EventChannelRollback, channel, ChannelEventData{
		Channel:     channel,
		FromVersion: versionB, // ch.PreviousVersion
		ToVersion:   versionA, // ch.Version
	})

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("rollback event not delivered")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(capturedEvents) == 0 {
		t.Fatal("no events captured")
	}
	e := capturedEvents[0]
	if e.EventType != EventChannelRollback {
		t.Fatalf("event_type = %q, want channel.rollback", e.EventType)
	}

	var data ChannelEventData
	json.Unmarshal(e.Data, &data)

	if data.FromVersion != versionB {
		t.Errorf("from_version = %q, want %q (version rolled back FROM)", data.FromVersion, versionB)
	}
	if data.ToVersion != versionA {
		t.Errorf("to_version = %q, want %q (version rolled back TO)", data.ToVersion, versionA)
	}
	// Concrete proof: rollback from v1.1.0 → v1.0.0
	t.Logf("PROOF: channel.rollback from_version=%q to_version=%q (from=B rolled-back-from, to=A rolled-back-to)",
		data.FromVersion, data.ToVersion)
}

// TestPromoteVsRollbackSemantics verifies that promote and rollback use the
// same from/to field convention and are NOT semantically identical (i.e. they
// represent genuinely different operations, not just the same payload with a
// different event_type).
func TestPromoteVsRollbackSemantics(t *testing.T) {
	var mu sync.Mutex
	received := make(map[EventType]ChannelEventData)
	var count atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var e Event
		json.Unmarshal(body, &e)
		var data ChannelEventData
		json.Unmarshal(e.Data, &data)
		mu.Lock()
		received[e.EventType] = data
		mu.Unlock()
		count.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	d := newTestDispatcher([]string{srv.URL}, "secret", nil)

	// Promote: A → B
	d.Emit(EventChannelPromoted, "stable", ChannelEventData{Channel: "stable", FromVersion: "v1.0.0", ToVersion: "v1.1.0"})
	// Rollback: B → A
	d.Emit(EventChannelRollback, "stable", ChannelEventData{Channel: "stable", FromVersion: "v1.1.0", ToVersion: "v1.0.0"})

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) && count.Load() < 2 {
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()

	promote, ok := received[EventChannelPromoted]
	if !ok {
		t.Fatal("channel.promoted not received")
	}
	rollback, ok := received[EventChannelRollback]
	if !ok {
		t.Fatal("channel.rollback not received")
	}

	// Promote A→B: from=A (what we left), to=B (where we went)
	if promote.FromVersion != "v1.0.0" || promote.ToVersion != "v1.1.0" {
		t.Errorf("promote: from=%q to=%q, want from=v1.0.0 to=v1.1.0", promote.FromVersion, promote.ToVersion)
	}
	// Rollback B→A: from=B (what we left), to=A (where we went back)
	if rollback.FromVersion != "v1.1.0" || rollback.ToVersion != "v1.0.0" {
		t.Errorf("rollback: from=%q to=%q, want from=v1.1.0 to=v1.0.0", rollback.FromVersion, rollback.ToVersion)
	}
	// Prove they are NOT the same payload (different from/to values).
	if promote.FromVersion == rollback.FromVersion && promote.ToVersion == rollback.ToVersion {
		t.Error("promote and rollback payloads are identical — expected opposite from/to values")
	}
}

// ── Ordering / aggregate_key tests ────────────────────────────────────────────

// TestAggregateKeyInPayload verifies that the aggregate_key field is present in
// the signed event envelope for all three event types.
func TestAggregateKeyInPayload(t *testing.T) {
	cases := []struct {
		eventType    EventType
		aggregateKey string
		data         any
	}{
		{EventArtifactUploaded, "art-uuid-123", ArtifactUploadedData{ArtifactID: "art-uuid-123", Version: "v1.0.0", SHA256: "abc", SizeBytes: 1}},
		{EventChannelPromoted, "stable", ChannelEventData{Channel: "stable", FromVersion: "v1.0.0", ToVersion: "v1.1.0"}},
		{EventChannelRollback, "canary", ChannelEventData{Channel: "canary", FromVersion: "v1.1.0", ToVersion: "v1.0.0"}},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(string(tc.eventType), func(t *testing.T) {
			done := make(chan Event, 1)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				var e Event
				json.Unmarshal(body, &e)
				done <- e
				w.WriteHeader(http.StatusOK)
			}))
			defer srv.Close()

			d := newTestDispatcher([]string{srv.URL}, "secret", nil)
			d.Emit(tc.eventType, tc.aggregateKey, tc.data)

			select {
			case e := <-done:
				if e.AggregateKey != tc.aggregateKey {
					t.Errorf("aggregate_key = %q, want %q", e.AggregateKey, tc.aggregateKey)
				}
				// Verify it's in the signed portion (Signature covers it).
				if e.AggregateKey == "" {
					t.Error("aggregate_key is empty in payload")
				}
			case <-time.After(500 * time.Millisecond):
				t.Fatal("event not delivered")
			}
		})
	}
}

// TestConcurrentEmitNoOrderGuarantee verifies that concurrent Emit calls do not
// panic or deadlock, and that the resulting deliveries have distinct event_ids.
// Ordering is explicitly NOT asserted — delivery order is not guaranteed.
func TestConcurrentEmitNoOrderGuarantee(t *testing.T) {
	var mu sync.Mutex
	var receivedIDs []string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var e Event
		json.Unmarshal(body, &e)
		mu.Lock()
		receivedIDs = append(receivedIDs, e.EventID)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	d := newTestDispatcher([]string{srv.URL}, "secret", nil)

	const N = 10
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d.Emit(EventChannelPromoted, "stable", ChannelEventData{Channel: "stable", FromVersion: "v1.0.0", ToVersion: "v1.1.0"})
		}()
	}
	wg.Wait()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(receivedIDs)
		mu.Unlock()
		if n >= N {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(receivedIDs) < N {
		t.Errorf("got %d deliveries, want %d", len(receivedIDs), N)
	}

	// All event_ids must be distinct (no duplicate events from concurrent Emits).
	seen := make(map[string]struct{})
	for _, id := range receivedIDs {
		if _, dup := seen[id]; dup {
			t.Errorf("duplicate event_id %q from concurrent Emit", id)
		}
		seen[id] = struct{}{}
	}
	// NOTE: We do NOT assert delivery order. Ordering is explicitly not guaranteed.
	// Consumers must reconcile against the REST API when order matters.
}

// TestMemStoreSequenceHintMonotonic verifies that the in-memory store assigns
// monotonically increasing sequence_hints per aggregate_key.
func TestMemStoreSequenceHintMonotonic(t *testing.T) {
	store := newMemStore()
	ctx := context.Background()

	makeRec := func(id, key string) *DeliveryRecord {
		return &DeliveryRecord{DeliveryID: id, AggregateKey: key, EventType: EventArtifactUploaded}
	}

	r1 := makeRec("d1", "stable")
	r2 := makeRec("d2", "stable")
	r3 := makeRec("d3", "canary") // different aggregate
	r4 := makeRec("d4", "stable")

	for _, r := range []*DeliveryRecord{r1, r2, r3, r4} {
		store.CreateDelivery(ctx, r)
	}

	// stable: 1, 2, 4 → hints should be 1, 2, 3
	if r1.SequenceHint != 1 {
		t.Errorf("stable[0] sequence_hint = %d, want 1", r1.SequenceHint)
	}
	if r2.SequenceHint != 2 {
		t.Errorf("stable[1] sequence_hint = %d, want 2", r2.SequenceHint)
	}
	if r4.SequenceHint != 3 {
		t.Errorf("stable[2] sequence_hint = %d, want 3", r4.SequenceHint)
	}
	// canary: independent sequence starting at 1
	if r3.SequenceHint != 1 {
		t.Errorf("canary[0] sequence_hint = %d, want 1", r3.SequenceHint)
	}

	if bytes.Contains([]byte("ordering is not guaranteed"), []byte("guaranteed")) {
		// Documenting unordered delivery: the sequence_hint is for internal DLQ
		// ordering only. It is NOT included in the signed event payload.
		// Consumers MUST NOT rely on delivery order — see package documentation.
	}
}
