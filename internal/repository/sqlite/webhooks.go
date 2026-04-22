package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/danharper32/artifact-registry-service/internal/webhooks"
)

const webhookSchema = `
CREATE TABLE IF NOT EXISTS webhook_deliveries (
	delivery_id    TEXT PRIMARY KEY,
	event_id       TEXT NOT NULL,
	event_type     TEXT NOT NULL,
	target_url     TEXT NOT NULL,
	aggregate_key  TEXT NOT NULL DEFAULT '',
	payload_json   TEXT NOT NULL,
	signature      TEXT NOT NULL,
	status         TEXT NOT NULL DEFAULT 'pending',
	attempt_count  INTEGER NOT NULL DEFAULT 0,
	last_error     TEXT NOT NULL DEFAULT '',
	sequence_hint  INTEGER NOT NULL DEFAULT 0,
	created_at     DATETIME NOT NULL,
	updated_at     DATETIME NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_status
	ON webhook_deliveries(status);
CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_aggregate
	ON webhook_deliveries(aggregate_key, sequence_hint);
`

// applyWebhookSchema creates the webhook_deliveries table if absent.
// Called from New() so it runs at startup.
func applyWebhookSchema(db interface{ Exec(string, ...any) (sql.Result, error) }) error {
	_, err := db.Exec(webhookSchema)
	return err
}

// CreateDelivery inserts a pending delivery record. The sequence_hint is
// assigned as MAX(sequence_hint)+1 for the same aggregate_key, atomically
// inside SQLite's single-writer serialization.
func (s *SQLite) CreateDelivery(ctx context.Context, d *webhooks.DeliveryRecord) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	row := s.db.QueryRowContext(ctx, `
		INSERT INTO webhook_deliveries
			(delivery_id, event_id, event_type, target_url, aggregate_key,
			 payload_json, signature, status, attempt_count, last_error,
			 sequence_hint, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', 0, '',
			(SELECT COALESCE(MAX(sequence_hint), 0) + 1
			 FROM webhook_deliveries WHERE aggregate_key = ?),
			?, ?)
		RETURNING sequence_hint`,
		d.DeliveryID, d.EventID, string(d.EventType), d.TargetURL,
		d.AggregateKey, d.PayloadJSON, d.Signature,
		d.AggregateKey, // for the subquery
		now, now,
	)
	return row.Scan(&d.SequenceHint)
}

// UpdateDelivery transitions the status and records attempt results.
func (s *SQLite) UpdateDelivery(ctx context.Context, deliveryID string, status webhooks.DeliveryStatus, attemptCount int, lastError string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(ctx, `
		UPDATE webhook_deliveries
		SET status = ?, attempt_count = ?, last_error = ?, updated_at = ?
		WHERE delivery_id = ?`,
		string(status), attemptCount, lastError, now, deliveryID,
	)
	return err
}

// GetDelivery retrieves a single record by delivery_id.
func (s *SQLite) GetDelivery(ctx context.Context, deliveryID string) (*webhooks.DeliveryRecord, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT delivery_id, event_id, event_type, target_url, aggregate_key,
		       payload_json, signature, status, attempt_count, last_error,
		       sequence_hint, created_at, updated_at
		FROM webhook_deliveries WHERE delivery_id = ?`, deliveryID)
	return scanDelivery(row)
}

// ListDeadLetters returns all dead_letter records ordered by created_at ASC.
func (s *SQLite) ListDeadLetters(ctx context.Context) ([]*webhooks.DeliveryRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT delivery_id, event_id, event_type, target_url, aggregate_key,
		       payload_json, signature, status, attempt_count, last_error,
		       sequence_hint, created_at, updated_at
		FROM webhook_deliveries WHERE status = 'dead_letter'
		ORDER BY created_at ASC`)
	if err != nil {
		return nil, fmt.Errorf("list dead letters: %w", err)
	}
	defer rows.Close()
	var out []*webhooks.DeliveryRecord
	for rows.Next() {
		d, err := scanDelivery(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func scanDelivery(s scanner) (*webhooks.DeliveryRecord, error) {
	var d webhooks.DeliveryRecord
	var createdStr, updatedStr, status, eventType string
	err := s.Scan(
		&d.DeliveryID, &d.EventID, &eventType, &d.TargetURL, &d.AggregateKey,
		&d.PayloadJSON, &d.Signature, &status, &d.AttemptCount, &d.LastError,
		&d.SequenceHint, &createdStr, &updatedStr,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, webhooks.ErrDeliveryNotFound
		}
		return nil, fmt.Errorf("scan delivery: %w", err)
	}
	d.EventType = webhooks.EventType(eventType)
	d.Status = webhooks.DeliveryStatus(status)
	d.CreatedAt, _ = time.Parse(time.RFC3339Nano, createdStr)
	d.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updatedStr)
	return &d, nil
}
