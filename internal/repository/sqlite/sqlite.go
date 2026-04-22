package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/danharper32/artifact-registry-service/internal/domain"
	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS artifacts (
	id           TEXT PRIMARY KEY,
	type         TEXT NOT NULL,
	name         TEXT NOT NULL,
	version      TEXT NOT NULL,
	sha256       TEXT NOT NULL,
	size_bytes   INTEGER NOT NULL,
	content_type TEXT NOT NULL,
	metadata     TEXT NOT NULL DEFAULT '{}',
	storage_key  TEXT NOT NULL,
	created_at   DATETIME NOT NULL,
	UNIQUE(type, name, version)
);

CREATE TABLE IF NOT EXISTS channels (
	channel          TEXT NOT NULL,
	type             TEXT NOT NULL,
	name             TEXT NOT NULL,
	version          TEXT NOT NULL,
	artifact_id      TEXT NOT NULL,
	previous_version TEXT NOT NULL DEFAULT '',
	promoted_at      DATETIME NOT NULL,
	promoted_by      TEXT NOT NULL DEFAULT '',
	PRIMARY KEY (channel, type, name)
);

CREATE TABLE IF NOT EXISTS channel_history (
	id               INTEGER PRIMARY KEY AUTOINCREMENT,
	channel          TEXT NOT NULL,
	type             TEXT NOT NULL,
	name             TEXT NOT NULL,
	version          TEXT NOT NULL,
	artifact_id      TEXT NOT NULL,
	previous_version TEXT NOT NULL DEFAULT '',
	promoted_at      DATETIME NOT NULL,
	promoted_by      TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_artifacts_type_name
	ON artifacts(type, name);
CREATE INDEX IF NOT EXISTS idx_channel_history_lookup
	ON channel_history(channel, type, name, promoted_at);
`

type SQLite struct {
	db *sql.DB
}

func New(path string) (*SQLite, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1) // SQLite is single-writer
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	if err := applyWebhookSchema(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("apply webhook schema: %w", err)
	}
	return &SQLite{db: db}, nil
}

func (s *SQLite) Close() error { return s.db.Close() }

// ── Artifacts ────────────────────────────────────────────────────────────────

func (s *SQLite) SaveArtifact(ctx context.Context, a *domain.Artifact) error {
	meta, err := json.Marshal(a.Metadata)
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO artifacts (id, type, name, version, sha256, size_bytes, content_type, metadata, storage_key, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		a.ID, a.Type, a.Name, a.Version, a.SHA256,
		a.SizeBytes, a.ContentType, string(meta), a.StorageKey,
		a.CreatedAt.UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		if isUniqueViolation(err) {
			return domain.ErrAlreadyExists
		}
		return fmt.Errorf("insert artifact: %w", err)
	}
	return nil
}

func (s *SQLite) GetArtifact(ctx context.Context, artifactType, name, version string) (*domain.Artifact, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, type, name, version, sha256, size_bytes, content_type, metadata, storage_key, created_at
		FROM artifacts WHERE type = ? AND name = ? AND version = ?`,
		artifactType, name, version,
	)
	return scanArtifact(row)
}

func (s *SQLite) ListVersions(ctx context.Context, artifactType, name string) ([]*domain.Artifact, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, type, name, version, sha256, size_bytes, content_type, metadata, storage_key, created_at
		FROM artifacts WHERE type = ? AND name = ? ORDER BY created_at DESC`,
		artifactType, name,
	)
	if err != nil {
		return nil, fmt.Errorf("list versions: %w", err)
	}
	defer rows.Close()
	var out []*domain.Artifact
	for rows.Next() {
		a, err := scanArtifact(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// ── Channels ─────────────────────────────────────────────────────────────────

func (s *SQLite) SaveChannel(ctx context.Context, ch *domain.ChannelPointer) error {
	now := ch.PromotedAt.UTC().Format(time.RFC3339Nano)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Upsert current pointer
	_, err = tx.ExecContext(ctx, `
		INSERT INTO channels (channel, type, name, version, artifact_id, previous_version, promoted_at, promoted_by)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(channel, type, name) DO UPDATE SET
			version=excluded.version, artifact_id=excluded.artifact_id,
			previous_version=excluded.previous_version, promoted_at=excluded.promoted_at,
			promoted_by=excluded.promoted_by`,
		ch.Channel, ch.Type, ch.Name, ch.Version, ch.ArtifactID,
		ch.PreviousVersion, now, ch.PromotedBy,
	)
	if err != nil {
		return fmt.Errorf("upsert channel: %w", err)
	}

	// Append to history
	_, err = tx.ExecContext(ctx, `
		INSERT INTO channel_history (channel, type, name, version, artifact_id, previous_version, promoted_at, promoted_by)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		ch.Channel, ch.Type, ch.Name, ch.Version, ch.ArtifactID,
		ch.PreviousVersion, now, ch.PromotedBy,
	)
	if err != nil {
		return fmt.Errorf("insert channel history: %w", err)
	}
	return tx.Commit()
}

func (s *SQLite) GetChannel(ctx context.Context, channel, artifactType, name string) (*domain.ChannelPointer, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT channel, type, name, version, artifact_id, previous_version, promoted_at, promoted_by
		FROM channels WHERE channel = ? AND type = ? AND name = ?`,
		channel, artifactType, name,
	)
	return scanChannel(row)
}

func (s *SQLite) ListChannelHistory(ctx context.Context, channel, artifactType, name string) ([]*domain.ChannelPointer, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT channel, type, name, version, artifact_id, previous_version, promoted_at, promoted_by
		FROM channel_history WHERE channel = ? AND type = ? AND name = ?
		ORDER BY promoted_at DESC`,
		channel, artifactType, name,
	)
	if err != nil {
		return nil, fmt.Errorf("list history: %w", err)
	}
	defer rows.Close()
	var out []*domain.ChannelPointer
	for rows.Next() {
		ch, err := scanChannel(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, ch)
	}
	return out, rows.Err()
}

// ── Scan helpers ─────────────────────────────────────────────────────────────

type scanner interface {
	Scan(dest ...any) error
}

func scanArtifact(s scanner) (*domain.Artifact, error) {
	var a domain.Artifact
	var metaJSON, createdStr string
	err := s.Scan(&a.ID, &a.Type, &a.Name, &a.Version, &a.SHA256,
		&a.SizeBytes, &a.ContentType, &metaJSON, &a.StorageKey, &createdStr)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, domain.ErrNotFound
		}
		return nil, fmt.Errorf("scan artifact: %w", err)
	}
	if err := json.Unmarshal([]byte(metaJSON), &a.Metadata); err != nil {
		a.Metadata = map[string]string{}
	}
	a.CreatedAt, _ = time.Parse(time.RFC3339Nano, createdStr)
	return &a, nil
}

func scanChannel(s scanner) (*domain.ChannelPointer, error) {
	var ch domain.ChannelPointer
	var promotedStr string
	err := s.Scan(&ch.Channel, &ch.Type, &ch.Name, &ch.Version,
		&ch.ArtifactID, &ch.PreviousVersion, &promotedStr, &ch.PromotedBy)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, domain.ErrNotFound
		}
		return nil, fmt.Errorf("scan channel: %w", err)
	}
	ch.PromotedAt, _ = time.Parse(time.RFC3339Nano, promotedStr)
	return &ch, nil
}

func isUniqueViolation(err error) bool {
	return err != nil && (contains(err.Error(), "UNIQUE constraint failed") ||
		contains(err.Error(), "unique constraint"))
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && containsStr(s, sub))
}

func containsStr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
