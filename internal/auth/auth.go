// Package auth provides API key validation with scope-based access control.
//
// Three scopes, additive:
//
//	read  — GET endpoints (list, resolve, download)
//	write — upload artifacts (implies read)
//	admin — promote, rollback (implies write + read)
//
// Keys are configured via environment variables and compared in constant time.
package auth

import (
	"crypto/subtle"
)

const (
	ScopeRead  = "read"
	ScopeWrite = "write"
	ScopeAdmin = "admin"
)

// KeyEntry pairs an API key with its granted scopes.
type KeyEntry struct {
	Key    string
	Scopes []string
}

// Store holds the active API keys and their scopes.
type Store struct {
	keys    []KeyEntry
	enabled bool
}

// NewStore creates a Store. When enabled=false every request passes (dev mode).
func NewStore(entries []KeyEntry, enabled bool) *Store {
	return &Store{keys: entries, enabled: enabled}
}

// Allowed returns true if the given raw key carries the required scope.
// Uses constant-time byte comparison to resist timing attacks.
func (s *Store) Allowed(rawKey, required string) bool {
	if !s.enabled {
		return true
	}
	if rawKey == "" {
		return false
	}
	bKey := []byte(rawKey)
	for _, entry := range s.keys {
		if subtle.ConstantTimeCompare(bKey, []byte(entry.Key)) != 1 {
			continue
		}
		for _, scope := range entry.Scopes {
			if scope == required || scope == ScopeAdmin {
				return true
			}
			if required == ScopeRead && (scope == ScopeWrite) {
				return true
			}
		}
		return false
	}
	return false
}

// Authenticated returns true if the key exists at all (any scope).
func (s *Store) Authenticated(rawKey string) bool {
	if !s.enabled {
		return true
	}
	bKey := []byte(rawKey)
	for _, entry := range s.keys {
		if subtle.ConstantTimeCompare(bKey, []byte(entry.Key)) == 1 {
			return true
		}
	}
	return false
}

// Enabled reports whether auth enforcement is active.
func (s *Store) Enabled() bool { return s.enabled }
