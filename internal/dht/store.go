package dht

import (
	"sync"
	"time"
)

//store maintains provider records mapping chunk hashes to provider Node IDs.
//It is safe for concurrent use, since it is read and written from
//per-connection RPC handler goroutines.

const (
	// maxProvidersPerKey bounds how many provider records are kept for a
	// single key, mirroring the Kademlia bucket size K. Without this, an
	// attacker can grow a single key's provider list without bound by
	// repeatedly announcing new provider IDs for it.
	maxProvidersPerKey = K

	// maxKeys bounds the number of distinct keys tracked. Without this, an
	// attacker announcing many distinct bogus keys can grow the store's
	// memory usage without bound (provider announcements are unauthenticated).
	maxKeys = 100_000
)

// providerTTL is how long a provider record remains valid without being
// re-announced. Expired records are pruned lazily on the next Add/Get for
// that key, so a peer that goes offline eventually stops being handed out
// instead of lingering forever. Var (not const) so tests can shrink it.
var providerTTL = 30 * time.Minute

type providerRecord struct {
	id        string
	expiresAt time.Time
}

type ProviderStore struct {
	mu   sync.Mutex
	data map[string][]providerRecord
}

func NewStore() *ProviderStore {
	return &ProviderStore{data: make(map[string][]providerRecord)}
}

func (s *ProviderStore) Add(chunk string, providerID string) {
	if chunk == "" || providerID == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	_, existed := s.data[chunk]
	providers := pruneExpired(s.data[chunk], now)

	for i, existing := range providers {
		if existing.id == providerID {
			providers[i].expiresAt = now.Add(providerTTL)
			s.data[chunk] = providers
			return
		}
	}

	if !existed && len(s.data) >= maxKeys {
		// At capacity for distinct keys; refuse to track a new one.
		return
	}

	if len(providers) >= maxProvidersPerKey {
		// Evict the oldest record to make room for the new one.
		providers = providers[1:]
	}

	s.data[chunk] = append(providers, providerRecord{id: providerID, expiresAt: now.Add(providerTTL)})
}

func (s *ProviderStore) Get(chunk string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	providers := pruneExpired(s.data[chunk], time.Now())
	if len(providers) == 0 {
		delete(s.data, chunk)
		return nil
	}
	s.data[chunk] = providers

	result := make([]string, len(providers))
	for i, p := range providers {
		result[i] = p.id
	}
	return result
}

func (s *ProviderStore) Remove(chunk string, providerID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	providers := s.data[chunk]
	filtered := providers[:0]
	for _, existing := range providers {
		if existing.id != providerID {
			filtered = append(filtered, existing)
		}
	}

	if len(filtered) == 0 {
		delete(s.data, chunk)
		return
	}

	s.data[chunk] = filtered
}

// pruneExpired filters out expired records in place, reusing providers'
// backing array. Callers must hold s.mu.
func pruneExpired(providers []providerRecord, now time.Time) []providerRecord {
	filtered := providers[:0]
	for _, p := range providers {
		if now.Before(p.expiresAt) {
			filtered = append(filtered, p)
		}
	}
	return filtered
}
