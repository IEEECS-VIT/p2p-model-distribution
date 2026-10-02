package dht

import (
	"sync"
	"time"
)

const (
	// maxProvidersPerKey bounds how many provider records are kept for a
	// single key, mirroring the Kademlia bucket size K. When a key is full
	// new providers are refused rather than evicting existing ones, so a
	// flood of announcements cannot push out providers that keep
	// re-announcing.
	maxProvidersPerKey = K

	// maxProvidersPerPrefixPerKey bounds how many of a key's providers may
	// share one public /24 (or /48), so one network cannot take every slot.
	maxProvidersPerPrefixPerKey = 2

	// maxKeysPerProvider bounds how many keys a single provider identity
	// can hold records for, so one peer cannot fill the store.
	maxKeysPerProvider = 1024

	// maxKeys bounds the number of distinct keys tracked overall.
	maxKeys = 100_000
)

// ProviderTTL is how long a provider record remains valid without being
// re-announced. Expired records are pruned lazily on access and by Sweep,
// so a peer that goes offline eventually stops being handed out instead of
// lingering forever.
const ProviderTTL = 30 * time.Minute

type providerRecord struct {
	node      Node
	expiresAt time.Time
}

// ProviderStore maintains provider records mapping keys (file IDs) to the
// nodes that serve them, including the address each provider can be
// dialed at. It is safe for concurrent use, since it is read and written
// from per-connection RPC handler goroutines.
type ProviderStore struct {
	mu   sync.Mutex
	ttl  time.Duration
	data map[string][]providerRecord
	// keysByProvider counts records per provider ID (for
	// maxKeysPerProvider). Records in keys not touched since they expired
	// are still counted until the next Sweep.
	keysByProvider map[string]int
}

func NewStore() *ProviderStore {
	return newStoreWithTTL(ProviderTTL)
}

func newStoreWithTTL(ttl time.Duration) *ProviderStore {
	return &ProviderStore{
		ttl:            ttl,
		data:           make(map[string][]providerRecord),
		keysByProvider: make(map[string]int),
	}
}

// Add records provider as serving key, refreshing its expiry (and address)
// if it is already recorded. It reports whether the record was stored.
func (s *ProviderStore) Add(key string, provider Node) bool {
	if key == "" || provider.ID == "" {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	providers, existed := s.pruneLocked(key, now)

	for i, existing := range providers {
		if existing.node.ID == provider.ID {
			providers[i] = providerRecord{node: provider, expiresAt: now.Add(s.ttl)}
			return true
		}
	}

	switch {
	case !existed && len(s.data) >= maxKeys:
		return false
	case len(providers) >= maxProvidersPerKey:
		return false
	case s.keysByProvider[provider.ID] >= maxKeysPerProvider:
		return false
	}
	if prefix, limited := networkPrefix(provider.IP); limited {
		same := 0
		for _, p := range providers {
			if pp, ok := networkPrefix(p.node.IP); ok && pp == prefix {
				same++
			}
		}
		if same >= maxProvidersPerPrefixPerKey {
			return false
		}
	}

	s.data[key] = append(providers, providerRecord{node: provider, expiresAt: now.Add(s.ttl)})
	s.keysByProvider[provider.ID]++
	return true
}

// Get returns the unexpired providers recorded for key.
func (s *ProviderStore) Get(key string) []Node {
	s.mu.Lock()
	defer s.mu.Unlock()

	providers, _ := s.pruneLocked(key, time.Now())
	if len(providers) == 0 {
		return nil
	}

	result := make([]Node, len(providers))
	for i, p := range providers {
		result[i] = p.node
	}
	return result
}

// Remove deletes provider's record for key.
func (s *ProviderStore) Remove(key string, providerID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	providers := s.data[key]
	filtered := providers[:0]
	for _, existing := range providers {
		if existing.node.ID == providerID {
			s.releaseLocked(providerID)
			continue
		}
		filtered = append(filtered, existing)
	}
	s.setLocked(key, filtered)
}

// Sweep prunes expired records across all keys.
func (s *ProviderStore) Sweep() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for key := range s.data {
		s.pruneLocked(key, now)
	}
}

// pruneLocked drops expired records for key (in place) and returns the
// remaining ones and whether the key existed. Callers must hold s.mu.
func (s *ProviderStore) pruneLocked(key string, now time.Time) ([]providerRecord, bool) {
	providers, existed := s.data[key]
	filtered := providers[:0]
	for _, p := range providers {
		if now.Before(p.expiresAt) {
			filtered = append(filtered, p)
		} else {
			s.releaseLocked(p.node.ID)
		}
	}
	s.setLocked(key, filtered)
	return filtered, existed
}

func (s *ProviderStore) setLocked(key string, providers []providerRecord) {
	if len(providers) == 0 {
		delete(s.data, key)
		return
	}
	s.data[key] = providers
}

func (s *ProviderStore) releaseLocked(providerID string) {
	if s.keysByProvider[providerID] <= 1 {
		delete(s.keysByProvider, providerID)
	} else {
		s.keysByProvider[providerID]--
	}
}
