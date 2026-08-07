package dht

import "sync"

//store maintains provider records mapping chunk hashes to provider Node IDs.
//It is safe for concurrent use, since it is read and written from
//per-connection RPC handler goroutines.

type ProviderStore struct {
	mu   sync.RWMutex
	data map[string][]string
}

func NewStore() *ProviderStore {
	return &ProviderStore{data: make(map[string][]string)}
}

func (s *ProviderStore) Add(chunk string, providerID string) {
	if chunk == "" || providerID == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	providers := s.data[chunk]
	for _, existing := range providers {
		if existing == providerID {
			return
		}
	}

	s.data[chunk] = append(providers, providerID)
}

func (s *ProviderStore) Get(chunk string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	providers := s.data[chunk]
	result := make([]string, len(providers))
	copy(result, providers)
	return result
}

func (s *ProviderStore) Remove(chunk string, providerID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	providers := s.data[chunk]
	filtered := providers[:0]
	for _, existing := range providers {
		if existing != providerID {
			filtered = append(filtered, existing)
		}
	}

	if len(filtered) == 0 {
		delete(s.data, chunk)
		return
	}

	s.data[chunk] = filtered
}
