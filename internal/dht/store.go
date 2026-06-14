package dht

//store maintains provider records mapping chunk hashes to provider Node IDs.

type ProviderStore map[string][]string

func NewStore() ProviderStore {
	return make(ProviderStore)
}

func (s ProviderStore) Add(chunk string, providerID string) {
	if chunk == "" || providerID == "" {
		return
	}

	providers := s[chunk]
	for _, existing := range providers {
		if existing == providerID {
			return
		}
	}

	s[chunk] = append(providers, providerID)
}

func (s ProviderStore) Get(chunk string) []string {
	providers := s[chunk]
	result := make([]string, len(providers))
	copy(result, providers)
	return result
}

func (s ProviderStore) Remove(chunk string, providerID string) {
	providers := s[chunk]
	filtered := providers[:0]
	for _, existing := range providers {
		if existing != providerID {
			filtered = append(filtered, existing)
		}
	}

	if len(filtered) == 0 {
		delete(s, chunk)
		return
	}

	s[chunk] = filtered
}
