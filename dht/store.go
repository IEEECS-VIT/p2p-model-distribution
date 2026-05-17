package dht

//store maintains provider records mapping chunk hashes to provider Node IDs.

type ProviderStore map[string][]string

func NewStore() ProviderStore {
	return make(ProviderStore)
}

func (s ProviderStore) Add(chunk string, providerID string) {
	s[chunk] = append(s[chunk], providerID)
}
