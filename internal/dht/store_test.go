package dht

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestProviderStoreConcurrentAccess reproduces concurrent STORE/FIND_VALUE
// traffic against a single ProviderStore. Before the mutex was added this
// crashed the process with "fatal error: concurrent map read and map
// write" under `go test -race`, or nondeterministically in production.
func TestProviderStoreConcurrentAccess(t *testing.T) {
	store := NewStore()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(3)

		go func(i int) {
			defer wg.Done()
			store.Add(fmt.Sprintf("chunk-%d", i%5), fmt.Sprintf("peer-%d", i))
		}(i)

		go func(i int) {
			defer wg.Done()
			_ = store.Get(fmt.Sprintf("chunk-%d", i%5))
		}(i)

		go func(i int) {
			defer wg.Done()
			store.Remove(fmt.Sprintf("chunk-%d", i%5), fmt.Sprintf("peer-%d", i))
		}(i)
	}
	wg.Wait()
}

// TestProviderStoreCapsProvidersPerKey reproduces unauthenticated provider
// announcements piling up without bound for a single popular key. Before
// maxProvidersPerKey existed, an attacker announcing endless bogus provider
// IDs for one key could grow that key's list forever.
func TestProviderStoreCapsProvidersPerKey(t *testing.T) {
	store := NewStore()

	for i := 0; i < maxProvidersPerKey*3; i++ {
		store.Add("popular-chunk", fmt.Sprintf("peer-%d", i))
	}

	got := len(store.Get("popular-chunk"))
	if got != maxProvidersPerKey {
		t.Fatalf("len(Get(...)) = %d, want %d (maxProvidersPerKey)", got, maxProvidersPerKey)
	}
}

// TestProviderStoreExpiresStaleProviders reproduces a provider that
// announced once and then went offline: without a TTL, that dead peer would
// be handed out to downloaders forever.
func TestProviderStoreExpiresStaleProviders(t *testing.T) {
	origTTL := providerTTL
	providerTTL = 20 * time.Millisecond
	defer func() { providerTTL = origTTL }()

	store := NewStore()
	store.Add("chunk", "stale-peer")

	if got := store.Get("chunk"); len(got) != 1 {
		t.Fatalf("Get(...) = %v, want 1 fresh provider", got)
	}

	time.Sleep(50 * time.Millisecond)

	if got := store.Get("chunk"); len(got) != 0 {
		t.Fatalf("Get(...) = %v, want expired provider to be pruned", got)
	}
}
