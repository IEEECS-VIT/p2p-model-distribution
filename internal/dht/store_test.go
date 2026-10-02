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
			store.Add(fmt.Sprintf("chunk-%d", i%5), Node{ID: fmt.Sprintf("peer-%d", i)})
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

// TestProviderStoreCapsProvidersPerKey reproduces provider announcements
// piling up without bound for a single popular key, and checks that a
// flood of new providers cannot evict the ones already recorded.
func TestProviderStoreCapsProvidersPerKey(t *testing.T) {
	store := NewStore()

	for i := 0; i < maxProvidersPerKey; i++ {
		store.Add("popular", Node{ID: fmt.Sprintf("honest-%d", i), IP: "10.0.0.1"})
	}
	for i := 0; i < maxProvidersPerKey*3; i++ {
		if store.Add("popular", Node{ID: fmt.Sprintf("flood-%d", i), IP: "10.0.0.2"}) {
			t.Fatal("full key accepted a new provider")
		}
	}

	got := store.Get("popular")
	if len(got) != maxProvidersPerKey {
		t.Fatalf("len(Get(...)) = %d, want %d (maxProvidersPerKey)", len(got), maxProvidersPerKey)
	}
	for _, p := range got {
		if p.ID[:6] != "honest" {
			t.Fatalf("existing provider was evicted by %s", p.ID)
		}
	}

	// Existing providers can still refresh.
	if !store.Add("popular", Node{ID: "honest-0", IP: "10.0.0.1"}) {
		t.Fatal("existing provider could not re-announce")
	}
}

func TestProviderStoreLimitsProvidersPerPrefix(t *testing.T) {
	store := NewStore()
	for i := 0; i < maxProvidersPerPrefixPerKey; i++ {
		if !store.Add("k", Node{ID: fmt.Sprintf("p%d", i), IP: fmt.Sprintf("198.51.100.%d", i+1)}) {
			t.Fatalf("provider %d rejected", i)
		}
	}
	if store.Add("k", Node{ID: "sybil", IP: "198.51.100.99"}) {
		t.Fatal("accepted another provider from a full /24")
	}
	if !store.Add("k", Node{ID: "other", IP: "192.0.2.1"}) {
		t.Fatal("rejected a provider from a different /24")
	}
}

func TestProviderStoreLimitsKeysPerProvider(t *testing.T) {
	store := NewStore()
	p := Node{ID: "greedy", IP: "10.0.0.1"}
	for i := 0; i < maxKeysPerProvider; i++ {
		if !store.Add(fmt.Sprintf("key-%d", i), p) {
			t.Fatalf("key %d rejected below the limit", i)
		}
	}
	if store.Add("one-too-many", p) {
		t.Fatal("provider exceeded maxKeysPerProvider")
	}
	store.Remove("key-0", "greedy")
	if !store.Add("one-too-many", p) {
		t.Fatal("freed slot was not reusable")
	}
}

func TestProviderStoreSweepReleasesExpiredRecords(t *testing.T) {
	store := newStoreWithTTL(10 * time.Millisecond)
	p := Node{ID: "p", IP: "10.0.0.1"}
	for i := 0; i < maxKeysPerProvider; i++ {
		store.Add(fmt.Sprintf("key-%d", i), p)
	}
	time.Sleep(20 * time.Millisecond)
	store.Sweep()
	if !store.Add("fresh", p) {
		t.Fatal("expired records still counted against the provider after Sweep")
	}
}

// TestProviderStoreExpiresStaleProviders reproduces a provider that
// announced once and then went offline: without a TTL, that dead peer would
// be handed out to downloaders forever.
func TestProviderStoreExpiresStaleProviders(t *testing.T) {
	store := newStoreWithTTL(20 * time.Millisecond)
	store.Add("chunk", Node{ID: "stale-peer"})

	if got := store.Get("chunk"); len(got) != 1 {
		t.Fatalf("Get(...) = %v, want 1 fresh provider", got)
	}

	time.Sleep(50 * time.Millisecond)

	if got := store.Get("chunk"); len(got) != 0 {
		t.Fatalf("Get(...) = %v, want expired provider to be pruned", got)
	}
}

func TestProviderStoreKeepsAddresses(t *testing.T) {
	store := NewStore()
	store.Add("file", Node{ID: "p1", IP: "10.0.0.1", Port: 9000})
	store.Add("file", Node{ID: "p1", IP: "10.0.0.2", Port: 9001}) // re-announce from a new address

	got := store.Get("file")
	if len(got) != 1 || got[0].Endpoint() != "10.0.0.2:9001" {
		t.Fatalf("Get = %+v, want one record at the latest address", got)
	}
}
