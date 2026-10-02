package dht

import (
	"context"
	"testing"
	"time"
)

// startService starts a service on loopback and, if bootstrap is given,
// joins through it synchronously.
func startService(t *testing.T, bootstrap ...string) *Service {
	t.Helper()
	s := NewService(newTestIdentity(t), "127.0.0.1:0", "", nil)
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(s.Stop)
	if len(bootstrap) > 0 {
		s.bootstrap = bootstrap
		s.bootstrapOnce()
	}
	return s
}

// TestLookup_FindsProvidersAcrossMultipleHops builds a chain of nodes in
// which each node only bootstraps from its predecessor, so the downloader
// at one end starts out knowing a single node. Finding the provider at the
// other end requires iterative lookups; the old one-hop FindProviders,
// which only asked already-connected peers, could not.
func TestLookup_FindsProvidersAcrossMultipleHops(t *testing.T) {
	const n = 10
	nodes := []*Service{startService(t)}
	for i := 1; i < n; i++ {
		nodes = append(nodes, startService(t, nodes[i-1].Self().Endpoint()))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	seeder, downloader := nodes[n-1], startService(t, nodes[0].Self().Endpoint())
	const key = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

	if got := seeder.AnnounceProvider(ctx, key); got == 0 {
		t.Fatal("announcement reached no nodes")
	}

	providers := downloader.FindProviders(ctx, key)
	if len(providers) != 1 || providers[0].ID != seeder.Self().ID {
		t.Fatalf("FindProviders = %+v, want only the seeder %s", providers, seeder.Self().ID)
	}
	if providers[0].Endpoint() != seeder.Self().Endpoint() {
		t.Fatalf("provider address = %s, want %s", providers[0].Endpoint(), seeder.Self().Endpoint())
	}

	// The returned address is dialable and the provider's ID is pinned.
	conn, err := downloader.Connect(ctx, providers[0])
	if err != nil {
		t.Fatalf("Connect to provider: %v", err)
	}
	if conn.PeerID() != seeder.Self().ID {
		t.Fatalf("connected to %s, want %s", conn.PeerID(), seeder.Self().ID)
	}
}

func TestLookup_ReturnsClosestNodes(t *testing.T) {
	boot := startService(t)
	var all []*Service
	for i := 0; i < 8; i++ {
		all = append(all, startService(t, boot.Self().Endpoint()))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	target := all[3].Self().ID
	closest, _ := all[0].lookup(ctx, target, false, 0)
	if len(closest) == 0 || closest[0].ID != target {
		t.Fatalf("lookup(%s) closest = %+v, want the target node first", target, closest)
	}
}
