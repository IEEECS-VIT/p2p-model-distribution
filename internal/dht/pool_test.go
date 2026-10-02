package dht

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IEEECS-VIT/p2p-model-distribution/internal/identity"
	"github.com/IEEECS-VIT/p2p-model-distribution/internal/network"
)

func newTestIdentity(t *testing.T) *identity.Identity {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	return id
}

// startPeer starts a bare server and returns its identity, address and a
// counter of accepted connections.
func startPeer(t *testing.T) (*identity.Identity, string, *atomic.Int32) {
	t.Helper()
	id := newTestIdentity(t)
	srv := network.NewServer("127.0.0.1:0", id.ServerTLSConfig())
	var accepted atomic.Int32
	srv.OnNewConnection = func(conn *network.Connection) {
		accepted.Add(1)
		conn.Start()
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	t.Cleanup(srv.Stop)
	return id, srv.Addr().String(), &accepted
}

func nodeAt(t *testing.T, id *identity.Identity, addr string) Node {
	t.Helper()
	ip, portStr := splitHostPort(addr)
	port, _ := strconv.Atoi(portStr)
	return Node{ID: id.ID(), IP: ip, Port: port}
}

func TestPool_RemovesClosedConnections(t *testing.T) {
	peerID, addr, _ := startPeer(t)
	p := newPool(newTestIdentity(t), network.NewRouter())
	defer p.closeAll()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := p.connect(ctx, nodeAt(t, peerID, addr))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	conn.Close()
	<-conn.Done()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, ok := p.get(peerID.ID()); !ok {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, ok := p.get(peerID.ID()); ok {
		t.Fatal("closed connection is still in the pool")
	}

	// A fresh connection is dialed transparently.
	again, err := p.connect(ctx, nodeAt(t, peerID, addr))
	if err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	if again == conn {
		t.Fatal("pool returned the closed connection")
	}
}

func TestPool_CoalescesConcurrentDials(t *testing.T) {
	peerID, addr, accepted := startPeer(t)
	p := newPool(newTestIdentity(t), network.NewRouter())
	defer p.closeAll()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	conns := make([]*network.Connection, 20)
	for i := range conns {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, err := p.connect(ctx, nodeAt(t, peerID, addr))
			if err != nil {
				t.Errorf("connect %d: %v", i, err)
			}
			conns[i] = c
		}(i)
	}
	wg.Wait()

	for _, c := range conns[1:] {
		if c != conns[0] {
			t.Fatal("concurrent connects returned different connections")
		}
	}
	time.Sleep(50 * time.Millisecond)
	if n := accepted.Load(); n != 1 {
		t.Fatalf("peer accepted %d connections, want 1", n)
	}
}

func TestPool_PinsExpectedNodeID(t *testing.T) {
	_, addr, _ := startPeer(t)
	impersonated := newTestIdentity(t)
	p := newPool(newTestIdentity(t), network.NewRouter())
	defer p.closeAll()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := p.connect(ctx, nodeAt(t, impersonated, addr)); err == nil {
		t.Fatal("connected to a node that does not own the expected ID")
	}
	if _, ok := p.get(impersonated.ID()); ok {
		t.Fatal("impersonated ID was registered in the pool")
	}
}
