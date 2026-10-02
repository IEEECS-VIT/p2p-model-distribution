package network

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"github.com/IEEECS-VIT/p2p-model-distribution/internal/identity"
)

func newIdentity(t *testing.T) *identity.Identity {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	return id
}

// newTestServer returns an unstarted server with a fresh identity.
func newTestServer(t *testing.T) (*Server, *identity.Identity) {
	t.Helper()
	id := newIdentity(t)
	return NewServer("127.0.0.1:0", id.ServerTLSConfig()), id
}

// dialTest opens an authenticated client connection to addr.
func dialTest(t *testing.T, addr string) *Connection {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := Dial(ctx, addr, newIdentity(t).ClientTLSConfig(""))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// dialRawTLS opens an authenticated TLS socket without wrapping it in a
// Connection, so tests can write arbitrary frames.
func dialRawTLS(t *testing.T, addr string) *tls.Conn {
	t.Helper()
	raw, err := tls.Dial("tcp", addr, newIdentity(t).ClientTLSConfig(""))
	if err != nil {
		t.Fatalf("tls.Dial: %v", err)
	}
	t.Cleanup(func() { raw.Close() })
	return raw
}
