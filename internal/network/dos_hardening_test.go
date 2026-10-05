package network

import (
	"context"
	"net"
	"testing"
	"time"
)

// TestConnection_IdleReadTimeoutDropsConnection reproduces a slow-loris peer:
// it connects but never sends a single byte. Before ReadIdleTimeout existed,
// the read loop blocked forever on ReadFrame, leaking a goroutine and file
// descriptor for the life of the process.
func TestConnection_IdleReadTimeoutDropsConnection(t *testing.T) {
	server, _ := newTestServer(t)
	server.SetReadIdleTimeout(100 * time.Millisecond)

	closed := make(chan struct{})
	server.OnNewConnection = func(conn *Connection) {
		conn.SetOnClose(func() { close(closed) })
		conn.Start()
	}

	if err := server.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer server.Stop()

	// Complete the handshake, then go silent.
	dialRawTLS(t, server.Addr().String())

	select {
	case <-closed:
		// expected: server dropped the idle connection.
	case <-time.After(2 * time.Second):
		t.Fatal("server did not drop idle connection within timeout")
	}
}

// TestServer_MaxConnectionsRejectsExcessConnections reproduces unbounded
// inbound connection growth: before the cap existed, a peer could open as
// many connections as it wanted, each holding a goroutine and fd open.
func TestServer_MaxConnectionsRejectsExcessConnections(t *testing.T) {
	server, _ := newTestServer(t)
	server.SetMaxConnections(2)

	held := make(chan struct{})
	server.OnNewConnection = func(conn *Connection) {
		conn.Start()
		<-held // keep accepted connections open for the duration of the test
	}

	if err := server.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer func() {
		close(held)
		server.Stop()
	}()

	addr := server.listener.Addr().String()

	var conns []net.Conn
	for i := 0; i < 2; i++ {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		conns = append(conns, c)
	}
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()

	// Give the server time to register the first two connections.
	deadline := time.Now().Add(1 * time.Second)
	for server.ActiveConnections() < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := server.ActiveConnections(); got != 2 {
		t.Fatalf("ActiveConnections() = %d, want 2 before over-cap dial", got)
	}

	// This third connection should be accepted at the TCP level and then
	// immediately closed by the server for being over the cap.
	third, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial third: %v", err)
	}
	defer third.Close()

	third.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	if _, err := third.Read(buf); err == nil {
		t.Fatal("expected the over-cap connection to be closed by the server")
	}
}

func TestServer_StopClosesConnectionsAndIsIdempotent(t *testing.T) {
	server, _ := newTestServer(t)
	server.OnNewConnection = func(conn *Connection) { conn.Start() }
	if err := server.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}

	raw, err := net.Dial("tcp", server.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer raw.Close()

	deadline := time.Now().Add(time.Second)
	for server.ActiveConnections() < 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	server.Stop()
	server.Stop() // must not panic

	raw.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := raw.Read(make([]byte, 1)); err == nil {
		t.Fatal("accepted connection still open after Stop")
	}
}

// TestServer_StalledHandshakeIsDropped reproduces a peer that opens a TCP
// connection and never starts the TLS handshake.
func TestServer_StalledHandshakeIsDropped(t *testing.T) {
	server, _ := newTestServer(t)
	server.SetHandshakeTimeout(100 * time.Millisecond)
	server.OnNewConnection = func(conn *Connection) { conn.Start() }
	if err := server.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer server.Stop()

	raw, err := net.Dial("tcp", server.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer raw.Close()

	raw.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := raw.Read(make([]byte, 1)); err == nil {
		t.Fatal("server kept a connection that never handshook")
	}
	deadline := time.Now().Add(time.Second)
	for server.ActiveConnections() != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := server.ActiveConnections(); n != 0 {
		t.Fatalf("ActiveConnections() = %d after stalled handshake was dropped", n)
	}
}

func TestServer_PeerIDIsAuthenticated(t *testing.T) {
	server, serverID := newTestServer(t)
	seen := make(chan string, 1)
	server.OnNewConnection = func(conn *Connection) {
		seen <- conn.PeerID()
		conn.Start()
	}
	if err := server.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer server.Stop()

	client := newIdentity(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Pinning the wrong ID fails.
	if _, err := Dial(ctx, server.Addr().String(), client.ClientTLSConfig(client.ID())); err == nil {
		t.Fatal("Dial succeeded with a mismatched expected peer ID")
	}

	conn, err := Dial(ctx, server.Addr().String(), client.ClientTLSConfig(serverID.ID()))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()
	if conn.PeerID() != serverID.ID() {
		t.Fatalf("client sees peer %s, want %s", conn.PeerID(), serverID.ID())
	}
	select {
	case got := <-seen:
		if got != client.ID() {
			t.Fatalf("server sees peer %s, want %s", got, client.ID())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server never accepted the connection")
	}
}
