package network

import (
	"net"
	"testing"
	"time"
)

// TestConnection_IdleReadTimeoutDropsConnection reproduces a slow-loris peer:
// it connects but never sends a single byte. Before ReadIdleTimeout existed,
// the read loop blocked forever on ReadFrame, leaking a goroutine and file
// descriptor for the life of the process.
func TestConnection_IdleReadTimeoutDropsConnection(t *testing.T) {
	origTimeout := ReadIdleTimeout
	ReadIdleTimeout = 100 * time.Millisecond
	defer func() { ReadIdleTimeout = origTimeout }()

	server := NewServer("127.0.0.1:0")

	closed := make(chan struct{})
	server.OnNewConnection = func(conn *Connection) {
		conn.SetOnClose(func() { close(closed) })
		conn.Start()
	}

	if err := server.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer server.Stop()

	rawConn, err := net.Dial("tcp", server.listener.Addr().String())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer rawConn.Close()

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
	server := NewServer("127.0.0.1:0")
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
