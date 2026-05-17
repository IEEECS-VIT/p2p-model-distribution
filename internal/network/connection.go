package network

import (
	"net"
	"sync"
	"time"
)

// Connection wraps a raw net.Conn to provide thread-safe writes
// and manage the connection lifecycle.
type Connection struct {
	conn net.Conn
	
	// mu protects concurrent writes to the underlying socket.
	// TCP is a stream. If Goroutine A and Goroutine B call Write()
	// at the exact same time without a mutex, their bytes might
	// interleave (e.g., A_half, B_full, A_rest), permanently
	// corrupting the stream for the receiver.
	mu   sync.Mutex

	// peerID is the logical identifier of the remote node.
	// In a real P2P system, this is populated during a cryptographic
	// handshake. For now, it will default to the IP address.
	peerID string

	connectedAt time.Time
}

// NewConnection wraps an existing net.Conn.
func NewConnection(conn net.Conn, peerID string) *Connection {
	return &Connection{
		conn:        conn,
		peerID:      peerID,
		connectedAt: time.Now(),
	}
}

// Close gracefully terminates the TCP connection.
func (c *Connection) Close() error {
	return c.conn.Close()
}

// RemoteAddr returns the network address of the remote peer.
func (c *Connection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// PeerID returns the logical ID of the connected peer.
func (c *Connection) PeerID() string {
	return c.peerID
}
