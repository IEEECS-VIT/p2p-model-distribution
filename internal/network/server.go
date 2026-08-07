package network

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
)

// DefaultMaxConnections caps how many inbound peer connections a Server will
// accept concurrently. Without a cap, a single peer (or many) opening
// connections in a loop can exhaust file descriptors/memory. Connections
// beyond the cap are accepted and immediately closed rather than left
// queued, so the accept loop never blocks.
const DefaultMaxConnections = 512

// Server handles listening for incoming TCP connections from other peers.
type Server struct {
	listenAddr string
	listener   net.Listener

	// OnNewConnection is a callback fired whenever a new peer connects to us.
	// This separates the low-level accept loop from the high-level P2P logic.
	OnNewConnection func(conn *Connection)

	// quit channel is used to signal the accept loop to shut down cleanly.
	quit chan struct{}
	// wg ensures we wait for all internal goroutines to finish before exiting.
	wg sync.WaitGroup

	// maxConns is the concurrent inbound connection cap.
	maxConns atomic.Int32
	// activeConns tracks how many accepted connections are currently open.
	activeConns atomic.Int32
}

// NewServer creates a new P2P TCP server.
func NewServer(listenAddr string) *Server {
	s := &Server{
		listenAddr: listenAddr,
		quit:       make(chan struct{}),
	}
	s.maxConns.Store(DefaultMaxConnections)
	return s
}

// SetMaxConnections overrides the concurrent inbound connection cap. Must be
// called before Start.
func (s *Server) SetMaxConnections(n int) {
	s.maxConns.Store(int32(n))
}

// Start opens the TCP port and begins accepting connections.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.listenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.listenAddr, err)
	}
	s.listener = ln

	s.wg.Add(1)
	go s.acceptLoop()

	return nil
}

// acceptLoop runs continuously until the server is stopped.
func (s *Server) acceptLoop() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			// Check if the error is because we intentionally closed the listener
			select {
			case <-s.quit:
				return // Graceful shutdown
			default:
				// In a real system, we'd log this via a structured logger (e.g. zap)
				fmt.Printf("[Server] Accept error: %v\n", err)
				continue
			}
		}

		if s.activeConns.Add(1) > s.maxConns.Load() {
			// Over the concurrent connection cap: reject immediately
			// instead of letting an unbounded number of peers hold a
			// goroutine and file descriptor open.
			s.activeConns.Add(-1)
			conn.Close()
			continue
		}

		// Wrap the raw net.Conn.
		// At this raw TCP stage, we don't know the cryptographic PeerID yet,
		// so we temporarily use the remote IP address.
		wrappedConn := NewConnection(conn, conn.RemoteAddr().String())
		wrappedConn.SetOnClose(func() {
			s.activeConns.Add(-1)
		})

		if s.OnNewConnection != nil {
			// Spawn a new goroutine to handle this connection.
			// If we didn't do this, a slow handshake with one peer would
			// block us from accepting connections from other peers.
			go s.OnNewConnection(wrappedConn)
		} else {
			// If no one is listening for connections, close it to avoid leaks.
			// The read loop never started, so onClose won't fire; account
			// for the slot here instead.
			wrappedConn.Close()
			s.activeConns.Add(-1)
		}
	}
}

// ActiveConnections returns the current number of accepted, open connections.
func (s *Server) ActiveConnections() int {
	return int(s.activeConns.Load())
}

// Addr returns the network address that the server listener is bound to.
func (s *Server) Addr() net.Addr {
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// Stop gracefully shuts down the listener and waits for the accept loop to exit.
func (s *Server) Stop() {
	close(s.quit)
	if s.listener != nil {
		s.listener.Close()
	}
	s.wg.Wait()
}

