package network

import (
	"fmt"
	"net"
	"sync"
)

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
}

// NewServer creates a new P2P TCP server.
func NewServer(listenAddr string) *Server {
	return &Server{
		listenAddr: listenAddr,
		quit:       make(chan struct{}),
	}
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

		// Wrap the raw net.Conn. 
		// At this raw TCP stage, we don't know the cryptographic PeerID yet,
		// so we temporarily use the remote IP address.
		wrappedConn := NewConnection(conn, conn.RemoteAddr().String())

		if s.OnNewConnection != nil {
			// Spawn a new goroutine to handle this connection.
			// If we didn't do this, a slow handshake with one peer would
			// block us from accepting connections from other peers.
			go s.OnNewConnection(wrappedConn)
		} else {
			// If no one is listening for connections, close it to avoid leaks.
			wrappedConn.Close()
		}
	}
}

// Stop gracefully shuts down the listener and waits for the accept loop to exit.
func (s *Server) Stop() {
	close(s.quit)
	if s.listener != nil {
		s.listener.Close()
	}
	s.wg.Wait()
}
