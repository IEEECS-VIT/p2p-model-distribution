package network

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IEEECS-VIT/p2p-model-distribution/internal/identity"
)

// DefaultMaxConnections caps how many inbound peer connections a Server will
// accept concurrently. Without a cap, a single peer (or many) opening
// connections in a loop can exhaust file descriptors/memory. Connections
// beyond the cap are accepted and immediately closed rather than left
// queued, so the accept loop never blocks.
const DefaultMaxConnections = 512

// HandshakeTimeout bounds the TLS handshake on both inbound and outbound
// connections, so a peer that connects and stalls cannot hold a slot.
// Var rather than const so tests can shrink it.
var HandshakeTimeout = 10 * time.Second

// Server accepts mutually authenticated TLS connections from other peers.
type Server struct {
	listenAddr string
	tlsConfig  *tls.Config
	listener   net.Listener

	// OnNewConnection is called (in its own goroutine) for every peer that
	// completes the TLS handshake. The Connection's PeerID is the peer's
	// authenticated node ID.
	OnNewConnection func(conn *Connection)

	// quit channel is used to signal the accept loop to shut down cleanly.
	quit chan struct{}
	// wg ensures we wait for all internal goroutines to finish before exiting.
	wg sync.WaitGroup

	// maxConns is the concurrent inbound connection cap.
	maxConns atomic.Int32
	// activeConns tracks how many accepted connections are currently open
	// (including those still handshaking).
	activeConns atomic.Int32

	// conns holds every accepted, still-open socket so Stop can close
	// them, including ones mid-handshake.
	connsMu sync.Mutex
	conns   map[net.Conn]struct{}

	stopOnce sync.Once
}

// NewServer creates a new P2P server. tlsConfig must require and verify
// client certificates (see identity.Identity.ServerTLSConfig).
func NewServer(listenAddr string, tlsConfig *tls.Config) *Server {
	s := &Server{
		listenAddr: listenAddr,
		tlsConfig:  tlsConfig,
		quit:       make(chan struct{}),
		conns:      make(map[net.Conn]struct{}),
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
	if s.tlsConfig == nil {
		return errors.New("server requires a TLS config")
	}
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

	var backoff time.Duration
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return // Graceful shutdown
			default:
			}
			// Back off on persistent errors (e.g. EMFILE) instead of
			// spinning, as net/http does.
			if backoff == 0 {
				backoff = 5 * time.Millisecond
			} else if backoff *= 2; backoff > time.Second {
				backoff = time.Second
			}
			slog.Warn("accept failed", "err", err, "retry_in", backoff)
			select {
			case <-time.After(backoff):
			case <-s.quit:
				return
			}
			continue
		}
		backoff = 0

		if s.activeConns.Add(1) > s.maxConns.Load() {
			// Over the concurrent connection cap: reject immediately
			// instead of letting an unbounded number of peers hold a
			// goroutine and file descriptor open.
			s.activeConns.Add(-1)
			conn.Close()
			continue
		}

		s.connsMu.Lock()
		s.conns[conn] = struct{}{}
		s.connsMu.Unlock()

		// The handshake runs off the accept loop so one slow peer cannot
		// block others from connecting.
		go s.handle(conn)
	}
}

func (s *Server) release(conn net.Conn) {
	s.connsMu.Lock()
	delete(s.conns, conn)
	s.connsMu.Unlock()
	s.activeConns.Add(-1)
}

func (s *Server) handle(raw net.Conn) {
	ctx, cancel := context.WithTimeout(context.Background(), HandshakeTimeout)
	defer cancel()

	tlsConn := tls.Server(raw, s.tlsConfig)
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		slog.Debug("inbound handshake failed", "remote", raw.RemoteAddr(), "err", err)
		raw.Close()
		s.release(raw)
		return
	}
	peerID, err := identity.PeerID(tlsConn.ConnectionState())
	if err != nil || s.OnNewConnection == nil {
		tlsConn.Close()
		s.release(raw)
		return
	}

	conn := NewConnection(tlsConn, peerID)
	conn.SetOnClose(func() { s.release(raw) })
	s.OnNewConnection(conn)
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

// Stop shuts down the listener, waits for the accept loop to exit, and
// closes every accepted connection. It is safe to call more than once.
func (s *Server) Stop() {
	s.stopOnce.Do(func() {
		close(s.quit)
		if s.listener != nil {
			s.listener.Close()
		}
		s.wg.Wait()

		s.connsMu.Lock()
		conns := make([]net.Conn, 0, len(s.conns))
		for c := range s.conns {
			conns = append(conns, c)
		}
		s.connsMu.Unlock()
		for _, c := range conns {
			_ = c.Close()
		}
	})
}

// Dial opens a mutually authenticated TLS connection to addr. The peer's
// identity is checked by tlsConfig (see identity.Identity.ClientTLSConfig,
// which can pin an expected node ID). The returned Connection is not yet
// started.
func Dial(ctx context.Context, addr string, tlsConfig *tls.Config) (*Connection, error) {
	ctx, cancel := context.WithTimeout(ctx, HandshakeTimeout)
	defer cancel()

	dialer := &tls.Dialer{
		NetDialer: &net.Dialer{KeepAlive: 30 * time.Second},
		Config:    tlsConfig,
	}
	c, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	tlsConn := c.(*tls.Conn)
	peerID, err := identity.PeerID(tlsConn.ConnectionState())
	if err != nil {
		tlsConn.Close()
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	return NewConnection(tlsConn, peerID), nil
}
