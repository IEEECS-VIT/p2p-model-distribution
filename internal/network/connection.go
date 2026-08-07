package network

import (
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/IEEECS-VIT/p2p-model-distribution/internal/protocol"
	"google.golang.org/protobuf/proto"
)

var (
	// ReadIdleTimeout bounds how long we wait for a single frame (header +
	// payload) to fully arrive, reset before every ReadFrame call. Without
	// it, a peer that opens a connection and trickles bytes indefinitely
	// (or never sends anything) ties up a goroutine and file descriptor
	// forever. Var rather than const so tests can shrink it.
	ReadIdleTimeout = 2 * time.Minute

	// WriteTimeout bounds a single write. Without it, a peer that never
	// drains its receive window can block a write forever while holding
	// the connection's write mutex, stalling every other pending
	// write/response on that connection.
	WriteTimeout = 15 * time.Second
)

// maxInFlightRoutes bounds how many Route() handler goroutines a single
// connection may have running concurrently. Without it, a peer that fires
// requests faster than we can answer them causes unbounded goroutine growth.
// Once the limit is hit, the read loop blocks acquiring a slot, which
// naturally applies backpressure to that peer instead of spawning more work.
const maxInFlightRoutes = 32

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

	// Incoming channel delivers successfully framed incoming messages to the application.
	Incoming chan []byte

	// pendingRequests holds channels waiting for specific response envelopes
	pendingRequests map[string]chan *protocol.Envelope

	// router holds the handler mapping for incoming RPC messages
	router *Router

	// routeSem bounds the number of concurrent Route() goroutines spawned
	// for inbound requests on this connection (see maxInFlightRoutes).
	routeSem chan struct{}

	// onClose, if set, is invoked exactly once after the read loop exits
	// and the connection is torn down.
	onClose func()
}

// NewConnection wraps an existing net.Conn.
func NewConnection(conn net.Conn, peerID string) *Connection {
	return &Connection{
		conn:            conn,
		peerID:          peerID,
		connectedAt:     time.Now(),
		Incoming:        make(chan []byte, 100), // buffered to prevent blocking the read loop immediately
		pendingRequests: make(map[string]chan *protocol.Envelope),
		routeSem:        make(chan struct{}, maxInFlightRoutes),
	}
}

// SetOnClose registers a callback invoked exactly once when the connection's
// read loop exits (i.e. the connection is torn down).
func (c *Connection) SetOnClose(fn func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onClose = fn
}

// SetRouter registers an RPC router for the connection.
func (c *Connection) SetRouter(router *Router) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.router = router
}

// Start spins up the connection's read loop to process incoming frames.
func (c *Connection) Start() {
	go c.readLoop()
}

// readLoop continuously reads framed messages from the socket.
func (c *Connection) readLoop() {
	defer func() {
		c.Close()
		close(c.Incoming)

		c.mu.Lock()
		for id, ch := range c.pendingRequests {
			close(ch)
			delete(c.pendingRequests, id)
		}
		onClose := c.onClose
		c.mu.Unlock()

		if onClose != nil {
			onClose()
		}
	}()

	for {
		if err := c.conn.SetReadDeadline(time.Now().Add(ReadIdleTimeout)); err != nil {
			return
		}

		data, err := ReadFrame(c.conn)
		if err != nil {
			return
		}

		// Attempt to parse the frame as a structured Protobuf Envelope
		var env protocol.Envelope
		if err := proto.Unmarshal(data, &env); err != nil {
			// Fall back to sending raw data to Incoming for backward compatibility
			c.Incoming <- data
			continue
		}

		// Check if a client is waiting for this message as an RPC response
		c.mu.Lock()
		ch, exists := c.pendingRequests[env.Id]
		c.mu.Unlock()

		if exists {
			ch <- &env
		} else {
			// Incoming request or notification
			c.mu.Lock()
			r := c.router
			c.mu.Unlock()

			if r != nil {
				c.routeSem <- struct{}{}
				go func(e *protocol.Envelope) {
					defer func() { <-c.routeSem }()
					_ = r.Route(c, e)
				}(&env)
			} else {
				// Fall back to pushing raw data to Incoming if no router is set
				c.Incoming <- data
			}
		}
	}
}

// WriteMessage writes a framed message to the underlying connection in a thread-safe manner.
func (c *Connection) WriteMessage(data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.conn.SetWriteDeadline(time.Now().Add(WriteTimeout)); err != nil {
		return err
	}
	return WriteFrame(c.conn, data)
}

// SendRequest sends a protobuf request, registers a response wait-channel, and blocks until completion.
func (c *Connection) SendRequest(ctx context.Context, msgType protocol.MessageType, req proto.Message) (*protocol.Envelope, error) {
	reqID := generateUUID()

	payload, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	env := &protocol.Envelope{
		Id:      reqID,
		Type:    msgType,
		Payload: payload,
	}

	envBytes, err := proto.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("marshal envelope: %w", err)
	}

	respChan := make(chan *protocol.Envelope, 1)

	c.mu.Lock()
	if c.pendingRequests == nil {
		c.pendingRequests = make(map[string]chan *protocol.Envelope)
	}
	c.pendingRequests[reqID] = respChan
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.pendingRequests, reqID)
		c.mu.Unlock()
	}()

	if err := c.WriteMessage(envBytes); err != nil {
		return nil, fmt.Errorf("write request message: %w", err)
	}

	select {
	case resp, ok := <-respChan:
		if !ok {
			return nil, fmt.Errorf("connection closed during request")
		}
		return resp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// WriteResponse marshals and writes a response to a request ID.
func (c *Connection) WriteResponse(reqID string, msgType protocol.MessageType, resp proto.Message) error {
	payload, err := proto.Marshal(resp)
	if err != nil {
		return fmt.Errorf("marshal response: %w", err)
	}

	env := &protocol.Envelope{
		Id:      reqID,
		Type:    msgType,
		Payload: payload,
	}

	envBytes, err := proto.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}

	return c.WriteMessage(envBytes)
}

// WriteRawResponse writes a pre-serialised payload as a response to a request.
// This is used by the DHT layer which uses JSON serialisation instead of protobuf.
func (c *Connection) WriteRawResponse(reqID string, msgType protocol.MessageType, payload []byte) error {
	env := &protocol.Envelope{
		Id:      reqID,
		Type:    msgType,
		Payload: payload,
	}

	envBytes, err := proto.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}

	return c.WriteMessage(envBytes)
}

// SendRaw sends a pre-serialised payload as a request and waits for the response envelope.
// This is the raw-payload counterpart of SendRequest, used by the DHT layer.
func (c *Connection) SendRaw(ctx context.Context, msgType protocol.MessageType, payload []byte) (*protocol.Envelope, error) {
	reqID := generateUUID()

	env := &protocol.Envelope{
		Id:      reqID,
		Type:    msgType,
		Payload: payload,
	}

	envBytes, err := proto.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("marshal envelope: %w", err)
	}

	respChan := make(chan *protocol.Envelope, 1)

	c.mu.Lock()
	if c.pendingRequests == nil {
		c.pendingRequests = make(map[string]chan *protocol.Envelope)
	}
	c.pendingRequests[reqID] = respChan
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.pendingRequests, reqID)
		c.mu.Unlock()
	}()

	if err := c.WriteMessage(envBytes); err != nil {
		return nil, fmt.Errorf("write request: %w", err)
	}

	select {
	case resp, ok := <-respChan:
		if !ok {
			return nil, fmt.Errorf("connection closed during request")
		}
		return resp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
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

// generateUUID creates a unique 16-byte identifier using standard rand reader.
func generateUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}


