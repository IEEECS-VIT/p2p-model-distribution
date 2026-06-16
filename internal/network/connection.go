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
}

// NewConnection wraps an existing net.Conn.
func NewConnection(conn net.Conn, peerID string) *Connection {
	return &Connection{
		conn:            conn,
		peerID:          peerID,
		connectedAt:     time.Now(),
		Incoming:        make(chan []byte, 100), // buffered to prevent blocking the read loop immediately
		pendingRequests: make(map[string]chan *protocol.Envelope),
	}
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
		c.mu.Unlock()
	}()

	for {
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
				go func(e *protocol.Envelope) {
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


