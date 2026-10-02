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
	// forever. Var rather than const so tests can shrink it; each
	// Connection captures the value when it is created.
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

	// writeMu serializes writes to the underlying socket.
	// TCP is a stream. If Goroutine A and Goroutine B call Write()
	// at the exact same time without a mutex, their bytes might
	// interleave (e.g., A_half, B_full, A_rest), permanently
	// corrupting the stream for the receiver. It is separate from mu so
	// a slow write never blocks the read loop's bookkeeping.
	writeMu sync.Mutex

	// mu protects pendingRequests, router and onClose.
	mu sync.Mutex

	// peerID is the remote node's ID. For connections created by Server
	// or Dial it is authenticated by the mutual TLS handshake.
	peerID string

	connectedAt time.Time

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

	// done is closed once the read loop has exited and the connection is
	// torn down.
	done chan struct{}

	// Timeouts are captured from the package defaults at construction.
	readIdleTimeout time.Duration
	writeTimeout    time.Duration
}

// NewConnection wraps an existing net.Conn.
func NewConnection(conn net.Conn, peerID string) *Connection {
	return &Connection{
		conn:            conn,
		peerID:          peerID,
		connectedAt:     time.Now(),
		done:            make(chan struct{}),
		readIdleTimeout: ReadIdleTimeout,
		writeTimeout:    WriteTimeout,
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

// Done returns a channel that is closed once the connection has been torn
// down (read loop exited, socket closed, pending requests failed).
func (c *Connection) Done() <-chan struct{} {
	return c.done
}

// readLoop continuously reads framed messages from the socket.
//
// Any frame that is not a valid Envelope is a protocol violation and
// closes the connection. Responses are delivered without blocking, so a
// peer cannot stall the loop by sending unsolicited or duplicate
// responses.
func (c *Connection) readLoop() {
	defer func() {
		c.Close()

		// Closing done under mu guarantees SendRaw either registers its
		// request before this cleanup (and gets its channel closed) or
		// observes done and fails fast.
		c.mu.Lock()
		for id, ch := range c.pendingRequests {
			close(ch)
			delete(c.pendingRequests, id)
		}
		onClose := c.onClose
		close(c.done)
		c.mu.Unlock()

		if onClose != nil {
			onClose()
		}
	}()

	for {
		if err := c.conn.SetReadDeadline(time.Now().Add(c.readIdleTimeout)); err != nil {
			return
		}

		data, err := ReadFrame(c.conn)
		if err != nil {
			return
		}

		env := &protocol.Envelope{}
		if err := proto.Unmarshal(data, env); err != nil {
			return
		}

		if env.IsResponse {
			// A response to one of our requests: hand it to the waiting
			// caller exactly once. Removing the entry here means a
			// duplicate (or late) response is dropped rather than
			// blocking on a full channel. Responses are never routed, so
			// two peers can never bounce error replies back and forth.
			c.mu.Lock()
			ch, exists := c.pendingRequests[env.Id]
			if exists {
				delete(c.pendingRequests, env.Id)
			}
			c.mu.Unlock()
			if exists {
				ch <- env // buffered with capacity 1 and only ever sent to once
			}
			continue
		}

		// Incoming request.
		c.mu.Lock()
		r := c.router
		c.mu.Unlock()
		if r == nil {
			// Outbound-only connection: requests are not served. Drop
			// rather than reply, so the read loop never blocks on a write.
			continue
		}
		c.routeSem <- struct{}{}
		go func(e *protocol.Envelope) {
			defer func() { <-c.routeSem }()
			_ = r.Route(c, e)
		}(env)
	}
}

// WriteMessage writes a framed message to the underlying connection in a thread-safe manner.
func (c *Connection) WriteMessage(data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout)); err != nil {
		return err
	}
	return WriteFrame(c.conn, data)
}

// SendRequest sends a protobuf request and blocks until the matching
// response arrives, the context is done, or the connection closes.
func (c *Connection) SendRequest(ctx context.Context, msgType protocol.MessageType, req proto.Message) (*protocol.Envelope, error) {
	payload, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}
	return c.SendRaw(ctx, msgType, payload)
}

// WriteResponse marshals and writes a response to a request ID.
func (c *Connection) WriteResponse(reqID string, msgType protocol.MessageType, resp proto.Message) error {
	payload, err := proto.Marshal(resp)
	if err != nil {
		return fmt.Errorf("marshal response: %w", err)
	}

	return c.WriteRawResponse(reqID, msgType, payload)
}

// WriteRawResponse writes a pre-serialised payload as a response to a request.
// This is used by the DHT layer which uses JSON serialisation instead of protobuf.
func (c *Connection) WriteRawResponse(reqID string, msgType protocol.MessageType, payload []byte) error {
	env := &protocol.Envelope{
		Id:         reqID,
		Type:       msgType,
		Payload:    payload,
		IsResponse: true,
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
	select {
	case <-c.done:
		c.mu.Unlock()
		return nil, fmt.Errorf("connection closed")
	default:
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
		if resp.Type == protocol.MessageType_MSG_ERROR {
			return nil, decodeError(resp)
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
