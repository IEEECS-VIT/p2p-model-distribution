package network

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/IEEECS-VIT/p2p-model-distribution/internal/protocol"
	"google.golang.org/protobuf/proto"
)

// HandlerFunc defines the signature for custom handlers processing inbound RPC requests.
type HandlerFunc func(conn *Connection, env *protocol.Envelope) error

// RemoteError is returned by SendRequest/SendRaw when the peer answered
// with an ErrorResponse.
type RemoteError struct {
	Message string
}

func (e *RemoteError) Error() string {
	return "remote error: " + e.Message
}

// ErrBadRequest can be wrapped by handlers to have its message returned to
// the requester; any other handler error is reported only as a generic
// failure so internal details (paths, OS errors) are not leaked to peers.
var ErrBadRequest = errors.New("bad request")

// Router maps message types to their respective handlers. It is safe for
// concurrent use.
type Router struct {
	mu       sync.RWMutex
	handlers map[protocol.MessageType]HandlerFunc
}

// NewRouter instantiates a new RPC message router.
func NewRouter() *Router {
	return &Router{
		handlers: make(map[protocol.MessageType]HandlerFunc),
	}
}

// Register maps a message type to a handler function.
func (r *Router) Register(msgType protocol.MessageType, handler HandlerFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[msgType] = handler
}

// Route executes the registered handler for the incoming message envelope
// type. If there is no handler, or the handler fails, the requester gets an
// ErrorResponse so it does not have to wait for its timeout.
func (r *Router) Route(conn *Connection, env *protocol.Envelope) error {
	r.mu.RLock()
	handler, exists := r.handlers[env.Type]
	r.mu.RUnlock()

	if !exists {
		err := fmt.Errorf("unsupported message type %d", env.Type)
		_ = conn.WriteError(env.Id, err.Error())
		return err
	}

	if err := handler(conn, env); err != nil {
		msg := "request failed"
		if errors.Is(err, ErrBadRequest) {
			msg = err.Error()
		}
		slog.Debug("handler failed", "type", env.Type, "peer", conn.PeerID(), "err", err)
		_ = conn.WriteError(env.Id, msg)
		return err
	}
	return nil
}

// WriteError sends an ErrorResponse for the request with the given ID.
func (c *Connection) WriteError(reqID, message string) error {
	return c.WriteResponse(reqID, protocol.MessageType_MSG_ERROR, &protocol.ErrorResponse{Error: message})
}

// decodeError converts an ErrorResponse envelope into a RemoteError.
func decodeError(env *protocol.Envelope) error {
	var resp protocol.ErrorResponse
	if err := proto.Unmarshal(env.Payload, &resp); err != nil {
		return &RemoteError{Message: "malformed error response"}
	}
	return &RemoteError{Message: resp.Error}
}
