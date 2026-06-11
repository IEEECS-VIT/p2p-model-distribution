package network

import (
	"fmt"

	"github.com/IEEECS-VIT/p2p-model-distribution/internal/protocol"
)

// HandlerFunc defines the signature for custom handlers processing inbound RPC requests.
type HandlerFunc func(conn *Connection, env *protocol.Envelope) error

// Router maps message types to their respective handlers.
type Router struct {
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
	r.handlers[msgType] = handler
}

// Route executes the registered handler for the incoming message envelope type.
func (r *Router) Route(conn *Connection, env *protocol.Envelope) error {
	handler, exists := r.handlers[env.Type]
	if !exists {
		return fmt.Errorf("no handler registered for message type: %v", env.Type)
	}
	return handler(conn, env)
}
