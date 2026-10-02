package dht

import (
	"context"
	"errors"
	"sync"

	"github.com/IEEECS-VIT/p2p-model-distribution/internal/identity"
	"github.com/IEEECS-VIT/p2p-model-distribution/internal/network"
)

// maxPooledConns bounds how many peer connections the pool keeps open.
// Inbound connections are additionally bounded by the server's cap.
const maxPooledConns = 256

var errPoolFull = errors.New("connection pool full")

// pool tracks open peer connections, keyed by authenticated node ID.
//
// Connections are removed as soon as they close, concurrent dials to the
// same peer or address are coalesced into one, and outbound dials pin the
// expected node ID whenever it is known, so a connection registered under
// an ID is always to the node that owns that ID.
type pool struct {
	id     *identity.Identity
	router *network.Router

	mu      sync.Mutex
	byID    map[string]*network.Connection
	dialing map[string]*dialCall
	closed  bool
}

type dialCall struct {
	done chan struct{}
	conn *network.Connection
	err  error
}

func newPool(id *identity.Identity, router *network.Router) *pool {
	return &pool{
		id:      id,
		router:  router,
		byID:    make(map[string]*network.Connection),
		dialing: make(map[string]*dialCall),
	}
}

// add registers an open connection (inbound or outbound) under its peer ID.
// If the peer already has a live connection, both stay open and the newer
// one is used for subsequent requests.
func (p *pool) add(conn *network.Connection) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		conn.Close()
		return
	}
	p.byID[conn.PeerID()] = conn
	p.mu.Unlock()

	go func() {
		<-conn.Done()
		p.mu.Lock()
		if p.byID[conn.PeerID()] == conn {
			delete(p.byID, conn.PeerID())
		}
		p.mu.Unlock()
	}()
}

// get returns the live connection to peerID, if any.
func (p *pool) get(peerID string) (*network.Connection, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	conn, ok := p.byID[peerID]
	return conn, ok
}

// connect returns a connection to node, dialing node.Endpoint() if needed.
// The dial pins node.ID, so it fails unless the remote end owns that ID.
func (p *pool) connect(ctx context.Context, node Node) (*network.Connection, error) {
	if conn, ok := p.get(node.ID); ok {
		return conn, nil
	}
	return p.dial(ctx, "id:"+node.ID, node.Endpoint(), node.ID)
}

// connectAddr returns a connection to whichever node answers at addr. It is
// used when only an address is known (bootstrap peers, -addr providers).
func (p *pool) connectAddr(ctx context.Context, addr string) (*network.Connection, error) {
	p.mu.Lock()
	for _, c := range p.byID {
		if c.RemoteAddr().String() == addr {
			p.mu.Unlock()
			return c, nil
		}
	}
	p.mu.Unlock()
	return p.dial(ctx, "addr:"+addr, addr, "")
}

func (p *pool) dial(ctx context.Context, key, addr, expectedID string) (*network.Connection, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, errors.New("connection pool closed")
	}
	if call, ok := p.dialing[key]; ok {
		p.mu.Unlock()
		select {
		case <-call.done:
			return call.conn, call.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if len(p.byID) >= maxPooledConns {
		p.mu.Unlock()
		return nil, errPoolFull
	}
	call := &dialCall{done: make(chan struct{})}
	p.dialing[key] = call
	p.mu.Unlock()

	// The dial itself is not bound to the first caller's context, since
	// other callers may be waiting on it; network.Dial applies its own
	// handshake timeout.
	conn, err := network.Dial(context.Background(), addr, p.id.ClientTLSConfig(expectedID))
	if err == nil {
		conn.SetRouter(p.router)
		conn.Start()
		p.add(conn)
	}

	p.mu.Lock()
	call.conn, call.err = conn, err
	delete(p.dialing, key)
	p.mu.Unlock()
	close(call.done)

	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return conn, nil
	}
}

// all returns a snapshot of every live connection.
func (p *pool) all() []*network.Connection {
	p.mu.Lock()
	defer p.mu.Unlock()
	conns := make([]*network.Connection, 0, len(p.byID))
	for _, c := range p.byID {
		conns = append(conns, c)
	}
	return conns
}

func (p *pool) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.byID)
}

// closeAll closes every connection and refuses new ones.
func (p *pool) closeAll() {
	p.mu.Lock()
	p.closed = true
	conns := make([]*network.Connection, 0, len(p.byID))
	for _, c := range p.byID {
		conns = append(conns, c)
	}
	p.mu.Unlock()
	for _, c := range conns {
		c.Close()
	}
}
