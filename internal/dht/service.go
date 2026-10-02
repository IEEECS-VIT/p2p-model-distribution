package dht

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"time"

	"github.com/IEEECS-VIT/p2p-model-distribution/internal/identity"
	"github.com/IEEECS-VIT/p2p-model-distribution/internal/network"
	"github.com/IEEECS-VIT/p2p-model-distribution/internal/protocol"
	"google.golang.org/protobuf/proto"
)

// Service integrates the DHT layer with the TCP network layer.
// It handles bootstrapping, connection management, provider
// announcement/lookup, and wires DHT message handlers into the
// network router so that every node can participate in the DHT.
type Service struct {
	id       *identity.Identity
	self     Node
	table    *RoutingTable
	store    *ProviderStore
	server   *network.Server
	router   *network.Router
	listen   string
	external string // address we advertise to other peers ("ip:port")

	pool *pool

	bootstrap []string // "ip:port" addresses of bootstrap peers

	cancel context.CancelFunc
}

// NewService creates a DHT service.
//   - id:       the node's identity; the node ID is derived from its key
//   - listen:   TCP address to listen on, e.g. "0.0.0.0:9000"
//   - external: address we advertise in the DHT, e.g. "192.168.1.5:9000" (empty = auto-detect)
//   - seeds:    bootstrap peer addresses ("ip:port")
func NewService(id *identity.Identity, listen, external string, seeds []string) *Service {
	self := Node{ID: id.ID(), IP: extractIP(external, listen), Port: extractPort(external, listen)}

	router := network.NewRouter()
	return &Service{
		id:        id,
		self:      self,
		table:     NewRoutingTable(self.ID),
		store:     NewStore(),
		server:    network.NewServer(listen, id.ServerTLSConfig()),
		router:    router,
		pool:      newPool(id, router),
		listen:    listen,
		external:  external,
		bootstrap: seeds,
	}
}

// Self returns this node's identity.
func (s *Service) Self() Node { return s.self }

// Router returns the network router so callers can register
// non-DHT handlers (e.g. metadata/chunk handlers).
func (s *Service) Router() *network.Router { return s.router }

// Server returns the underlying TCP server.
func (s *Service) Server() *network.Server { return s.server }

// Start begins listening for TCP connections, registers DHT
// message handlers on the router, and bootstraps to the network.
func (s *Service) Start() error {
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel

	s.registerHandlers()

	// Every inbound connection gets our router so it can handle both DHT
	// and file-transfer RPCs.
	s.server.OnNewConnection = func(conn *network.Connection) {
		conn.SetRouter(s.router)
		conn.Start()
		s.pool.add(conn)
	}

	if err := s.server.Start(); err != nil {
		return fmt.Errorf("start server: %w", err)
	}

	// Update self port if it was ephemeral
	if addr, ok := s.server.Addr().(*net.TCPAddr); ok && s.external == "" {
		s.self.Port = addr.Port
	}

	log.Printf("[DHT] Node %s listening on %s (advertised: %s:%d)",
		s.self.ID, s.listen, s.self.IP, s.self.Port)

	if len(s.bootstrap) > 0 {
		go s.bootstrapLoop(ctx)
	}

	return nil
}

// Stop shuts down the DHT service and all peer connections.
func (s *Service) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
	s.pool.closeAll()
	s.server.Stop()
}

//---------------------------------------------------------------------
// Provider API

// AnnounceProvider records this node as a provider for key in the local
// store and sends ADD_PROVIDER to every connected peer.
func (s *Service) AnnounceProvider(key string) int {
	s.store.Add(key, s.self)

	peers := s.pool.all()
	for _, c := range peers {
		go func(conn *network.Connection) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, _ = s.sendDHTRequest(ctx, conn, protocol.MessageType_MSG_DHT_ADD_PROVIDER, key)
		}(c)
	}
	return len(peers)
}

// ConnectedPeersCount returns the number of active peer connections.
func (s *Service) ConnectedPeersCount() int {
	return s.pool.count()
}

// FindProviders returns the providers (with addresses) known for key,
// from the local store and from FIND_VALUE queries to the closest
// connected peers. This node is never included.
func (s *Service) FindProviders(ctx context.Context, key string) []Node {
	seen := map[string]bool{s.self.ID: true}
	var providers []Node
	addProvider := func(n Node) {
		if !seen[n.ID] {
			seen[n.ID] = true
			providers = append(providers, n)
		}
	}
	for _, p := range s.store.Get(key) {
		addProvider(p)
	}

	for _, peer := range s.table.ClosestNodes(key, K) {
		conn, ok := s.pool.get(peer.ID)
		if !ok {
			continue
		}

		reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		resp, err := s.sendDHTRequest(reqCtx, conn, protocol.MessageType_MSG_DHT_FIND_VALUE, key)
		cancel()
		if err != nil {
			continue
		}

		for _, p := range resp.Providers {
			if node, ok := nodeFromWire(p); ok {
				addProvider(node)
			}
		}
		for _, n := range resp.CloserPeers {
			if node, ok := nodeFromWire(n); ok {
				s.table.AddNode(node)
			}
		}
	}

	return providers
}

// Connect returns a connection to node, dialing it if necessary. The dial
// fails unless the remote end proves it owns node.ID.
func (s *Service) Connect(ctx context.Context, node Node) (*network.Connection, error) {
	return s.pool.connect(ctx, node)
}

// ConnectAddr returns a connection to whichever node answers at addr.
func (s *Service) ConnectAddr(ctx context.Context, addr string) (*network.Connection, error) {
	return s.pool.connectAddr(ctx, addr)
}

func (s *Service) sendDHTRequest(ctx context.Context, conn *network.Connection, msgType protocol.MessageType, key string) (*protocol.DHTResponse, error) {
	req := &protocol.DHTRequest{ListenPort: uint32(s.self.Port), Key: key}
	respEnv, err := conn.SendRequest(ctx, msgType, req)
	if err != nil {
		return nil, err
	}

	resp := &protocol.DHTResponse{}
	if err := proto.Unmarshal(respEnv.Payload, resp); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}

	// The responder's identity comes from the TLS handshake, never from
	// self-reported fields in the message body.
	s.trackPeer(conn, resp.ListenPort)

	return resp, nil
}

//---------------------------------------------------------------------
// Internal

func (s *Service) registerHandlers() {
	s.router.Register(protocol.MessageType_MSG_DHT_PING, s.dhtHandler(protocol.MessageType_MSG_DHT_PONG, nil))
	s.router.Register(protocol.MessageType_MSG_DHT_FIND_NODE, s.dhtHandler(protocol.MessageType_MSG_DHT_FIND_NODE_RESPONSE, s.handleFindNode))
	s.router.Register(protocol.MessageType_MSG_DHT_FIND_VALUE, s.dhtHandler(protocol.MessageType_MSG_DHT_FIND_VALUE_RESPONSE, s.handleFindValue))
	s.router.Register(protocol.MessageType_MSG_DHT_ADD_PROVIDER, s.dhtHandler(protocol.MessageType_MSG_DHT_ADD_PROVIDER_RESPONSE, s.handleAddProvider))
}

// dhtHandler decodes a DHTRequest, records the sender, runs handle (if
// non-nil) to fill in the response, and writes it back as respType.
func (s *Service) dhtHandler(respType protocol.MessageType, handle func(conn *network.Connection, from Node, req *protocol.DHTRequest, resp *protocol.DHTResponse)) network.HandlerFunc {
	return func(conn *network.Connection, env *protocol.Envelope) error {
		req := &protocol.DHTRequest{}
		if err := proto.Unmarshal(env.Payload, req); err != nil {
			return fmt.Errorf("%w: malformed DHT request", network.ErrBadRequest)
		}

		// The sender's identity comes from the TLS handshake, never from
		// self-reported fields in the message body.
		from := s.trackPeer(conn, req.ListenPort)

		resp := &protocol.DHTResponse{ListenPort: uint32(s.self.Port)}
		if handle != nil {
			handle(conn, from, req, resp)
		}
		return conn.WriteResponse(env.Id, respType, resp)
	}
}

func (s *Service) handleFindNode(_ *network.Connection, _ Node, req *protocol.DHTRequest, resp *protocol.DHTResponse) {
	resp.CloserPeers = nodesToWire(s.table.ClosestNodes(req.Key, K))
}

func (s *Service) handleFindValue(conn *network.Connection, _ Node, req *protocol.DHTRequest, resp *protocol.DHTResponse) {
	resp.CloserPeers = nodesToWire(s.table.ClosestNodes(req.Key, K))
	providers := s.store.Get(req.Key)
	for i, p := range providers {
		if p.ID == s.self.ID {
			providers[i] = s.selfAddrFor(conn)
		}
	}
	resp.Providers = nodesToWire(providers)
}

// handleAddProvider records the sender as a provider for the key. A peer
// can only announce itself, at the address we observe it on, and only if
// it accepts connections.
func (s *Service) handleAddProvider(_ *network.Connection, from Node, req *protocol.DHTRequest, _ *protocol.DHTResponse) {
	if req.Key != "" && from.Port > 0 {
		s.store.Add(req.Key, from)
	}
}

// selfAddrFor returns this node's address as reachable by the peer on
// conn: the configured external address if set, otherwise the local IP
// the peer reached us on.
func (s *Service) selfAddrFor(conn *network.Connection) Node {
	self := s.self
	if s.external == "" {
		if tcp, ok := conn.LocalAddr().(*net.TCPAddr); ok {
			self.IP = tcp.IP.String()
		}
	}
	return self
}

// trackPeer adds the peer on conn to the routing table. The peer's address is the IP observed on
// the socket plus the listen port it advertises (the only self-reported
// field we use): trusting a self-reported IP would let a peer point other
// nodes at arbitrary third-party hosts.
func (s *Service) trackPeer(conn *network.Connection, listenPort uint32) Node {
	peerID := conn.PeerID()
	ip, _ := splitHostPort(conn.RemoteAddr().String())
	node := Node{ID: peerID, IP: ip, Port: int(listenPort)}
	if listenPort == 0 || listenPort > 65535 {
		node.Port = 0
		return node
	}
	s.table.AddNode(node)
	return node
}

// bootstrapLoop periodically connects to bootstrap peers to stay
// connected to the DHT network.
func (s *Service) bootstrapLoop(ctx context.Context) {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	s.bootstrapOnce()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.bootstrapOnce()
		}
	}
}

func (s *Service) bootstrapOnce() {
	for _, addr := range s.bootstrap {
		conn, err := s.pool.connectAddr(context.Background(), addr)
		if err != nil {
			log.Printf("[DHT] bootstrap dial %s: %v", addr, err)
			continue
		}

		// Look up our own ID to discover peers near us.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		resp, err := s.sendDHTRequest(ctx, conn, protocol.MessageType_MSG_DHT_FIND_NODE, s.self.ID)
		cancel()
		if err != nil {
			log.Printf("[DHT] bootstrap FIND_NODE to %s: %v", addr, err)
			continue
		}

		for _, n := range resp.CloserPeers {
			if node, ok := nodeFromWire(n); ok {
				s.table.AddNode(node)
			}
		}

		log.Printf("[DHT] Bootstrap %s returned %d peers", addr, len(resp.CloserPeers))
	}
}

//---------------------------------------------------------------------
// Helpers

// nodeFromWire converts and validates a peer received from the network.
func nodeFromWire(p *protocol.DHTPeer) (Node, bool) {
	if p == nil || !identity.ValidID(p.Id) || net.ParseIP(p.Ip) == nil || p.Port == 0 || p.Port > 65535 {
		return Node{}, false
	}
	return Node{ID: p.Id, IP: p.Ip, Port: int(p.Port)}, true
}

func nodesToWire(nodes []Node) []*protocol.DHTPeer {
	out := make([]*protocol.DHTPeer, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, &protocol.DHTPeer{Id: n.ID, Ip: n.IP, Port: uint32(n.Port)})
	}
	return out
}

func extractIP(external, listen string) string {
	if external != "" {
		ip, _ := splitHostPort(external)
		if ip != "" {
			return ip
		}
	}
	ip, _ := splitHostPort(listen)
	if ip == "0.0.0.0" || ip == "" {
		return "127.0.0.1"
	}
	return ip
}

func extractPort(external, listen string) int {
	addr := listen
	if external != "" {
		addr = external
	}
	_, portStr := splitHostPort(addr)
	port, _ := strconv.Atoi(portStr)
	if port == 0 {
		port = 9000
	}
	return port
}

func splitHostPort(addr string) (string, string) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr, ""
	}
	return host, port
}
