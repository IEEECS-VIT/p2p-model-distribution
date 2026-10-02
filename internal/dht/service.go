package dht

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"
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

	mu    sync.RWMutex
	conns map[string]*network.Connection // peerID -> active connection
	addrs map[string]string              // peerID -> "ip:port" for reconnecting

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

	return &Service{
		id:        id,
		self:      self,
		table:     NewRoutingTable(self.ID),
		store:     NewStore(),
		server:    network.NewServer(listen, id.ServerTLSConfig()),
		router:    network.NewRouter(),
		listen:    listen,
		external:  external,
		conns:     make(map[string]*network.Connection),
		addrs:     make(map[string]string),
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
	s.mu.Lock()
	for id, c := range s.conns {
		_ = c.Close()
		delete(s.conns, id)
	}
	s.mu.Unlock()
	s.server.Stop()
}

//---------------------------------------------------------------------
// Provider API

// AnnounceProvider records this node as a provider for key in the local
// store and sends ADD_PROVIDER to every connected peer.
func (s *Service) AnnounceProvider(key string) int {
	s.store.Add(key, s.self.ID)

	s.mu.RLock()
	peers := make([]*network.Connection, 0, len(s.conns))
	for _, c := range s.conns {
		peers = append(peers, c)
	}
	s.mu.RUnlock()

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
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.conns)
}

// FindProviders returns provider IDs for key from the local store and
// from FIND_VALUE queries to the closest connected peers.
func (s *Service) FindProviders(ctx context.Context, key string) []string {
	local := s.store.Get(key)
	seen := make(map[string]bool)
	for _, p := range local {
		seen[p] = true
	}
	providers := append([]string(nil), local...)

	for _, peer := range s.table.ClosestNodes(key, K) {
		s.mu.RLock()
		conn, ok := s.conns[peer.ID]
		s.mu.RUnlock()
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
			if !seen[p.Id] {
				seen[p.Id] = true
				providers = append(providers, p.Id)
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

// AddPeerAddr registers a peer's address so the service can connect
// to it later.  This is how we translate DHT Node IDs to dial-able
// TCP addresses.
func (s *Service) AddPeerAddr(peerID, addr string) {
	s.mu.Lock()
	s.addrs[peerID] = addr
	s.mu.Unlock()
}

// ResolvePeerID returns the dialable address for a peer ID by checking either the
// registered addrs map or scanning the routing table buckets.
func (s *Service) ResolvePeerID(peerID string) (string, bool) {
	s.mu.RLock()
	addr, ok := s.addrs[peerID]
	s.mu.RUnlock()
	if ok && addr != "" {
		return addr, true
	}

	if p, ok := s.table.FindNode(peerID); ok {
		if endpoint := p.Endpoint(); endpoint != "" {
			return endpoint, true
		}
	}
	return "", false
}

// QueryNodeAddress queries the DHT network for a peer ID to resolve its dialable IP and Port.
func (s *Service) QueryNodeAddress(ctx context.Context, targetID string) (string, bool) {
	if addr, ok := s.ResolvePeerID(targetID); ok && addr != "" {
		return addr, true
	}

	for _, peer := range s.table.ClosestNodes(targetID, K) {
		s.mu.RLock()
		conn, ok := s.conns[peer.ID]
		s.mu.RUnlock()
		if !ok {
			continue
		}

		reqCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		resp, err := s.sendDHTRequest(reqCtx, conn, protocol.MessageType_MSG_DHT_FIND_NODE, targetID)
		cancel()
		if err != nil {
			continue
		}

		for _, n := range resp.CloserPeers {
			node, ok := nodeFromWire(n)
			if !ok {
				continue
			}
			s.table.AddNode(node)
			if node.ID == targetID {
				return node.Endpoint(), true
			}
		}
	}

	return "", false
}

// GetConnection returns (or creates) a connection to a peer by address.
func (s *Service) GetConnection(addr string) (*network.Connection, error) {
	s.mu.RLock()
	for _, c := range s.conns {
		if c.RemoteAddr().String() == addr {
			s.mu.RUnlock()
			return c, nil
		}
	}
	s.mu.RUnlock()

	conn, err := network.Dial(context.Background(), addr, s.id.ClientTLSConfig(""))
	if err != nil {
		return nil, err
	}
	conn.SetRouter(s.router)
	conn.Start()

	s.mu.Lock()
	s.conns[conn.PeerID()] = conn
	s.mu.Unlock()

	return conn, nil
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
func (s *Service) dhtHandler(respType protocol.MessageType, handle func(from Node, req *protocol.DHTRequest, resp *protocol.DHTResponse)) network.HandlerFunc {
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
			handle(from, req, resp)
		}
		return conn.WriteResponse(env.Id, respType, resp)
	}
}

func (s *Service) handleFindNode(_ Node, req *protocol.DHTRequest, resp *protocol.DHTResponse) {
	resp.CloserPeers = nodesToWire(s.table.ClosestNodes(req.Key, K))
}

func (s *Service) handleFindValue(_ Node, req *protocol.DHTRequest, resp *protocol.DHTResponse) {
	resp.CloserPeers = nodesToWire(s.table.ClosestNodes(req.Key, K))
	for _, id := range s.store.Get(req.Key) {
		resp.Providers = append(resp.Providers, &protocol.DHTPeer{Id: id})
	}
}

func (s *Service) handleAddProvider(from Node, req *protocol.DHTRequest, _ *protocol.DHTResponse) {
	if req.Key != "" {
		s.store.Add(req.Key, from.ID)
	}
}

// trackPeer records conn under the peer's authenticated node ID and adds
// the peer to the routing table. The peer's address is the IP observed on
// the socket plus the listen port it advertises (the only self-reported
// field we use): trusting a self-reported IP would let a peer point other
// nodes at arbitrary third-party hosts.
func (s *Service) trackPeer(conn *network.Connection, listenPort uint32) Node {
	peerID := conn.PeerID()

	s.mu.Lock()
	for k, v := range s.conns {
		if v == conn && k != peerID {
			delete(s.conns, k)
			break
		}
	}
	s.conns[peerID] = conn
	s.mu.Unlock()

	ip, _ := splitHostPort(conn.RemoteAddr().String())
	node := Node{ID: peerID, IP: ip, Port: int(listenPort)}
	if listenPort > 0 && listenPort <= 65535 {
		s.table.AddNode(node)
		s.AddPeerAddr(peerID, node.Endpoint())
	}
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
		conn, err := s.GetConnection(addr)
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
				s.AddPeerAddr(node.ID, node.Endpoint())
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
