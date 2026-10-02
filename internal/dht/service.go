package dht

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/IEEECS-VIT/p2p-model-distribution/internal/network"
	"github.com/IEEECS-VIT/p2p-model-distribution/internal/protocol"
)

// Service integrates the DHT layer with the TCP network layer.
// It handles bootstrapping, connection management, provider
// announcement/lookup, and wires DHT message handlers into the
// network router so that every node can participate in the DHT.
type Service struct {
	self     Node
	dht      *DHT
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
//   - nodeID:   optional; if empty, a random 8-byte hex ID is generated
//   - listen:   TCP address to listen on, e.g. "0.0.0.0:9000"
//   - external: address we advertise in the DHT, e.g. "192.168.1.5:9000" (empty = auto-detect)
//   - seeds:    bootstrap peer addresses ("ip:port")
func NewService(nodeID, listen, external string, seeds []string) *Service {
	if nodeID == "" {
		b := make([]byte, 8)
		_, _ = rand.Read(b)
		nodeID = fmt.Sprintf("%x", b)
	}
	self := Node{ID: nodeID, IP: extractIP(external, listen), Port: extractPort(external, listen)}

	d := NewDHT(self)
	router := network.NewRouter()
	srv := network.NewServer(listen)

	return &Service{
		self:      self,
		dht:       d,
		server:    srv,
		router:    router,
		listen:    listen,
		external:  external,
		conns:     make(map[string]*network.Connection),
		addrs:     make(map[string]string),
		bootstrap: seeds,
	}
}

// Self returns this node's identity.
func (s *Service) Self() Node { return s.self }

// DHT returns the underlying DHT instance.
func (s *Service) DHT() *DHT { return s.dht }

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

	// Register DHT message handlers
	s.registerHandlers()

	// Wire up new-connection callback: every new TCP connection
	// gets our router so it can handle both DHT and file-transfer RPCs.
	s.server.OnNewConnection = func(conn *network.Connection) {
		conn.SetRouter(s.router)
		conn.Start()
	}

	if err := s.server.Start(); err != nil {
		return fmt.Errorf("start server: %w", err)
	}

	// Update self port if it was ephemeral
	if addr, ok := s.server.Addr().(*net.TCPAddr); ok {
		s.self.Port = addr.Port
		if s.dht != nil {
			s.dht.Self.Port = addr.Port
		}
	}

	log.Printf("[DHT] Node %s listening on %s (advertised: %s:%d)",
		s.self.ID, s.listen, s.self.IP, s.self.Port)

	// Bootstrap to the DHT network in the background.
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

// AnnounceProvider records this node as a provider for the given key
// (typically a file ID or chunk hash) in the local DHT store and
// propagates a STORE message to nearby peers so the record spreads.
func (s *Service) AnnounceProvider(key string) int {
	s.dht.RecordProvider(key, s.self.ID)

	// Propagate to every connected peer so the record spreads.
	s.mu.RLock()
	peers := make([]*network.Connection, 0, len(s.conns))
	for _, c := range s.conns {
		peers = append(peers, c)
	}
	s.mu.RUnlock()

	for _, c := range peers {
		go func(conn *network.Connection) {
			body := &protocol.DHTMessageBody{
				Type:     "STORE",
				FromID:   s.self.ID,
				FromIP:   s.self.IP,
				FromPort: s.self.Port,
				Key:      key,
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, _ = s.sendDHTRequest(ctx, conn, protocol.MSG_DHT_STORE, body)
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

// FindProviders queries connected peers for providers of the given key.
// It first checks the local provider store, then sends FIND_VALUE
// queries to the closest known peers from the routing table.
func (s *Service) FindProviders(ctx context.Context, key string) []string {
	// Check local store first.
	local := s.dht.ProvidersFor(key)
	seen := make(map[string]bool)
	for _, p := range local {
		seen[p] = true
	}
	providers := append([]string(nil), local...)

	// Query closest peers from routing table.
	s.mu.RLock()
	closest := s.dht.ClosestPeers(key, K)
	s.mu.RUnlock()

	for _, peer := range closest {
		if seen[peer.ID] {
			continue
		}
		s.mu.RLock()
		conn, ok := s.conns[peer.ID]
		s.mu.RUnlock()
		if !ok {
			continue
		}

		body := &protocol.DHTMessageBody{
			Type:     "FIND_VALUE",
			FromID:   s.self.ID,
			FromIP:   s.self.IP,
			FromPort: s.self.Port,
			Key:      key,
		}

		reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		resp, err := s.sendDHTRequest(reqCtx, conn, protocol.MSG_DHT_FIND_VALUE, body)
		cancel()
		if err != nil {
			continue
		}

		for _, p := range resp.Providers {
			if !seen[p] {
				seen[p] = true
				providers = append(providers, p)
			}
		}
		// Add returned nodes to routing table.
		for _, n := range resp.Nodes {
			s.dht.AddPeer(Node{ID: n.ID, IP: n.IP, Port: n.Port})
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

	// Scan routing table
	if s.dht != nil && s.dht.RoutingTable != nil {
		if p, ok := s.dht.RoutingTable.FindNode(peerID); ok {
			if endpoint := p.Endpoint(); endpoint != "" {
				return endpoint, true
			}
		}
	}
	return "", false
}

// QueryNodeAddress queries the DHT network for a peer ID to resolve its dialable IP and Port.
func (s *Service) QueryNodeAddress(ctx context.Context, targetID string) (string, bool) {
	// 1. Check local cache/routing table first
	if addr, ok := s.ResolvePeerID(targetID); ok && addr != "" {
		return addr, true
	}

	// 2. Query closest peers for the target ID
	s.mu.RLock()
	closest := s.dht.ClosestPeers(targetID, K)
	s.mu.RUnlock()

	for _, peer := range closest {
		if peer.ID == s.self.ID {
			continue
		}
		s.mu.RLock()
		conn, ok := s.conns[peer.ID]
		s.mu.RUnlock()
		if !ok {
			continue
		}

		body := &protocol.DHTMessageBody{
			Type:     "FIND_NODE",
			FromID:   s.self.ID,
			FromIP:   s.self.IP,
			FromPort: s.self.Port,
			TargetID: targetID,
		}

		reqCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		resp, err := s.sendDHTRequest(reqCtx, conn, protocol.MSG_DHT_FIND_NODE, body)
		cancel()
		if err != nil {
			continue
		}

		// Look through the returned nodes to find the targetID
		for _, n := range resp.Nodes {
			// Add to local routing table so we store the address
			s.dht.AddPeer(Node{ID: n.ID, IP: n.IP, Port: n.Port})
			if n.ID == targetID {
				endpoint := fmt.Sprintf("%s:%d", n.IP, n.Port)
				if n.IP != "" && n.Port != 0 {
					return endpoint, true
				}
			}
		}
	}

	return "", false
}

// GetConnection returns (or creates) a connection to a peer by address.
func (s *Service) GetConnection(addr string) (*network.Connection, error) {
	// Fast path: check existing connections.
	s.mu.RLock()
	for _, c := range s.conns {
		if c.RemoteAddr().String() == addr {
			s.mu.RUnlock()
			return c, nil
		}
	}
	s.mu.RUnlock()

	raw, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	conn := network.NewConnection(raw, addr)
	conn.SetRouter(s.router)
	conn.Start()

	// Send a handshake / identity exchange so the remote peer knows who we are.
	// For now we simply track the connection.
	s.mu.Lock()
	s.conns[addr] = conn // key by addr until we learn the peer ID
	s.mu.Unlock()

	return conn, nil
}

func (s *Service) sendDHTRequest(ctx context.Context, conn *network.Connection, msgType protocol.MessageType, body *protocol.DHTMessageBody) (*protocol.DHTMessageBody, error) {
	payload, err := protocol.MarshalDHTMessage(body)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	respEnv, err := conn.SendRaw(ctx, msgType, payload)
	if err != nil {
		return nil, err
	}

	resp, err := protocol.UnmarshalDHTMessage(respEnv.Payload)
	if err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}

	// Update connection mapping with the peer's resolved ID
	if resp.FromID != "" {
		s.mu.Lock()
		// Remove old addr-based or different ID-based key for the same connection
		for k, v := range s.conns {
			if v == conn && k != resp.FromID {
				delete(s.conns, k)
				break
			}
		}
		s.conns[resp.FromID] = conn
		s.addrs[resp.FromID] = conn.RemoteAddr().String()
		s.mu.Unlock()

		if resp.FromIP != "" && resp.FromPort != 0 {
			s.dht.AddPeer(Node{ID: resp.FromID, IP: resp.FromIP, Port: resp.FromPort})
		}
	}

	return resp, nil
}

//---------------------------------------------------------------------
// Internal

func (s *Service) registerHandlers() {
	s.router.Register(protocol.MSG_DHT_PING, s.handleDHTMessage)
	s.router.Register(protocol.MSG_DHT_FIND_NODE, s.handleDHTMessage)
	s.router.Register(protocol.MSG_DHT_FIND_VALUE, s.handleDHTMessage)
	s.router.Register(protocol.MSG_DHT_STORE, s.handleDHTMessage)
}

func (s *Service) handleDHTMessage(conn *network.Connection, env *protocol.Envelope) error {
	body, err := protocol.UnmarshalDHTMessage(env.Payload)
	if err != nil {
		return fmt.Errorf("unmarshal DHT message: %w", err)
	}

	// Convert wire format to dht.Message.
	req := Message{
		Type:     body.Type,
		TargetID: body.TargetID,
		Key:      body.Key,
		Value:    body.Value,
		From:     s.resolvePeer(body, conn),
		To:       s.self,
	}

	// Update connection map: key by peer ID so FindProviders can find them.
	if body.FromID != "" {
		s.mu.Lock()
		// Remove old addr-based key
		for k, v := range s.conns {
			if v == conn && k != body.FromID {
				delete(s.conns, k)
				break
			}
		}
		s.conns[body.FromID] = conn
		s.addrs[body.FromID] = conn.RemoteAddr().String()
		s.mu.Unlock()
	}

	// Let the DHT core process it.
	resp := s.dht.HandleMessage(req)

	// Build wire response.
	respBody := &protocol.DHTMessageBody{
		Type:      resp.Type,
		FromID:    s.self.ID,
		FromIP:    s.self.IP,
		FromPort:  s.self.Port,
		TargetID:  resp.TargetID,
		Key:       resp.Key,
		Value:     resp.Value,
		Error:     resp.Error,
		Providers: resp.Providers,
	}
	for _, n := range resp.Nodes {
		respBody.Nodes = append(respBody.Nodes, protocol.DHTNode{
			ID: n.ID, IP: n.IP, Port: n.Port,
		})
	}

	respPayload, err := protocol.MarshalDHTMessage(respBody)
	if err != nil {
		return fmt.Errorf("marshal DHT response: %w", err)
	}

	respType := protocol.ResponseMessageType(body.Type)
	return conn.WriteRawResponse(env.Id, respType, respPayload)
}

// resolvePeer maps a FromID from a wire message to a dht.Node.
// If we don't have the peer in our routing table, we extract
// address info from the connection and add it.
func (s *Service) resolvePeer(body *protocol.DHTMessageBody, conn *network.Connection) Node {
	fromID := body.FromID
	// Check routing table first.
	if p, ok := s.dht.RoutingTable.FindNode(fromID); ok {
		if body.FromPort != 0 && p.Port != body.FromPort {
			p.Port = body.FromPort
			s.dht.AddPeer(p)
		}
		return p
	}

	// Unknown peer — add it with the connection's remote address.
	addr := conn.RemoteAddr().String()
	ip, portStr := splitHostPort(addr)
	port := 0
	_, _ = fmt.Sscanf(portStr, "%d", &port)

	if body.FromIP != "" {
		ip = body.FromIP
	}
	if body.FromPort != 0 {
		port = body.FromPort
	}

	node := Node{ID: fromID, IP: ip, Port: port}
	s.dht.AddPeer(node)
	return node
}

// bootstrapLoop periodically connects to bootstrap peers to stay
// connected to the DHT network.
func (s *Service) bootstrapLoop(ctx context.Context) {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	// Initial bootstrap immediately.
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

		// Send a FIND_NODE for our own ID to discover peers.
		body := &protocol.DHTMessageBody{
			Type:     "FIND_NODE",
			FromID:   s.self.ID,
			FromIP:   s.self.IP,
			FromPort: s.self.Port,
			TargetID: s.self.ID,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		resp, err := s.sendDHTRequest(ctx, conn, protocol.MSG_DHT_FIND_NODE, body)
		cancel()
		if err != nil {
			log.Printf("[DHT] bootstrap FIND_NODE to %s: %v", addr, err)
			continue
		}

		// Add returned nodes to routing table and store their addresses.
		for _, n := range resp.Nodes {
			s.dht.AddPeer(Node{ID: n.ID, IP: n.IP, Port: n.Port})
			peerAddr := fmt.Sprintf("%s:%d", n.IP, n.Port)
			s.AddPeerAddr(n.ID, peerAddr)
		}

		log.Printf("[DHT] Bootstrap %s returned %d peers", addr, len(resp.Nodes))
	}
}

//---------------------------------------------------------------------
// DHT Transport implementation (implements dht.Transport)

// Send implements dht.Transport by finding or creating a connection
// to the destination peer and sending the message as a DHT RPC.
func (s *Service) Send(msg Message) error {
	addr, ok := s.addrs[msg.To.ID]
	if !ok {
		addr = fmt.Sprintf("%s:%d", msg.To.IP, msg.To.Port)
	}
	conn, err := s.GetConnection(addr)
	if err != nil {
		return fmt.Errorf("send to %s: %w", addr, err)
	}

	body := &protocol.DHTMessageBody{
		Type:     msg.Type,
		FromID:   s.self.ID,
		TargetID: msg.TargetID,
		Key:      msg.Key,
		Value:    msg.Value,
	}
	payload, _ := protocol.MarshalDHTMessage(body)

	msgType := protocol.RequestMessageType(msg.Type)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = conn.SendRaw(ctx, msgType, payload)
	return err
}

//---------------------------------------------------------------------
// Helpers

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
	var port int
	_, _ = fmt.Sscanf(portStr, "%d", &port)
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

// Ensure Service satisfies the Transport interface.
var _ Transport = (*Service)(nil)
