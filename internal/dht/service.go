package dht

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
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

	pool *pool

	// Background routing-table maintenance (see addVerified and
	// verifyInbound): in-progress work is deduplicated per node ID and
	// inbound verification dials are bounded by verifySem.
	pendingMu     sync.Mutex
	pendingVerify map[string]bool
	pendingEvict  map[string]bool
	verifySem     chan struct{}

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
		id:     id,
		self:   self,
		table:  NewRoutingTable(self.ID),
		store:  NewStore(),
		server: network.NewServer(listen, id.ServerTLSConfig()),
		router: router,
		pool:   newPool(id, router),

		pendingVerify: make(map[string]bool),
		pendingEvict:  make(map[string]bool),
		verifySem:     make(chan struct{}, maxConcurrentVerifications),
		listen:        listen,
		external:      external,
		bootstrap:     seeds,
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

	slog.Info("DHT node listening", "id", s.self.ID, "listen", s.listen, "advertised", s.self.Endpoint())

	go s.maintain(ctx)

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

// AnnounceProvider records this node as a provider for key and sends
// ADD_PROVIDER to the K nodes closest to key, found by an iterative
// lookup. It returns how many of them accepted the record.
func (s *Service) AnnounceProvider(ctx context.Context, key string) int {
	s.store.Add(key, s.self)

	closest, _ := s.lookup(ctx, key, false, 0)

	accepted := make(chan bool, len(closest))
	for _, n := range closest {
		go func(n Node) {
			_, err := s.query(ctx, n, protocol.MessageType_MSG_DHT_ADD_PROVIDER, key)
			accepted <- err == nil
		}(n)
	}
	count := 0
	for range closest {
		if <-accepted {
			count++
		}
	}
	return count
}

// ConnectedPeersCount returns the number of active peer connections.
func (s *Service) ConnectedPeersCount() int {
	return s.pool.count()
}

// FindProviders returns providers (with addresses) for key from the
// local store and an iterative FIND_VALUE lookup. This node is never
// included.
func (s *Service) FindProviders(ctx context.Context, key string) []Node {
	seen := map[string]bool{s.self.ID: true}
	var providers []Node
	add := func(n Node) {
		if !seen[n.ID] {
			seen[n.ID] = true
			providers = append(providers, n)
		}
	}
	for _, p := range s.store.Get(key) {
		add(p)
	}

	_, found := s.lookup(ctx, key, true, K)
	for _, p := range found {
		add(p)
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

// maxConcurrentVerifications bounds outbound dials made to verify the
// listen addresses of peers that connected to us.
const maxConcurrentVerifications = 16

// trackPeer returns the peer on conn as a Node and feeds it to the routing
// table. The node ID is always the TLS-authenticated one.
//
// For a connection we dialed, the dialed address is verified, so the node
// is added directly. For an inbound connection the address is the IP
// observed on the socket plus the listen port the peer advertises (the
// only self-reported field we use: trusting a self-reported IP would let
// a peer point other nodes at arbitrary hosts), and it is only added
// after we dial it back successfully (see verifyInbound).
func (s *Service) trackPeer(conn *network.Connection, listenPort uint32) Node {
	peerID := conn.PeerID()
	ip, portStr := splitHostPort(conn.RemoteAddr().String())

	if conn.Outbound() {
		port, _ := strconv.Atoi(portStr)
		node := Node{ID: peerID, IP: ip, Port: port}
		s.addVerified(node)
		return node
	}

	if listenPort == 0 || listenPort > 65535 {
		return Node{ID: peerID, IP: ip}
	}
	node := Node{ID: peerID, IP: ip, Port: int(listenPort)}
	if existing, ok := s.table.FindNode(peerID); ok && existing.Endpoint() == node.Endpoint() {
		s.table.Touch(peerID)
	} else {
		s.verifyInbound(node)
	}
	return node
}

// addVerified adds a verified node to the routing table. If its bucket is
// full, the bucket's oldest node is pinged in the background and only
// replaced if it does not answer.
func (s *Service) addVerified(node Node) {
	res, oldest := s.table.AddNode(node)
	if res != BucketFull {
		return
	}

	s.pendingMu.Lock()
	if s.pendingEvict[oldest.ID] {
		s.pendingMu.Unlock()
		return
	}
	s.pendingEvict[oldest.ID] = true
	s.pendingMu.Unlock()

	go func() {
		defer func() {
			s.pendingMu.Lock()
			delete(s.pendingEvict, oldest.ID)
			s.pendingMu.Unlock()
		}()
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		defer cancel()
		if _, err := s.query(ctx, oldest, protocol.MessageType_MSG_DHT_PING, ""); err != nil {
			s.table.Replace(oldest, node)
		} else {
			s.table.Touch(oldest.ID)
		}
	}()
}

// verifyInbound dials node's advertised address with its ID pinned and,
// if it answers a PING, adds it to the routing table. The dialed
// connection is kept in the pool.
func (s *Service) verifyInbound(node Node) {
	s.pendingMu.Lock()
	if s.pendingVerify[node.ID] {
		s.pendingMu.Unlock()
		return
	}
	select {
	case s.verifySem <- struct{}{}:
	default:
		// Too many verifications in flight; the peer will be retried on
		// its next request.
		s.pendingMu.Unlock()
		return
	}
	s.pendingVerify[node.ID] = true
	s.pendingMu.Unlock()

	go func() {
		defer func() {
			<-s.verifySem
			s.pendingMu.Lock()
			delete(s.pendingVerify, node.ID)
			s.pendingMu.Unlock()
		}()

		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		defer cancel()
		conn, err := network.Dial(ctx, node.Endpoint(), s.id.ClientTLSConfig(node.ID))
		if err != nil {
			return
		}
		conn.SetRouter(s.router)
		conn.Start()
		s.pool.add(conn)
		// The PING's response is handled by trackPeer, which adds the
		// node now that the connection is outbound.
		_, _ = s.sendDHTRequest(ctx, conn, protocol.MessageType_MSG_DHT_PING, "")
	}()
}

// refreshInterval is how often the routing table is refreshed and expired
// provider records are swept. Var so tests can shrink it.
var refreshInterval = 10 * time.Minute

// maxBootstrapBackoff caps the delay between failed join attempts.
const maxBootstrapBackoff = time.Minute

// maintain joins the network through the bootstrap peers (retrying with
// exponential backoff until the routing table is non-empty), then
// periodically refreshes the routing table and sweeps expired provider
// records. If the table ever empties, it re-joins through the bootstrap
// peers.
func (s *Service) maintain(ctx context.Context) {
	backoff := time.Second
	for len(s.bootstrap) > 0 && s.table.Size() == 0 {
		s.bootstrapFrom(s.bootstrap)
		if s.table.Size() > 0 {
			break
		}
		slog.Warn("could not join the network; retrying", "retry_in", backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > maxBootstrapBackoff {
			backoff = maxBootstrapBackoff
		}
	}

	ticker := time.NewTicker(refreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		s.store.Sweep()
		if s.table.Size() == 0 {
			s.bootstrapFrom(s.bootstrap)
			continue
		}
		// Refresh: a self-lookup keeps our neighbourhood current, and a
		// lookup for a random ID refreshes distant buckets.
		lookupCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		s.lookup(lookupCtx, s.self.ID, false, 0)
		s.lookup(lookupCtx, randomID(), false, 0)
		cancel()
	}
}

// RoutingTableSize returns the number of verified peers in the routing
// table.
func (s *Service) RoutingTableSize() int {
	return s.table.Size()
}

// bootstrapFrom contacts each address in addrs and then looks up our own
// ID to populate the routing table.
func (s *Service) bootstrapFrom(addrs []string) {
	for _, addr := range addrs {
		conn, err := s.pool.connectAddr(context.Background(), addr)
		if err != nil {
			slog.Warn("bootstrap peer unreachable", "addr", addr, "err", err)
			continue
		}

		// Look up our own ID to discover peers near us.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		resp, err := s.sendDHTRequest(ctx, conn, protocol.MessageType_MSG_DHT_FIND_NODE, s.self.ID)
		cancel()
		if err != nil {
			slog.Warn("bootstrap lookup failed", "addr", addr, "err", err)
			continue
		}

		slog.Debug("bootstrap peer answered", "addr", addr, "peers", len(resp.CloserPeers))
	}

	// Look up our own ID through the network to fill the routing table
	// with the nodes closest to us (the standard Kademlia join).
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s.lookup(ctx, s.self.ID, false, 0)
}

//---------------------------------------------------------------------
// Helpers

func randomID() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

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
