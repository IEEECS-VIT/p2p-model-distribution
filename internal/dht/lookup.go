package dht

import (
	"bytes"
	"context"
	"sort"
	"time"

	"github.com/IEEECS-VIT/p2p-model-distribution/internal/protocol"
)

// rpcTimeout bounds a single DHT RPC (including dialing) during a lookup.
var rpcTimeout = 5 * time.Second

// maxShortlist bounds how many candidates a lookup tracks, so responses
// from many peers cannot grow a lookup's memory without bound.
const maxShortlist = 4 * K

type candidateState int

const (
	unqueried candidateState = iota
	inflight
	succeeded
	failed
)

type candidate struct {
	node     Node
	distance []byte
	state    candidateState
}

type queryResult struct {
	c    *candidate
	resp *protocol.DHTResponse
	err  error
}

// lookup runs an iterative Kademlia lookup for target: it repeatedly
// queries the Alpha closest not-yet-queried nodes it knows of, learning
// closer nodes from each response, until the K closest known nodes have
// all been queried.
//
// With findValue set it sends FIND_VALUE and collects providers, stopping
// early once maxProviders are known (if maxProviders > 0). It returns the
// closest nodes that answered, and any providers found.
//
// Nodes learned from responses are only claims; a node is added to the
// routing table only after we have dialed it with its ID pinned and it
// has answered.
func (s *Service) lookup(ctx context.Context, target string, findValue bool, maxProviders int) ([]Node, []Node) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	msgType := protocol.MessageType_MSG_DHT_FIND_NODE
	if findValue {
		msgType = protocol.MessageType_MSG_DHT_FIND_VALUE
	}

	known := make(map[string]*candidate)
	var shortlist []*candidate
	addCandidate := func(n Node) {
		if n.ID == s.self.ID || known[n.ID] != nil {
			return
		}
		c := &candidate{node: n, distance: distanceBytes(target, n.ID)}
		known[n.ID] = c
		shortlist = append(shortlist, c)
	}
	sortShortlist := func() {
		sort.Slice(shortlist, func(i, j int) bool {
			return bytes.Compare(shortlist[i].distance, shortlist[j].distance) < 0
		})
		// Drop the farthest unqueried candidates beyond the bound; queried
		// ones are kept so they are never re-added and re-queried.
		if len(shortlist) > maxShortlist {
			kept := shortlist[:0]
			for i, c := range shortlist {
				if i < maxShortlist || c.state != unqueried {
					kept = append(kept, c)
				}
			}
			shortlist = kept
		}
	}

	for _, n := range s.table.ClosestNodes(target, K) {
		addCandidate(n)
	}
	sortShortlist()

	seenProviders := map[string]bool{s.self.ID: true}
	var providers []Node

	results := make(chan queryResult, Alpha)
	running := 0

	for {
		// Launch queries to the closest unqueried candidates among the K
		// closest live (non-failed) ones, keeping at most Alpha in flight.
		live := 0
		for _, c := range shortlist {
			if running >= Alpha || live >= K {
				break
			}
			if c.state == failed {
				continue
			}
			live++
			if c.state != unqueried {
				continue
			}
			c.state = inflight
			running++
			go func(c *candidate) {
				resp, err := s.query(ctx, c.node, msgType, target)
				results <- queryResult{c, resp, err}
			}(c)
		}

		if running == 0 {
			break
		}

		r := <-results
		running--
		if r.err != nil {
			r.c.state = failed
			continue
		}
		r.c.state = succeeded

		closer := r.resp.CloserPeers
		if len(closer) > K {
			closer = closer[:K]
		}
		for _, p := range closer {
			if n, ok := nodeFromWire(p); ok {
				addCandidate(n)
			}
		}
		sortShortlist()

		if findValue {
			for _, p := range r.resp.Providers {
				if n, ok := nodeFromWire(p); ok && !seenProviders[n.ID] {
					seenProviders[n.ID] = true
					providers = append(providers, n)
				}
			}
			if maxProviders > 0 && len(providers) >= maxProviders {
				break
			}
		}
	}

	// Drain in-flight queries (cancelled by the deferred cancel) so their
	// goroutines don't block on the results channel.
	cancel()
	for ; running > 0; running-- {
		<-results
	}

	var closest []Node
	for _, c := range shortlist {
		if c.state == succeeded {
			closest = append(closest, c.node)
			if len(closest) == K {
				break
			}
		}
	}
	return closest, providers
}

// query dials node (pinning its ID) and sends a single DHT request.
func (s *Service) query(ctx context.Context, node Node, msgType protocol.MessageType, key string) (*protocol.DHTResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	conn, err := s.pool.connect(ctx, node)
	if err != nil {
		return nil, err
	}
	return s.sendDHTRequest(ctx, conn, msgType, key)
}
