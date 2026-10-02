package dht

import (
	"bytes"
	"net"
	"sort"
	"sync"
)

// RoutingTable holds k-buckets for a node. It is safe for concurrent use,
// since it is read and written from per-connection RPC handler goroutines.
//
// Callers must only add nodes whose ID and address have been verified
// (i.e. we reached the node at that address with its ID pinned), so the
// table never hands out unauthenticated claims to other peers.
type RoutingTable struct {
	SelfID  string
	Buckets []*Bucket

	mu sync.RWMutex
}

const (
	K     = 20
	Alpha = 3

	// IP diversity limits (as in S/Kademlia and libp2p's diversity
	// filter): an attacker controlling one network prefix cannot fill a
	// bucket, or the table, with Sybil identities. Loopback and private
	// addresses are exempt so LAN and local deployments are unaffected.
	maxPerPrefixPerBucket = 2
	maxPerPrefixPerTable  = 10
)

// AddResult describes what AddNode did.
type AddResult int

const (
	// Added means the node was inserted or refreshed.
	Added AddResult = iota
	// BucketFull means the node's bucket is full; the caller should ping
	// the returned oldest node and call Replace if it does not answer.
	BucketFull
	// Rejected means the node was refused (self, zero, or over an IP
	// diversity limit).
	Rejected
)

func NewRoutingTable(selfID string) *RoutingTable {
	buckets := make([]*Bucket, 256)
	for i := range buckets {
		buckets[i] = &Bucket{Size: K}
	}

	return &RoutingTable{SelfID: selfID, Buckets: buckets}
}

// AddNode inserts a verified node or, if it is already present, marks it
// most recently seen (updating its address). When the node's bucket is
// full it returns BucketFull and the bucket's oldest node: Kademlia keeps
// long-lived nodes, so the newcomer only gets in if the oldest node fails
// a liveness check (see Replace).
func (rt *RoutingTable) AddNode(node Node) (AddResult, Node) {
	if rt == nil || node.IsZero() || node.ID == "" || node.ID == rt.SelfID {
		return Rejected, Node{}
	}

	rt.mu.Lock()
	defer rt.mu.Unlock()

	bucket := rt.bucketFor(node.ID)
	if bucket == nil {
		return Rejected, Node{}
	}
	if bucket.touch(node) {
		return Added, Node{}
	}
	if !rt.diversityAllows(bucket, node) {
		return Rejected, Node{}
	}
	if bucket.full() {
		return BucketFull, bucket.oldest()
	}
	bucket.Peers = append(bucket.Peers, node)
	return Added, Node{}
}

// Replace evicts old (if still present) in favour of node. It is called
// after old failed a liveness check following a BucketFull result.
func (rt *RoutingTable) Replace(old, node Node) bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	bucket := rt.bucketFor(node.ID)
	if bucket == nil || !bucket.Has(old.ID) || bucket.Has(node.ID) {
		return false
	}
	bucket.Remove(old.ID)
	if !rt.diversityAllows(bucket, node) {
		bucket.Peers = append([]Node{old}, bucket.Peers...)
		return false
	}
	bucket.Peers = append(bucket.Peers, node)
	return true
}

// Touch marks the node with the given ID as most recently seen.
func (rt *RoutingTable) Touch(id string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if bucket := rt.bucketFor(id); bucket != nil {
		if i := bucket.index(id); i >= 0 {
			bucket.touch(bucket.Peers[i])
		}
	}
}

// Remove deletes the node with the given ID.
func (rt *RoutingTable) Remove(id string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if bucket := rt.bucketFor(id); bucket != nil {
		bucket.Remove(id)
	}
}

// Size returns the number of nodes in the table.
func (rt *RoutingTable) Size() int {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	n := 0
	for _, b := range rt.Buckets {
		n += len(b.Peers)
	}
	return n
}

func (rt *RoutingTable) ClosestNodes(targetID string, count int) []Node {
	if rt == nil || count <= 0 {
		return nil
	}

	rt.mu.RLock()
	defer rt.mu.RUnlock()

	var peers []Node
	for _, bucket := range rt.Buckets {
		peers = append(peers, bucket.Peers...)
	}

	sort.SliceStable(peers, func(i, j int) bool {
		return bytes.Compare(distanceBytes(targetID, peers[i].ID), distanceBytes(targetID, peers[j].ID)) < 0
	})

	if len(peers) > count {
		peers = peers[:count]
	}

	return peers
}

// FindNode looks up a peer by ID across all buckets. Callers must not
// reach into Buckets/Peers directly, since that bypasses locking.
func (rt *RoutingTable) FindNode(id string) (Node, bool) {
	if rt == nil {
		return Node{}, false
	}

	rt.mu.RLock()
	defer rt.mu.RUnlock()

	if bucket := rt.bucketFor(id); bucket != nil {
		if i := bucket.index(id); i >= 0 {
			return bucket.Peers[i], true
		}
	}
	return Node{}, false
}

func (rt *RoutingTable) bucketFor(peerID string) *Bucket {
	if rt == nil || len(rt.Buckets) == 0 {
		return nil
	}

	index := bucketIndex(rt.SelfID, peerID)
	if index < 0 {
		return nil
	}

	if index >= len(rt.Buckets) {
		index = len(rt.Buckets) - 1
	}

	return rt.Buckets[index]
}

// diversityAllows reports whether adding node keeps its network prefix
// within the per-bucket and per-table limits. Callers must hold rt.mu.
func (rt *RoutingTable) diversityAllows(bucket *Bucket, node Node) bool {
	prefix, limited := networkPrefix(node.IP)
	if !limited {
		return true
	}
	inBucket, inTable := 0, 0
	for _, b := range rt.Buckets {
		for _, p := range b.Peers {
			if pp, ok := networkPrefix(p.IP); ok && pp == prefix {
				inTable++
				if b == bucket {
					inBucket++
				}
			}
		}
	}
	return inBucket < maxPerPrefixPerBucket && inTable < maxPerPrefixPerTable
}

// networkPrefix returns the /24 (IPv4) or /48 (IPv6) containing ip, and
// whether diversity limits apply to it (they don't for loopback, private
// or link-local addresses).
func networkPrefix(ip string) (string, bool) {
	parsed := net.ParseIP(ip)
	if parsed == nil || parsed.IsLoopback() || parsed.IsPrivate() || parsed.IsLinkLocalUnicast() || parsed.IsUnspecified() {
		return "", false
	}
	if v4 := parsed.To4(); v4 != nil {
		return v4.Mask(net.CIDRMask(24, 32)).String(), true
	}
	return parsed.Mask(net.CIDRMask(48, 128)).String(), true
}
