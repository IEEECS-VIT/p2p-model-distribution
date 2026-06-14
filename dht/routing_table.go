package dht

import (
	"bytes"
	"sort"
)

// routingTable holds k-buckets for a node.
type RoutingTable struct {
	SelfID  string
	Buckets []*Bucket
}

const (
	K     = 20
	Alpha = 3
)

func NewRoutingTable(selfID string) *RoutingTable {
	buckets := make([]*Bucket, 256)
	for i := range buckets {
		buckets[i] = &Bucket{Size: K}
	}

	return &RoutingTable{SelfID: selfID, Buckets: buckets}
}

func (rt *RoutingTable) AddNode(node Node) {
	if rt == nil || node.IsZero() || node.ID == rt.SelfID {
		return
	}

	bucket := rt.bucketFor(node.ID)
	if bucket == nil {
		return
	}

	bucket.Add(node)
}

func (rt *RoutingTable) ClosestNodes(targetID string, count int) []Node {
	if rt == nil || count <= 0 {
		return nil
	}

	var peers []Node
	seen := make(map[string]struct{}) //deduplication
	for _, bucket := range rt.Buckets {
		for _, peer := range bucket.Peers {
			if peer.IsZero() {
				continue
			}
			if _, ok := seen[peer.ID]; ok {
				continue
			}
			seen[peer.ID] = struct{}{} 
			peers = append(peers, peer)
		}
	}

	sort.SliceStable(peers, func(i, j int) bool {
		return bytes.Compare(distanceBytes(targetID, peers[i].ID), distanceBytes(targetID, peers[j].ID)) < 0
	})

	if len(peers) > count {
		peers = peers[:count]
	}

	return peers
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
