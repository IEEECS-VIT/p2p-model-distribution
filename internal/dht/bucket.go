package dht

//bucket represents a k-bucket in the routing table.
type Bucket struct {
	Peers []Node
	Size  int
}

func (b *Bucket) Add(n Node) {
	if n.IsZero() {
		return
	}

	if b.Has(n.ID) {
		b.Remove(n.ID)
	}

	if b.Size > 0 && len(b.Peers) >= b.Size {
		b.Peers = b.Peers[1:]
	}

	b.Peers = append(b.Peers, n)
}

func (b *Bucket) Has(id string) bool {
	for _, peer := range b.Peers {
		if peer.ID == id {
			return true
		}
	}

	return false
}

func (b *Bucket) Remove(id string) {
	filtered := b.Peers[:0]
	for _, peer := range b.Peers {
		if peer.ID != id {
			filtered = append(filtered, peer)
		}
	}

	b.Peers = filtered
}

func (b *Bucket) List() []Node {
	peers := make([]Node, len(b.Peers))
	copy(peers, b.Peers)
	return peers
}
