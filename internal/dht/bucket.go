package dht

// Bucket is a k-bucket: up to Size peers ordered from least to most
// recently seen.
type Bucket struct {
	Peers []Node
	Size  int
}

func (b *Bucket) index(id string) int {
	for i, peer := range b.Peers {
		if peer.ID == id {
			return i
		}
	}
	return -1
}

// Has reports whether the bucket contains a peer with the given ID.
func (b *Bucket) Has(id string) bool {
	return b.index(id) >= 0
}

// Remove deletes the peer with the given ID, if present.
func (b *Bucket) Remove(id string) {
	if i := b.index(id); i >= 0 {
		b.Peers = append(b.Peers[:i], b.Peers[i+1:]...)
	}
}

// touch moves an existing peer to the most-recently-seen end, updating its
// stored address. It reports whether the peer was present.
func (b *Bucket) touch(n Node) bool {
	i := b.index(n.ID)
	if i < 0 {
		return false
	}
	b.Peers = append(b.Peers[:i], b.Peers[i+1:]...)
	b.Peers = append(b.Peers, n)
	return true
}

func (b *Bucket) full() bool {
	return b.Size > 0 && len(b.Peers) >= b.Size
}

// oldest returns the least recently seen peer.
func (b *Bucket) oldest() Node {
	return b.Peers[0]
}

func (b *Bucket) List() []Node {
	peers := make([]Node, len(b.Peers))
	copy(peers, b.Peers)
	return peers
}
