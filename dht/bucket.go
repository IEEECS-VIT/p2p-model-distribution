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
