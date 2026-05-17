package dht

//bucket represents a k-bucket in the routing table.
type Bucket struct {
	Peers []Node
}

func (b *Bucket) Add(n Node) {
	b.Peers = append(b.Peers, n)
}
