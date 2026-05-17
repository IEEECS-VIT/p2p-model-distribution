package dht

//routingTable holds k-buckets for a node.
type RoutingTable struct {
	SelfID string
	Buckets []*Bucket
}

const (
	K     = 20
	Alpha = 3
)
