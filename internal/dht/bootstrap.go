package dht

// SeedNodes returns well-known bootstrap peers for initial DHT discovery.
// In production these should be stable seed nodes. For development/testing,
// bootstrap addresses are configured via CLI flags (see cmd/node/main.go).
func SeedNodes() []Node {
	return nil
}
