package dht

import (
	"fmt"
	"sync"
	"testing"
)

// TestRoutingTableConcurrentAccess reproduces concurrent AddNode/ClosestNodes/
// FindNode calls, mirroring how DHT RPC handlers
// touch the routing table from per-connection goroutines. Before locking was
// added this raced on Bucket.Peers under `go test -race`.
func TestRoutingTableConcurrentAccess(t *testing.T) {
	rt := NewRoutingTable("self")

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(3)

		go func(i int) {
			defer wg.Done()
			rt.AddNode(Node{ID: fmt.Sprintf("peer-%d", i), IP: "127.0.0.1", Port: 9000 + i})
		}(i)

		go func(i int) {
			defer wg.Done()
			_ = rt.ClosestNodes(fmt.Sprintf("peer-%d", i), K)
		}(i)

		go func(i int) {
			defer wg.Done()
			_, _ = rt.FindNode(fmt.Sprintf("peer-%d", i))
		}(i)
	}
	wg.Wait()
}
