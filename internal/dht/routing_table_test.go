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

// idsInBucket returns n distinct IDs that all land in the same bucket of a
// table owned by selfID.
func idsInBucket(selfID string, n int) []string {
	byBucket := map[int][]string{}
	for i := 0; ; i++ {
		id := fmt.Sprintf("peer-%d", i)
		b := bucketIndex(selfID, id)
		byBucket[b] = append(byBucket[b], id)
		if len(byBucket[b]) == n {
			return byBucket[b]
		}
	}
}

func TestRoutingTable_FullBucketKeepsOldNodesUntilReplaced(t *testing.T) {
	rt := NewRoutingTable("self")
	ids := idsInBucket("self", K+1)
	for i, id := range ids[:K] {
		if res, _ := rt.AddNode(Node{ID: id, IP: "10.0.0.1", Port: 9000 + i}); res != Added {
			t.Fatalf("AddNode(%d) = %v, want Added", i, res)
		}
	}

	newcomer := Node{ID: ids[K], IP: "10.0.0.1", Port: 9999}
	res, oldest := rt.AddNode(newcomer)
	if res != BucketFull || oldest.ID != ids[0] {
		t.Fatalf("AddNode on full bucket = (%v, %s), want (BucketFull, %s)", res, oldest.ID, ids[0])
	}
	if _, ok := rt.FindNode(newcomer.ID); ok {
		t.Fatal("newcomer was inserted into a full bucket without an eviction")
	}

	// Oldest answered its ping: it stays and becomes most recently seen.
	rt.Touch(oldest.ID)
	if _, next := rt.AddNode(newcomer); next.ID != ids[1] {
		t.Fatalf("after Touch, oldest = %s, want %s", next.ID, ids[1])
	}

	// Oldest failed its ping: it is replaced.
	if !rt.Replace(Node{ID: ids[1]}, newcomer) {
		t.Fatal("Replace returned false")
	}
	if _, ok := rt.FindNode(ids[1]); ok {
		t.Fatal("unresponsive node still present after Replace")
	}
	if _, ok := rt.FindNode(newcomer.ID); !ok {
		t.Fatal("newcomer missing after Replace")
	}
}

func TestRoutingTable_EnforcesIPDiversity(t *testing.T) {
	rt := NewRoutingTable("self")
	ids := idsInBucket("self", maxPerPrefixPerBucket+1)
	for i, id := range ids[:maxPerPrefixPerBucket] {
		if res, _ := rt.AddNode(Node{ID: id, IP: fmt.Sprintf("203.0.113.%d", i+1), Port: 9000}); res != Added {
			t.Fatalf("AddNode(%d) = %v, want Added", i, res)
		}
	}
	if res, _ := rt.AddNode(Node{ID: ids[maxPerPrefixPerBucket], IP: "203.0.113.200", Port: 9000}); res != Rejected {
		t.Fatalf("third node from the same /24 = %v, want Rejected", res)
	}

	// Private and loopback addresses are exempt.
	for i, id := range idsInBucket("self", maxPerPrefixPerBucket+1) {
		if res, _ := rt.AddNode(Node{ID: id + "-lan", IP: "192.168.1.10", Port: 9000 + i}); res == Rejected {
			t.Fatalf("private address rejected by diversity limit")
		}
	}
}

func TestRoutingTable_RejectsSelfAndRefreshesAddress(t *testing.T) {
	rt := NewRoutingTable("self")
	if res, _ := rt.AddNode(Node{ID: "self", IP: "10.0.0.1", Port: 1}); res != Rejected {
		t.Fatal("table accepted its own ID")
	}
	rt.AddNode(Node{ID: "p", IP: "10.0.0.1", Port: 1})
	rt.AddNode(Node{ID: "p", IP: "10.0.0.2", Port: 2})
	if n, _ := rt.FindNode("p"); n.Endpoint() != "10.0.0.2:2" || rt.Size() != 1 {
		t.Fatalf("FindNode = %+v (size %d), want one entry at the new address", n, rt.Size())
	}
}
