package dht

import (
	"fmt"
	"sync"
	"testing"
)

// TestProviderStoreConcurrentAccess reproduces concurrent STORE/FIND_VALUE
// traffic against a single ProviderStore. Before the mutex was added this
// crashed the process with "fatal error: concurrent map read and map
// write" under `go test -race`, or nondeterministically in production.
func TestProviderStoreConcurrentAccess(t *testing.T) {
	store := NewStore()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(3)

		go func(i int) {
			defer wg.Done()
			store.Add(fmt.Sprintf("chunk-%d", i%5), fmt.Sprintf("peer-%d", i))
		}(i)

		go func(i int) {
			defer wg.Done()
			_ = store.Get(fmt.Sprintf("chunk-%d", i%5))
		}(i)

		go func(i int) {
			defer wg.Done()
			store.Remove(fmt.Sprintf("chunk-%d", i%5), fmt.Sprintf("peer-%d", i))
		}(i)
	}
	wg.Wait()
}
