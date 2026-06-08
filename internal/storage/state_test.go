package storage

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

func TestStateStoreRoundtrip(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "state.json")

	// 1. Initial creation
	ss, err := NewStateStore(filePath, "file-abc")
	if err != nil {
		t.Fatalf("Failed to create StateStore: %v", err)
	}

	state := ss.GetState()
	if state.FileID != "file-abc" {
		t.Errorf("Expected FileID 'file-abc', got %q", state.FileID)
	}
	if state.Status != "downloading" {
		t.Errorf("Expected Status 'downloading', got %q", state.Status)
	}

	// 2. Modify State
	if err := ss.MarkChunkDownloaded("chunk_0"); err != nil {
		t.Fatalf("Failed to mark downloaded: %v", err)
	}
	if err := ss.MarkChunkVerified("chunk_0"); err != nil {
		t.Fatalf("Failed to mark verified: %v", err)
	}
	if err := ss.UpdateStatus("completed"); err != nil {
		t.Fatalf("Failed to update status: %v", err)
	}

	// 3. Load from existing file
	ss2, err := NewStateStore(filePath, "file-abc")
	if err != nil {
		t.Fatalf("Failed to load StateStore: %v", err)
	}

	state2 := ss2.GetState()
	if state2.Status != "completed" {
		t.Errorf("Expected loaded Status 'completed', got %q", state2.Status)
	}
	if !state2.Downloaded["chunk_0"] {
		t.Error("Expected chunk_0 to be downloaded")
	}
	if !state2.Verified["chunk_0"] {
		t.Error("Expected chunk_0 to be verified")
	}
}

func TestStateStoreConcurrency(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "state.json")

	ss, err := NewStateStore(filePath, "file-concurrency")
	if err != nil {
		t.Fatalf("Failed to create StateStore: %v", err)
	}

	const workers = 10
	const updatesPerWorker = 30
	var wg sync.WaitGroup

	// Concurrently write downloaded/verified flags
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < updatesPerWorker; i++ {
				chunkKey := fmt.Sprintf("chunk_%d_%d", workerID, i)
				_ = ss.MarkChunkDownloaded(chunkKey)
				if i%2 == 0 {
					_ = ss.MarkChunkVerified(chunkKey)
				}
				// Concurrent read
				_ = ss.GetState()
			}
		}(w)
	}

	wg.Wait()

	finalState := ss.GetState()
	expectedDownloadedCount := workers * updatesPerWorker
	if len(finalState.Downloaded) != expectedDownloadedCount {
		t.Errorf("Expected %d downloaded chunks, got %d", expectedDownloadedCount, len(finalState.Downloaded))
	}
}
