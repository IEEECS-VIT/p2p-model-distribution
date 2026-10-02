package filemeta

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
)

func VerifyChunk(data []byte, expectedHash string) error {
	got := HashBytes(data)
	if got != expectedHash {
		return fmt.Errorf("chunk hash mismatch: got %s, want %s", got, expectedHash)
	}
	return nil
}

func VerifyFile(filePath, modelHash string) error {
	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("open for verify: %w", err)
	}
	defer f.Close()

	got, err := HashReader(f)
	if err != nil {
		return err
	}
	if got != modelHash {
		return fmt.Errorf("file hash mismatch: got %s, want %s", got, modelHash)
	}
	return nil
}

// VerifyAllChunks verifies every chunk file in chunkDir against the manifest.
// Returns a list of indices that failed — empty slice means all OK.
// This function runs verification concurrently using a worker pool.
func VerifyAllChunks(chunkDir string, chunks []ChunkMeta) []int {
	if len(chunks) == 0 {
		return nil
	}

	numWorkers := runtime.NumCPU()
	if numWorkers > len(chunks) {
		numWorkers = len(chunks)
	}
	if numWorkers < 1 {
		numWorkers = 1
	}

	type task struct {
		index int
		hash  string
	}

	tasks := make(chan task, len(chunks))
	results := make(chan int, len(chunks))

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range tasks {
				chunkPath := filepath.Join(chunkDir, fmt.Sprintf("%d.chunk", t.index))
				data, err := os.ReadFile(chunkPath)
				if err != nil || VerifyChunk(data, t.hash) != nil {
					results <- t.index
				}
			}
		}()
	}

	for _, c := range chunks {
		tasks <- task{index: c.Index, hash: c.Hash}
	}
	close(tasks)

	wg.Wait()
	close(results)

	var failed []int
	for idx := range results {
		failed = append(failed, idx)
	}

	// Keep results sorted deterministically
	sort.Ints(failed)
	return failed
}
