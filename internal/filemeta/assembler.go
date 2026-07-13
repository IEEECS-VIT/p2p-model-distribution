package filemeta

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

func AssembleChunks(chunkDir, outputPath string, chunks []ChunkMeta) error {
	if len(chunks) == 0 {
		return fmt.Errorf("no chunks provided for assembly")
	}

	out, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	defer out.Close()

	// Create a copy of the slice to avoid modifying the caller's slice
	sortedChunks := make([]ChunkMeta, len(chunks))
	copy(sortedChunks, chunks)

	// Sort chunks by Index
	sort.Slice(sortedChunks, func(i, j int) bool {
		return sortedChunks[i].Index < sortedChunks[j].Index
	})

	// Verify chunk indices are contiguous starting from 0
	for i, c := range sortedChunks {
		if c.Index != i {
			return fmt.Errorf("invalid chunk layout: expected chunk index %d at position %d, got %d", i, i, c.Index)
		}

		chunkPath := filepath.Join(chunkDir, fmt.Sprintf("%d.chunk", c.Index))
		cf, err := os.Open(chunkPath)
		if err != nil {
			return fmt.Errorf("open chunk %d: %w", c.Index, err)
		}
		if _, err := io.Copy(out, cf); err != nil {
			cf.Close()
			return fmt.Errorf("write chunk %d: %w", c.Index, err)
		}
		cf.Close()
	}

	return nil
}
