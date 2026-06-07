package filemeta

import (
	"fmt"
	"os"
	"path/filepath"
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
func VerifyAllChunks(chunkDir string, chunks []ChunkMeta) []int {
	var failed []int
	for _, c := range chunks {
		data, err := os.ReadFile(filepath.Join(chunkDir, fmt.Sprintf("%d.chunk", c.Index)))
		if err != nil || VerifyChunk(data, c.Hash) != nil {
			failed = append(failed, c.Index)
		}
	}
	return failed
}
