package filemeta

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// AssembleChunks concatenates the chunk files in chunkDir into outputPath
// and checks the result against modelHash. The output is written to a
// temporary file in the same directory, synced, verified, and only then
// renamed into place, so outputPath never holds a partial or corrupt file
// (an existing file at outputPath is left untouched on failure).
func AssembleChunks(chunkDir, outputPath string, chunks []ChunkMeta, modelHash string) error {
	sortedChunks := make([]ChunkMeta, len(chunks))
	copy(sortedChunks, chunks)
	sort.Slice(sortedChunks, func(i, j int) bool {
		return sortedChunks[i].Index < sortedChunks[j].Index
	})

	tmp, err := os.CreateTemp(filepath.Dir(outputPath), "."+filepath.Base(outputPath)+".partial-*")
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath) // no-op once renamed

	hasher := sha256.New()
	if err := copyChunks(io.MultiWriter(tmp, hasher), chunkDir, sortedChunks); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync output: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close output: %w", err)
	}

	if got := "sha256:" + hex.EncodeToString(hasher.Sum(nil)); got != modelHash {
		return fmt.Errorf("file hash mismatch: got %s, want %s", got, modelHash)
	}

	// CreateTemp uses 0600; give the final file the usual permissions.
	if err := os.Chmod(tmpPath, 0o644); err != nil {
		return fmt.Errorf("chmod output: %w", err)
	}
	if err := os.Rename(tmpPath, outputPath); err != nil {
		return fmt.Errorf("move output into place: %w", err)
	}
	return nil
}

func copyChunks(w io.Writer, chunkDir string, chunks []ChunkMeta) error {
	for i, c := range chunks {
		if c.Index != i {
			return fmt.Errorf("invalid chunk layout: expected chunk index %d at position %d, got %d", i, i, c.Index)
		}

		cf, err := os.Open(filepath.Join(chunkDir, fmt.Sprintf("%d.chunk", c.Index)))
		if err != nil {
			return fmt.Errorf("open chunk %d: %w", c.Index, err)
		}
		_, err = io.Copy(w, cf)
		cf.Close()
		if err != nil {
			return fmt.Errorf("write chunk %d: %w", c.Index, err)
		}
	}
	return nil
}
