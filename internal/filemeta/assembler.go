package filemeta

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

func AssembleChunks(chunkDir, outputPath string, chunks []ChunkMeta) error {
	out, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	defer out.Close()

	for _, c := range chunks {
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
