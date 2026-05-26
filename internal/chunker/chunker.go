package chunker

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/IEEECS-VIT/p2p-model-distribution/internal/filemeta"
)

func SplitFile(srcPath, chunkDir string, chunkSize int) ([]filemeta.ChunkMeta, string, error) {
	f, err := os.Open(srcPath)
	if err != nil {
		return nil, "", fmt.Errorf("open source: %w", err)
	}
	defer f.Close()

	if err := os.MkdirAll(chunkDir, 0755); err != nil {
		return nil, "", fmt.Errorf("mkdir chunk dir: %w", err)
	}

	fileHasher := sha256.New()
	reader := io.TeeReader(f, fileHasher)

	var chunks []filemeta.ChunkMeta
	buf := make([]byte, chunkSize)
	index := 0

	for {
		n, readErr := io.ReadFull(reader, buf)
		if n == 0 {
			break
		}
		if readErr != nil && readErr != io.ErrUnexpectedEOF {
			return nil, "", fmt.Errorf("read chunk %d: %w", index, readErr)
		}

		data := buf[:n]
		chunkHash := HashBytes(data)

		outPath := filepath.Join(chunkDir, fmt.Sprintf("%d.chunk", index))
		if err := os.WriteFile(outPath, data, 0644); err != nil {
			return nil, "", fmt.Errorf("write chunk %d: %w", index, err)
		}

		chunks = append(chunks, filemeta.ChunkMeta{
			Index: index,
			Hash:  chunkHash,
			Size:  n,
		})
		index++

		if readErr == io.ErrUnexpectedEOF {
			break
		}
	}

	modelHash := "sha256:" + hex.EncodeToString(fileHasher.Sum(nil))
	return chunks, modelHash, nil
}
