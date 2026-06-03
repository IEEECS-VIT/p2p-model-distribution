package filemeta

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

const manifestVersion = "v1"

func BuildManifest(
	srcPath string,
	fileID string,
	chunkSize int,
	chunkDir string,
	manifestDir string,
) (FileMeta, string, string, error) {
	if chunkSize <= 0 {
		return FileMeta{}, "", "", fmt.Errorf("invalid chunk size: %d", chunkSize)
	}

	f, err := os.Open(srcPath)
	if err != nil {
		return FileMeta{}, "", "", fmt.Errorf("open source: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return FileMeta{}, "", "", fmt.Errorf("stat source: %w", err)
	}

	if chunkDir != "" {
		if err := os.MkdirAll(chunkDir, 0755); err != nil {
			return FileMeta{}, "", "", fmt.Errorf("create chunk dir: %w", err)
		}
	}

	fileHasher := sha256.New()
	reader := io.TeeReader(f, fileHasher)

	var chunks []ChunkMeta
	buf := make([]byte, chunkSize)
	index := 0

	for {
		n, readErr := io.ReadFull(reader, buf)
		if n == 0 {
			break
		}
		if readErr != nil && readErr != io.ErrUnexpectedEOF {
			return FileMeta{}, "", "", fmt.Errorf("read chunk %d: %w", index, readErr)
		}

		data := buf[:n]
		chunkDigest := sha256.Sum256(data)
		chunkHash := "sha256:" + hex.EncodeToString(chunkDigest[:])

		chunkPath := filepath.Join(chunkDir, fmt.Sprintf("%d.chunk", index))
		if err := os.WriteFile(chunkPath, data, 0644); err != nil {
			return FileMeta{}, "", "", fmt.Errorf("write chunk %d: %w", index, err)
		}

		chunks = append(chunks, ChunkMeta{
			Index: index,
			CID:   chunkHash,
			Hash:  chunkHash,
			Size:  n,
		})
		index++

		if readErr == io.ErrUnexpectedEOF {
			break
		}
	}

	meta := FileMeta{
		FileID:    fileID,
		FileName:  filepath.Base(srcPath),
		FileSize:  info.Size(),
		ModelHash: "sha256:" + hex.EncodeToString(fileHasher.Sum(nil)),
		ChunkSize: chunkSize,
		NumChunks: len(chunks),
		Version:   manifestVersion,
		CreatedAt: time.Now().Unix(),
		Chunks:    chunks,
	}

	cid, err := GenerateManifestCID(meta)
	if err != nil {
		return FileMeta{}, "", "", err
	}

	if manifestDir != "" {
		if err := os.MkdirAll(manifestDir, 0755); err != nil {
			return FileMeta{}, "", "", fmt.Errorf("create manifest dir: %w", err)
		}
	}

	outPath := filepath.Join(manifestDir, cid+".json")
	if err := SaveManifest(meta, outPath); err != nil {
		return FileMeta{}, "", "", fmt.Errorf("save manifest: %w", err)
	}

	return meta, cid, outPath, nil
}

func SaveManifest(meta FileMeta, path string) error {
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(path, data, 0644)
}

func LoadManifest(path string) (FileMeta, error) {
	var meta FileMeta

	data, err := os.ReadFile(path)
	if err != nil {
		return meta, err
	}

	err = json.Unmarshal(data, &meta)

	return meta, err
}

func GenerateManifestCID(meta FileMeta) (string, error) {
	data, err := json.Marshal(meta)
	if err != nil {
		return "", err
	}

	hash := sha256.Sum256(data)

	return hex.EncodeToString(hash[:]), nil
}
