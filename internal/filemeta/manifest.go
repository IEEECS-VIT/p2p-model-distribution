package filemeta

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"
)

const manifestVersion = "v1"

func BuildManifest(
	src io.Reader,
	fileID string,
	fileName string,
	fileSize int64,
	chunkSize int,
) (FileMeta, string, error) {
	return BuildManifestWithChunkWriter(src, fileID, fileName, fileSize, chunkSize, nil)
}

func BuildManifestWithChunkWriter(
	src io.Reader,
	fileID string,
	fileName string,
	fileSize int64,
	chunkSize int,
	writeChunk func(index int, data []byte) error,
) (FileMeta, string, error) {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}

	fileHasher := sha256.New()
	reader := io.TeeReader(src, fileHasher)

	var chunks []ChunkMeta
	buf := make([]byte, chunkSize)
	index := 0

	for {
		n, readErr := io.ReadFull(reader, buf)
		if n == 0 {
			break
		}
		if readErr != nil && readErr != io.ErrUnexpectedEOF {
			return FileMeta{}, "", fmt.Errorf("read chunk %d: %w", index, readErr)
		}

		data := append([]byte(nil), buf[:n]...)
		chunkDigest := sha256.Sum256(data)
		chunkHash := "sha256:" + hex.EncodeToString(chunkDigest[:])

		chunks = append(chunks, ChunkMeta{
			Index: index,
			CID:   chunkHash,
			Hash:  chunkHash,
			Size:  n,
		})

		if writeChunk != nil {
			if err := writeChunk(index, data); err != nil {
				return FileMeta{}, "", fmt.Errorf("write chunk %d: %w", index, err)
			}
		}

		index++
		if readErr == io.ErrUnexpectedEOF {
			break
		}
	}

	meta := FileMeta{
		FileID:    fileID,
		FileName:  fileName,
		FileSize:  fileSize,
		ModelHash: "sha256:" + hex.EncodeToString(fileHasher.Sum(nil)),
		ChunkSize: chunkSize,
		NumChunks: len(chunks),
		Version:   manifestVersion,
		CreatedAt: time.Now().Unix(),
		Chunks:    chunks,
	}

	cid, err := GenerateManifestCID(meta)
	if err != nil {
		return FileMeta{}, "", err
	}

	return meta, cid, nil
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

// LoadManifestBytes returns the raw JSON bytes of a manifest file.
// Use this when you need to serve the manifest over HTTP without
// round-tripping through unmarshal → marshal.
func LoadManifestBytes(path string) ([]byte, error) {
	return os.ReadFile(path)
}

func GenerateManifestCID(meta FileMeta) (string, error) {
	// Use only content-identifying fields to compute the CID.
	// This ensures that the CID is identical for the same content, even if CreatedAt,
	// FileName, or FileID differ.
	type StableMeta struct {
		FileSize  int64       `json:"file_size"`
		ModelHash string      `json:"model_hash"`
		ChunkSize int         `json:"chunk_size"`
		NumChunks int         `json:"num_chunks"`
		Version   string      `json:"version"`
		Chunks    []ChunkMeta `json:"chunks"`
	}

	stable := StableMeta{
		FileSize:  meta.FileSize,
		ModelHash: meta.ModelHash,
		ChunkSize: meta.ChunkSize,
		Version:   meta.Version,
		NumChunks: meta.NumChunks,
		Chunks:    meta.Chunks,
	}

	data, err := json.Marshal(stable)
	if err != nil {
		return "", err
	}

	hash := sha256.Sum256(data)

	return hex.EncodeToString(hash[:]), nil
}
