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

// BuildManifest reads src in chunkSize pieces and returns the manifest
// and its CID. The CID is also stored in FileMeta.FileID.
func BuildManifest(
	src io.Reader,
	fileName string,
	fileSize int64,
	chunkSize int,
) (FileMeta, string, error) {
	return BuildManifestWithChunkWriter(src, fileName, fileSize, chunkSize, nil)
}

// BuildManifestWithChunkWriter is BuildManifest but also hands every chunk
// to writeChunk (if non-nil) so callers can persist chunks in the same
// streaming pass.
func BuildManifestWithChunkWriter(
	src io.Reader,
	fileName string,
	fileSize int64,
	chunkSize int,
	writeChunk func(index int, data []byte) error,
) (FileMeta, string, error) {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	if chunkSize < MinChunkSize || chunkSize > MaxChunkSize {
		return FileMeta{}, "", fmt.Errorf("chunk size %d outside [%d, %d]", chunkSize, MinChunkSize, MaxChunkSize)
	}
	if n := (fileSize + int64(chunkSize) - 1) / int64(chunkSize); n > MaxChunks {
		return FileMeta{}, "", fmt.Errorf("file needs %d chunks at chunk size %d, max is %d; use a larger chunk size", n, chunkSize, MaxChunks)
	}

	fileHasher := sha256.New()
	reader := io.TeeReader(src, fileHasher)

	var chunks []ChunkMeta
	var total int64
	buf := make([]byte, chunkSize)
	index := 0

	for {
		n, readErr := io.ReadFull(reader, buf)
		if n == 0 {
			if readErr != nil && readErr != io.EOF {
				return FileMeta{}, "", fmt.Errorf("read chunk %d: %w", index, readErr)
			}
			break
		}
		if readErr != nil && readErr != io.ErrUnexpectedEOF {
			return FileMeta{}, "", fmt.Errorf("read chunk %d: %w", index, readErr)
		}

		data := buf[:n]
		chunks = append(chunks, ChunkMeta{
			Index: index,
			Hash:  HashBytes(data),
			Size:  n,
		})
		total += int64(n)

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

	if total != fileSize {
		return FileMeta{}, "", fmt.Errorf("read %d bytes, expected %d (file changed while chunking?)", total, fileSize)
	}

	meta := FileMeta{
		FileName:  fileName,
		FileSize:  fileSize,
		ModelHash: "sha256:" + hex.EncodeToString(fileHasher.Sum(nil)),
		ChunkSize: chunkSize,
		NumChunks: len(chunks),
		Version:   ManifestVersion,
		CreatedAt: time.Now().Unix(),
		Chunks:    chunks,
	}

	cid, err := GenerateManifestCID(meta)
	if err != nil {
		return FileMeta{}, "", err
	}
	meta.FileID = cid

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

// GenerateManifestCID returns the hex SHA-256 of the manifest's
// content-identifying fields. It deliberately excludes FileID (which is
// the CID itself), FileName and CreatedAt, so the same content always gets
// the same ID. FileName is therefore unauthenticated metadata and must be
// validated before use (see ValidateFileName).
//
// The encoding is the JSON serialization of a fixed struct; Go's encoder
// emits struct fields in declaration order with no insignificant
// whitespace, so it is canonical for a given set of values.
func GenerateManifestCID(meta FileMeta) (string, error) {
	type StableMeta struct {
		FileSize  int64       `json:"file_size"`
		ModelHash string      `json:"model_hash"`
		ChunkSize int         `json:"chunk_size"`
		NumChunks int         `json:"num_chunks"`
		Version   string      `json:"version"`
		Chunks    []ChunkMeta `json:"chunks"`
	}

	chunks := meta.Chunks
	if chunks == nil {
		// nil and empty must hash identically ("[]", not "null").
		chunks = []ChunkMeta{}
	}

	stable := StableMeta{
		FileSize:  meta.FileSize,
		ModelHash: meta.ModelHash,
		ChunkSize: meta.ChunkSize,
		Version:   meta.Version,
		NumChunks: meta.NumChunks,
		Chunks:    chunks,
	}

	data, err := json.Marshal(stable)
	if err != nil {
		return "", err
	}

	hash := sha256.Sum256(data)

	return hex.EncodeToString(hash[:]), nil
}
