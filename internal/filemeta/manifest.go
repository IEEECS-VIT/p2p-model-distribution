package filemeta

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"time"
)

func BuildManifest(
	fileID string,
	fileName string,
	fileSize int64,
	modelHash string,
	chunkSize int,
	chunks []ChunkMeta,
) FileMeta {

	return FileMeta{
		FileID:    fileID,
		FileName:  fileName,
		FileSize:  fileSize,
		ModelHash: modelHash,
		ChunkSize: chunkSize,
		NumChunks: len(chunks),
		Version:   "v1",
		CreatedAt: time.Now().Unix(),
		Chunks:    chunks,
	}
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
