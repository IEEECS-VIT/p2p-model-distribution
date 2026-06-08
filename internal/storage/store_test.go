package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/IEEECS-VIT/p2p-model-distribution/internal/filemeta"
)

func TestStoreOperations(t *testing.T) {
	tempDir := t.TempDir()
	store := NewStore(tempDir)
	fileID := "file-xyz"

	// 1. InitializeDirectories
	if err := store.InitializeFileDirectories(fileID); err != nil {
		t.Fatalf("InitializeFileDirectories failed: %v", err)
	}

	chunksDir := store.Layout().ChunksDir(fileID)
	if info, err := os.Stat(chunksDir); err != nil || !info.IsDir() {
		t.Fatalf("expected chunks directory to exist and be a directory")
	}

	// 2. Write and Read Chunk
	chunkData := []byte("chunk contents here")
	if err := store.WriteChunk(fileID, 0, chunkData); err != nil {
		t.Fatalf("WriteChunk failed: %v", err)
	}

	readData, err := store.ReadChunk(fileID, 0)
	if err != nil {
		t.Fatalf("ReadChunk failed: %v", err)
	}
	if !bytes.Equal(chunkData, readData) {
		t.Errorf("read chunk contents %q, want %q", readData, chunkData)
	}

	// 3. Save and Load Manifest
	meta := filemeta.FileMeta{
		FileID:    fileID,
		FileName:  "test.bin",
		FileSize:  1000,
		ModelHash: "sha256:abcd",
		ChunkSize: 100,
		NumChunks: 10,
		Version:   "v1",
		Chunks: []filemeta.ChunkMeta{
			{Index: 0, CID: "sha256:chunk0", Hash: "sha256:chunk0", Size: 100},
		},
	}

	if err := store.SaveManifest(meta); err != nil {
		t.Fatalf("SaveManifest failed: %v", err)
	}

	loadedMeta, err := store.LoadManifest(fileID)
	if err != nil {
		t.Fatalf("LoadManifest failed: %v", err)
	}
	if loadedMeta.FileID != meta.FileID || loadedMeta.FileName != meta.FileName {
		t.Errorf("loaded manifest mismatch: got %v, want %v", loadedMeta, meta)
	}

	// 4. WriteChunksFromReader
	readerData := []byte("AABBCCDDEEFF")
	reader := bytes.NewReader(readerData)
	if err := store.WriteChunksFromReader(reader, "file-abc", 4); err != nil {
		t.Fatalf("WriteChunksFromReader failed: %v", err)
	}

	// Verify chunk files exist
	for i := 0; i < 3; i++ {
		chunkPath := filepath.Join(store.Layout().ChunksDir("file-abc"), fmt.Sprintf("%d.chunk", i))
		if _, err := os.Stat(chunkPath); err != nil {
			t.Errorf("expected chunk %d to exist", i)
		}
	}
}

func TestDefaultChunkSizeFallback(t *testing.T) {
	tempDir := t.TempDir()
	store := NewStore(tempDir)
	srcPath := filepath.Join(tempDir, "sample.bin")

	// Create 2.5MB file data
	data := make([]byte, 2*1024*1024 + 512*1024)
	if err := os.WriteFile(srcPath, data, 0644); err != nil {
		t.Fatalf("failed to write sample file: %v", err)
	}

	// Passing 0 should fall back to 1MB (DefaultChunkSize)
	meta, _, err := store.StoreModel(srcPath, "file-fallback", 0)
	if err != nil {
		t.Fatalf("StoreModel failed: %v", err)
	}

	if meta.ChunkSize != filemeta.DefaultChunkSize {
		t.Errorf("expected chunk size to be %d (1MB), got %d", filemeta.DefaultChunkSize, meta.ChunkSize)
	}
	if meta.NumChunks != 3 {
		t.Errorf("expected 3 chunks for 2.5MB data with 1MB chunk size, got %d", meta.NumChunks)
	}
}
