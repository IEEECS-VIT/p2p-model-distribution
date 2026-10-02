package storage

import (
	"bytes"
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
		FileSize:  100,
		ModelHash: filemeta.HashBytes([]byte("model")),
		ChunkSize: filemeta.MinChunkSize,
		NumChunks: 1,
		Version:   filemeta.ManifestVersion,
		Chunks: []filemeta.ChunkMeta{
			{Index: 0, Hash: filemeta.HashBytes([]byte("chunk0")), Size: 100},
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

}

func TestDefaultChunkSizeFallback(t *testing.T) {
	tempDir := t.TempDir()
	store := NewStore(tempDir)
	srcPath := filepath.Join(tempDir, "sample.bin")

	// Create 2.5MB file data
	data := make([]byte, 2*1024*1024+512*1024)
	if err := os.WriteFile(srcPath, data, 0644); err != nil {
		t.Fatalf("failed to write sample file: %v", err)
	}

	// Passing 0 should fall back to 1MB (DefaultChunkSize)
	meta, err := store.StoreModel(srcPath, 0)
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

func TestStoreModelIsContentAddressed(t *testing.T) {
	tempDir := t.TempDir()
	store := NewStore(filepath.Join(tempDir, "store"))
	srcPath := filepath.Join(tempDir, "model.bin")
	if err := os.WriteFile(srcPath, bytes.Repeat([]byte("weights"), 5000), 0o644); err != nil {
		t.Fatal(err)
	}

	meta, err := store.StoreModel(srcPath, filemeta.MinChunkSize)
	if err != nil {
		t.Fatalf("StoreModel: %v", err)
	}
	if err := meta.VerifyID(meta.FileID); err != nil {
		t.Fatalf("FileID is not the manifest CID: %v", err)
	}
	if !store.HasCompleteFile(meta.FileID) {
		t.Fatal("HasCompleteFile = false after StoreModel")
	}

	// Storing identical content again yields the same ID and is a no-op.
	again, err := store.StoreModel(srcPath, filemeta.MinChunkSize)
	if err != nil {
		t.Fatalf("second StoreModel: %v", err)
	}
	if again.FileID != meta.FileID {
		t.Fatalf("same content got different IDs: %s vs %s", meta.FileID, again.FileID)
	}

	// The staging area is cleaned up.
	entries, err := os.ReadDir(filepath.Join(tempDir, "store", stagingDirName))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("staging dir not cleaned up: %d entries", len(entries))
	}
}

func TestCompleteFilesSkipsPartialDownloads(t *testing.T) {
	tempDir := t.TempDir()
	store := NewStore(filepath.Join(tempDir, "store"))
	src := filepath.Join(tempDir, "model.bin")
	if err := os.WriteFile(src, bytes.Repeat([]byte("x"), 5000), 0o644); err != nil {
		t.Fatal(err)
	}
	meta, err := store.StoreModel(src, filemeta.MinChunkSize)
	if err != nil {
		t.Fatal(err)
	}
	// A partial download: chunks but no manifest.
	if err := store.WriteChunk("aaaa", 0, []byte("partial")); err != nil {
		t.Fatal(err)
	}

	ids, err := store.CompleteFiles()
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != meta.FileID {
		t.Fatalf("CompleteFiles = %v, want [%s]", ids, meta.FileID)
	}
}
