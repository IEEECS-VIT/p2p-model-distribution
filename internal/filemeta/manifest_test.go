package filemeta

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestBuildManifestPopulatesDerivedFields(t *testing.T) {
	srcPath := filepath.Join(t.TempDir(), "model.bin")
	srcData := []byte("012345678901234567890123456789")
	if err := os.WriteFile(srcPath, srcData, 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	chunkDir := t.TempDir()
	manifestDir := t.TempDir()

	meta, cid, outPath, err := BuildManifest(
		srcPath,
		"file-123",
		10,
		chunkDir,
		manifestDir,
	)
	if err != nil {
		t.Fatalf("BuildManifest: %v", err)
	}

	if meta.FileID != "file-123" {
		t.Fatalf("FileID = %q, want %q", meta.FileID, "file-123")
	}
	if meta.FileName != "model.bin" {
		t.Fatalf("FileName = %q, want %q", meta.FileName, "model.bin")
	}
	if meta.FileSize != int64(len(srcData)) {
		t.Fatalf("FileSize = %d, want %d", meta.FileSize, len(srcData))
	}
	sum := sha256.Sum256(srcData)
	expectedModelHash := "sha256:" + hex.EncodeToString(sum[:])
	if meta.ModelHash != expectedModelHash {
		t.Fatalf("ModelHash = %q, want %q", meta.ModelHash, expectedModelHash)
	}
	if meta.Version != manifestVersion {
		t.Fatalf("Version = %q, want %q", meta.Version, manifestVersion)
	}
	if meta.CreatedAt == 0 {
		t.Fatal("CreatedAt = 0, want non-zero unix timestamp")
	}
	if meta.NumChunks != 3 {
		t.Fatalf("NumChunks = %d, want %d", meta.NumChunks, 3)
	}
	if meta.Chunks[0].CID != meta.Chunks[0].Hash {
		t.Fatalf("chunk 0 CID = %q, want %q", meta.Chunks[0].CID, meta.Chunks[0].Hash)
	}
	if meta.Chunks[2].Size != 10 {
		t.Fatalf("chunk 2 size = %d, want %d", meta.Chunks[2].Size, 10)
	}

	expectedCID, err := GenerateManifestCID(meta)
	if err != nil {
		t.Fatalf("GenerateManifestCID: %v", err)
	}
	if cid != expectedCID {
		t.Fatalf("cid = %q, want %q", cid, expectedCID)
	}

	expectedPath := filepath.Join(manifestDir, cid+".json")
	if outPath != expectedPath {
		t.Fatalf("outPath = %q, want %q", outPath, expectedPath)
	}
	if _, err := os.Stat(outPath); err != nil {
		t.Fatalf("expected BuildManifest to write file, stat err = %v", err)
	}
	for i := range meta.Chunks {
		chunkPath := filepath.Join(chunkDir, fmt.Sprintf("%d.chunk", i))
		if _, err := os.Stat(chunkPath); err != nil {
			t.Fatalf("expected chunk file %d.chunk, stat err = %v", i, err)
		}
	}
}

func TestGenerateManifestCIDStable(t *testing.T) {
	meta := FileMeta{
		FileID:    "file-123",
		FileName:  "model.bin",
		FileSize:  30,
		ModelHash: "sha256:model",
		ChunkSize: 1024,
		NumChunks: 1,
		Version:   manifestVersion,
		CreatedAt: 1234567890,
		Chunks: []ChunkMeta{
			{Index: 0, CID: "sha256:aaa", Hash: "sha256:aaa", Size: 30},
		},
	}

	first, err := GenerateManifestCID(meta)
	if err != nil {
		t.Fatalf("first GenerateManifestCID: %v", err)
	}

	second, err := GenerateManifestCID(meta)
	if err != nil {
		t.Fatalf("second GenerateManifestCID: %v", err)
	}

	if first != second {
		t.Fatalf("GenerateManifestCID unstable: %q != %q", first, second)
	}
}

func TestSaveLoadManifestRoundTrip(t *testing.T) {
	meta := FileMeta{
		FileID:    "file-123",
		FileName:  "model.bin",
		FileSize:  30,
		ModelHash: "sha256:model",
		ChunkSize: 1024,
		NumChunks: 1,
		Version:   manifestVersion,
		CreatedAt: 1234567890,
		Chunks: []ChunkMeta{
			{Index: 0, CID: "sha256:aaa", Hash: "sha256:aaa", Size: 30},
		},
	}
	outPath := filepath.Join(t.TempDir(), "manifest.json")

	if err := SaveManifest(meta, outPath); err != nil {
		t.Fatalf("SaveManifest: %v", err)
	}

	loaded, err := LoadManifest(outPath)
	if err != nil {
		t.Fatalf("LoadManifest: %v", err)
	}

	if !reflect.DeepEqual(loaded, meta) {
		t.Fatalf("loaded manifest mismatch:\n got: %#v\nwant: %#v", loaded, meta)
	}
}
