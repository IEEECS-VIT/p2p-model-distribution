package filemeta_test

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/IEEECS-VIT/p2p-model-distribution/internal/filemeta"
	"github.com/IEEECS-VIT/p2p-model-distribution/internal/storage"
)

func TestRoundTrip(t *testing.T) {
	const fileSize = 5*1024*1024 + 123
	original := make([]byte, fileSize)
	for i := range original {
		original[i] = byte(i % 251)
	}
	srcDir := t.TempDir()
	srcPath := srcDir + "/model.bin"
	if err := os.WriteFile(srcPath, original, 0644); err != nil {
		t.Fatal(err)
	}

	const chunkSize = 256 * 1024
	store := storage.NewStore(t.TempDir())
	meta, err := store.StoreModel(srcPath, chunkSize)
	if err != nil {
		t.Fatalf("StoreModel failed: %v", err)
	}
	chunks := meta.Chunks
	modelHash := meta.ModelHash
	chunkDir := store.Layout().ChunksDir(meta.FileID)

	expectedChunks := (fileSize + chunkSize - 1) / chunkSize
	if len(chunks) != expectedChunks {
		t.Errorf("expected %d chunks, got %d", expectedChunks, len(chunks))
	}

	for _, c := range chunks {
		data, err := os.ReadFile(fmt.Sprintf("%s/%d.chunk", chunkDir, c.Index))
		if err != nil {
			t.Errorf("read chunk %d: %v", c.Index, err)
			continue
		}
		if err := filemeta.VerifyChunk(data, c.Hash); err != nil {
			t.Errorf("chunk %d verify: %v", c.Index, err)
		}
		if c.Index < len(chunks)-1 && c.Size != chunkSize {
			t.Errorf("chunk %d: expected size %d, got %d", c.Index, chunkSize, c.Size)
		}
	}

	lastSize := fileSize % chunkSize
	if lastSize == 0 {
		lastSize = chunkSize
	}
	if chunks[len(chunks)-1].Size != lastSize {
		t.Errorf("last chunk size: expected %d, got %d", lastSize, chunks[len(chunks)-1].Size)
	}

	outPath := t.TempDir() + "/reassembled.bin"
	if err := filemeta.AssembleChunks(chunkDir, outPath, chunks, modelHash); err != nil {
		t.Fatalf("AssembleChunks: %v", err)
	}

	result, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(original, result) {
		t.Error("reassembled file differs from original")
	}

	if err := filemeta.VerifyFile(outPath, modelHash); err != nil {
		t.Errorf("VerifyFile: %v", err)
	}
}

func TestSingleChunk(t *testing.T) {
	data := []byte("hello world this is a small model file")
	srcPath := t.TempDir() + "/small.bin"
	os.WriteFile(srcPath, data, 0644)

	store := storage.NewStore(t.TempDir())
	meta, err := store.StoreModel(srcPath, 256*1024)
	if err != nil {
		t.Fatal(err)
	}
	chunks := meta.Chunks
	if len(chunks) != 1 {
		t.Errorf("expected 1 chunk, got %d", len(chunks))
	}
	if chunks[0].Size != len(data) {
		t.Errorf("size mismatch")
	}
}

func TestVerifyChunkRejectsCorruption(t *testing.T) {
	data := []byte("legitimate chunk data")
	hash := filemeta.HashBytes(data)

	corrupted := make([]byte, len(data))
	copy(corrupted, data)
	corrupted[5] ^= 0xFF

	if err := filemeta.VerifyChunk(corrupted, hash); err == nil {
		t.Error("expected error for corrupted chunk, got nil")
	}
}

func TestAssembleChunksHandlesOutofOrder(t *testing.T) {
	chunkDir := t.TempDir()
	outPath := t.TempDir() + "/ordered.bin"

	// Write three chunks
	c0 := []byte("Chunk0-")
	c1 := []byte("Chunk1-")
	c2 := []byte("Chunk2")

	os.WriteFile(fmt.Sprintf("%s/0.chunk", chunkDir), c0, 0644)
	os.WriteFile(fmt.Sprintf("%s/1.chunk", chunkDir), c1, 0644)
	os.WriteFile(fmt.Sprintf("%s/2.chunk", chunkDir), c2, 0644)

	// Provide chunks out of order
	chunks := []filemeta.ChunkMeta{
		{Index: 2, Size: len(c2)},
		{Index: 0, Size: len(c0)},
		{Index: 1, Size: len(c1)},
	}

	expectedHash := filemeta.HashBytes([]byte("Chunk0-Chunk1-Chunk2"))
	if err := filemeta.AssembleChunks(chunkDir, outPath, chunks, expectedHash); err != nil {
		t.Fatalf("AssembleChunks failed: %v", err)
	}

	result, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatal(err)
	}

	expected := "Chunk0-Chunk1-Chunk2"
	if string(result) != expected {
		t.Errorf("Expected assembled file content %q, got %q", expected, string(result))
	}
}

func TestAssembleChunksRejectsGaps(t *testing.T) {
	chunkDir := t.TempDir()
	outPath := t.TempDir() + "/broken.bin"

	// Write chunks but skip index 1
	os.WriteFile(fmt.Sprintf("%s/0.chunk", chunkDir), []byte("A"), 0644)
	os.WriteFile(fmt.Sprintf("%s/2.chunk", chunkDir), []byte("C"), 0644)

	chunks := []filemeta.ChunkMeta{
		{Index: 0, Size: 1},
		{Index: 2, Size: 1},
	}

	if err := filemeta.AssembleChunks(chunkDir, outPath, chunks, filemeta.HashBytes([]byte("AC"))); err == nil {
		t.Error("Expected error due to missing chunk at index 1, got nil")
	}
	if _, err := os.Stat(outPath); !os.IsNotExist(err) {
		t.Errorf("output exists after failed assembly (stat err = %v)", err)
	}
}

func TestAssembleChunksLeavesNoFileOnHashMismatch(t *testing.T) {
	chunkDir := t.TempDir()
	outDir := t.TempDir()
	outPath := outDir + "/model.bin"

	os.WriteFile(fmt.Sprintf("%s/0.chunk", chunkDir), []byte("corrupted"), 0644)
	chunks := []filemeta.ChunkMeta{{Index: 0, Size: 9}}

	err := filemeta.AssembleChunks(chunkDir, outPath, chunks, filemeta.HashBytes([]byte("genuine")))
	if err == nil {
		t.Fatal("AssembleChunks succeeded despite hash mismatch")
	}
	entries, _ := os.ReadDir(outDir)
	if len(entries) != 0 {
		t.Fatalf("output dir not empty after failed assembly: %v", entries)
	}
}

func TestVerifyAllChunksParallel(t *testing.T) {
	chunkDir := t.TempDir()

	c0 := []byte("data0")
	c1 := []byte("data1-corrupted")
	c2 := []byte("data2")

	h0 := filemeta.HashBytes(c0)
	h1 := filemeta.HashBytes([]byte("data1-expected")) // will mismatch
	h2 := filemeta.HashBytes(c2)

	os.WriteFile(fmt.Sprintf("%s/0.chunk", chunkDir), c0, 0644)
	os.WriteFile(fmt.Sprintf("%s/1.chunk", chunkDir), c1, 0644)
	os.WriteFile(fmt.Sprintf("%s/2.chunk", chunkDir), c2, 0644)

	chunks := []filemeta.ChunkMeta{
		{Index: 0, Hash: h0, Size: len(c0)},
		{Index: 1, Hash: h1, Size: len(c1)},
		{Index: 2, Hash: h2, Size: len(c2)},
	}

	failed := filemeta.VerifyAllChunks(chunkDir, chunks)
	if len(failed) != 1 || failed[0] != 1 {
		t.Errorf("Expected only chunk 1 to fail verification, got %v", failed)
	}
}
