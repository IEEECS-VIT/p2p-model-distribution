package chunker_test

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/IEEECS-VIT/p2p-model-distribution/internal/chunker"
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

	chunkDir := t.TempDir()
	const chunkSize = 256 * 1024
	chunks, modelHash, err := chunker.SplitFile(srcPath, chunkDir, chunkSize)
	if err != nil {
		t.Fatalf("SplitFile: %v", err)
	}

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
		if err := chunker.VerifyChunk(data, c.Hash); err != nil {
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
	if err := chunker.AssembleChunks(chunkDir, outPath, chunks); err != nil {
		t.Fatalf("AssembleChunks: %v", err)
	}

	result, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(original, result) {
		t.Error("reassembled file differs from original")
	}

	if err := chunker.VerifyFile(outPath, modelHash); err != nil {
		t.Errorf("VerifyFile: %v", err)
	}
}

func TestSingleChunk(t *testing.T) {
	data := []byte("hello world this is a small model file")
	srcPath := t.TempDir() + "/small.bin"
	os.WriteFile(srcPath, data, 0644)

	chunks, _, err := chunker.SplitFile(srcPath, t.TempDir(), 256*1024)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 1 {
		t.Errorf("expected 1 chunk, got %d", len(chunks))
	}
	if chunks[0].Size != len(data) {
		t.Errorf("size mismatch")
	}
}

func TestVerifyChunkRejectsCorruption(t *testing.T) {
	data := []byte("legitimate chunk data")
	hash := chunker.HashBytes(data)

	corrupted := make([]byte, len(data))
	copy(corrupted, data)
	corrupted[5] ^= 0xFF

	if err := chunker.VerifyChunk(corrupted, hash); err == nil {
		t.Error("expected error for corrupted chunk, got nil")
	}
}
