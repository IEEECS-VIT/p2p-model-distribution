package seeder_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/IEEECS-VIT/p2p-model-distribution/internal/filemeta"
	"github.com/IEEECS-VIT/p2p-model-distribution/internal/seeder"
	"github.com/IEEECS-VIT/p2p-model-distribution/internal/storage"
)

// setup creates a temp seeder with a single chunked test file.
// Returns the seeder, the manifest CID, and a cleanup func.
func setup(t *testing.T) (*seeder.Seeder, string, func()) {
	t.Helper()

	dataDir := t.TempDir()
	manifestDir := filepath.Join(dataDir, "manifests")
	chunkDir := filepath.Join(dataDir, "chunks")
	os.MkdirAll(manifestDir, 0755)

	// 3MB test file — produces 3 chunks at 1MB each
	const fileSize = 3 * 1024 * 1024
	data := make([]byte, fileSize)
	for i := range data {
		data[i] = byte(i % 251)
	}

	// Build manifest
	meta, cid, err := filemeta.BuildManifest(
		strings.NewReader(string(data)),
		"test-file-id",
		"test.bin",
		int64(fileSize),
		filemeta.DefaultChunkSize,
	)
	if err != nil {
		t.Fatalf("BuildManifest: %v", err)
	}

	// Save manifest
	if err := filemeta.SaveManifest(meta, filepath.Join(manifestDir, cid+".json")); err != nil {
		t.Fatalf("SaveManifest: %v", err)
	}

	// Write chunks
	store, _ := storage.NewChunkStore(chunkDir)
	offset := 0
	for _, chunk := range meta.Chunks {
		end := offset + chunk.Size
		if err := store.WriteChunk(cid, chunk.Index, data[offset:end]); err != nil {
			t.Fatalf("WriteChunk %d: %v", chunk.Index, err)
		}
		offset = end
	}

	s, err := seeder.New(manifestDir, chunkDir)
	if err != nil {
		t.Fatalf("seeder.New: %v", err)
	}

	return s, cid, func() {} // TempDir cleaned up automatically
}

func TestHealth(t *testing.T) {
	s, _, _ := setup(t)
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

func TestGetManifest(t *testing.T) {
	s, cid, _ := setup(t)
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	resp, err := http.Get(fmt.Sprintf("%s/manifest/%s", srv.URL, cid))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var meta filemeta.FileMeta
	if err := json.NewDecoder(resp.Body).Decode(&meta); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}

	if meta.NumChunks != 3 {
		t.Errorf("expected 3 chunks, got %d", meta.NumChunks)
	}
	if meta.FileSize != 3*1024*1024 {
		t.Errorf("unexpected file size: %d", meta.FileSize)
	}
}

func TestGetChunk(t *testing.T) {
	s, cid, _ := setup(t)
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	// Fetch chunk 0
	resp, err := http.Get(fmt.Sprintf("%s/chunk/%s/0", srv.URL, cid))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)

	// Verify size
	if len(body) != filemeta.DefaultChunkSize {
		t.Errorf("expected chunk size %d, got %d", filemeta.DefaultChunkSize, len(body))
	}

	// Verify hash matches X-Chunk-Hash header
	expectedHash := resp.Header.Get("X-Chunk-Hash")
	digest := sha256.Sum256(body)
	actualHash := "sha256:" + hex.EncodeToString(digest[:])
	if actualHash != expectedHash {
		t.Errorf("hash mismatch:\n  got  %s\n  want %s", actualHash, expectedHash)
	}
}

func TestChunkHashMatchesManifest(t *testing.T) {
	s, cid, _ := setup(t)
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	// Get manifest first
	resp, _ := http.Get(fmt.Sprintf("%s/manifest/%s", srv.URL, cid))
	var meta filemeta.FileMeta
	json.NewDecoder(resp.Body).Decode(&meta)

	// Verify every chunk's hash matches manifest
	for _, chunk := range meta.Chunks {
		resp, err := http.Get(fmt.Sprintf("%s/chunk/%s/%d", srv.URL, cid, chunk.Index))
		if err != nil {
			t.Fatal(err)
		}

		body, _ := io.ReadAll(resp.Body)
		digest := sha256.Sum256(body)
		actual := "sha256:" + hex.EncodeToString(digest[:])

		if actual != chunk.Hash {
			t.Errorf("chunk %d: hash mismatch\n  got  %s\n  want %s",
				chunk.Index, actual, chunk.Hash)
		}
	}
}

func TestReassemblyMatchesOriginal(t *testing.T) {
	s, cid, _ := setup(t)
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	// Get manifest
	resp, _ := http.Get(fmt.Sprintf("%s/manifest/%s", srv.URL, cid))
	var meta filemeta.FileMeta
	json.NewDecoder(resp.Body).Decode(&meta)

	// Download all chunks in order, concatenate
	var reassembled []byte
	for i := 0; i < meta.NumChunks; i++ {
		r, err := http.Get(fmt.Sprintf("%s/chunk/%s/%d", srv.URL, cid, i))
		if err != nil {
			t.Fatalf("chunk %d: %v", i, err)
		}
		body, _ := io.ReadAll(r.Body)
		reassembled = append(reassembled, body...)
	}

	// Verify against ModelHash
	digest := sha256.Sum256(reassembled)
	actual := "sha256:" + hex.EncodeToString(digest[:])
	if actual != meta.ModelHash {
		t.Errorf("reassembled file hash mismatch\n  got  %s\n  want %s", actual, meta.ModelHash)
	}
}

func TestErrors(t *testing.T) {
	s, cid, _ := setup(t)
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	cases := []struct {
		url      string
		wantCode int
		desc     string
	}{
		{"/manifest/000000deadbeef", 404, "unknown cid"},
		{fmt.Sprintf("/chunk/%s/999", cid), 404, "index out of range"},
		{fmt.Sprintf("/chunk/%s/abc", cid), 400, "non-numeric index"},
		{"/chunk/", 400, "missing cid and index"},
	}

	for _, tc := range cases {
		resp, err := http.Get(srv.URL + tc.url)
		if err != nil {
			t.Fatalf("%s: %v", tc.desc, err)
		}
		if resp.StatusCode != tc.wantCode {
			t.Errorf("%s: expected %d, got %d", tc.desc, tc.wantCode, resp.StatusCode)
		}
	}
}
