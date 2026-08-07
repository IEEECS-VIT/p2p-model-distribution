package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestValidFileID(t *testing.T) {
	valid := []string{
		"file-xyz",
		"a1b2c3d4e5f6a7b8", // 16-hex-char generated ID
		"under_score",
		"MixedCase123",
	}
	for _, id := range valid {
		if !ValidFileID(id) {
			t.Errorf("ValidFileID(%q) = false, want true", id)
		}
	}

	invalid := []string{
		"",
		"../../../../etc/passwd",
		"..",
		"foo/bar",
		"foo\\bar",
		"/etc/passwd",
		"foo/../../bar",
		"foo\x00bar",
		string(make([]byte, maxFileIDLength+1)),
	}
	for _, id := range invalid {
		if ValidFileID(id) {
			t.Errorf("ValidFileID(%q) = true, want false", id)
		}
	}
}

// TestStoreRejectsPathTraversalFileID reproduces a malicious peer sending a
// GetMetadataRequest/GetChunkRequest with a FileId crafted to escape the
// store's base directory (e.g. "../../../../tmp/evil"). Before ValidFileID
// was enforced, filepath.Join(baseDir, fileID, ...) would happily resolve
// outside baseDir with no allowlist check, unlike the HTTP seeder path which
// already validates cid against known manifests.
func TestStoreRejectsPathTraversalFileID(t *testing.T) {
	tempDir := t.TempDir()
	store := NewStore(tempDir)

	// A sibling directory outside the store's base dir that a traversal
	// payload might try to reach.
	outsideDir := filepath.Join(filepath.Dir(tempDir), "escaped-"+filepath.Base(tempDir))
	defer os.RemoveAll(outsideDir)

	maliciousIDs := []string{
		"../../../../etc/passwd",
		"..",
		"../" + filepath.Base(outsideDir),
		"a/../../b",
	}

	for _, fileID := range maliciousIDs {
		if err := store.InitializeFileDirectories(fileID); !errors.Is(err, ErrInvalidFileID) {
			t.Errorf("InitializeFileDirectories(%q) error = %v, want ErrInvalidFileID", fileID, err)
		}
		if _, err := store.LoadManifest(fileID); !errors.Is(err, ErrInvalidFileID) {
			t.Errorf("LoadManifest(%q) error = %v, want ErrInvalidFileID", fileID, err)
		}
		if _, err := store.ReadChunk(fileID, 0); !errors.Is(err, ErrInvalidFileID) {
			t.Errorf("ReadChunk(%q) error = %v, want ErrInvalidFileID", fileID, err)
		}
	}

	if _, err := os.Stat(outsideDir); !os.IsNotExist(err) {
		t.Fatalf("expected no directory to be created outside the store base dir, but %s exists", outsideDir)
	}
}
