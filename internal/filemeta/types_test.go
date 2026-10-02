package filemeta

import (
	"strings"
	"testing"
)

// TestFileMetaValidateRejectsMalformedManifests reproduces the manifests a
// malicious/compromised provider could send: before Validate() existed, a
// downloader trusted NumChunks directly into `make(chan int, meta.NumChunks)`
// and indexed `meta.Chunks[i]` for i in [0, NumChunks), which panicked the
// whole process on a negative or oversized NumChunks.
func TestFileMetaValidateRejectsMalformedManifests(t *testing.T) {
	cases := []struct {
		name string
		meta FileMeta
	}{
		{
			name: "negative num_chunks",
			meta: FileMeta{FileName: "m.bin", NumChunks: -1, Chunks: nil},
		},
		{
			name: "path traversal in file name",
			meta: FileMeta{FileName: "../../home/user/.bashrc"},
		},
		{
			name: "num_chunks exceeds chunks length",
			meta: FileMeta{
				FileName:  "m.bin",
				NumChunks: 5,
				Chunks:    []ChunkMeta{{Index: 0, Hash: "sha256:abc", Size: 1}},
			},
		},
		{
			name: "num_chunks less than chunks length",
			meta: FileMeta{
				FileName:  "m.bin",
				NumChunks: 1,
				Chunks: []ChunkMeta{
					{Index: 0, Hash: "sha256:abc", Size: 1},
					{Index: 1, Hash: "sha256:def", Size: 1},
				},
			},
		},
		{
			name: "chunk index out of order",
			meta: FileMeta{
				FileName:  "m.bin",
				NumChunks: 2,
				Chunks: []ChunkMeta{
					{Index: 1, Hash: "sha256:abc", Size: 1},
					{Index: 0, Hash: "sha256:def", Size: 1},
				},
			},
		},
		{
			name: "chunk missing hash",
			meta: FileMeta{
				FileName:  "m.bin",
				NumChunks: 1,
				Chunks:    []ChunkMeta{{Index: 0, Hash: "", Size: 1}},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.meta.Validate(); err == nil {
				t.Fatalf("Validate() = nil, want error for malformed manifest")
			}
		})
	}
}

func TestFileMetaValidateAcceptsWellFormedManifest(t *testing.T) {
	meta := FileMeta{
		FileName:  "model.bin",
		NumChunks: 2,
		Chunks: []ChunkMeta{
			{Index: 0, Hash: "sha256:abc", Size: 1},
			{Index: 1, Hash: "sha256:def", Size: 1},
		},
	}
	if err := meta.Validate(); err != nil {
		t.Fatalf("Validate() = %v, want nil for well-formed manifest", err)
	}
}

func TestFileMetaValidateAcceptsEmptyManifest(t *testing.T) {
	meta := FileMeta{FileName: "empty.bin", NumChunks: 0, Chunks: nil}
	if err := meta.Validate(); err != nil {
		t.Fatalf("Validate() = %v, want nil for empty manifest", err)
	}
}

func TestValidateFileName(t *testing.T) {
	bad := []string{
		"",
		".",
		"..",
		"../evil",
		"a/b",
		`a\b`,
		"/etc/passwd",
		`C:evil`,
		"nul\x00byte",
		"bell\x07",
		"bad\xffutf8",
		strings.Repeat("a", 256),
	}
	for _, name := range bad {
		if err := ValidateFileName(name); err == nil {
			t.Errorf("ValidateFileName(%q) = nil, want error", name)
		}
	}

	good := []string{"model.bin", "llama-3 8B.gguf", ".hidden", "模型.safetensors"}
	for _, name := range good {
		if err := ValidateFileName(name); err != nil {
			t.Errorf("ValidateFileName(%q) = %v, want nil", name, err)
		}
	}
}
