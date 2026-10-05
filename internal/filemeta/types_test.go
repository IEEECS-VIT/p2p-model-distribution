package filemeta

import (
	"bytes"
	"strings"
	"testing"
)

// validManifest returns a well-formed three-chunk manifest that each
// malformed case below mutates in exactly one way.
func validManifest(t *testing.T) FileMeta {
	t.Helper()
	data := bytes.Repeat([]byte{0xAB}, 2*MinChunkSize+10)
	meta, _, err := BuildManifest(bytes.NewReader(data), "model.bin", int64(len(data)), MinChunkSize)
	if err != nil {
		t.Fatalf("BuildManifest: %v", err)
	}
	return meta
}

// TestFileMetaValidateRejectsMalformedManifests reproduces the manifests a
// malicious/compromised provider could send: before Validate() existed, a
// downloader trusted NumChunks directly into `make(chan int, meta.NumChunks)`
// and indexed `meta.Chunks[i]` for i in [0, NumChunks), which panicked the
// whole process on a negative or oversized NumChunks.
func TestFileMetaValidateRejectsMalformedManifests(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(m *FileMeta)
	}{
		{"unsupported version", func(m *FileMeta) { m.Version = "v1" }},
		{"path traversal in file name", func(m *FileMeta) { m.FileName = "../../home/user/.bashrc" }},
		{"malformed model hash", func(m *FileMeta) { m.ModelHash = "sha256:abc" }},
		{"chunk size too small", func(m *FileMeta) { m.ChunkSize = MinChunkSize - 1 }},
		{"chunk size too large", func(m *FileMeta) { m.ChunkSize = MaxChunkSize + 1 }},
		{"negative file size", func(m *FileMeta) { m.FileSize = -1 }},
		{"negative num_chunks", func(m *FileMeta) { m.NumChunks = -1 }},
		{"too many chunks", func(m *FileMeta) { m.NumChunks = MaxChunks + 1 }},
		{"num_chunks exceeds chunks length", func(m *FileMeta) { m.NumChunks = len(m.Chunks) + 1 }},
		{"num_chunks less than chunks length", func(m *FileMeta) { m.NumChunks = len(m.Chunks) - 1 }},
		{"file size inconsistent with chunks", func(m *FileMeta) { m.FileSize += int64(m.ChunkSize) }},
		{"chunk index out of order", func(m *FileMeta) { m.Chunks[0].Index, m.Chunks[1].Index = 1, 0 }},
		{"chunk missing hash", func(m *FileMeta) { m.Chunks[1].Hash = "" }},
		{"chunk malformed hash", func(m *FileMeta) { m.Chunks[1].Hash = "md5:abc" }},
		{"middle chunk wrong size", func(m *FileMeta) { m.Chunks[1].Size-- }},
		{"last chunk wrong size", func(m *FileMeta) { m.Chunks[2].Size++ }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			meta := validManifest(t)
			tc.mutate(&meta)
			if err := meta.Validate(); err == nil {
				t.Fatalf("Validate() = nil, want error for malformed manifest")
			}
		})
	}
}

func TestFileMetaValidateAcceptsWellFormedManifest(t *testing.T) {
	if err := validManifest(t).Validate(); err != nil {
		t.Fatalf("Validate() = %v, want nil for well-formed manifest", err)
	}
}

func TestFileMetaValidateAcceptsEmptyManifest(t *testing.T) {
	meta, _, err := BuildManifest(bytes.NewReader(nil), "empty.bin", 0, MinChunkSize)
	if err != nil {
		t.Fatalf("BuildManifest: %v", err)
	}
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
		"CON",
		"nul.txt",
		"Aux.tar.gz",
		"com1",
		"LPT9.bin",
		"COM¹",
		"model?.bin",
		`a"b`,
		"trailing.",
		"trailing ",
	}
	for _, name := range bad {
		if err := ValidateFileName(name); err == nil {
			t.Errorf("ValidateFileName(%q) = nil, want error", name)
		}
	}

	good := []string{"model.bin", "llama-3 8B.gguf", ".hidden", "模型.safetensors", "console.bin", "com10.bin", "nullable.txt"}
	for _, name := range good {
		if err := ValidateFileName(name); err != nil {
			t.Errorf("ValidateFileName(%q) = %v, want nil", name, err)
		}
	}
}
