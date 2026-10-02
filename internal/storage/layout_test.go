package storage

import (
	"path/filepath"
	"testing"
)

func TestLayoutPaths(t *testing.T) {
	tests := []struct {
		name         string
		baseDir      string
		fileID       string
		wantFile     string
		wantChunk    string
		wantManifest string
	}{
		{
			name:         "DefaultBaseDir",
			baseDir:      "",
			fileID:       "file-123",
			wantFile:     filepath.Join(DefaultBaseDir, "file-123"),
			wantChunk:    filepath.Join(DefaultBaseDir, "file-123", "chunks"),
			wantManifest: filepath.Join(DefaultBaseDir, "file-123", "manifest.json"),
		},
		{
			name:         "CustomBaseDir",
			baseDir:      "custom/path",
			fileID:       "file-456",
			wantFile:     filepath.Join("custom/path", "file-456"),
			wantChunk:    filepath.Join("custom/path", "file-456", "chunks"),
			wantManifest: filepath.Join("custom/path", "file-456", "manifest.json"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := NewLayout(tt.baseDir)
			if got := l.FileDir(tt.fileID); got != tt.wantFile {
				t.Errorf("FileDir() = %q, want %q", got, tt.wantFile)
			}
			if got := l.ChunksDir(tt.fileID); got != tt.wantChunk {
				t.Errorf("ChunksDir() = %q, want %q", got, tt.wantChunk)
			}
			if got := l.ManifestPath(tt.fileID); got != tt.wantManifest {
				t.Errorf("ManifestPath() = %q, want %q", got, tt.wantManifest)
			}
		})
	}
}
