package storage

import (
	"path/filepath"
)

// DefaultBaseDir is the default root directory for data files.
const DefaultBaseDir = "data/files"

// Layout defines the paths for storing files, chunks, manifests, and state files.
type Layout struct {
	baseDir string
}

// NewLayout creates a new Layout instance. If baseDir is empty, DefaultBaseDir is used.
func NewLayout(baseDir string) *Layout {
	if baseDir == "" {
		baseDir = DefaultBaseDir
	}
	return &Layout{
		baseDir: baseDir,
	}
}

// BaseDir returns the root base directory configured for this Layout.
func (l *Layout) BaseDir() string {
	return l.baseDir
}

// FileDir returns the path to the directory for a specific fileID.
func (l *Layout) FileDir(fileID string) string {
	return filepath.Join(l.baseDir, fileID)
}

// ChunksDir returns the path to the chunks directory for a specific fileID.
func (l *Layout) ChunksDir(fileID string) string {
	return filepath.Join(l.FileDir(fileID), "chunks")
}

// ManifestPath returns the path to the manifest.json file for a specific fileID.
func (l *Layout) ManifestPath(fileID string) string {
	return filepath.Join(l.FileDir(fileID), "manifest.json")
}

// StatePath returns the path to the state.json file for a specific fileID.
func (l *Layout) StatePath(fileID string) string {
	return filepath.Join(l.FileDir(fileID), "state.json")
}