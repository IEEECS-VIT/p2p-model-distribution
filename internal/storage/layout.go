package storage

import (
	"errors"
	"path/filepath"
)

// DefaultBaseDir is the default root directory for data files.
const DefaultBaseDir = "data/files"

// ErrInvalidFileID is returned when a caller-supplied file ID is not safe to
// use as a filesystem path component.
var ErrInvalidFileID = errors.New("invalid file id")

// stagingDirName is the directory under the base dir where StoreModel
// writes chunks before the file ID is known. The leading dot makes it an
// invalid file ID, so it can never collide with a real file directory.
const stagingDirName = ".staging"

// maxFileIDLength is a generous bound on file ID length; real IDs are short
// hex strings (16 or 64 chars).
const maxFileIDLength = 256

// ValidFileID reports whether id is safe to use as a single filesystem path
// component. File IDs arrive over the network from other peers (as the
// FileId field of GetMetadataRequest/GetChunkRequest), so this rejects path
// separators, "..", and anything else that could let a peer make Store
// read from or write outside its intended per-file directory.
func ValidFileID(id string) bool {
	if id == "" || len(id) > maxFileIDLength {
		return false
	}
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '-' || r == '_':
		default:
			return false
		}
	}
	return true
}

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
