package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/IEEECS-VIT/p2p-model-distribution/internal/filemeta"
)

// Store handles the file system storage layout, writes chunk files,
// and saves/loads manifests.
type Store struct {
	layout *Layout
}

// NewStore creates a new Store instance with the given base directory.
func NewStore(baseDir string) *Store {
	return &Store{
		layout: NewLayout(baseDir),
	}
}

// Layout returns the underlying storage layout configuration.
func (s *Store) Layout() *Layout {
	return s.layout
}

// InitializeFileDirectories creates the chunk and base directories for a specific fileID.
func (s *Store) InitializeFileDirectories(fileID string) error {
	if !ValidFileID(fileID) {
		return fmt.Errorf("initialize directories for %q: %w", fileID, ErrInvalidFileID)
	}

	chunksDir := s.layout.ChunksDir(fileID)
	if err := os.MkdirAll(chunksDir, 0755); err != nil {
		return fmt.Errorf("create chunks dir: %w", err)
	}
	return nil
}

// SaveManifest stores the manifest file inside the file's storage directory.
func (s *Store) SaveManifest(meta filemeta.FileMeta) error {
	fileID := meta.FileID
	if err := s.InitializeFileDirectories(fileID); err != nil {
		return err
	}

	path := s.layout.ManifestPath(fileID)
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}

	if err := writeFileAtomic(path, data); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}
	return nil
}

// LoadManifest reads the manifest file from the file's storage directory.
func (s *Store) LoadManifest(fileID string) (filemeta.FileMeta, error) {
	if !ValidFileID(fileID) {
		return filemeta.FileMeta{}, fmt.Errorf("load manifest for %q: %w", fileID, ErrInvalidFileID)
	}

	path := s.layout.ManifestPath(fileID)
	data, err := os.ReadFile(path)
	if err != nil {
		return filemeta.FileMeta{}, err
	}

	var meta filemeta.FileMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return filemeta.FileMeta{}, fmt.Errorf("unmarshal manifest: %w", err)
	}

	return meta, nil
}

// WriteChunk writes a chunk's content to the chunks storage directory.
func (s *Store) WriteChunk(fileID string, index int, data []byte) error {
	if err := s.InitializeFileDirectories(fileID); err != nil {
		return err
	}

	if index < 0 {
		return fmt.Errorf("write chunk: negative index %d", index)
	}
	chunkPath := filepath.Join(s.layout.ChunksDir(fileID), chunkFileName(index))
	if err := writeFileAtomic(chunkPath, data); err != nil {
		return fmt.Errorf("write chunk %d: %w", index, err)
	}
	return nil
}

// ReadChunk reads a chunk's content from the chunks storage directory.
func (s *Store) ReadChunk(fileID string, index int) ([]byte, error) {
	if !ValidFileID(fileID) {
		return nil, fmt.Errorf("read chunk for %q: %w", fileID, ErrInvalidFileID)
	}

	if index < 0 {
		return nil, fmt.Errorf("read chunk: negative index %d", index)
	}
	chunkPath := filepath.Join(s.layout.ChunksDir(fileID), chunkFileName(index))
	return os.ReadFile(chunkPath)
}

// StoreModel chunks the file at srcPath into the store and saves its
// manifest. Since the file ID is the manifest CID, which is only known once
// the whole file has been read, chunks are first written to a staging
// directory and moved into place once the CID is known.
func (s *Store) StoreModel(srcPath string, chunkSize int) (filemeta.FileMeta, error) {
	f, err := os.Open(srcPath)
	if err != nil {
		return filemeta.FileMeta{}, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return filemeta.FileMeta{}, err
	}
	if !info.Mode().IsRegular() {
		return filemeta.FileMeta{}, fmt.Errorf("%s is not a regular file", srcPath)
	}
	name := filepath.Base(srcPath)
	if err := filemeta.ValidateFileName(name); err != nil {
		return filemeta.FileMeta{}, err
	}

	stagingRoot := filepath.Join(s.layout.BaseDir(), stagingDirName)
	if err := os.MkdirAll(stagingRoot, 0o755); err != nil {
		return filemeta.FileMeta{}, fmt.Errorf("create staging dir: %w", err)
	}
	staging, err := os.MkdirTemp(stagingRoot, "import-")
	if err != nil {
		return filemeta.FileMeta{}, fmt.Errorf("create staging dir: %w", err)
	}
	defer os.RemoveAll(staging)

	stagingChunks := filepath.Join(staging, "chunks")
	if err := os.Mkdir(stagingChunks, 0o755); err != nil {
		return filemeta.FileMeta{}, fmt.Errorf("create staging chunks dir: %w", err)
	}

	meta, cid, err := filemeta.BuildManifestWithChunkWriter(f, name, info.Size(), chunkSize, func(index int, data []byte) error {
		return writeFileAtomic(filepath.Join(stagingChunks, chunkFileName(index)), data)
	})
	if err != nil {
		return filemeta.FileMeta{}, err
	}

	if s.HasCompleteFile(cid) {
		// Identical content is already stored; nothing to do.
		return meta, nil
	}

	// Discard any partial state for this ID (e.g. an interrupted
	// download) and move the freshly written chunks into place.
	if err := os.RemoveAll(s.layout.FileDir(cid)); err != nil {
		return filemeta.FileMeta{}, fmt.Errorf("clear existing file dir: %w", err)
	}
	if err := os.Rename(staging, s.layout.FileDir(cid)); err != nil {
		return filemeta.FileMeta{}, fmt.Errorf("move staged chunks into place: %w", err)
	}

	// The manifest is written last: its presence marks the file complete.
	if err := s.SaveManifest(meta); err != nil {
		return filemeta.FileMeta{}, err
	}

	return meta, nil
}

// HasCompleteFile reports whether a manifest whose CID matches fileID is
// stored. Manifests are only written once every chunk is on disk, so this
// is the marker for a complete file.
func (s *Store) HasCompleteFile(fileID string) bool {
	meta, err := s.LoadManifest(fileID)
	if err != nil {
		return false
	}
	return meta.VerifyID(fileID) == nil
}

// CompleteFiles returns the IDs of every complete file in the store.
// Partial downloads, the staging area and anything else in the base
// directory are skipped.
func (s *Store) CompleteFiles() ([]string, error) {
	entries, err := os.ReadDir(s.layout.BaseDir())
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var ids []string
	for _, e := range entries {
		if e.IsDir() && ValidFileID(e.Name()) && s.HasCompleteFile(e.Name()) {
			ids = append(ids, e.Name())
		}
	}
	return ids, nil
}

// writeFileAtomic writes data to path via a synced temporary file and a
// rename, so readers never observe a partially written file.
func writeFileAtomic(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath) // no-op once renamed

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

func chunkFileName(index int) string {
	return fmt.Sprintf("%d.chunk", index)
}
