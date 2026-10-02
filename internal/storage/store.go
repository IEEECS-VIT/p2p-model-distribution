package storage

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/IEEECS-VIT/p2p-model-distribution/internal/filemeta"
)

// Store handles the file system storage layout, writes chunk files,
// and saves/loads manifests and state files.
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

	// Write atomically using a temporary file
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("write tmp manifest: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("rename manifest: %w", err)
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

	chunkPath := filepath.Join(s.layout.ChunksDir(fileID), fmt.Sprintf("%d.chunk", index))
	tmpPath := chunkPath + ".tmp"

	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("write tmp chunk %d: %w", index, err)
	}
	if err := os.Rename(tmpPath, chunkPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("rename chunk %d: %w", index, err)
	}

	return nil
}

// ReadChunk reads a chunk's content from the chunks storage directory.
func (s *Store) ReadChunk(fileID string, index int) ([]byte, error) {
	if !ValidFileID(fileID) {
		return nil, fmt.Errorf("read chunk for %q: %w", fileID, ErrInvalidFileID)
	}

	chunkPath := filepath.Join(s.layout.ChunksDir(fileID), fmt.Sprintf("%d.chunk", index))
	return os.ReadFile(chunkPath)
}

// WriteChunksFromReader splits a stream into chunk files and stores them directly to disk.
func (s *Store) WriteChunksFromReader(src io.Reader, fileID string, chunkSize int) error {
	if chunkSize <= 0 {
		chunkSize = filemeta.DefaultChunkSize
	}
	if err := s.InitializeFileDirectories(fileID); err != nil {
		return err
	}

	buf := make([]byte, chunkSize)
	index := 0

	for {
		n, readErr := io.ReadFull(src, buf)
		if n == 0 {
			break
		}
		if readErr != nil && readErr != io.ErrUnexpectedEOF {
			return fmt.Errorf("read chunk %d: %w", index, readErr)
		}

		if err := s.WriteChunk(fileID, index, buf[:n]); err != nil {
			return err
		}
		index++

		if readErr == io.ErrUnexpectedEOF {
			break
		}
	}

	return nil
}

// StoreModel takes a source file path and chunks it, saves chunks and the manifest.
func (s *Store) StoreModel(srcPath string, fileID string, chunkSize int) (filemeta.FileMeta, string, error) {
	f, err := os.Open(srcPath)
	if err != nil {
		return filemeta.FileMeta{}, "", err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return filemeta.FileMeta{}, "", err
	}

	// Build the manifest while writing chunk files in a single streaming pass.
	if err := s.InitializeFileDirectories(fileID); err != nil {
		return filemeta.FileMeta{}, "", err
	}

	meta, cid, err := filemeta.BuildManifestWithChunkWriter(f, fileID, filepath.Base(srcPath), info.Size(), chunkSize, func(index int, data []byte) error {
		return s.WriteChunk(fileID, index, data)
	})
	if err != nil {
		return filemeta.FileMeta{}, "", err
	}

	// Save the manifest JSON to disk
	if err := s.SaveManifest(meta); err != nil {
		return filemeta.FileMeta{}, "", err
	}

	return meta, cid, nil
}
