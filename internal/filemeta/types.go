package filemeta

import "fmt"

// DefaultChunkSize is the default size of each chunk (1MB).
const DefaultChunkSize = 1024 * 1024

type ChunkMeta struct {
	Index int    `json:"index"`
	CID   string `json:"cid"`
	Hash  string `json:"hash"`
	Size  int    `json:"size"`
}

type FileMeta struct {
	FileID    string      `json:"file_id"`
	FileName  string      `json:"file_name"`
	FileSize  int64       `json:"file_size"`
	ModelHash string      `json:"model_hash"`
	ChunkSize int         `json:"chunk_size"`
	NumChunks int         `json:"num_chunks"`
	Version   string      `json:"version"`
	CreatedAt int64       `json:"created_at"`
	Chunks    []ChunkMeta `json:"chunks"`
}

// Validate checks structural invariants that downstream code (chunk
// download, reassembly) relies on without further bounds checking.
// It must be called on any FileMeta parsed from an untrusted source
// (e.g. a manifest fetched from a remote peer) before use, since a
// mismatched NumChunks/Chunks would otherwise panic on index access.
func (m FileMeta) Validate() error {
	if m.NumChunks < 0 {
		return fmt.Errorf("invalid manifest: negative num_chunks %d", m.NumChunks)
	}
	if m.NumChunks != len(m.Chunks) {
		return fmt.Errorf("invalid manifest: num_chunks (%d) does not match chunks length (%d)", m.NumChunks, len(m.Chunks))
	}
	for i, c := range m.Chunks {
		if c.Index != i {
			return fmt.Errorf("invalid manifest: chunk at position %d has index %d", i, c.Index)
		}
		if c.Hash == "" {
			return fmt.Errorf("invalid manifest: chunk %d missing hash", i)
		}
	}
	return nil
}

type DownloadState struct {
	FileID     string          `json:"file_id"`
	Status     string          `json:"status"`
	Downloaded map[string]bool `json:"downloaded"`
	Verified   map[string]bool `json:"verified"`
	UpdatedAt  int64           `json:"updated_at"`
}
