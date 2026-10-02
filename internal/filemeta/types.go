package filemeta

import (
	"fmt"
	"regexp"
)

const (
	// DefaultChunkSize is the default size of each chunk (1 MiB).
	DefaultChunkSize = 1024 * 1024

	// MinChunkSize and MaxChunkSize bound the chunk size accepted in a
	// manifest. MaxChunkSize must stay comfortably below
	// network.MaxMessageSize (10 MiB) so a chunk plus its envelope always
	// fits in a single frame.
	MinChunkSize = 1024
	MaxChunkSize = 8 * 1024 * 1024

	// MaxChunks bounds the number of chunks in a manifest so a serialized
	// manifest (~100 bytes per chunk) always fits in a single frame.
	MaxChunks = 65536

	// ManifestVersion is the current manifest format. v2 manifests are
	// content addressed: the file ID is the manifest CID.
	ManifestVersion = "v2"
)

var hashPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

type ChunkMeta struct {
	Index int    `json:"index"`
	Hash  string `json:"hash"`
	Size  int    `json:"size"`
}

// FileMeta is a file manifest. FileID is always the manifest CID (see
// GenerateManifestCID), so a manifest fetched from an untrusted peer can
// be authenticated by recomputing its CID and comparing it with the ID
// that was requested (see VerifyID).
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
	if m.Version != ManifestVersion {
		return fmt.Errorf("invalid manifest: unsupported version %q (want %q)", m.Version, ManifestVersion)
	}
	if err := ValidateFileName(m.FileName); err != nil {
		return fmt.Errorf("invalid manifest: %w", err)
	}
	if !hashPattern.MatchString(m.ModelHash) {
		return fmt.Errorf("invalid manifest: malformed model hash %q", m.ModelHash)
	}
	if m.ChunkSize < MinChunkSize || m.ChunkSize > MaxChunkSize {
		return fmt.Errorf("invalid manifest: chunk_size %d outside [%d, %d]", m.ChunkSize, MinChunkSize, MaxChunkSize)
	}
	if m.FileSize < 0 {
		return fmt.Errorf("invalid manifest: negative file_size %d", m.FileSize)
	}
	if m.NumChunks < 0 || m.NumChunks > MaxChunks {
		return fmt.Errorf("invalid manifest: num_chunks %d outside [0, %d]", m.NumChunks, MaxChunks)
	}
	if m.NumChunks != len(m.Chunks) {
		return fmt.Errorf("invalid manifest: num_chunks (%d) does not match chunks length (%d)", m.NumChunks, len(m.Chunks))
	}
	chunkSize := int64(m.ChunkSize)
	if want := (m.FileSize + chunkSize - 1) / chunkSize; int64(m.NumChunks) != want {
		return fmt.Errorf("invalid manifest: num_chunks %d inconsistent with file_size %d and chunk_size %d", m.NumChunks, m.FileSize, m.ChunkSize)
	}
	for i, c := range m.Chunks {
		if c.Index != i {
			return fmt.Errorf("invalid manifest: chunk at position %d has index %d", i, c.Index)
		}
		if !hashPattern.MatchString(c.Hash) {
			return fmt.Errorf("invalid manifest: chunk %d has malformed hash %q", i, c.Hash)
		}
		want := chunkSize
		if i == m.NumChunks-1 {
			want = m.FileSize - chunkSize*int64(m.NumChunks-1)
		}
		if int64(c.Size) != want {
			return fmt.Errorf("invalid manifest: chunk %d size %d, want %d", i, c.Size, want)
		}
	}
	return nil
}

// VerifyID checks that the manifest's content hashes to id. A downloader
// must call this on any manifest received from a peer: since file IDs are
// manifest CIDs, a match proves the manifest (and so every chunk hash and
// the whole-file hash inside it) is the one the user asked for.
func (m FileMeta) VerifyID(id string) error {
	cid, err := GenerateManifestCID(m)
	if err != nil {
		return err
	}
	if cid != id {
		return fmt.Errorf("manifest CID %s does not match requested file ID %s", cid, id)
	}
	return nil
}
