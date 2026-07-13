package filemeta

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

type DownloadState struct {
	FileID     string          `json:"file_id"`
	Status     string          `json:"status"`
	Downloaded map[string]bool `json:"downloaded"`
	Verified   map[string]bool `json:"verified"`
	UpdatedAt  int64           `json:"updated_at"`
}
