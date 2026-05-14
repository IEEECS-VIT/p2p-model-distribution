package filemeta

type ChunkMeta struct {
	Index int    `json:"index"`
	CID   string `json:"cid"`  // DHT content address (you generate this)
	Hash  string `json:"hash"` // "sha256:<hex>"
	Size  int    `json:"size"` // actual bytes — last chunk differs
}

type FileMeta struct {
	FileID    string      `json:"file_id"`
	FileName  string      `json:"file_name"`
	FileSize  int64       `json:"file_size"`  // int64, not int — models are >2GB
	ModelHash string      `json:"model_hash"` // sha256 of full reassembled file
	ChunkSize int         `json:"chunk_size"`
	NumChunks int         `json:"num_chunks"`
	Version   string      `json:"version"`
	CreatedAt int64       `json:"created_at"` // unix timestamp
	Chunks    []ChunkMeta `json:"chunks"`
}

type DownloadState struct {
	FileID     string          `json:"file_id"`
	Status     string          `json:"status"` // pending|downloading|complete|failed
	Downloaded map[string]bool `json:"downloaded"`
	Verified   map[string]bool `json:"verified"`
	UpdatedAt  int64           `json:"updated_at"`
}
