package seeder

import (
	"fmt"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/IEEECS-VIT/p2p-model-distribution/internal/filemeta"
	"github.com/IEEECS-VIT/p2p-model-distribution/internal/storage"
)

type Seeder struct {
	manifestDir string
	store       *storage.ChunkStore
	manifests   map[string]filemeta.FileMeta
}

// New creates a Seeder. manifestDir is where .json files live.
// chunkBaseDir is the parent directory — chunks for a given CID live at
// chunkBaseDir/<cid>/*.chunk.
func New(manifestDir, chunkBaseDir string) (*Seeder, error) {
	manifests, err := loadAllManifests(manifestDir)
	if err != nil {
		return nil, fmt.Errorf("load manifests: %w", err)
	}

	store, err := storage.NewChunkStore(chunkBaseDir)
	if err != nil {
		return nil, fmt.Errorf("init chunk store: %w", err)
	}

	return &Seeder{
		manifestDir: manifestDir,
		store:       store,
		manifests:   manifests,
	}, nil
}

// RegisterManifest adds a freshly built manifest to the in-memory map
// without needing a restart. Call this right after BuildManifest.
func (s *Seeder) RegisterManifest(cid string, meta filemeta.FileMeta) {
	s.manifests[cid] = meta
}

// Handler returns an http.Handler with all routes attached.
// Mount it wherever you like — plain ListenAndServe or inside a larger mux.
func (s *Seeder) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/manifest/", s.handleManifest)
	mux.HandleFunc("/chunk/", s.handleChunk)
	mux.HandleFunc("/health", s.handleHealth)
	return mux
}

// Serve starts the HTTP server on addr (e.g. ":8080") and blocks.
func (s *Seeder) Serve(addr string) error {
	return http.ListenAndServe(addr, s.Handler())
}

// GET /manifest/{cid}
func (s *Seeder) handleManifest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	cid := strings.TrimPrefix(r.URL.Path, "/manifest/")
	if cid == "" {
		http.Error(w, "missing cid", http.StatusBadRequest)
		return
	}

	_, ok := s.manifests[cid]
	if !ok {
		http.Error(w, "manifest not found", http.StatusNotFound)
		return
	}

	manifestPath := filepath.Join(s.manifestDir, cid+".json")
	data, err := filemeta.LoadManifestBytes(manifestPath)
	if err != nil {
		http.Error(w, "failed to read manifest", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

// GET /chunk/{cid}/{index}
func (s *Seeder) handleChunk(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/chunk/"), "/")
	if len(parts) != 2 {
		http.Error(w, "usage: /chunk/{cid}/{index}", http.StatusBadRequest)
		return
	}

	cid := parts[0]
	indexStr := parts[1]

	index, err := strconv.Atoi(indexStr)
	if err != nil {
		http.Error(w, "invalid chunk index", http.StatusBadRequest)
		return
	}

	meta, ok := s.manifests[cid]
	if !ok {
		http.Error(w, "manifest not found", http.StatusNotFound)
		return
	}

	if index < 0 || index >= meta.NumChunks {
		http.Error(w,
			fmt.Sprintf("index %d out of range (file has %d chunks)", index, meta.NumChunks),
			http.StatusNotFound,
		)
		return
	}

	data, err := s.store.ReadChunk(cid, index)
	if err != nil {
		http.Error(w, "chunk not found on disk", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Chunk-Hash", meta.Chunks[index].Hash)
	w.Header().Set("X-Chunk-Index", indexStr)
	w.Header().Set("X-Manifest-CID", cid)
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

// GET /health
func (s *Seeder) handleHealth(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, `{"status":"ok","manifests":%d}`, len(s.manifests))
}

// loadAllManifests reads every .json in manifestDir into memory.
func loadAllManifests(manifestDir string) (map[string]filemeta.FileMeta, error) {
	manifests := make(map[string]filemeta.FileMeta)

	entries, err := filepath.Glob(filepath.Join(manifestDir, "*.json"))
	if err != nil {
		return nil, err
	}

	for _, path := range entries {
		meta, err := filemeta.LoadManifest(path)
		if err != nil {
			return nil, fmt.Errorf("load %s: %w", path, err)
		}
		cid, err := filemeta.GenerateManifestCID(meta)
		if err != nil {
			return nil, fmt.Errorf("cid for %s: %w", path, err)
		}
		manifests[cid] = meta
	}

	return manifests, nil
}
