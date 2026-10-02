// Package transfer serves stored files to peers: manifests via
// GET_METADATA and chunks via GET_CHUNK.
package transfer

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/IEEECS-VIT/p2p-model-distribution/internal/filemeta"
	"github.com/IEEECS-VIT/p2p-model-distribution/internal/network"
	"github.com/IEEECS-VIT/p2p-model-distribution/internal/protocol"
	"github.com/IEEECS-VIT/p2p-model-distribution/internal/storage"
	"google.golang.org/protobuf/proto"
)

// maxCachedManifests bounds the in-memory manifest cache.
const maxCachedManifests = 256

// errUnavailable is the only failure reason sent to peers, so responses
// never reveal local paths or OS errors.
const errUnavailable = "not available"

// Server answers file-transfer requests from a store. Only complete files
// (whose stored manifest matches the file ID) are served, and every chunk
// is checked against the manifest before it is sent, so a corrupted or
// tampered file on disk is never propagated to other peers.
type Server struct {
	store *storage.Store

	mu        sync.Mutex
	manifests map[string]filemeta.FileMeta
}

// NewServer creates a Server for store.
func NewServer(store *storage.Store) *Server {
	return &Server{store: store, manifests: make(map[string]filemeta.FileMeta)}
}

// Register installs the GET_METADATA and GET_CHUNK handlers on router.
func (s *Server) Register(router *network.Router) {
	router.Register(protocol.MessageType_MSG_GET_METADATA_REQUEST, s.handleMetadata)
	router.Register(protocol.MessageType_MSG_GET_CHUNK_REQUEST, s.handleChunk)
}

// manifest returns the verified manifest for fileID, if the file is
// complete in the store.
func (s *Server) manifest(fileID string) (filemeta.FileMeta, bool) {
	if !storage.ValidFileID(fileID) {
		return filemeta.FileMeta{}, false
	}

	s.mu.Lock()
	meta, ok := s.manifests[fileID]
	s.mu.Unlock()
	if ok {
		return meta, true
	}

	meta, err := s.store.LoadManifest(fileID)
	if err != nil || meta.Validate() != nil || meta.VerifyID(fileID) != nil {
		return filemeta.FileMeta{}, false
	}

	s.mu.Lock()
	if len(s.manifests) >= maxCachedManifests {
		clear(s.manifests)
	}
	s.manifests[fileID] = meta
	s.mu.Unlock()
	return meta, true
}

func (s *Server) handleMetadata(conn *network.Connection, env *protocol.Envelope) error {
	var req protocol.GetMetadataRequest
	if err := proto.Unmarshal(env.Payload, &req); err != nil {
		return fmt.Errorf("%w: malformed metadata request", network.ErrBadRequest)
	}

	resp := &protocol.GetMetadataResponse{FileId: req.FileId}
	if meta, ok := s.manifest(req.FileId); ok {
		data, err := json.Marshal(meta)
		if err != nil {
			return err
		}
		resp.Success, resp.MetadataJson = true, data
	} else {
		resp.Error = errUnavailable
	}
	return conn.WriteResponse(env.Id, protocol.MessageType_MSG_GET_METADATA_RESPONSE, resp)
}

func (s *Server) handleChunk(conn *network.Connection, env *protocol.Envelope) error {
	var req protocol.GetChunkRequest
	if err := proto.Unmarshal(env.Payload, &req); err != nil {
		return fmt.Errorf("%w: malformed chunk request", network.ErrBadRequest)
	}

	resp := &protocol.GetChunkResponse{FileId: req.FileId, ChunkIndex: req.ChunkIndex, Error: errUnavailable}
	if data, ok := s.verifiedChunk(req.FileId, int(req.ChunkIndex)); ok {
		resp.Success, resp.Data, resp.Error = true, data, ""
	}
	return conn.WriteResponse(env.Id, protocol.MessageType_MSG_GET_CHUNK_RESPONSE, resp)
}

func (s *Server) verifiedChunk(fileID string, index int) ([]byte, bool) {
	meta, ok := s.manifest(fileID)
	if !ok || index < 0 || index >= meta.NumChunks {
		return nil, false
	}
	data, err := s.store.ReadChunk(fileID, index)
	if err != nil {
		slog.Warn("stored chunk unreadable", "file", fileID, "chunk", index, "err", err)
		return nil, false
	}
	chunk := meta.Chunks[index]
	if len(data) != chunk.Size || filemeta.VerifyChunk(data, chunk.Hash) != nil {
		slog.Error("stored chunk is corrupt; not serving it", "file", fileID, "chunk", index)
		return nil, false
	}
	return data, true
}
