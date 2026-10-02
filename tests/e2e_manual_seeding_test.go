package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/IEEECS-VIT/p2p-model-distribution/internal/dht"
	"github.com/IEEECS-VIT/p2p-model-distribution/internal/downloader"
	"github.com/IEEECS-VIT/p2p-model-distribution/internal/filemeta"
	"github.com/IEEECS-VIT/p2p-model-distribution/internal/identity"
	"github.com/IEEECS-VIT/p2p-model-distribution/internal/network"
	"github.com/IEEECS-VIT/p2p-model-distribution/internal/protocol"
	"github.com/IEEECS-VIT/p2p-model-distribution/internal/storage"
	"google.golang.org/protobuf/proto"
)

func newIdentity(t *testing.T) *identity.Identity {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	return id
}

func TestEndToEnd_P2PDistribution(t *testing.T) {
	// 1. Prepare temporary workspaces for Seeder and Downloader nodes
	seederDir := t.TempDir()
	downloaderDir := t.TempDir()

	// Create a mock model file (105 KB)
	originalContent := make([]byte, 105*1024)
	for i := range originalContent {
		originalContent[i] = byte((i*73 + 13) % 256)
	}

	srcPath := filepath.Join(t.TempDir(), "model.bin")
	if err := os.WriteFile(srcPath, originalContent, 0644); err != nil {
		t.Fatalf("failed to write original file: %v", err)
	}

	// 2. Seeder Node Setup: Store model locally (creating manifest & chunks)
	seederStore := storage.NewStore(seederDir)
	chunkSize := 10 * 1024 // 10 KB chunk size
	meta, err := seederStore.StoreModel(srcPath, chunkSize)
	if err != nil {
		t.Fatalf("failed to seed model locally: %v", err)
	}
	fileID := meta.FileID

	if len(meta.Chunks) != 11 {
		t.Errorf("expected 11 chunks for 105KB file with 10KB chunk size, got %d", len(meta.Chunks))
	}

	// 3. Start Seeder TCP Server and register RPC handlers
	seederServer := network.NewServer("127.0.0.1:0", newIdentity(t).ServerTLSConfig())
	router := network.NewRouter()

	// Handle GetMetadata RPC
	router.Register(protocol.MessageType_MSG_GET_METADATA_REQUEST, func(conn *network.Connection, env *protocol.Envelope) error {
		var req protocol.GetMetadataRequest
		if err := proto.Unmarshal(env.Payload, &req); err != nil {
			return err
		}

		// Load manifest from storage
		meta, err := seederStore.LoadManifest(req.FileId)
		if err != nil {
			resp := &protocol.GetMetadataResponse{
				FileId:  req.FileId,
				Success: false,
				Error:   err.Error(),
			}
			return conn.WriteResponse(env.Id, protocol.MessageType_MSG_GET_METADATA_RESPONSE, resp)
		}

		manifestBytes, err := json.Marshal(meta)
		if err != nil {
			resp := &protocol.GetMetadataResponse{
				FileId:  req.FileId,
				Success: false,
				Error:   err.Error(),
			}
			return conn.WriteResponse(env.Id, protocol.MessageType_MSG_GET_METADATA_RESPONSE, resp)
		}

		resp := &protocol.GetMetadataResponse{
			FileId:       req.FileId,
			Success:      true,
			MetadataJson: manifestBytes,
		}
		return conn.WriteResponse(env.Id, protocol.MessageType_MSG_GET_METADATA_RESPONSE, resp)
	})

	// Handle GetChunk RPC
	router.Register(protocol.MessageType_MSG_GET_CHUNK_REQUEST, func(conn *network.Connection, env *protocol.Envelope) error {
		var req protocol.GetChunkRequest
		if err := proto.Unmarshal(env.Payload, &req); err != nil {
			return err
		}

		chunkData, err := seederStore.ReadChunk(req.FileId, int(req.ChunkIndex))
		if err != nil {
			resp := &protocol.GetChunkResponse{
				FileId:     req.FileId,
				ChunkIndex: req.ChunkIndex,
				Success:    false,
				Error:      err.Error(),
			}
			return conn.WriteResponse(env.Id, protocol.MessageType_MSG_GET_CHUNK_RESPONSE, resp)
		}

		resp := &protocol.GetChunkResponse{
			FileId:     req.FileId,
			ChunkIndex: req.ChunkIndex,
			Success:    true,
			Data:       chunkData,
		}
		return conn.WriteResponse(env.Id, protocol.MessageType_MSG_GET_CHUNK_RESPONSE, resp)
	})

	seederServer.OnNewConnection = func(conn *network.Connection) {
		conn.SetRouter(router)
		conn.Start()
	}

	if err := seederServer.Start(); err != nil {
		t.Fatalf("failed to start seeder server: %v", err)
	}
	defer seederServer.Stop()

	// 4. Downloader Node Setup: Connect to Seeder and run RPC download loop
	seederAddr := seederServer.Addr().String()
	downloaderConn, err := network.Dial(context.Background(), seederAddr, newIdentity(t).ClientTLSConfig(""))
	if err != nil {
		t.Fatalf("failed to dial seeder node: %v", err)
	}
	defer downloaderConn.Close()
	downloaderConn.Start()

	downloaderStore := storage.NewStore(downloaderDir)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Step A: Request Manifest (FileMeta)
	metaReq := &protocol.GetMetadataRequest{
		FileId: fileID,
	}
	metaRespEnv, err := downloaderConn.SendRequest(ctx, protocol.MessageType_MSG_GET_METADATA_REQUEST, metaReq)
	if err != nil {
		t.Fatalf("failed to request file metadata: %v", err)
	}

	if metaRespEnv.Type != protocol.MessageType_MSG_GET_METADATA_RESPONSE {
		t.Fatalf("expected MSG_GET_METADATA_RESPONSE, got %v", metaRespEnv.Type)
	}

	var metaResp protocol.GetMetadataResponse
	if err := proto.Unmarshal(metaRespEnv.Payload, &metaResp); err != nil {
		t.Fatalf("failed to unmarshal metadata response: %v", err)
	}

	if !metaResp.Success {
		t.Fatalf("seeder metadata retrieval error: %s", metaResp.Error)
	}

	var downloadedMeta filemeta.FileMeta
	if err := json.Unmarshal(metaResp.MetadataJson, &downloadedMeta); err != nil {
		t.Fatalf("failed to unmarshal filemeta JSON: %v", err)
	}

	// Verify manifest identifiers match
	if downloadedMeta.FileID != fileID {
		t.Errorf("manifest file ID mismatch: got %s, want %s", downloadedMeta.FileID, fileID)
	}

	// Step B: Initialize directories for downloader storage
	if err := downloaderStore.InitializeFileDirectories(fileID); err != nil {
		t.Fatalf("failed to initialize directories: %v", err)
	}

	// Step C: Request, Verify, and Save Chunks in a loop
	for _, chunkMeta := range downloadedMeta.Chunks {
		chunkReq := &protocol.GetChunkRequest{
			FileId:     fileID,
			ChunkIndex: int32(chunkMeta.Index),
		}

		chunkRespEnv, err := downloaderConn.SendRequest(ctx, protocol.MessageType_MSG_GET_CHUNK_REQUEST, chunkReq)
		if err != nil {
			t.Fatalf("failed to request chunk %d: %v", chunkMeta.Index, err)
		}

		if chunkRespEnv.Type != protocol.MessageType_MSG_GET_CHUNK_RESPONSE {
			t.Fatalf("expected MSG_GET_CHUNK_RESPONSE, got %v", chunkRespEnv.Type)
		}

		var chunkResp protocol.GetChunkResponse
		if err := proto.Unmarshal(chunkRespEnv.Payload, &chunkResp); err != nil {
			t.Fatalf("failed to unmarshal chunk response: %v", err)
		}

		if !chunkResp.Success {
			t.Fatalf("seeder chunk retrieval error for index %d: %s", chunkMeta.Index, chunkResp.Error)
		}

		// Verify chunk integrity using the expected hash
		if err := filemeta.VerifyChunk(chunkResp.Data, chunkMeta.Hash); err != nil {
			t.Fatalf("integrity check failed for chunk %d: %v", chunkMeta.Index, err)
		}

		// Write to downloader storage
		if err := downloaderStore.WriteChunk(fileID, chunkMeta.Index, chunkResp.Data); err != nil {
			t.Fatalf("failed to write chunk %d to storage: %v", chunkMeta.Index, err)
		}
	}

	// Save the manifest on downloader side to prepare for reassembly
	if err := downloaderStore.SaveManifest(downloadedMeta); err != nil {
		t.Fatalf("failed to save manifest on downloader side: %v", err)
	}

	// Step D: Reassemble the chunks into the final destination file
	assembledPath := filepath.Join(downloaderDir, downloadedMeta.FileName)

	// Convert downloaderStore paths into slice of ChunkMeta
	if err := filemeta.AssembleChunks(
		downloaderStore.Layout().ChunksDir(fileID),
		assembledPath,
		downloadedMeta.Chunks,
		downloadedMeta.ModelHash,
	); err != nil {
		t.Fatalf("failed to reassemble chunks: %v", err)
	}

	// 5. Verification of the downloaded and reassembled file
	// Verify overall file signature / hash matches the original
	if err := filemeta.VerifyFile(assembledPath, downloadedMeta.ModelHash); err != nil {
		t.Fatalf("file verification failed: %v", err)
	}

	// Check content match exactly
	assembledContent, err := os.ReadFile(assembledPath)
	if err != nil {
		t.Fatalf("failed to read reassembled file: %v", err)
	}

	if !bytes.Equal(assembledContent, originalContent) {
		t.Error("reassembled file contents do not match the original seeded content")
	}
}

func TestEndToEnd_DHTDistribution(t *testing.T) {
	// 1. Prepare directories
	seederDir := t.TempDir()
	downloaderDir := t.TempDir()

	originalContent := make([]byte, 150*1024) // 150 KB
	for i := range originalContent {
		originalContent[i] = byte((i*97 + 31) % 256)
	}

	srcPath := filepath.Join(t.TempDir(), "model.bin")
	if err := os.WriteFile(srcPath, originalContent, 0644); err != nil {
		t.Fatalf("failed to write original file: %v", err)
	}

	// 2. Start Bootstrap Node
	bootstrapSvc := dht.NewService(newIdentity(t), "127.0.0.1:0", "", nil)
	if err := bootstrapSvc.Start(); err != nil {
		t.Fatalf("failed to start bootstrap service: %v", err)
	}
	defer bootstrapSvc.Stop()
	bootstrapAddr := bootstrapSvc.Self().Endpoint()

	// 3. Start Seeder Node with file registered
	seederStore := storage.NewStore(seederDir)
	seeded, err := seederStore.StoreModel(srcPath, 20*1024) // 20 KB chunks
	fileID := seeded.FileID
	if err != nil {
		t.Fatalf("failed to store model on seeder: %v", err)
	}

	seederSvc := dht.NewService(newIdentity(t), "127.0.0.1:0", "", []string{bootstrapAddr})

	// Register file transfer handlers on seeder DHT router
	seederRouter := seederSvc.Router()
	seederRouter.Register(protocol.MessageType_MSG_GET_METADATA_REQUEST, func(conn *network.Connection, env *protocol.Envelope) error {
		var req protocol.GetMetadataRequest
		proto.Unmarshal(env.Payload, &req)
		loadedMeta, _ := seederStore.LoadManifest(req.FileId)
		mBytes, _ := json.Marshal(loadedMeta)
		resp := &protocol.GetMetadataResponse{FileId: req.FileId, Success: true, MetadataJson: mBytes}
		return conn.WriteResponse(env.Id, protocol.MessageType_MSG_GET_METADATA_RESPONSE, resp)
	})
	seederRouter.Register(protocol.MessageType_MSG_GET_CHUNK_REQUEST, func(conn *network.Connection, env *protocol.Envelope) error {
		var req protocol.GetChunkRequest
		proto.Unmarshal(env.Payload, &req)
		chunkData, _ := seederStore.ReadChunk(req.FileId, int(req.ChunkIndex))
		resp := &protocol.GetChunkResponse{FileId: req.FileId, ChunkIndex: req.ChunkIndex, Success: true, Data: chunkData}
		return conn.WriteResponse(env.Id, protocol.MessageType_MSG_GET_CHUNK_RESPONSE, resp)
	})

	if err := seederSvc.Start(); err != nil {
		t.Fatalf("failed to start seeder service: %v", err)
	}
	defer seederSvc.Stop()

	// Wait for bootstrap exchange
	time.Sleep(200 * time.Millisecond)

	// Announce file ID
	seederSvc.AnnounceProvider(context.Background(), fileID)

	// 4. Start Downloader Node
	downloaderStore := storage.NewStore(downloaderDir)
	downloaderSvc := dht.NewService(newIdentity(t), "127.0.0.1:0", "", []string{bootstrapAddr})
	if err := downloaderSvc.Start(); err != nil {
		t.Fatalf("failed to start downloader service: %v", err)
	}
	defer downloaderSvc.Stop()

	// Wait for bootstrap exchange
	time.Sleep(200 * time.Millisecond)

	// 5. Run Downloader with DHT fallback enabled (passing nil initialProviders)
	dl := downloader.New(fileID, downloaderStore, downloaderSvc, downloader.Options{Concurrency: 2})

	assembledPath := filepath.Join(downloaderDir, "dht-assembled.bin")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = dl.Download(ctx, assembledPath)
	if err != nil {
		t.Fatalf("DHT download failed: %v", err)
	}

	// 6. Verification
	assembledContent, err := os.ReadFile(assembledPath)
	if err != nil {
		t.Fatalf("failed to read assembled file: %v", err)
	}

	if !bytes.Equal(assembledContent, originalContent) {
		t.Error("reassembled file contents in DHT mode do not match original")
	}
}
