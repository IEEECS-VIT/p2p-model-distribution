package downloader

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

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

func TestDownloader_HappyPathParallel(t *testing.T) {
	// 1. Directories setup
	tempDir, err := os.MkdirTemp("", "p2p-downloader-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	seederDir := filepath.Join(tempDir, "seeder")
	downloaderDir := filepath.Join(tempDir, "downloader")
	os.MkdirAll(seederDir, 0755)
	os.MkdirAll(downloaderDir, 0755)

	// 2. Create sample source file on disk
	srcFilePath := filepath.Join(tempDir, "source.model")
	dummyData := make([]byte, 5*1024*1024) // 5 MB file
	for i := range dummyData {
		dummyData[i] = byte(i % 256)
	}
	if err := os.WriteFile(srcFilePath, dummyData, 0644); err != nil {
		t.Fatalf("failed to write dummy source: %v", err)
	}

	// 3. Chunk and seed file using StoreModel
	seederStore := storage.NewStore(seederDir)
	seeded, err := seederStore.StoreModel(srcFilePath, 1024*1024)
	fileID := seeded.FileID
	if err != nil {
		t.Fatalf("failed to seed model: %v", err)
	}

	// 4. Start Seeder TCP Server
	seederServer := network.NewServer("127.0.0.1:0", newIdentity(t).ServerTLSConfig())
	router := network.NewRouter()

	router.Register(protocol.MessageType_MSG_GET_METADATA_REQUEST, func(conn *network.Connection, env *protocol.Envelope) error {
		var req protocol.GetMetadataRequest
		if err := proto.Unmarshal(env.Payload, &req); err != nil {
			return err
		}
		loadedMeta, err := seederStore.LoadManifest(req.FileId)
		if err != nil {
			resp := &protocol.GetMetadataResponse{FileId: req.FileId, Success: false, Error: err.Error()}
			return conn.WriteResponse(env.Id, protocol.MessageType_MSG_GET_METADATA_RESPONSE, resp)
		}
		mBytes, err := json.Marshal(loadedMeta)
		if err != nil {
			resp := &protocol.GetMetadataResponse{FileId: req.FileId, Success: false, Error: err.Error()}
			return conn.WriteResponse(env.Id, protocol.MessageType_MSG_GET_METADATA_RESPONSE, resp)
		}
		resp := &protocol.GetMetadataResponse{FileId: req.FileId, Success: true, MetadataJson: mBytes}
		return conn.WriteResponse(env.Id, protocol.MessageType_MSG_GET_METADATA_RESPONSE, resp)
	})

	router.Register(protocol.MessageType_MSG_GET_CHUNK_REQUEST, func(conn *network.Connection, env *protocol.Envelope) error {
		var req protocol.GetChunkRequest
		if err := proto.Unmarshal(env.Payload, &req); err != nil {
			return err
		}
		chunkData, err := seederStore.ReadChunk(req.FileId, int(req.ChunkIndex))
		if err != nil {
			resp := &protocol.GetChunkResponse{FileId: req.FileId, ChunkIndex: req.ChunkIndex, Success: false, Error: err.Error()}
			return conn.WriteResponse(env.Id, protocol.MessageType_MSG_GET_CHUNK_RESPONSE, resp)
		}
		resp := &protocol.GetChunkResponse{FileId: req.FileId, ChunkIndex: req.ChunkIndex, Success: true, Data: chunkData}
		return conn.WriteResponse(env.Id, protocol.MessageType_MSG_GET_CHUNK_RESPONSE, resp)
	})

	seederServer.OnNewConnection = func(conn *network.Connection) {
		conn.SetRouter(router)
		conn.Start()
	}

	if err := seederServer.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer seederServer.Stop()

	seederAddr := seederServer.Addr().String()

	// 5. Run parallel downloader
	dlStore := storage.NewStore(downloaderDir)
	dl := New(fileID, downloaderDir, nil, dlStore, []string{seederAddr}, 4, newIdentity(t).ClientTLSConfig(""))

	destPath := filepath.Join(tempDir, "assembled.model")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = dl.Download(ctx, destPath)
	if err != nil {
		t.Fatalf("download failed: %v", err)
	}

	// 6. Verify contents
	downloadedData, err := os.ReadFile(destPath)
	if err != nil {
		t.Fatalf("failed to read downloaded file: %v", err)
	}

	if len(downloadedData) != len(dummyData) {
		t.Errorf("size mismatch: expected %d, got %d", len(dummyData), len(downloadedData))
	}
	for i := range dummyData {
		if downloadedData[i] != dummyData[i] {
			t.Fatalf("mismatch at byte %d", i)
		}
	}
}

func TestDownloader_FailoverAndRetry(t *testing.T) {
	// 1. Directories setup
	tempDir, err := os.MkdirTemp("", "p2p-downloader-failover-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	seederDir := filepath.Join(tempDir, "seeder")
	downloaderDir := filepath.Join(tempDir, "downloader")
	os.MkdirAll(seederDir, 0755)
	os.MkdirAll(downloaderDir, 0755)

	// 2. Create sample source file data
	srcFilePath := filepath.Join(tempDir, "source.model")
	dummyData := make([]byte, 2*1024*1024) // 2 MB file (2 chunks)
	for i := range dummyData {
		dummyData[i] = byte(i % 256)
	}
	if err := os.WriteFile(srcFilePath, dummyData, 0644); err != nil {
		t.Fatalf("failed to write dummy source: %v", err)
	}

	// 3. Chunk and seed using StoreModel
	seederStore := storage.NewStore(seederDir)
	seeded, err := seederStore.StoreModel(srcFilePath, 1024*1024)
	fileID := seeded.FileID
	if err != nil {
		t.Fatalf("failed to seed model: %v", err)
	}

	// 4. Start Healthy Seeder TCP Server
	healthyServer := network.NewServer("127.0.0.1:0", newIdentity(t).ServerTLSConfig())
	router := network.NewRouter()

	router.Register(protocol.MessageType_MSG_GET_METADATA_REQUEST, func(conn *network.Connection, env *protocol.Envelope) error {
		loadedMeta, _ := seederStore.LoadManifest(fileID)
		mBytes, _ := json.Marshal(loadedMeta)
		resp := &protocol.GetMetadataResponse{FileId: fileID, Success: true, MetadataJson: mBytes}
		return conn.WriteResponse(env.Id, protocol.MessageType_MSG_GET_METADATA_RESPONSE, resp)
	})

	router.Register(protocol.MessageType_MSG_GET_CHUNK_REQUEST, func(conn *network.Connection, env *protocol.Envelope) error {
		var req protocol.GetChunkRequest
		proto.Unmarshal(env.Payload, &req)
		chunkData, _ := seederStore.ReadChunk(req.FileId, int(req.ChunkIndex))
		resp := &protocol.GetChunkResponse{FileId: req.FileId, ChunkIndex: req.ChunkIndex, Success: true, Data: chunkData}
		return conn.WriteResponse(env.Id, protocol.MessageType_MSG_GET_CHUNK_RESPONSE, resp)
	})

	healthyServer.OnNewConnection = func(conn *network.Connection) {
		conn.SetRouter(router)
		conn.Start()
	}
	healthyServer.Start()
	defer healthyServer.Stop()

	// 5. Start Broken Seeder TCP Server (closes connection or returns errors)
	brokenServer := network.NewServer("127.0.0.1:0", newIdentity(t).ServerTLSConfig())
	brokenRouter := network.NewRouter()

	brokenRouter.Register(protocol.MessageType_MSG_GET_METADATA_REQUEST, func(conn *network.Connection, env *protocol.Envelope) error {
		loadedMeta, _ := seederStore.LoadManifest(fileID)
		mBytes, _ := json.Marshal(loadedMeta)
		resp := &protocol.GetMetadataResponse{FileId: fileID, Success: true, MetadataJson: mBytes}
		return conn.WriteResponse(env.Id, protocol.MessageType_MSG_GET_METADATA_RESPONSE, resp)
	})

	brokenRouter.Register(protocol.MessageType_MSG_GET_CHUNK_REQUEST, func(conn *network.Connection, env *protocol.Envelope) error {
		// Returns bad chunk data (corrupt hashes) or fails
		resp := &protocol.GetChunkResponse{FileId: fileID, ChunkIndex: 0, Success: false, Error: "mock broken server error"}
		return conn.WriteResponse(env.Id, protocol.MessageType_MSG_GET_CHUNK_RESPONSE, resp)
	})

	brokenServer.OnNewConnection = func(conn *network.Connection) {
		conn.SetRouter(brokenRouter)
		conn.Start()
	}
	brokenServer.Start()
	defer brokenServer.Stop()

	healthyAddr := healthyServer.Addr().String()
	brokenAddr := brokenServer.Addr().String()

	// 6. Downloader Setup with both seeder addresses
	dlStore := storage.NewStore(downloaderDir)
	// We put the broken server address first so the downloader hits it initially, fails, and recovers
	dl := New(fileID, downloaderDir, nil, dlStore, []string{brokenAddr, healthyAddr}, 2, newIdentity(t).ClientTLSConfig(""))

	destPath := filepath.Join(tempDir, "assembled_failover.model")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = dl.Download(ctx, destPath)
	if err != nil {
		t.Fatalf("download failed despite healthy seeder: %v", err)
	}

	// 7. Verify contents
	downloadedData, err := os.ReadFile(destPath)
	if err != nil {
		t.Fatalf("failed to read downloaded file: %v", err)
	}

	if len(downloadedData) != len(dummyData) {
		t.Errorf("size mismatch: expected %d, got %d", len(dummyData), len(downloadedData))
	}
}

// TestDownloader_RejectsSubstitutedManifest simulates a malicious provider
// that answers every metadata request with a manifest for content of its
// choosing. The manifest is internally consistent (its chunk hashes match
// the chunks it serves), so only the CID check can catch it.
func TestDownloader_RejectsSubstitutedManifest(t *testing.T) {
	tempDir := t.TempDir()

	writeFile := func(name string, data []byte) string {
		p := filepath.Join(tempDir, name)
		if err := os.WriteFile(p, data, 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}

	evilStore := storage.NewStore(filepath.Join(tempDir, "evil"))
	evil, err := evilStore.StoreModel(writeFile("evil.bin", []byte("malicious weights")), 0)
	if err != nil {
		t.Fatal(err)
	}
	wanted, err := storage.NewStore(filepath.Join(tempDir, "genuine")).StoreModel(writeFile("genuine.bin", []byte("genuine weights")), 0)
	if err != nil {
		t.Fatal(err)
	}

	server := network.NewServer("127.0.0.1:0", newIdentity(t).ServerTLSConfig())
	router := network.NewRouter()
	router.Register(protocol.MessageType_MSG_GET_METADATA_REQUEST, func(conn *network.Connection, env *protocol.Envelope) error {
		mBytes, _ := json.Marshal(evil)
		resp := &protocol.GetMetadataResponse{FileId: wanted.FileID, Success: true, MetadataJson: mBytes}
		return conn.WriteResponse(env.Id, protocol.MessageType_MSG_GET_METADATA_RESPONSE, resp)
	})
	router.Register(protocol.MessageType_MSG_GET_CHUNK_REQUEST, func(conn *network.Connection, env *protocol.Envelope) error {
		var req protocol.GetChunkRequest
		_ = proto.Unmarshal(env.Payload, &req)
		data, _ := evilStore.ReadChunk(evil.FileID, int(req.ChunkIndex))
		resp := &protocol.GetChunkResponse{FileId: req.FileId, ChunkIndex: req.ChunkIndex, Success: true, Data: data}
		return conn.WriteResponse(env.Id, protocol.MessageType_MSG_GET_CHUNK_RESPONSE, resp)
	})
	server.OnNewConnection = func(conn *network.Connection) {
		conn.SetRouter(router)
		conn.Start()
	}
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	defer server.Stop()

	dlDir := filepath.Join(tempDir, "dl")
	dl := New(wanted.FileID, dlDir, nil, storage.NewStore(dlDir), []string{server.Addr().String()}, 2, newIdentity(t).ClientTLSConfig(""))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	destPath := filepath.Join(tempDir, "out.bin")
	if _, err := dl.Download(ctx, destPath); err == nil {
		t.Fatal("download succeeded with a substituted manifest")
	}
	if _, err := os.Stat(destPath); !os.IsNotExist(err) {
		t.Fatalf("output file was created despite rejected manifest (stat err = %v)", err)
	}
}
