package transfer

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/IEEECS-VIT/p2p-model-distribution/internal/filemeta"
	"github.com/IEEECS-VIT/p2p-model-distribution/internal/identity"
	"github.com/IEEECS-VIT/p2p-model-distribution/internal/network"
	"github.com/IEEECS-VIT/p2p-model-distribution/internal/protocol"
	"github.com/IEEECS-VIT/p2p-model-distribution/internal/storage"
	"google.golang.org/protobuf/proto"
)

func setup(t *testing.T) (*storage.Store, filemeta.FileMeta, *network.Connection) {
	t.Helper()
	dir := t.TempDir()
	src := filepath.Join(dir, "model.bin")
	data := make([]byte, 3*filemeta.MinChunkSize+100)
	for i := range data {
		data[i] = byte(i)
	}
	if err := os.WriteFile(src, data, 0o644); err != nil {
		t.Fatal(err)
	}
	store := storage.NewStore(filepath.Join(dir, "store"))
	meta, err := store.StoreModel(src, filemeta.MinChunkSize)
	if err != nil {
		t.Fatal(err)
	}

	serverID, _ := identity.Generate()
	clientID, _ := identity.Generate()
	router := network.NewRouter()
	NewServer(store).Register(router)
	srv := network.NewServer("127.0.0.1:0", serverID.ServerTLSConfig())
	srv.OnNewConnection = func(c *network.Connection) {
		c.SetRouter(router)
		c.Start()
	}
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Stop)

	conn, err := network.Dial(context.Background(), srv.Addr().String(), clientID.ClientTLSConfig(""))
	if err != nil {
		t.Fatal(err)
	}
	conn.Start()
	t.Cleanup(func() { conn.Close() })
	return store, meta, conn
}

func getChunk(t *testing.T, conn *network.Connection, fileID string, index int32) *protocol.GetChunkResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	env, err := conn.SendRequest(ctx, protocol.MessageType_MSG_GET_CHUNK_REQUEST, &protocol.GetChunkRequest{FileId: fileID, ChunkIndex: index})
	if err != nil {
		t.Fatalf("GET_CHUNK: %v", err)
	}
	var resp protocol.GetChunkResponse
	if err := proto.Unmarshal(env.Payload, &resp); err != nil {
		t.Fatal(err)
	}
	return &resp
}

func TestServesManifestAndChunks(t *testing.T) {
	_, meta, conn := setup(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	env, err := conn.SendRequest(ctx, protocol.MessageType_MSG_GET_METADATA_REQUEST, &protocol.GetMetadataRequest{FileId: meta.FileID})
	if err != nil {
		t.Fatal(err)
	}
	var resp protocol.GetMetadataResponse
	_ = proto.Unmarshal(env.Payload, &resp)
	var got filemeta.FileMeta
	if !resp.Success || json.Unmarshal(resp.MetadataJson, &got) != nil || got.VerifyID(meta.FileID) != nil {
		t.Fatalf("metadata response invalid: %+v", &resp)
	}

	for i := 0; i < meta.NumChunks; i++ {
		c := getChunk(t, conn, meta.FileID, int32(i))
		if !c.Success || filemeta.VerifyChunk(c.Data, meta.Chunks[i].Hash) != nil {
			t.Fatalf("chunk %d not served correctly", i)
		}
	}
}

func TestDoesNotServeCorruptChunks(t *testing.T) {
	store, meta, conn := setup(t)
	path := filepath.Join(store.Layout().ChunksDir(meta.FileID), "1.chunk")
	if err := os.WriteFile(path, make([]byte, meta.Chunks[1].Size), 0o644); err != nil {
		t.Fatal(err)
	}
	if c := getChunk(t, conn, meta.FileID, 1); c.Success {
		t.Fatal("served a chunk that does not match the manifest")
	}
}

func TestRejectsBadRequests(t *testing.T) {
	_, meta, conn := setup(t)
	for _, tc := range []struct {
		id  string
		idx int32
	}{
		{"../../etc", 0},
		{meta.FileID, -1},
		{meta.FileID, int32(meta.NumChunks)},
		{"0000000000000000000000000000000000000000000000000000000000000000", 0},
	} {
		if c := getChunk(t, conn, tc.id, tc.idx); c.Success || c.Error != errUnavailable {
			t.Errorf("GET_CHUNK(%q, %d) = success=%v error=%q, want generic failure", tc.id, tc.idx, c.Success, c.Error)
		}
	}
}

func TestDoesNotServeIncompleteFiles(t *testing.T) {
	store, meta, conn := setup(t)
	// A manifest that doesn't match the ID it's stored under (e.g. a
	// partially written or foreign file) must not be served.
	other := meta
	other.FileID = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	if err := store.SaveManifest(other); err != nil {
		t.Fatal(err)
	}
	if c := getChunk(t, conn, other.FileID, 0); c.Success {
		t.Fatal("served a chunk for a file whose manifest does not match its ID")
	}
}
