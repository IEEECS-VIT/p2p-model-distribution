package downloader

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IEEECS-VIT/p2p-model-distribution/internal/dht"
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

// fastBackoff shrinks retry timings for the duration of a test.
func fastBackoff(t *testing.T) {
	t.Helper()
	ob, om, ow := baseBackoff, maxBackoff, providerWaitTimeout
	baseBackoff, maxBackoff, providerWaitTimeout = 5*time.Millisecond, 20*time.Millisecond, 2*time.Second
	t.Cleanup(func() { baseBackoff, maxBackoff, providerWaitTimeout = ob, om, ow })
}

// seedFile stores data in a fresh store and returns the store and manifest.
func seedFile(t *testing.T, data []byte, chunkSize int) (*storage.Store, filemeta.FileMeta) {
	t.Helper()
	dir := t.TempDir()
	src := filepath.Join(dir, "model.bin")
	if err := os.WriteFile(src, data, 0o644); err != nil {
		t.Fatal(err)
	}
	store := storage.NewStore(filepath.Join(dir, "store"))
	meta, err := store.StoreModel(src, chunkSize)
	if err != nil {
		t.Fatalf("StoreModel: %v", err)
	}
	return store, meta
}

// seeder configures how a test seeder answers requests.
type seeder struct {
	store *storage.Store
	// manifest, if set, is served for every metadata request.
	manifest *filemeta.FileMeta
	// failChunks makes every chunk request fail.
	failChunks    bool
	chunkRequests atomic.Int32
}

// start runs the seeder and returns its address.
func (s *seeder) start(t *testing.T) string {
	t.Helper()
	router := network.NewRouter()
	router.Register(protocol.MessageType_MSG_GET_METADATA_REQUEST, func(conn *network.Connection, env *protocol.Envelope) error {
		var req protocol.GetMetadataRequest
		_ = proto.Unmarshal(env.Payload, &req)
		meta := s.manifest
		if meta == nil {
			loaded, err := s.store.LoadManifest(req.FileId)
			if err != nil {
				return conn.WriteResponse(env.Id, protocol.MessageType_MSG_GET_METADATA_RESPONSE, &protocol.GetMetadataResponse{Error: "not found"})
			}
			meta = &loaded
		}
		b, _ := json.Marshal(meta)
		return conn.WriteResponse(env.Id, protocol.MessageType_MSG_GET_METADATA_RESPONSE, &protocol.GetMetadataResponse{FileId: req.FileId, Success: true, MetadataJson: b})
	})
	router.Register(protocol.MessageType_MSG_GET_CHUNK_REQUEST, func(conn *network.Connection, env *protocol.Envelope) error {
		s.chunkRequests.Add(1)
		var req protocol.GetChunkRequest
		_ = proto.Unmarshal(env.Payload, &req)
		if s.failChunks {
			return conn.WriteResponse(env.Id, protocol.MessageType_MSG_GET_CHUNK_RESPONSE, &protocol.GetChunkResponse{Error: "mock broken server error"})
		}
		fileID := req.FileId
		if s.manifest != nil {
			fileID = s.manifest.FileID
		}
		data, err := s.store.ReadChunk(fileID, int(req.ChunkIndex))
		if err != nil {
			return conn.WriteResponse(env.Id, protocol.MessageType_MSG_GET_CHUNK_RESPONSE, &protocol.GetChunkResponse{Error: "not found"})
		}
		return conn.WriteResponse(env.Id, protocol.MessageType_MSG_GET_CHUNK_RESPONSE, &protocol.GetChunkResponse{FileId: req.FileId, ChunkIndex: req.ChunkIndex, Success: true, Data: data})
	})

	srv := network.NewServer("127.0.0.1:0", newIdentity(t).ServerTLSConfig())
	srv.OnNewConnection = func(conn *network.Connection) {
		conn.SetRouter(router)
		conn.Start()
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("start seeder: %v", err)
	}
	t.Cleanup(srv.Stop)
	return srv.Addr().String()
}

// newNode starts a DHT node to download through.
func newNode(t *testing.T) *dht.Service {
	t.Helper()
	svc := dht.NewService(newIdentity(t), "127.0.0.1:0", "", nil)
	if err := svc.Start(); err != nil {
		t.Fatalf("start node: %v", err)
	}
	t.Cleanup(svc.Stop)
	return svc
}

func testData(n int) []byte {
	data := make([]byte, n)
	for i := range data {
		data[i] = byte(i*31 + i/7)
	}
	return data
}

func TestDownloader_HappyPathParallel(t *testing.T) {
	data := testData(5*1024*1024 + 77)
	store, meta := seedFile(t, data, 512*1024)
	addr := (&seeder{store: store}).start(t)

	dlStore := storage.NewStore(t.TempDir())
	var progressCalls atomic.Int32
	dl := New(meta.FileID, dlStore, newNode(t), Options{
		Providers:   []string{addr},
		Concurrency: 4,
		Progress:    func(done, total int) { progressCalls.Add(1) },
	})

	dest := filepath.Join(t.TempDir(), "out.bin")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := dl.Download(ctx, dest); err != nil {
		t.Fatalf("download failed: %v", err)
	}

	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("downloaded file differs from original")
	}
	if int(progressCalls.Load()) != meta.NumChunks {
		t.Fatalf("progress called %d times, want %d", progressCalls.Load(), meta.NumChunks)
	}
	if !dlStore.HasCompleteFile(meta.FileID) {
		t.Fatal("downloaded file not recorded as complete in the store")
	}
}

func TestDownloader_FailoverAndRetry(t *testing.T) {
	fastBackoff(t)
	data := testData(2*1024*1024 + 5)
	store, meta := seedFile(t, data, 256*1024)
	broken := (&seeder{store: store, failChunks: true}).start(t)
	healthy := (&seeder{store: store}).start(t)

	dl := New(meta.FileID, storage.NewStore(t.TempDir()), newNode(t), Options{Providers: []string{broken, healthy}, Concurrency: 2})
	dest := filepath.Join(t.TempDir(), "out.bin")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := dl.Download(ctx, dest); err != nil {
		t.Fatalf("download failed despite healthy seeder: %v", err)
	}
	got, _ := os.ReadFile(dest)
	if !bytes.Equal(got, data) {
		t.Fatal("downloaded file differs from original")
	}
}

// TestDownloader_SpreadsLoadAcrossProviders reproduces the old behaviour
// where every worker picked the first provider with the fewest failures,
// so all chunks came from a single provider.
func TestDownloader_SpreadsLoadAcrossProviders(t *testing.T) {
	data := testData(64 * 32 * 1024)
	store, meta := seedFile(t, data, 32*1024)
	a, b := &seeder{store: store}, &seeder{store: store}
	addrs := []string{a.start(t), b.start(t)}

	dl := New(meta.FileID, storage.NewStore(t.TempDir()), newNode(t), Options{Providers: addrs, Concurrency: 4})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := dl.Download(ctx, filepath.Join(t.TempDir(), "out.bin")); err != nil {
		t.Fatalf("download failed: %v", err)
	}

	na, nb := a.chunkRequests.Load(), b.chunkRequests.Load()
	if na == 0 || nb == 0 {
		t.Fatalf("chunk requests per provider = %d and %d; want both providers used", na, nb)
	}
}

// TestDownloader_FailsFastAndStopsWorkers checks that when a chunk cannot
// be fetched from anyone, the download fails without the other workers
// continuing through every remaining chunk.
func TestDownloader_FailsFastAndStopsWorkers(t *testing.T) {
	fastBackoff(t)
	data := testData(200 * 4 * 1024)
	store, meta := seedFile(t, data, 4*1024)
	s := &seeder{store: store, failChunks: true}
	addr := s.start(t)

	dl := New(meta.FileID, storage.NewStore(t.TempDir()), newNode(t), Options{Providers: []string{addr}, Concurrency: 4})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := dl.Download(ctx, filepath.Join(t.TempDir(), "out.bin")); err == nil {
		t.Fatal("download succeeded with a provider that serves no chunks")
	}
	if n := int(s.chunkRequests.Load()); n > 4*maxChunkAttempts {
		t.Fatalf("%d chunk requests made after the first fatal failure; workers were not cancelled", n)
	}
}

// TestDownloader_RejectsSubstitutedManifest simulates a malicious provider
// that answers every metadata request with a manifest for content of its
// choosing. The manifest is internally consistent (its chunk hashes match
// the chunks it serves), so only the CID check can catch it.
func TestDownloader_RejectsSubstitutedManifest(t *testing.T) {
	fastBackoff(t)
	evilStore, evil := seedFile(t, []byte("malicious weights"), 0)
	_, wanted := seedFile(t, []byte("genuine weights"), 0)
	addr := (&seeder{store: evilStore, manifest: &evil}).start(t)

	dl := New(wanted.FileID, storage.NewStore(t.TempDir()), newNode(t), Options{Providers: []string{addr}})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dest := filepath.Join(t.TempDir(), "out.bin")
	if _, err := dl.Download(ctx, dest); err == nil {
		t.Fatal("download succeeded with a substituted manifest")
	}
	if _, err := os.Stat(dest); !os.IsNotExist(err) {
		t.Fatalf("output file was created despite rejected manifest (stat err = %v)", err)
	}
}

func TestDownloader_RejectsInvalidFileID(t *testing.T) {
	dl := New("../../etc", storage.NewStore(t.TempDir()), newNode(t), Options{Providers: []string{"127.0.0.1:1"}})
	if _, err := dl.Download(context.Background(), ""); err == nil {
		t.Fatal("download accepted an invalid file ID")
	}
}
