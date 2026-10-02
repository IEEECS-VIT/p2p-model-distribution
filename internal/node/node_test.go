package node

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/IEEECS-VIT/p2p-model-distribution/internal/downloader"
)

func startNode(t *testing.T, bootstrap ...string) *Node {
	t.Helper()
	n, err := New(Config{DataDir: t.TempDir(), ListenAddr: "127.0.0.1:0", Bootstrap: bootstrap})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := n.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(n.Stop)
	return n
}

func waitForPeers(t *testing.T, n *Node) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := n.WaitForPeers(ctx); err != nil {
		t.Fatal(err)
	}
}

// waitForProvider polls until n can find provider for fileID.
func waitForProvider(t *testing.T, n *Node, fileID, providerID string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		found := n.Service().FindProviders(ctx, fileID)
		cancel()
		for _, p := range found {
			if p.ID == providerID {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("provider %s for %s never became discoverable", providerID, fileID)
}

// TestSwarm_DownloadersBecomeSeeders checks that a node that finished a
// download serves the file to others: after the original seeder goes
// away, a new node can still download the file from the first downloader.
func TestSwarm_DownloadersBecomeSeeders(t *testing.T) {
	data := bytes.Repeat([]byte("model weights "), 20000)
	src := filepath.Join(t.TempDir(), "model.bin")
	if err := os.WriteFile(src, data, 0o644); err != nil {
		t.Fatal(err)
	}

	boot := startNode(t)
	seeder := startNode(t, boot.Self().Endpoint())
	waitForPeers(t, seeder)
	meta, err := seeder.AddFile(src, 16*1024)
	if err != nil {
		t.Fatal(err)
	}

	first := startNode(t, boot.Self().Endpoint())
	waitForPeers(t, first)
	waitForProvider(t, first, meta.FileID, seeder.Self().ID)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := first.Download(ctx, meta.FileID, filepath.Join(t.TempDir(), "a.bin"), downloader.Options{}); err != nil {
		t.Fatalf("first download: %v", err)
	}

	seeder.Stop()

	second := startNode(t, boot.Self().Endpoint())
	waitForPeers(t, second)
	waitForProvider(t, second, meta.FileID, first.Self().ID)

	out := filepath.Join(t.TempDir(), "b.bin")
	if _, err := second.Download(ctx, meta.FileID, out, downloader.Options{}); err != nil {
		t.Fatalf("second download (seeder offline): %v", err)
	}
	got, _ := os.ReadFile(out)
	if !bytes.Equal(got, data) {
		t.Fatal("file downloaded from the first downloader differs from the original")
	}
}

// TestNode_SeedsStoredFilesOnRestart checks that a node announces the
// complete files already in its data directory when it starts.
func TestNode_SeedsStoredFilesOnRestart(t *testing.T) {
	dataDir := t.TempDir()
	src := filepath.Join(t.TempDir(), "model.bin")
	if err := os.WriteFile(src, bytes.Repeat([]byte("x"), 50000), 0o644); err != nil {
		t.Fatal(err)
	}

	first, err := New(Config{DataDir: dataDir, ListenAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	meta, err := first.AddFile(src, 0)
	if err != nil {
		t.Fatal(err)
	}
	// A partial download in the same store must not be announced.
	if err := first.Store().WriteChunk("deadbeef", 0, []byte("partial")); err != nil {
		t.Fatal(err)
	}

	boot := startNode(t)
	restarted, err := New(Config{DataDir: dataDir, ListenAddr: "127.0.0.1:0", Bootstrap: []string{boot.Self().Endpoint()}})
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.Start(); err != nil {
		t.Fatal(err)
	}
	defer restarted.Stop()

	if got := restarted.Seeding(); len(got) != 1 || got[0] != meta.FileID {
		t.Fatalf("Seeding() = %v, want [%s]", got, meta.FileID)
	}
	if restarted.Self().ID != first.Self().ID {
		t.Fatal("node identity changed across restarts")
	}

	other := startNode(t, boot.Self().Endpoint())
	waitForPeers(t, other)
	waitForProvider(t, other, meta.FileID, restarted.Self().ID)
}
