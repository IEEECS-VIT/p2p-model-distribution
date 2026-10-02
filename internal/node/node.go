// Package node runs a full peer: the DHT service, the file store, the
// file-transfer handlers, and provider announcements for every complete
// file the node holds.
package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	"github.com/IEEECS-VIT/p2p-model-distribution/internal/dht"
	"github.com/IEEECS-VIT/p2p-model-distribution/internal/downloader"
	"github.com/IEEECS-VIT/p2p-model-distribution/internal/filemeta"
	"github.com/IEEECS-VIT/p2p-model-distribution/internal/identity"
	"github.com/IEEECS-VIT/p2p-model-distribution/internal/storage"
	"github.com/IEEECS-VIT/p2p-model-distribution/internal/transfer"
)

// RepublishInterval is how often every seeded file is re-announced. It
// must be well under dht.ProviderTTL so records never lapse while the
// node is up. Var so tests can shrink it.
var RepublishInterval = 10 * time.Minute

// announceTimeout bounds a single announcement round for one file.
const announceTimeout = 60 * time.Second

// Config configures a Node.
type Config struct {
	// DataDir holds the file store and, unless Identity is set, the node
	// key (DataDir/node.key).
	DataDir string
	// ListenAddr is the TCP address to accept peers on, e.g. ":9000".
	ListenAddr string
	// ExternalAddr, if set, is the address advertised to peers.
	ExternalAddr string
	// Bootstrap lists peers ("ip:port") to join the network through.
	Bootstrap []string
	// Identity overrides the key stored in DataDir.
	Identity *identity.Identity
}

// Node is a running peer.
type Node struct {
	svc   *dht.Service
	store *storage.Store

	mu      sync.Mutex
	seeding map[string]bool
	kick    chan struct{}

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New creates a Node. Call Start to begin serving.
func New(cfg Config) (*Node, error) {
	id := cfg.Identity
	if id == nil {
		var err error
		id, err = identity.LoadOrCreate(filepath.Join(cfg.DataDir, "node.key"))
		if err != nil {
			return nil, fmt.Errorf("load node identity: %w", err)
		}
	}
	store := storage.NewStore(cfg.DataDir)
	svc := dht.NewService(id, cfg.ListenAddr, cfg.ExternalAddr, cfg.Bootstrap)
	transfer.NewServer(store).Register(svc.Router())

	return &Node{
		svc:     svc,
		store:   store,
		seeding: make(map[string]bool),
		kick:    make(chan struct{}, 1),
	}, nil
}

// Start begins accepting peers, joins the network, and seeds every
// complete file already in the store.
func (n *Node) Start() error {
	if err := n.svc.Start(); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	n.cancel = cancel

	ids, err := n.store.CompleteFiles()
	if err != nil {
		slog.Warn("could not list stored files", "err", err)
	}
	n.mu.Lock()
	for _, id := range ids {
		n.seeding[id] = true
	}
	n.mu.Unlock()

	n.wg.Add(1)
	go n.announceLoop(ctx)
	return nil
}

// Stop stops announcing and shuts down all connections.
func (n *Node) Stop() {
	if n.cancel != nil {
		n.cancel()
	}
	n.wg.Wait()
	n.svc.Stop()
}

// Self returns this node's ID and advertised address.
func (n *Node) Self() dht.Node { return n.svc.Self() }

// Service returns the underlying DHT service.
func (n *Node) Service() *dht.Service { return n.svc }

// Store returns the node's file store.
func (n *Node) Store() *storage.Store { return n.store }

// Seeding returns the IDs of the files this node announces.
func (n *Node) Seeding() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	ids := make([]string, 0, len(n.seeding))
	for id := range n.seeding {
		ids = append(ids, id)
	}
	return ids
}

// AddFile imports the file at path into the store and starts seeding it.
func (n *Node) AddFile(path string, chunkSize int) (filemeta.FileMeta, error) {
	meta, err := n.store.StoreModel(path, chunkSize)
	if err != nil {
		return filemeta.FileMeta{}, err
	}
	n.seed(meta.FileID)
	return meta, nil
}

// Download fetches fileID into the store, writes it to outPath, and then
// seeds it so other peers can download from this node too.
func (n *Node) Download(ctx context.Context, fileID, outPath string, opts downloader.Options) (filemeta.FileMeta, error) {
	meta, err := downloader.New(fileID, n.store, n.svc, opts).Download(ctx, outPath)
	if err != nil {
		return filemeta.FileMeta{}, err
	}
	n.seed(fileID)
	return meta, nil
}

// WaitForPeers blocks until the node has at least one verified peer in its
// routing table, or ctx is done.
func (n *Node) WaitForPeers(ctx context.Context) error {
	for n.svc.RoutingTableSize() == 0 {
		select {
		case <-ctx.Done():
			return errors.New("no peers reachable")
		case <-time.After(100 * time.Millisecond):
		}
	}
	return nil
}

func (n *Node) seed(fileID string) {
	n.mu.Lock()
	n.seeding[fileID] = true
	n.mu.Unlock()
	select {
	case n.kick <- struct{}{}:
	default:
	}
}

// announceLoop announces every seeded file when the node first joins the
// network, whenever a file is added, and every RepublishInterval.
func (n *Node) announceLoop(ctx context.Context) {
	defer n.wg.Done()

	ticker := time.NewTicker(RepublishInterval)
	defer ticker.Stop()

	joined := make(chan struct{})
	go func() {
		if n.WaitForPeers(ctx) == nil {
			close(joined)
		}
	}()

	announced := map[string]bool{} // files announced since the node joined
	for {
		all := false
		select {
		case <-ctx.Done():
			return
		case <-joined:
			joined = nil
			all = true
		case <-ticker.C:
			all = true
		case <-n.kick:
		}
		if n.svc.RoutingTableSize() == 0 {
			continue // nobody to announce to yet; "joined" will fire later
		}

		for _, id := range n.Seeding() {
			if !all && announced[id] {
				continue
			}
			actx, cancel := context.WithTimeout(ctx, announceTimeout)
			count := n.svc.AnnounceProvider(actx, id)
			cancel()
			announced[id] = true
			slog.Debug("announced file", "file", id, "accepted_by", count)
		}
	}
}
