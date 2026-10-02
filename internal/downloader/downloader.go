// Package downloader fetches a file from peers: it authenticates the
// manifest against the file ID, downloads chunks in parallel from every
// available provider, verifies each chunk, and reassembles the file.
package downloader

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IEEECS-VIT/p2p-model-distribution/internal/dht"
	"github.com/IEEECS-VIT/p2p-model-distribution/internal/filemeta"
	"github.com/IEEECS-VIT/p2p-model-distribution/internal/network"
	"github.com/IEEECS-VIT/p2p-model-distribution/internal/protocol"
	"github.com/IEEECS-VIT/p2p-model-distribution/internal/storage"
	"google.golang.org/protobuf/proto"
)

const (
	// DefaultConcurrency is the number of chunks fetched in parallel.
	DefaultConcurrency = 4

	// requestTimeout bounds a single metadata or chunk request (a chunk
	// is at most filemeta.MaxChunkSize).
	requestTimeout = 60 * time.Second

	// maxChunkAttempts is how many times a chunk is requested (from any
	// providers) before the download fails.
	maxChunkAttempts = 8

	// rediscoverInterval rate-limits DHT lookups for more providers.
	rediscoverInterval = 10 * time.Second
)

// Vars rather than consts so tests can shrink them.
var (
	// Failing providers are retried after an exponential backoff.
	baseBackoff = 500 * time.Millisecond
	maxBackoff  = 30 * time.Second

	// providerWaitTimeout is how long a worker waits for any provider to
	// become available before giving up.
	providerWaitTimeout = 2 * time.Minute
)

// Network is the subset of dht.Service the downloader needs.
type Network interface {
	FindProviders(ctx context.Context, key string) []dht.Node
	Connect(ctx context.Context, node dht.Node) (*network.Connection, error)
	ConnectAddr(ctx context.Context, addr string) (*network.Connection, error)
}

// Options configures a Downloader.
type Options struct {
	// Providers are addresses ("ip:port") to download from in addition
	// to providers discovered through the DHT.
	Providers []string
	// Concurrency is the number of chunks fetched in parallel
	// (DefaultConcurrency if <= 0).
	Concurrency int
	// Progress, if set, is called after each chunk is stored.
	Progress func(done, total int)
}

type provider struct {
	addr     string
	node     *dht.Node // set when found via the DHT; its ID is pinned when dialing
	inflight int
	failures int
	retryAt  time.Time
}

// Downloader downloads a single file by ID.
type Downloader struct {
	fileID string
	store  *storage.Store
	nw     Network
	opts   Options

	mu              sync.Mutex
	providers       map[string]*provider // by address
	lastDiscovery   time.Time
	providerChanged chan struct{} // closed and replaced when providers are added
}

// New creates a Downloader for fileID that stores chunks in store and
// finds and reaches providers through nw.
func New(fileID string, store *storage.Store, nw Network, opts Options) *Downloader {
	if opts.Concurrency <= 0 {
		opts.Concurrency = DefaultConcurrency
	}
	d := &Downloader{
		fileID:          fileID,
		store:           store,
		nw:              nw,
		opts:            opts,
		providers:       make(map[string]*provider),
		providerChanged: make(chan struct{}),
	}
	for _, addr := range opts.Providers {
		if _, _, err := net.SplitHostPort(addr); err == nil {
			d.providers[addr] = &provider{addr: addr}
		}
	}
	return d
}

// Download fetches and verifies the manifest and every chunk, then
// reassembles the file at outPath (or a file named after the manifest in
// the OS temp dir if outPath is empty). It returns the manifest.
func (d *Downloader) Download(ctx context.Context, outPath string) (filemeta.FileMeta, error) {
	if !storage.ValidFileID(d.fileID) {
		return filemeta.FileMeta{}, fmt.Errorf("invalid file ID %q", d.fileID)
	}

	d.discover(ctx, true)
	if d.providerCount() == 0 {
		return filemeta.FileMeta{}, errors.New("no providers found for this file")
	}

	meta, err := d.downloadManifest(ctx)
	if err != nil {
		return filemeta.FileMeta{}, fmt.Errorf("download manifest: %w", err)
	}
	slog.Info("manifest retrieved", "file", meta.FileName, "chunks", meta.NumChunks, "bytes", meta.FileSize)

	if err := d.store.InitializeFileDirectories(d.fileID); err != nil {
		return filemeta.FileMeta{}, fmt.Errorf("prepare directories: %w", err)
	}

	if err := d.downloadChunks(ctx, meta); err != nil {
		return filemeta.FileMeta{}, err
	}

	// Every chunk is on disk and verified; saving the manifest marks the
	// file complete in the store.
	if err := d.store.SaveManifest(meta); err != nil {
		return filemeta.FileMeta{}, fmt.Errorf("save manifest: %w", err)
	}

	if outPath == "" {
		// meta.FileName comes from a remote peer; Validate() already
		// rejected names that could escape the target directory, but
		// re-check here since this is where the name becomes a path.
		if err := filemeta.ValidateFileName(meta.FileName); err != nil {
			return filemeta.FileMeta{}, fmt.Errorf("unsafe file name in manifest: %w", err)
		}
		outPath = filepath.Join(os.TempDir(), meta.FileName)
	}

	// AssembleChunks verifies the whole-file hash before moving the
	// output into place, so outPath never holds an unverified file.
	if err := filemeta.AssembleChunks(d.store.Layout().ChunksDir(d.fileID), outPath, meta.Chunks, meta.ModelHash); err != nil {
		return filemeta.FileMeta{}, fmt.Errorf("reassembly failed: %w", err)
	}

	return meta, nil
}

func (d *Downloader) downloadManifest(ctx context.Context) (filemeta.FileMeta, error) {
	var lastErr error
	for attempt := 0; attempt < maxChunkAttempts; attempt++ {
		p, err := d.acquire(ctx)
		if err != nil {
			if lastErr != nil {
				return filemeta.FileMeta{}, fmt.Errorf("%w (last provider error: %v)", err, lastErr)
			}
			return filemeta.FileMeta{}, err
		}
		meta, err := d.fetchManifest(ctx, p)
		d.release(p, err)
		if err == nil {
			return meta, nil
		}
		if ctx.Err() != nil {
			return filemeta.FileMeta{}, ctx.Err()
		}
		lastErr = err
	}
	return filemeta.FileMeta{}, fmt.Errorf("no provider returned a valid manifest: %w", lastErr)
}

func (d *Downloader) fetchManifest(ctx context.Context, p *provider) (filemeta.FileMeta, error) {
	conn, err := d.connect(ctx, p)
	if err != nil {
		return filemeta.FileMeta{}, err
	}

	reqCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	respEnv, err := conn.SendRequest(reqCtx, protocol.MessageType_MSG_GET_METADATA_REQUEST, &protocol.GetMetadataRequest{FileId: d.fileID})
	if err != nil {
		return filemeta.FileMeta{}, err
	}

	var resp protocol.GetMetadataResponse
	if err := proto.Unmarshal(respEnv.Payload, &resp); err != nil {
		return filemeta.FileMeta{}, err
	}
	if !resp.Success {
		return filemeta.FileMeta{}, fmt.Errorf("provider returned error: %s", resp.Error)
	}

	var meta filemeta.FileMeta
	if err := json.Unmarshal(resp.MetadataJson, &meta); err != nil {
		return filemeta.FileMeta{}, err
	}
	if err := meta.Validate(); err != nil {
		return filemeta.FileMeta{}, fmt.Errorf("provider sent invalid manifest: %w", err)
	}
	// The file ID is the manifest CID, so this is what authenticates the
	// manifest (and every hash inside it) against what the user asked
	// for. Without it a malicious provider could serve any content with
	// self-consistent hashes.
	if err := meta.VerifyID(d.fileID); err != nil {
		return filemeta.FileMeta{}, fmt.Errorf("provider sent a manifest for different content: %w", err)
	}
	meta.FileID = d.fileID
	return meta, nil
}

func (d *Downloader) downloadChunks(ctx context.Context, meta filemeta.FileMeta) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	// Resume: chunks left on disk by an interrupted download are kept if
	// they still match the manifest; only missing or corrupt ones are
	// fetched (corrupt files are overwritten).
	needed := filemeta.VerifyAllChunks(d.store.Layout().ChunksDir(d.fileID), meta.Chunks)
	if have := meta.NumChunks - len(needed); have > 0 {
		slog.Info("resuming download", "verified_chunks", have, "remaining", len(needed))
		if d.opts.Progress != nil {
			d.opts.Progress(have, meta.NumChunks)
		}
	}

	jobs := make(chan int, len(needed))
	for _, i := range needed {
		jobs <- i
	}
	close(jobs)

	var done atomic.Int32
	done.Store(int32(meta.NumChunks - len(needed)))
	var wg sync.WaitGroup
	for w := 0; w < d.opts.Concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				if ctx.Err() != nil {
					return
				}
				if err := d.downloadChunk(ctx, meta.Chunks[idx]); err != nil {
					// The first failure cancels every other worker.
					cancel(err)
					return
				}
				n := int(done.Add(1))
				if d.opts.Progress != nil {
					d.opts.Progress(n, meta.NumChunks)
				}
			}
		}()
	}
	wg.Wait()

	return context.Cause(ctx)
}

func (d *Downloader) downloadChunk(ctx context.Context, chunk filemeta.ChunkMeta) error {
	var lastErr error
	for attempt := 0; attempt < maxChunkAttempts; attempt++ {
		p, err := d.acquire(ctx)
		if err != nil {
			if lastErr != nil {
				return fmt.Errorf("chunk %d: %w (last provider error: %v)", chunk.Index, err, lastErr)
			}
			return fmt.Errorf("chunk %d: %w", chunk.Index, err)
		}

		data, err := d.fetchChunk(ctx, p, chunk)
		d.release(p, err)
		if err != nil {
			if ctx.Err() != nil {
				return context.Cause(ctx)
			}
			slog.Debug("chunk request failed", "chunk", chunk.Index, "provider", p.addr, "err", err)
			lastErr = err
			continue
		}

		if err := d.store.WriteChunk(d.fileID, chunk.Index, data); err != nil {
			// A local disk error is not the provider's fault and won't be
			// fixed by retrying elsewhere.
			return fmt.Errorf("write chunk %d: %w", chunk.Index, err)
		}
		return nil
	}
	return fmt.Errorf("chunk %d failed after %d attempts: %w", chunk.Index, maxChunkAttempts, lastErr)
}

func (d *Downloader) fetchChunk(ctx context.Context, p *provider, chunk filemeta.ChunkMeta) ([]byte, error) {
	conn, err := d.connect(ctx, p)
	if err != nil {
		return nil, err
	}

	reqCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	req := &protocol.GetChunkRequest{FileId: d.fileID, ChunkIndex: int32(chunk.Index)}
	respEnv, err := conn.SendRequest(reqCtx, protocol.MessageType_MSG_GET_CHUNK_REQUEST, req)
	if err != nil {
		return nil, err
	}

	var resp protocol.GetChunkResponse
	if err := proto.Unmarshal(respEnv.Payload, &resp); err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, fmt.Errorf("provider returned error: %s", resp.Error)
	}
	if len(resp.Data) != chunk.Size {
		return nil, fmt.Errorf("chunk %d has %d bytes, want %d", chunk.Index, len(resp.Data), chunk.Size)
	}
	if err := filemeta.VerifyChunk(resp.Data, chunk.Hash); err != nil {
		return nil, err
	}
	return resp.Data, nil
}

func (d *Downloader) connect(ctx context.Context, p *provider) (*network.Connection, error) {
	if p.node != nil {
		return d.nw.Connect(ctx, *p.node)
	}
	return d.nw.ConnectAddr(ctx, p.addr)
}

// acquire picks the available provider with the fewest requests in
// flight (ties broken by fewest recent failures, then randomly), so load
// spreads across every provider. If none is available it triggers DHT
// rediscovery and waits for a provider to appear or come out of backoff.
func (d *Downloader) acquire(ctx context.Context) (*provider, error) {
	deadline := time.Now().Add(providerWaitTimeout)
	for {
		d.mu.Lock()
		now := time.Now()
		var best *provider
		var nextRetry time.Time
		ties := 0
		for _, p := range d.providers {
			if now.Before(p.retryAt) {
				if nextRetry.IsZero() || p.retryAt.Before(nextRetry) {
					nextRetry = p.retryAt
				}
				continue
			}
			switch {
			case best == nil || p.inflight < best.inflight ||
				(p.inflight == best.inflight && p.failures < best.failures):
				best, ties = p, 1
			case p.inflight == best.inflight && p.failures == best.failures:
				// Reservoir sampling for a uniform random tie-break.
				ties++
				if rand.IntN(ties) == 0 {
					best = p
				}
			}
		}
		if best != nil {
			best.inflight++
			d.mu.Unlock()
			return best, nil
		}
		changed := d.providerChanged
		d.mu.Unlock()

		if time.Now().After(deadline) {
			return nil, errors.New("no provider available")
		}

		go d.discover(context.WithoutCancel(ctx), false)

		wait := time.Until(deadline)
		if !nextRetry.IsZero() && time.Until(nextRetry) < wait {
			wait = time.Until(nextRetry)
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, context.Cause(ctx)
		case <-changed:
		case <-timer.C:
		}
		timer.Stop()
	}
}

// release returns a provider acquired with acquire, recording whether the
// request succeeded.
func (d *Downloader) release(p *provider, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	p.inflight--
	if err == nil {
		p.failures = 0
		p.retryAt = time.Time{}
		return
	}
	p.failures++
	backoff := min(baseBackoff<<min(p.failures-1, 16), maxBackoff)
	p.retryAt = time.Now().Add(backoff)
	slog.Debug("provider failed", "provider", p.addr, "failures", p.failures, "retry_in", backoff, "err", err)
}

// discover looks up providers in the DHT and adds any new ones. Unless
// force is set it is rate-limited to once per rediscoverInterval.
func (d *Downloader) discover(ctx context.Context, force bool) {
	d.mu.Lock()
	if !force && time.Since(d.lastDiscovery) < rediscoverInterval {
		d.mu.Unlock()
		return
	}
	d.lastDiscovery = time.Now()
	d.mu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	found := d.nw.FindProviders(ctx, d.fileID)

	d.mu.Lock()
	defer d.mu.Unlock()
	added := 0
	for _, n := range found {
		addr := n.Endpoint()
		if addr == "" {
			continue
		}
		node := n
		if p, ok := d.providers[addr]; ok {
			if p.node == nil {
				p.node = &node
			}
			continue
		}
		d.providers[addr] = &provider{addr: addr, node: &node}
		added++
	}
	if added > 0 {
		slog.Debug("discovered providers", "new", added, "total", len(d.providers))
		close(d.providerChanged)
		d.providerChanged = make(chan struct{})
	}
}

func (d *Downloader) providerCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.providers)
}
