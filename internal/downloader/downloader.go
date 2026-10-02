package downloader

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
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

// ANSI terminal color codes
const (
	colorReset  = "\033[0m"
	colorBold   = "\033[1m"
	colorGreen  = "\033[32m"
	colorCyan   = "\033[36m"
	colorBlue   = "\033[34m"
	colorYellow = "\033[33m"
	colorRed    = "\033[31m"
)

type Downloader struct {
	fileID           string
	dataDir          string
	svc              *dht.Service
	store            *storage.Store
	concurrency      int
	initialProviders []string

	mu               sync.Mutex
	providerAddrs    []string
	providerFailures map[string]int
	connections      map[string]*network.Connection // active connections dialed locally (when svc == nil)
}

func New(fileID string, dataDir string, svc *dht.Service, store *storage.Store, initialProviders []string, concurrency int) *Downloader {
	if concurrency <= 0 {
		concurrency = 4
	}
	return &Downloader{
		fileID:           fileID,
		dataDir:          dataDir,
		svc:              svc,
		store:            store,
		concurrency:      concurrency,
		initialProviders: initialProviders,
		providerFailures: make(map[string]int),
		connections:      make(map[string]*network.Connection),
	}
}

func (d *Downloader) ResolveProviders(ctx context.Context) error {
	var resolvedAddrs []string
	for _, p := range d.initialProviders {
		if p == "" {
			continue
		}
		if isAddress(p) {
			resolvedAddrs = append(resolvedAddrs, p)
		} else if d.svc != nil {
			addr, ok := d.svc.QueryNodeAddress(ctx, p)
			if ok && addr != "" {
				resolvedAddrs = append(resolvedAddrs, addr)
			}
		}
	}

	d.mu.Lock()
	d.providerAddrs = resolvedAddrs
	needDHTLookup := len(d.providerAddrs) == 0 && d.svc != nil
	d.mu.Unlock()

	if needDHTLookup {
		// Try a direct lookup on the DHT network
		newProviders := d.svc.FindProviders(ctx, d.fileID)
		var dhtResolved []string
		for _, p := range newProviders {
			if p == d.svc.Self().ID || p == "" {
				continue
			}
			addr, ok := d.svc.QueryNodeAddress(ctx, p)
			if ok && addr != "" {
				dhtResolved = append(dhtResolved, addr)
			}
		}

		d.mu.Lock()
		d.providerAddrs = append(d.providerAddrs, dhtResolved...)
		d.mu.Unlock()
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.providerAddrs) == 0 {
		return fmt.Errorf("no dialable provider addresses resolved")
	}

	return nil
}

// Download fetches the manifest, downloads all chunks in parallel, verifies them, and reassembles the file.
func (d *Downloader) Download(ctx context.Context, outPath string) (filemeta.FileMeta, error) {
	// 1. Resolve providers
	if err := d.ResolveProviders(ctx); err != nil {
		return filemeta.FileMeta{}, err
	}

	// 2. Download manifest
	meta, err := d.downloadManifest(ctx)
	if err != nil {
		return filemeta.FileMeta{}, fmt.Errorf("download manifest: %w", err)
	}

	fmt.Printf("%s[DOWNLOAD]%s ✓ Manifest retrieved. File: %s%s%s (%d chunks, size: %.2f MB)\n",
		colorGreen, colorReset, colorBold, meta.FileName, colorReset, meta.NumChunks, float64(meta.FileSize)/(1024*1024))

	// Initialize downloader folders
	if err := d.store.InitializeFileDirectories(d.fileID); err != nil {
		return filemeta.FileMeta{}, fmt.Errorf("failed to prepare directories: %w", err)
	}

	// 3. Download all chunks in parallel
	start := time.Now()
	jobs := make(chan int, meta.NumChunks)
	for i := 0; i < meta.NumChunks; i++ {
		jobs <- i
	}
	close(jobs)

	var wg sync.WaitGroup
	errChan := make(chan error, meta.NumChunks)
	var completedCount int32

	for w := 0; w < d.concurrency; w++ {
		wg.Add(1)
		go d.worker(ctx, jobs, errChan, &wg, meta, &completedCount)
	}

	wg.Wait()
	close(errChan)

	// Clean up local connections if we dialed any
	d.cleanupLocalConnections()

	// Check if any workers returned errors
	if len(errChan) > 0 {
		return filemeta.FileMeta{}, <-errChan
	}

	duration := time.Since(start)
	fmt.Printf("\n%s[DOWNLOAD]%s ✓ All chunks downloaded and verified in %v!\n", colorGreen, colorReset, duration)

	// 4. Save manifest locally
	if err := d.store.SaveManifest(meta); err != nil {
		return filemeta.FileMeta{}, fmt.Errorf("failed to save manifest: %w", err)
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

	// 5. Reassemble
	fmt.Printf("%s[DOWNLOAD]%s Reassembling chunks into target destination: %s%s%s...\n",
		colorBlue, colorReset, colorBold, outPath, colorReset)

	if err := filemeta.AssembleChunks(d.store.Layout().ChunksDir(d.fileID), outPath, meta.Chunks); err != nil {
		return filemeta.FileMeta{}, fmt.Errorf("reassembly failed: %w", err)
	}

	// 6. Final verification
	fmt.Printf("%s[DOWNLOAD]%s Running final file SHA-256 validation...\n", colorBlue, colorReset)
	if err := filemeta.VerifyFile(outPath, meta.ModelHash); err != nil {
		return filemeta.FileMeta{}, fmt.Errorf("final file integrity check failed: %w", err)
	}

	fmt.Printf("%s[DOWNLOAD]%s %s★ SUCCESS! File reassembled and hash verified cleanly ★%s\n",
		colorGreen, colorReset, colorBold, colorReset)

	return meta, nil
}

func (d *Downloader) downloadManifest(ctx context.Context) (filemeta.FileMeta, error) {
	d.mu.Lock()
	addrs := append([]string(nil), d.providerAddrs...)
	d.mu.Unlock()

	var lastErr error
	for _, addr := range addrs {
		conn, err := d.getConnection(addr)
		if err != nil {
			lastErr = err
			d.markProviderFailed(addr, err)
			continue
		}

		reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		metaReq := &protocol.GetMetadataRequest{FileId: d.fileID}
		respEnv, err := conn.SendRequest(reqCtx, protocol.MessageType_MSG_GET_METADATA_REQUEST, metaReq)
		cancel()

		if err != nil {
			lastErr = err
			d.markProviderFailed(addr, err)
			continue
		}

		var metaResp protocol.GetMetadataResponse
		if err := proto.Unmarshal(respEnv.Payload, &metaResp); err != nil {
			lastErr = err
			d.markProviderFailed(addr, err)
			continue
		}

		if !metaResp.Success {
			lastErr = fmt.Errorf("provider returned error: %s", metaResp.Error)
			d.markProviderFailed(addr, lastErr)
			continue
		}

		var meta filemeta.FileMeta
		if err := json.Unmarshal(metaResp.MetadataJson, &meta); err != nil {
			lastErr = err
			d.markProviderFailed(addr, err)
			continue
		}

		if err := meta.Validate(); err != nil {
			lastErr = fmt.Errorf("provider sent invalid manifest: %w", err)
			d.markProviderFailed(addr, lastErr)
			continue
		}

		// The file ID is the manifest CID, so this is what authenticates
		// the manifest (and every hash inside it) against what the user
		// asked for. Without it a malicious provider could serve any
		// content with self-consistent hashes.
		if err := meta.VerifyID(d.fileID); err != nil {
			lastErr = fmt.Errorf("provider sent a manifest for different content: %w", err)
			d.markProviderFailed(addr, lastErr)
			continue
		}
		meta.FileID = d.fileID

		// Reset failure count on success
		d.mu.Lock()
		d.providerFailures[addr] = 0
		d.mu.Unlock()

		return meta, nil
	}

	return filemeta.FileMeta{}, fmt.Errorf("failed to fetch manifest from any provider, last error: %v", lastErr)
}

func (d *Downloader) worker(ctx context.Context, jobs <-chan int, errChan chan<- error, wg *sync.WaitGroup, meta filemeta.FileMeta, completedCount *int32) {
	defer wg.Done()

	for chunkIdx := range jobs {
		chunkMeta := meta.Chunks[chunkIdx]

		var downloaded bool
		var lastErr error

		d.mu.Lock()
		numProviders := len(d.providerAddrs)
		d.mu.Unlock()

		maxRetries := numProviders * 2
		if maxRetries < 3 {
			maxRetries = 3
		}

		for attempt := 0; attempt < maxRetries; attempt++ {
			select {
			case <-ctx.Done():
				select {
				case errChan <- ctx.Err():
				default:
				}
				return
			default:
			}

			addr, err := d.getHealthyProvider(ctx)
			if err != nil {
				lastErr = err
				break
			}

			conn, err := d.getConnection(addr)
			if err != nil {
				d.markProviderFailed(addr, err)
				lastErr = err
				continue
			}

			reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			chunkReq := &protocol.GetChunkRequest{
				FileId:     d.fileID,
				ChunkIndex: int32(chunkIdx),
			}
			respEnv, err := conn.SendRequest(reqCtx, protocol.MessageType_MSG_GET_CHUNK_REQUEST, chunkReq)
			cancel()

			if err != nil {
				d.markProviderFailed(addr, err)
				lastErr = err
				continue
			}

			var chunkResp protocol.GetChunkResponse
			if err := proto.Unmarshal(respEnv.Payload, &chunkResp); err != nil {
				d.markProviderFailed(addr, err)
				lastErr = err
				continue
			}

			if !chunkResp.Success {
				err := fmt.Errorf("provider returned error: %s", chunkResp.Error)
				d.markProviderFailed(addr, err)
				lastErr = err
				continue
			}

			// Verify chunk integrity
			if len(chunkResp.Data) != chunkMeta.Size {
				err := fmt.Errorf("chunk %d has %d bytes, want %d", chunkIdx, len(chunkResp.Data), chunkMeta.Size)
				d.markProviderFailed(addr, err)
				lastErr = err
				continue
			}
			if err := filemeta.VerifyChunk(chunkResp.Data, chunkMeta.Hash); err != nil {
				err := fmt.Errorf("chunk hash mismatch: %w", err)
				d.markProviderFailed(addr, err)
				lastErr = err
				continue
			}

			// Save chunk to disk
			if err := d.store.WriteChunk(d.fileID, chunkIdx, chunkResp.Data); err != nil {
				lastErr = fmt.Errorf("failed to write chunk: %w", err)
				// Disk error is fatal for this download job, not provider specific
				break
			}

			// Success! Reset failures.
			d.mu.Lock()
			d.providerFailures[addr] = 0
			d.mu.Unlock()

			downloaded = true
			newCompleted := atomic.AddInt32(completedCount, 1)
			fmt.Printf("\r%s[DOWNLOAD]%s Fetching chunks: %d/%d [%d%%]",
				colorYellow, colorReset, newCompleted, meta.NumChunks, (newCompleted * 100 / int32(meta.NumChunks)))
			break
		}

		if !downloaded {
			select {
			case errChan <- fmt.Errorf("failed to download chunk %d after retries: %v", chunkIdx, lastErr):
			default:
			}
			return
		}
	}
}

func (d *Downloader) getHealthyProvider(ctx context.Context) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	var bestAddr string
	minFailures := 999999

	for _, addr := range d.providerAddrs {
		failCount := d.providerFailures[addr]
		if failCount < 3 && failCount < minFailures {
			minFailures = failCount
			bestAddr = addr
		}
	}

	if bestAddr != "" {
		return bestAddr, nil
	}

	// If all providers have >= 3 failures, try to discover more via DHT
	if d.svc != nil {
		d.mu.Unlock()
		log.Printf("[DOWNLOADER] All providers failing. Querying DHT for new providers...")
		newProviders := d.svc.FindProviders(ctx, d.fileID)
		var resolvedAddrs []string
		for _, p := range newProviders {
			if p == d.svc.Self().ID || p == "" {
				continue
			}
			addr, ok := d.svc.QueryNodeAddress(ctx, p)
			if ok && addr != "" {
				resolvedAddrs = append(resolvedAddrs, addr)
			}
		}
		d.mu.Lock()

		for _, addr := range resolvedAddrs {
			found := false
			for _, existing := range d.providerAddrs {
				if existing == addr {
					found = true
					break
				}
			}
			if !found {
				d.providerAddrs = append(d.providerAddrs, addr)
				d.providerFailures[addr] = 0
				log.Printf("[DOWNLOADER] Discovered new provider address from DHT: %s", addr)
			}
		}
	}

	// If we still have no providers or all are failing, reset failure counters of existing ones and try again
	if len(d.providerAddrs) > 0 {
		for _, addr := range d.providerAddrs {
			d.providerFailures[addr] = 0
		}
		return d.providerAddrs[0], nil
	}

	return "", fmt.Errorf("no providers available")
}

func (d *Downloader) getConnection(addr string) (*network.Connection, error) {
	if d.svc != nil {
		return d.svc.GetConnection(addr)
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if conn, exists := d.connections[addr]; exists {
		return conn, nil
	}

	rawConn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, err
	}

	conn := network.NewConnection(rawConn, addr)
	conn.Start()
	d.connections[addr] = conn
	return conn, nil
}

func (d *Downloader) markProviderFailed(addr string, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.providerFailures[addr]++
	log.Printf("[DOWNLOADER] Error from provider %s: %v (fail count: %d)", addr, err, d.providerFailures[addr])

	// Close and remove the cached connection if we dialed it locally (legacy mode)
	if d.svc == nil {
		if conn, exists := d.connections[addr]; exists {
			_ = conn.Close()
			delete(d.connections, addr)
		}
	}
}

func (d *Downloader) cleanupLocalConnections() {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.svc == nil {
		for addr, conn := range d.connections {
			_ = conn.Close()
			delete(d.connections, addr)
		}
	}
}

func isAddress(s string) bool {
	_, _, err := net.SplitHostPort(s)
	return err == nil
}
