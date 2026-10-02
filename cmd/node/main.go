// Command node runs a peer of the P2P model distribution network.
//
// Every node joins the DHT, serves the complete files in its data
// directory, and announces them so other peers can find them.
//
//	node -seed model.bin                      import a file and seed it
//	node -bootstrap 1.2.3.4:9000              serve stored files
//	node -bootstrap 1.2.3.4:9000 -download ID download a file, then keep seeding it
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/IEEECS-VIT/p2p-model-distribution/internal/downloader"
	"github.com/IEEECS-VIT/p2p-model-distribution/internal/node"
)

type options struct {
	dataDir           string
	listen            string
	external          string
	bootstrap         []string
	seedFile          string
	chunkSizeKB       int
	downloadID        string
	peers             []string
	outPath           string
	exitAfterDownload bool
	timeout           time.Duration
	logLevel          slog.Level
}

func main() {
	opts, err := parseFlags(os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(2)
	}

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: opts.logLevel})))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, opts); err != nil {
		ui.errorf("%v", err)
		os.Exit(1)
	}
}

func parseFlags(args []string) (options, error) {
	var o options
	var bootstrap, peers, logLevel string

	fs := flag.NewFlagSet("node", flag.ContinueOnError)
	fs.StringVar(&o.dataDir, "data", "./node-data", "directory for the node key, chunks and manifests")
	fs.StringVar(&o.listen, "listen", ":9000", "address to accept peer connections on")
	fs.StringVar(&o.external, "external", "", "address advertised to peers (ip:port), if different from the listen address")
	fs.StringVar(&bootstrap, "bootstrap", "", "comma-separated bootstrap peer addresses (ip:port)")
	fs.StringVar(&o.seedFile, "seed", "", "import this file into the data directory and seed it")
	fs.IntVar(&o.chunkSizeKB, "chunk-kb", 1024, "chunk size in KiB for -seed (1 to 8192)")
	fs.StringVar(&o.downloadID, "download", "", "file ID to download")
	fs.StringVar(&peers, "peer", "", "comma-separated provider addresses to download from directly, in addition to the DHT")
	fs.StringVar(&o.outPath, "out", "", "destination for the downloaded file (default: OS temp dir)")
	fs.BoolVar(&o.exitAfterDownload, "exit-after-download", false, "exit after downloading instead of seeding the file")
	fs.DurationVar(&o.timeout, "timeout", 0, "give up on the download after this long (0 = no limit)")
	fs.StringVar(&logLevel, "log-level", "info", "log level: debug, info, warn or error")
	if err := fs.Parse(args); err != nil {
		return o, err
	}
	if fs.NArg() > 0 {
		return o, fmt.Errorf("unexpected arguments: %v", fs.Args())
	}

	o.bootstrap = splitList(bootstrap)
	o.peers = splitList(peers)
	if err := o.logLevel.UnmarshalText([]byte(logLevel)); err != nil {
		return o, fmt.Errorf("invalid -log-level %q", logLevel)
	}
	if o.chunkSizeKB < 1 || o.chunkSizeKB > 8192 {
		return o, fmt.Errorf("-chunk-kb must be between 1 and 8192, got %d", o.chunkSizeKB)
	}
	if o.downloadID == "" && (o.outPath != "" || len(o.peers) > 0 || o.exitAfterDownload || o.timeout != 0) {
		return o, errors.New("-out, -peer, -exit-after-download and -timeout require -download")
	}
	if o.downloadID != "" && len(o.bootstrap) == 0 && len(o.peers) == 0 {
		return o, errors.New("-download needs -bootstrap or -peer to find providers")
	}
	return o, nil
}

func run(ctx context.Context, o options) error {
	ui.header()

	n, err := node.New(node.Config{
		DataDir:      o.dataDir,
		ListenAddr:   o.listen,
		ExternalAddr: o.external,
		Bootstrap:    o.bootstrap,
	})
	if err != nil {
		return err
	}
	if err := n.Start(); err != nil {
		return err
	}
	defer func() {
		ui.infof("Stopping...")
		n.Stop()
	}()

	self := n.Self()
	if o.external != "" {
		ui.infof("Node ID %s, listening on %s (advertised as %s)", ui.bold(self.ID), o.listen, self.Endpoint())
	} else {
		ui.infof("Node ID %s, listening on %s", ui.bold(self.ID), o.listen)
	}

	if o.seedFile != "" {
		ui.infof("Importing %s...", o.seedFile)
		meta, err := n.AddFile(o.seedFile, o.chunkSizeKB*1024)
		if err != nil {
			return fmt.Errorf("import %s: %w", o.seedFile, err)
		}
		ui.successf("Seeding %s (%d chunks, %.2f MiB)", meta.FileName, meta.NumChunks, float64(meta.FileSize)/(1<<20))
		ui.successf("File ID: %s", ui.bold(meta.FileID))
	}

	if len(o.bootstrap) > 0 {
		waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := n.WaitForPeers(waitCtx)
		cancel()
		if err != nil && ctx.Err() == nil {
			ui.warnf("No bootstrap peer reachable yet; retrying in the background")
		}
	}

	if o.downloadID != "" {
		if err := download(ctx, n, o); err != nil {
			return err
		}
		if o.exitAfterDownload {
			return nil
		}
	}

	if seeding := n.Seeding(); len(seeding) > 0 {
		ui.infof("Serving %d file(s); press Ctrl-C to stop", len(seeding))
	} else {
		ui.infof("Serving as a DHT node; press Ctrl-C to stop")
	}
	<-ctx.Done()
	return nil
}

func download(ctx context.Context, n *node.Node, o options) error {
	if o.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, o.timeout)
		defer cancel()
	}

	ui.infof("Downloading %s...", ui.bold(o.downloadID))
	start := time.Now()
	meta, err := n.Download(ctx, o.downloadID, o.outPath, downloader.Options{
		Providers: o.peers,
		Progress:  ui.progress,
	})
	ui.endProgress()
	if err != nil {
		return fmt.Errorf("download failed: %w", err)
	}

	out := o.outPath
	if out == "" {
		out = filepath.Join(os.TempDir(), meta.FileName)
	}
	ui.successf("Downloaded and verified %s (%.2f MiB) in %v", meta.FileName, float64(meta.FileSize)/(1<<20), time.Since(start).Round(time.Millisecond))
	ui.successf("Saved to %s", out)
	if !o.exitAfterDownload {
		ui.infof("Now seeding %s to other peers", meta.FileName)
	}
	return nil
}

func splitList(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}
