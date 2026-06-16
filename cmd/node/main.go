package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
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

func main() {
	mode := flag.String("mode", "seed", "running mode: 'seed' or 'download'")
	port := flag.String("port", "9000", "port to listen on")
	file := flag.String("file", "", "path to the file to seed")
	dataDir := flag.String("data", "./node-data", "directory where chunks and metadata are stored")
	chunkSizeKB := flag.Int("chunk-kb", 1024, "chunk size in KB (default 1024KB / 1MB)")

	// Download params (legacy, non-DHT)
	addr := flag.String("addr", "", "target seeder address to dial (e.g. 127.0.0.1:9000)")
	fileID := flag.String("file-id", "", "the unique File ID of the manifest to download")
	outPath := flag.String("out", "", "destination path for the reassembled file")

	// DHT flags
	dhtEnable := flag.Bool("dht", false, "enable DHT peer discovery")
	dhtBootstrap := flag.String("dht-bootstrap", "", "comma-separated bootstrap peer addresses (ip:port)")
	dhtNodeID := flag.String("dht-node-id", "", "node ID (random if empty)")
	dhtExternal := flag.String("dht-external", "", "external address advertised to peers (ip:port)")

	flag.Parse()

	printHeader()

	if *dhtEnable {
		runWithDHT(*mode, *port, *file, *dataDir, *chunkSizeKB*1024,
			*addr, *fileID, *outPath, *dhtNodeID, *dhtExternal, *dhtBootstrap)
	} else {
		runLegacy(*mode, *port, *file, *dataDir, *chunkSizeKB*1024,
			*addr, *fileID, *outPath)
	}
}

//---------------------------------------------------------------------
// Legacy (non-DHT) mode — original behaviour.

func runLegacy(mode, port, filePath, dataDir string, chunkSize int, addr, fileID, outPath string) {
	if mode == "seed" {
		runSeeder(port, filePath, dataDir, chunkSize)
	} else if mode == "download" {
		if addr == "" || fileID == "" {
			fmt.Printf("%s[ERROR]%s -addr and -file-id are required for download mode\n", colorRed, colorReset)
			flag.Usage()
			os.Exit(1)
		}
		runDownloader(addr, fileID, dataDir, outPath)
	} else {
		fmt.Printf("%s[ERROR]%s invalid mode '%s'. Choose 'seed' or 'download'\n", colorRed, colorReset, mode)
		os.Exit(1)
	}
}

//---------------------------------------------------------------------
// DHT-enabled mode.

func runWithDHT(mode, port, filePath, dataDir string, chunkSize int,
	addr, fileID, outPath, nodeID, external, bootstrapCSV string) {

	listenAddr := "0.0.0.0:" + port
	var seeds []string
	if bootstrapCSV != "" {
		for _, s := range strings.Split(bootstrapCSV, ",") {
			s = strings.TrimSpace(s)
			if s != "" {
				seeds = append(seeds, s)
			}
		}
	}

	// Create the DHT service.
	svc := dht.NewService(nodeID, listenAddr, external, seeds)
	store := storage.NewStore(dataDir)

	// Register file-transfer handlers on the DHT service's router.
	registerFileHandlers(svc.Router(), store)

	// Start the DHT service (TCP listener + bootstrap).
	if err := svc.Start(); err != nil {
		log.Fatalf("failed to start DHT service: %v", err)
	}

	self := svc.Self()
	fmt.Printf("%s[DHT]%s Node ID: %s%s%s  (advertised: %s:%d)\n",
		colorCyan, colorReset, colorBold, self.ID, colorReset, self.IP, self.Port)

	// Clean shutdown
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)

	if mode == "seed" {
		runDHSeeder(filePath, dataDir, chunkSize, svc, store)
		<-shutdown
	} else if mode == "download" {
		runDHDownloader(fileID, outPath, dataDir, svc, store)
		<-shutdown
	} else {
		fmt.Printf("%s[ERROR]%s invalid mode '%s'\n", colorRed, colorReset, mode)
		svc.Stop()
		os.Exit(1)
	}

	fmt.Printf("\n%s[SERVER]%s Stopping...\n", colorYellow, colorReset)
	svc.Stop()
	fmt.Printf("%s[SERVER]%s Stopped cleanly.\n", colorGreen, colorReset)
}

func registerFileHandlers(router *network.Router, store *storage.Store) {
	// Metadata handler
	router.Register(protocol.MessageType_MSG_GET_METADATA_REQUEST, func(conn *network.Connection, env *protocol.Envelope) error {
		var req protocol.GetMetadataRequest
		if err := proto.Unmarshal(env.Payload, &req); err != nil {
			return err
		}
		fmt.Printf("%s[RPC]%s Metadata Request for File ID: %s from %s\n",
			colorYellow, colorReset, req.FileId, conn.RemoteAddr().String())

		meta, err := store.LoadManifest(req.FileId)
		if err != nil {
			resp := &protocol.GetMetadataResponse{
				FileId:  req.FileId,
				Success: false,
				Error:   fmt.Sprintf("manifest not found: %v", err),
			}
			return conn.WriteResponse(env.Id, protocol.MessageType_MSG_GET_METADATA_RESPONSE, resp)
		}
		manifestBytes, err := json.Marshal(meta)
		if err != nil {
			resp := &protocol.GetMetadataResponse{
				FileId:  req.FileId,
				Success: false,
				Error:   fmt.Sprintf("failed to marshal manifest: %v", err),
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

	// Chunk handler
	router.Register(protocol.MessageType_MSG_GET_CHUNK_REQUEST, func(conn *network.Connection, env *protocol.Envelope) error {
		var req protocol.GetChunkRequest
		if err := proto.Unmarshal(env.Payload, &req); err != nil {
			return err
		}
		chunkData, err := store.ReadChunk(req.FileId, int(req.ChunkIndex))
		if err != nil {
			resp := &protocol.GetChunkResponse{
				FileId:     req.FileId,
				ChunkIndex: req.ChunkIndex,
				Success:    false,
				Error:      fmt.Sprintf("chunk read failed: %v", err),
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
}

//---------------------------------------------------------------------
// DHT Seeder

func runDHSeeder(filePath, dataDir string, chunkSize int, svc *dht.Service, store *storage.Store) {
	fileID := ""

	if filePath != "" {
		info, err := os.Stat(filePath)
		if err != nil {
			log.Fatalf("failed to open source file: %v", err)
		}

		fileID = generateRandomID()
		fmt.Printf("%s[SEEDER]%s Chunking %s%s%s (%.2f MB)...\n",
			colorBlue, colorReset, colorBold, info.Name(), colorReset, float64(info.Size())/(1024*1024))

		meta, cid, err := store.StoreModel(filePath, fileID, chunkSize)
		if err != nil {
			log.Fatalf("failed to split and seed model: %v", err)
		}

		fmt.Printf("%s[SEEDER]%s ✓ Chunks stored under: %s/%s/chunks\n", colorGreen, colorReset, dataDir, fileID)
		fmt.Printf("%s[SEEDER]%s ✓ File ID: %s%s%s (Use this ID to download)\n", colorGreen, colorReset, colorBold, fileID, colorReset)
		fmt.Printf("%s[SEEDER]%s ✓ Manifest CID: %s\n", colorGreen, colorReset, cid)

		// Announce file ID and each chunk hash in the DHT.
		fmt.Printf("%s[DHT]%s Announcing file in DHT...\n", colorBlue, colorReset)
		svc.AnnounceProvider(fileID)
		for _, c := range meta.Chunks {
			svc.AnnounceProvider(c.CID)
		}
		fmt.Printf("%s[DHT]%s ✓ File announced to %d connected peers\n", colorGreen, colorReset, len(svc.Self().ID))
	} else {
		// Serving existing data: try to discover FileIDs from the data directory.
		entries, err := os.ReadDir(dataDir)
		if err == nil {
			for _, e := range entries {
				if e.IsDir() {
					svc.AnnounceProvider(e.Name())
				}
			}
		}
		fmt.Printf("%s[SEEDER]%s Serving existing data from: %s\n", colorBlue, colorReset, dataDir)
	}

	fmt.Printf("%s[SERVER]%s TCP Server listening with DHT enabled...\n", colorGreen, colorReset)
}

//---------------------------------------------------------------------
// DHT Downloader

func runDHDownloader(fileID, outPath, dataDir string, svc *dht.Service, store *storage.Store) {
	if fileID == "" {
		log.Fatalf("-file-id is required for download mode")
	}

	fmt.Printf("%s[DHT]%s Looking up providers for File ID: %s%s%s...\n",
		colorBlue, colorReset, colorBold, fileID, colorReset)

	// Wait a moment for bootstrap to discover peers.
	time.Sleep(2 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	providers := svc.FindProviders(ctx, fileID)
	if len(providers) == 0 {
		log.Fatalf("no providers found for file ID %s", fileID)
	}

	fmt.Printf("%s[DHT]%s Found %d provider(s)\n", colorGreen, colorReset, len(providers))

	// Pick the first provider (or ourselves — skip if so).
	var targetAddr string
	selfID := svc.Self().ID
	for _, p := range providers {
		if p == selfID || p == "" {
			continue
		}
		// Look up the address from the service's address map.
		// We use the node ID to find the connection.
		targetAddr = p
		break
	}

	// If we only know providers by ID but not address, try to find anyone
	// in the routing table who can help.
	if targetAddr == "" {
		// Try connecting to bootstrap or routing-table peers and ask them.
		closest := svc.DHT().ClosestPeers(fileID, dht.K)
		for _, peer := range closest {
			if peer.ID == selfID {
				continue
			}
			addr := fmt.Sprintf("%s:%d", peer.IP, peer.Port)
			if addr == ":0" {
				continue
			}
			targetAddr = addr
			break
		}
	}

	if targetAddr == "" {
		log.Fatalf("could not resolve any provider address for file ID %s", fileID)
	}

	fmt.Printf("%s[DOWNLOAD]%s Connecting to provider at %s...\n", colorBlue, colorReset, targetAddr)

	rawConn, err := net.Dial("tcp", targetAddr)
	if err != nil {
		log.Fatalf("failed to connect to provider: %v", err)
	}
	defer rawConn.Close()

	conn := network.NewConnection(rawConn, "downloader-peer")
	conn.SetRouter(svc.Router())
	conn.Start()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel2()

	// 1. Fetch metadata
	fmt.Printf("%s[DOWNLOAD]%s → Requesting manifest for ID: %s%s%s...\n",
		colorBlue, colorReset, colorBold, fileID, colorReset)
	metaReq := &protocol.GetMetadataRequest{FileId: fileID}

	respEnv, err := conn.SendRequest(ctx2, protocol.MessageType_MSG_GET_METADATA_REQUEST, metaReq)
	if err != nil {
		log.Fatalf("failed to fetch metadata: %v", err)
	}

	var metaResp protocol.GetMetadataResponse
	if err := proto.Unmarshal(respEnv.Payload, &metaResp); err != nil {
		log.Fatalf("failed to decode metadata response: %v", err)
	}
	if !metaResp.Success {
		log.Fatalf("seeder returned error: %s", metaResp.Error)
	}

	var meta filemeta.FileMeta
	if err := json.Unmarshal(metaResp.MetadataJson, &meta); err != nil {
		log.Fatalf("failed to unmarshal manifest JSON: %v", err)
	}

	fmt.Printf("%s[DOWNLOAD]%s ✓ Manifest retrieved. File: %s%s%s (%d chunks, size: %.2f MB)\n",
		colorGreen, colorReset, colorBold, meta.FileName, colorReset, meta.NumChunks, float64(meta.FileSize)/(1024*1024))

	// Initialize downloader folders
	if err := store.InitializeFileDirectories(fileID); err != nil {
		log.Fatalf("failed to prepare directories: %v", err)
	}

	// 2. Fetch all chunks
	start := time.Now()
	for i, chunkMeta := range meta.Chunks {
		fmt.Printf("%s[DOWNLOAD]%s [%d/%d] Fetching chunk %d (%d KB)... ",
			colorYellow, colorReset, i+1, meta.NumChunks, chunkMeta.Index, chunkMeta.Size/1024)

		chunkReq := &protocol.GetChunkRequest{
			FileId:     fileID,
			ChunkIndex: int32(chunkMeta.Index),
		}

		chunkEnv, err := conn.SendRequest(ctx2, protocol.MessageType_MSG_GET_CHUNK_REQUEST, chunkReq)
		if err != nil {
			fmt.Printf("%s[FAIL]%s\n", colorRed, colorReset)
			log.Fatalf("failed to fetch chunk %d: %v", chunkMeta.Index, err)
		}

		var chunkResp protocol.GetChunkResponse
		if err := proto.Unmarshal(chunkEnv.Payload, &chunkResp); err != nil {
			fmt.Printf("%s[FAIL]%s\n", colorRed, colorReset)
			log.Fatalf("failed to decode chunk %d response: %v", chunkMeta.Index, err)
		}
		if !chunkResp.Success {
			fmt.Printf("%s[FAIL]%s\n", colorRed, colorReset)
			log.Fatalf("seeder chunk fetch error: %s", chunkResp.Error)
		}

		// Verify chunk integrity
		if err := filemeta.VerifyChunk(chunkResp.Data, chunkMeta.Hash); err != nil {
			fmt.Printf("%s[CORRUPT]%s\n", colorRed, colorReset)
			log.Fatalf("chunk integrity verification failed: %v", err)
		}

		// Save to disk
		if err := store.WriteChunk(fileID, chunkMeta.Index, chunkResp.Data); err != nil {
			fmt.Printf("%s[FAIL]%s\n", colorRed, colorReset)
			log.Fatalf("failed to write chunk to storage: %v", err)
		}

		fmt.Printf("%s[OK]%s\n", colorGreen, colorReset)
	}

	// Save manifest
	if err := store.SaveManifest(meta); err != nil {
		log.Fatalf("failed to save manifest: %v", err)
	}

	duration := time.Since(start)
	fmt.Printf("%s[DOWNLOAD]%s ✓ All chunks downloaded and verified in %v!\n", colorGreen, colorReset, duration)

	// 3. Reassemble
	if outPath == "" {
		outPath = filepath.Join(os.TempDir(), meta.FileName)
	}
	fmt.Printf("%s[DOWNLOAD]%s Reassembling chunks into: %s%s%s...\n",
		colorBlue, colorReset, colorBold, outPath, colorReset)

	if err := filemeta.AssembleChunks(store.Layout().ChunksDir(fileID), outPath, meta.Chunks); err != nil {
		log.Fatalf("reassembly failed: %v", err)
	}

	// 4. Final integrity check
	fmt.Printf("%s[DOWNLOAD]%s Running final SHA-256 validation...\n", colorBlue, colorReset)
	if err := filemeta.VerifyFile(outPath, meta.ModelHash); err != nil {
		log.Fatalf("final file integrity check failed: %v", err)
	}

	fmt.Printf("%s[DOWNLOAD]%s %s★ SUCCESS! File reassembled and hash verified ★%s\n",
		colorGreen, colorReset, colorBold, colorReset)
}

//---------------------------------------------------------------------
// Legacy seeder (no DHT)

func runSeeder(port, filePath, dataDir string, chunkSize int) {
	store := storage.NewStore(dataDir)

	if filePath != "" {
		info, err := os.Stat(filePath)
		if err != nil {
			log.Fatalf("failed to open source file: %v", err)
		}
		genFileID := generateRandomID()
		fmt.Printf("%s[SEEDER]%s Chunking original file %s%s%s (%.2f MB)...\n",
			colorBlue, colorReset, colorBold, info.Name(), colorReset, float64(info.Size())/(1024*1024))

		_, _, err = store.StoreModel(filePath, genFileID, chunkSize)
		if err != nil {
			log.Fatalf("failed to split and seed model: %v", err)
		}
		fmt.Printf("%s[SEEDER]%s ✓ Chunks stored under: %s/%s/chunks\n", colorGreen, colorReset, dataDir, genFileID)
		fmt.Printf("%s[SEEDER]%s ✓ File ID: %s%s%s (Use this ID to download)\n\n", colorGreen, colorReset, colorBold, genFileID, colorReset)
	} else {
		fmt.Printf("%s[SEEDER]%s Starting seeder from existing data directory: %s\n", colorBlue, colorReset, dataDir)
	}

	listenAddr := "0.0.0.0:" + port
	server := network.NewServer(listenAddr)
	router := network.NewRouter()

	router.Register(protocol.MessageType_MSG_GET_METADATA_REQUEST, func(conn *network.Connection, env *protocol.Envelope) error {
		var req protocol.GetMetadataRequest
		if err := proto.Unmarshal(env.Payload, &req); err != nil {
			return err
		}
		fmt.Printf("%s[RPC]%s Received Metadata Request for File ID: %s from %s\n",
			colorYellow, colorReset, req.FileId, conn.RemoteAddr().String())

		meta, err := store.LoadManifest(req.FileId)
		if err != nil {
			resp := &protocol.GetMetadataResponse{
				FileId:  req.FileId,
				Success: false,
				Error:   fmt.Sprintf("manifest not found: %v", err),
			}
			return conn.WriteResponse(env.Id, protocol.MessageType_MSG_GET_METADATA_RESPONSE, resp)
		}
		manifestBytes, err := json.Marshal(meta)
		if err != nil {
			resp := &protocol.GetMetadataResponse{
				FileId:  req.FileId,
				Success: false,
				Error:   fmt.Sprintf("failed to marshal manifest: %v", err),
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

	router.Register(protocol.MessageType_MSG_GET_CHUNK_REQUEST, func(conn *network.Connection, env *protocol.Envelope) error {
		var req protocol.GetChunkRequest
		if err := proto.Unmarshal(env.Payload, &req); err != nil {
			return err
		}
		chunkData, err := store.ReadChunk(req.FileId, int(req.ChunkIndex))
		if err != nil {
			resp := &protocol.GetChunkResponse{
				FileId:     req.FileId,
				ChunkIndex: req.ChunkIndex,
				Success:    false,
				Error:      fmt.Sprintf("chunk read failed: %v", err),
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

	server.OnNewConnection = func(conn *network.Connection) {
		fmt.Printf("%s[SERVER]%s Connected to new peer: %s\n", colorGreen, colorReset, conn.RemoteAddr().String())
		conn.SetRouter(router)
		conn.Start()
	}

	if err := server.Start(); err != nil {
		log.Fatalf("failed to start server: %v", err)
	}
	fmt.Printf("%s[SERVER]%s TCP Server is listening on %s%s%s...\n", colorGreen, colorReset, colorBold, listenAddr, colorReset)

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)
	<-shutdown

	fmt.Printf("\n%s[SERVER]%s Stopping TCP Server...\n", colorYellow, colorReset)
	server.Stop()
	fmt.Printf("%s[SERVER]%s Seeder stopped cleanly.\n", colorGreen, colorReset)
}

//---------------------------------------------------------------------
// Legacy downloader (no DHT)

func runDownloader(seederAddr, fileID, dataDir, outPath string) {
	fmt.Printf("%s[DOWNLOAD]%s Connecting to seeder at %s%s%s...\n", colorBlue, colorReset, colorBold, seederAddr, colorReset)

	rawConn, err := net.Dial("tcp", seederAddr)
	if err != nil {
		log.Fatalf("failed to connect to seeder: %v", err)
	}
	defer rawConn.Close()

	conn := network.NewConnection(rawConn, "downloader-peer")
	conn.Start()

	store := storage.NewStore(dataDir)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fmt.Printf("%s[DOWNLOAD]%s → Requesting manifest for ID: %s%s%s...\n", colorBlue, colorReset, colorBold, fileID, colorReset)
	metaReq := &protocol.GetMetadataRequest{FileId: fileID}

	respEnv, err := conn.SendRequest(ctx, protocol.MessageType_MSG_GET_METADATA_REQUEST, metaReq)
	if err != nil {
		log.Fatalf("failed to fetch metadata: %v", err)
	}

	var metaResp protocol.GetMetadataResponse
	if err := proto.Unmarshal(respEnv.Payload, &metaResp); err != nil {
		log.Fatalf("failed to decode metadata response: %v", err)
	}
	if !metaResp.Success {
		log.Fatalf("seeder returned error: %s", metaResp.Error)
	}

	var meta filemeta.FileMeta
	if err := json.Unmarshal(metaResp.MetadataJson, &meta); err != nil {
		log.Fatalf("failed to unmarshal manifest JSON: %v", err)
	}

	fmt.Printf("%s[DOWNLOAD]%s ✓ Manifest retrieved. File: %s%s%s (%d chunks, size: %.2f MB)\n",
		colorGreen, colorReset, colorBold, meta.FileName, colorReset, meta.NumChunks, float64(meta.FileSize)/(1024*1024))

	if err := store.InitializeFileDirectories(fileID); err != nil {
		log.Fatalf("failed to prepare directories: %v", err)
	}

	start := time.Now()
	for i, chunkMeta := range meta.Chunks {
		fmt.Printf("%s[DOWNLOAD]%s [%d/%d] Fetching chunk %d (%d KB)... ",
			colorYellow, colorReset, i+1, meta.NumChunks, chunkMeta.Index, chunkMeta.Size/1024)

		chunkReq := &protocol.GetChunkRequest{
			FileId:     fileID,
			ChunkIndex: int32(chunkMeta.Index),
		}

		chunkEnv, err := conn.SendRequest(ctx, protocol.MessageType_MSG_GET_CHUNK_REQUEST, chunkReq)
		if err != nil {
			fmt.Printf("%s[FAIL]%s\n", colorRed, colorReset)
			log.Fatalf("failed to fetch chunk %d: %v", chunkMeta.Index, err)
		}

		var chunkResp protocol.GetChunkResponse
		if err := proto.Unmarshal(chunkEnv.Payload, &chunkResp); err != nil {
			fmt.Printf("%s[FAIL]%s\n", colorRed, colorReset)
			log.Fatalf("failed to decode chunk %d response: %v", chunkMeta.Index, err)
		}
		if !chunkResp.Success {
			fmt.Printf("%s[FAIL]%s\n", colorRed, colorReset)
			log.Fatalf("seeder chunk fetch error: %s", chunkResp.Error)
		}

		if err := filemeta.VerifyChunk(chunkResp.Data, chunkMeta.Hash); err != nil {
			fmt.Printf("%s[CORRUPT]%s\n", colorRed, colorReset)
			log.Fatalf("chunk integrity verification failed: %v", err)
		}

		if err := store.WriteChunk(fileID, chunkMeta.Index, chunkResp.Data); err != nil {
			fmt.Printf("%s[FAIL]%s\n", colorRed, colorReset)
			log.Fatalf("failed to write chunk to storage: %v", err)
		}

		fmt.Printf("%s[OK]%s\n", colorGreen, colorReset)
	}

	if err := store.SaveManifest(meta); err != nil {
		log.Fatalf("failed to save manifest: %v", err)
	}

	duration := time.Since(start)
	fmt.Printf("%s[DOWNLOAD]%s ✓ All chunks downloaded and verified in %v!\n", colorGreen, colorReset, duration)

	if outPath == "" {
		outPath = filepath.Join(os.TempDir(), meta.FileName)
	}
	fmt.Printf("%s[DOWNLOAD]%s Reassembling chunks into target destination: %s%s%s...\n",
		colorBlue, colorReset, colorBold, outPath, colorReset)

	if err := filemeta.AssembleChunks(store.Layout().ChunksDir(fileID), outPath, meta.Chunks); err != nil {
		log.Fatalf("reassembly failed: %v", err)
	}

	fmt.Printf("%s[DOWNLOAD]%s Running final file SHA-256 validation...\n", colorBlue, colorReset)
	if err := filemeta.VerifyFile(outPath, meta.ModelHash); err != nil {
		log.Fatalf("final file integrity check failed: %v", err)
	}

	fmt.Printf("%s[DOWNLOAD]%s %s★ SUCCESS! File reassembled and hash verified cleanly ★%s\n",
		colorGreen, colorReset, colorBold, colorReset)
}

//---------------------------------------------------------------------
// Helpers

func printHeader() {
	fmt.Printf("%s%s┌────────────────────────────────────────────────────────┐%s\n", colorBold, colorCyan, colorReset)
	fmt.Printf("%s%s│             P2P MODEL DISTRIBUTION NODE                │%s\n", colorBold, colorCyan, colorReset)
	fmt.Printf("%s%s└────────────────────────────────────────────────────────┘%s\n\n", colorBold, colorCyan, colorReset)
}

func generateRandomID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x", b)
}
