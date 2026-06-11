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
	"syscall"
	"time"

	"github.com/IEEECS-VIT/p2p-model-distribution/internal/filemeta"
	"github.com/IEEECS-VIT/p2p-model-distribution/internal/network"
	"github.com/IEEECS-VIT/p2p-model-distribution/internal/protocol"
	"github.com/IEEECS-VIT/p2p-model-distribution/internal/storage"
	"google.golang.org/protobuf/proto"
)

// ANSI terminal color codes for premium styling
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
	port := flag.String("port", "9000", "port to listen on (for seeder mode)")
	file := flag.String("file", "", "path to the file to seed (for seeding fresh files)")
	dataDir := flag.String("data", "./node-data", "directory where chunks and metadata are stored")
	chunkSizeKB := flag.Int("chunk-kb", 1024, "chunk size in KB (default 1024KB / 1MB)")

	// Download parameters
	addr := flag.String("addr", "", "target seeder address to dial (e.g. 127.0.0.1:9000)")
	fileID := flag.String("file-id", "", "the unique File ID of the manifest to download")
	outPath := flag.String("out", "", "destination path for the reassembled file")

	flag.Parse()

	printHeader()

	if *mode == "seed" {
		runSeeder(*port, *file, *dataDir, *chunkSizeKB*1024)
	} else if *mode == "download" {
		if *addr == "" || *fileID == "" {
			fmt.Printf("%s[ERROR]%s -addr and -file-id are required for download mode\n", colorRed, colorReset)
			flag.Usage()
			os.Exit(1)
		}
		runDownloader(*addr, *fileID, *dataDir, *outPath)
	} else {
		fmt.Printf("%s[ERROR]%s invalid mode '%s'. Choose 'seed' or 'download'\n", colorRed, colorReset, *mode)
		os.Exit(1)
	}
}

func printHeader() {
	fmt.Printf("%s%s┌────────────────────────────────────────────────────────┐%s\n", colorBold, colorCyan, colorReset)
	fmt.Printf("%s%s│             P2P MODEL DISTRIBUTION NODE                │%s\n", colorBold, colorCyan, colorReset)
	fmt.Printf("%s%s└────────────────────────────────────────────────────────┘%s\n\n", colorBold, colorCyan, colorReset)
}

func runSeeder(port, filePath, dataDir string, chunkSize int) {
	store := storage.NewStore(dataDir)

	if filePath != "" {
		// Verify file exists
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

	// Start TCP listener
	listenAddr := "0.0.0.0:" + port
	server := network.NewServer(listenAddr)

	router := network.NewRouter()

	// Register Metadata Handler
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

	// Register Chunk Handler
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

	// Clean shutdown channel
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)
	<-shutdown

	fmt.Printf("\n%s[SERVER]%s Stopping TCP Server...\n", colorYellow, colorReset)
	server.Stop()
	fmt.Printf("%s[SERVER]%s Seeder stopped cleanly.\n", colorGreen, colorReset)
}

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

	// 1. Fetch metadata
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

		// Verify chunk signature
		if err := filemeta.VerifyChunk(chunkResp.Data, chunkMeta.Hash); err != nil {
			fmt.Printf("%s[CORRUPT]%s\n", colorRed, colorReset)
			log.Fatalf("chunk integrity verification failed: %v", err)
		}

		// Save chunk to disk
		if err := store.WriteChunk(fileID, chunkMeta.Index, chunkResp.Data); err != nil {
			fmt.Printf("%s[FAIL]%s\n", colorRed, colorReset)
			log.Fatalf("failed to write chunk to storage: %v", err)
		}

		fmt.Printf("%s[OK]%s\n", colorGreen, colorReset)
	}

	// Save manifest on downloader side
	if err := store.SaveManifest(meta); err != nil {
		log.Fatalf("failed to save manifest: %v", err)
	}

	duration := time.Since(start)
	fmt.Printf("%s[DOWNLOAD]%s ✓ All chunks downloaded and verified in %v!\n", colorGreen, colorReset, duration)

	// 3. Reassemble
	if outPath == "" {
		outPath = filepath.Join(os.TempDir(), meta.FileName)
	}

	fmt.Printf("%s[DOWNLOAD]%s Reassembling chunks into target destination: %s%s%s...\n",
		colorBlue, colorReset, colorBold, outPath, colorReset)

	if err := filemeta.AssembleChunks(store.Layout().ChunksDir(fileID), outPath, meta.Chunks); err != nil {
		log.Fatalf("reassembly failed: %v", err)
	}

	// 4. Verify reassembled file hash
	fmt.Printf("%s[DOWNLOAD]%s Running final file SHA-256 validation...\n", colorBlue, colorReset)
	if err := filemeta.VerifyFile(outPath, meta.ModelHash); err != nil {
		log.Fatalf("final file integrity check failed: %v", err)
	}

	fmt.Printf("%s[DOWNLOAD]%s %s★ SUCCESS! File reassembled and hash verified cleanly ★%s\n",
		colorGreen, colorReset, colorBold, colorReset)
}

func generateRandomID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x", b)
}
