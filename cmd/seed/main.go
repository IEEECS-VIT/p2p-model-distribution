package main

import (
	"crypto/rand"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/IEEECS-VIT/p2p-model-distribution/internal/filemeta"
	"github.com/IEEECS-VIT/p2p-model-distribution/internal/seeder"
	"github.com/IEEECS-VIT/p2p-model-distribution/internal/storage"
)

func main() {
	filePath := flag.String("file", "", "path to file to seed (omit to seed existing data only)")
	port := flag.String("port", "8080", "port to listen on")
	dataDir := flag.String("data", "./data", "base directory for chunks and manifests")
	chunkSizeKB := flag.Int("chunk-kb", 1024, "chunk size in KB (default 1MB)")
	flag.Parse()

	manifestDir := filepath.Join(*dataDir, "manifests")
	chunkDir := filepath.Join(*dataDir, "chunks")

	if *filePath != "" {
		if err := chunkFile(*filePath, manifestDir, chunkDir, *chunkSizeKB*1024); err != nil {
			log.Fatalf("chunking failed: %v", err)
		}
	}

	s, err := seeder.New(manifestDir, chunkDir)
	if err != nil {
		log.Fatalf("init seeder: %v", err)
	}

	addr := ":" + *port
	printStartupInfo(addr, manifestDir, chunkDir)

	if err := s.Serve(addr); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

func chunkFile(filePath, manifestDir, chunkDir string, chunkSize int) error {
	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat file: %w", err)
	}

	fmt.Printf("→ chunking %s (%.1f MB)...\n", info.Name(), float64(info.Size())/(1024*1024))

	meta, cid, err := filemeta.BuildManifest(f, generateFileID(), info.Name(), info.Size(), chunkSize)
	if err != nil {
		return fmt.Errorf("build manifest: %w", err)
	}

	if err := os.MkdirAll(manifestDir, 0755); err != nil {
		return err
	}
	manifestPath := filepath.Join(manifestDir, cid+".json")
	if err := filemeta.SaveManifest(meta, manifestPath); err != nil {
		return fmt.Errorf("save manifest: %w", err)
	}

	store, err := storage.NewChunkStore(chunkDir)
	if err != nil {
		return err
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek to start: %w", err)
	}

	buf := make([]byte, chunkSize)
	for _, chunk := range meta.Chunks {
		n, err := io.ReadFull(f, buf)
		if err != nil && err != io.ErrUnexpectedEOF {
			return fmt.Errorf("read chunk %d: %w", chunk.Index, err)
		}

		if err := store.WriteChunk(cid, chunk.Index, buf[:n]); err != nil {
			return fmt.Errorf("write chunk %d: %w", chunk.Index, err)
		}

		if err == io.ErrUnexpectedEOF {
			break
		}
	}

	fmt.Printf("✓ %d chunks written\n", meta.NumChunks)
	fmt.Printf("✓ manifest CID: %s\n", cid)
	fmt.Printf("✓ manifest saved → %s\n", manifestPath)
	return nil
}

func printStartupInfo(addr, manifestDir, chunkDir string) {
	fmt.Println()
	fmt.Println("┌─────────────────────────────────────────┐")
	fmt.Printf("│  seeder running on %-22s│\n", addr)
	fmt.Println("├─────────────────────────────────────────┤")
	fmt.Printf("│  manifests  %-28s│\n", manifestDir)
	fmt.Printf("│  chunks     %-28s│\n", chunkDir)
	fmt.Println("├─────────────────────────────────────────┤")
	fmt.Println("│  GET /manifest/{cid}                    │")
	fmt.Println("│  GET /chunk/{cid}/{index}               │")
	fmt.Println("│  GET /health                            │")
	fmt.Println("└─────────────────────────────────────────┘")
	fmt.Println()
}

func generateFileID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		log.Fatalf("generate file ID: %v", err)
	}
	return fmt.Sprintf("%x", b)
}
