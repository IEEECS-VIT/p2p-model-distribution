package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IEEECS-VIT/p2p-model-distribution/internal/nat"
)

func main() {
	port := flag.Int("stun-port", 3478, "STUN server UDP port")
	dhtPort := flag.Int("dht-port", 9000, "DHT bootstrap TCP port")
	flag.Parse()

	stun := nat.NewSTUNServer(fmt.Sprintf(":%d", *port))
	if err := stun.Start(); err != nil {
		log.Fatalf("start STUN server on :%d: %v", *port, err)
	}

	secondary := nat.NewSTUNServer(fmt.Sprintf(":%d", *port+1))
	if err := secondary.Start(); err != nil {
		log.Fatalf("start secondary STUN server on :%d: %v", *port+1, err)
	}

	fmt.Printf("\n")
	fmt.Printf("┌──────────────────────────────────────────┐\n")
	fmt.Printf("│      Bootstrap Node Running              │\n")
	fmt.Printf("├──────────────────────────────────────────┤\n")
	fmt.Printf("│  STUN  UDP :%d / :%d              │\n", *port, *port+1)
	fmt.Printf("│  DHT   TCP :%d                       │\n", *dhtPort)
	fmt.Printf("├──────────────────────────────────────────┤\n")
	fmt.Printf("│  DHT team: wire your DHT bootstrap      │\n")
	fmt.Printf("│  server above. Peers advertise           │\n")
	fmt.Printf("│  mgr.PublicAddr() onto the DHT.          │\n")
	fmt.Printf("└──────────────────────────────────────────┘\n\n")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	fmt.Println("\nshutting down...")
	stun.Stop()
	secondary.Stop()
}
