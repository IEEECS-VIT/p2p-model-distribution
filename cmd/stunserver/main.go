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
	port := flag.Int("port", 3478, "primary STUN port (NAT detection also uses port+1)")
	flag.Parse()

	primary := nat.NewSTUNServer(fmt.Sprintf(":%d", *port))
	if err := primary.Start(); err != nil {
		log.Fatalf("start primary STUN server on :%d: %v", *port, err)
	}

	secondary := nat.NewSTUNServer(fmt.Sprintf(":%d", *port+1))
	if err := secondary.Start(); err != nil {
		log.Fatalf("start secondary STUN server on :%d: %v", *port+1, err)
	}

	fmt.Printf("\n")
	fmt.Printf("┌──────────────────────────────────────┐\n")
	fmt.Printf("│         STUN Server Running          │\n")
	fmt.Printf("├──────────────────────────────────────┤\n")
	fmt.Printf("│  Primary   UDP :%d (Binding Req)    │\n", *port)
	fmt.Printf("│  Secondary UDP :%d (NAT detection)  │\n", *port+1)
	fmt.Printf("├──────────────────────────────────────┤\n")
	fmt.Printf("│  Peers use this server to discover   │\n")
	fmt.Printf("│  their public IP:port before         │\n")
	fmt.Printf("│  joining the DHT.                    │\n")
	fmt.Printf("└──────────────────────────────────────┘\n\n")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	fmt.Println("\nshutting down...")
	primary.Stop()
	secondary.Stop()
}
