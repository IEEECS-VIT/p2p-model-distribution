package nat

import (
	"fmt"
	"log"
	"net"
	"time"

	"github.com/IEEECS-VIT/p2p-model-distribution/internal/network"
)

type Manager struct {
	stunHost   string
	localPort  int
	publicAddr PublicAddr
	natType    NATType
}

func NewManager(stunHost string, localPort int) *Manager {
	return &Manager{
		stunHost:  stunHost,
		localPort: localPort,
	}
}

func (m *Manager) Setup() error {
	addr, err := DiscoverPublicAddr(
		fmt.Sprintf("%s:3478", m.stunHost),
		m.localPort,
		DefaultSTUNTimeout,
	)
	if err != nil {
		return fmt.Errorf("STUN discovery failed: %w", err)
	}
	m.publicAddr = addr
	log.Printf("[NAT] public address: %s", addr)

	natType, err := DetectNATType(m.stunHost, m.localPort)
	if err != nil {
		log.Printf("[NAT] type detection failed (non-fatal): %v", err)
		m.natType = NATTypeUnknown
	} else {
		m.natType = natType
		log.Printf("[NAT] type: %s", natType)
	}

	if !natType.HolePunchable() {
		log.Printf("[NAT] WARNING: symmetric NAT detected — hole punching unreliable. " +
			"Peer connections may fail. A relay (TURN) is needed post-MVP.")
	}

	return nil
}

func (m *Manager) PublicAddr() string {
	if m.publicAddr.IsZero() {
		return ""
	}
	return m.publicAddr.String()
}

func (m *Manager) NATType() NATType {
	return m.natType
}

// ConnectToPeer tries to establish a TCP connection to a remote peer.
// It tries two strategies:
//
//  1. Direct TCP dial (works if same LAN, port forwarding, or no NAT)
//  2. UDP hole punch → TCP (works for ~80% of home NATs)
//
// The remote peer must have already called PrepareForPeer() (via DHT signalling)
// to open their NAT entry before this is called.
func (m *Manager) ConnectToPeer(remotePublicAddr string) (*network.Connection, error) {
	log.Printf("[NAT] trying direct TCP → %s", remotePublicAddr)
	if conn, err := net.DialTimeout("tcp", remotePublicAddr, 3*time.Second); err == nil {
		log.Printf("[NAT] direct TCP succeeded → %s", remotePublicAddr)
		return network.NewConnection(conn, remotePublicAddr), nil
	}

	log.Printf("[NAT] direct TCP failed, trying hole punch → %s", remotePublicAddr)

	result, err := PunchHole(m.localPort, remotePublicAddr, DefaultPunchTimeout)
	if err != nil {
		return nil, fmt.Errorf(
			"all connection strategies failed for %s: %w\n"+
				"hint: peer may be behind symmetric NAT — TURN relay needed (post-MVP)",
			remotePublicAddr, err,
		)
	}

	if !result.Success {
		return nil, fmt.Errorf("hole punch to %s failed — no route found", remotePublicAddr)
	}

	log.Printf("[NAT] hole punch succeeded → %s", remotePublicAddr)
	return network.NewConnection(result.Conn, remotePublicAddr), nil
}

// PrepareForPeer is the responder-side call.
// When the DHT/signalling layer says "peer X wants to connect to you",
// call this to open a NAT entry for them. It sends a burst of UDP probes
// and returns immediately — no waiting for a connection.
//
// The initiator (peer X) then calls ConnectToPeer() which will punch through.
func (m *Manager) PrepareForPeer(remotePublicAddr string) error {
	log.Printf("[NAT] preparing for peer → %s", remotePublicAddr)
	return SendProbe(m.localPort, remotePublicAddr)
}
