package nat

import (
	"fmt"
	"log"
	"net"
	"time"
)

const (
	punchInterval       = 200 * time.Millisecond
	DefaultPunchTimeout = 10 * time.Second
	punchMsg            = "P2P-PUNCH"
	probeCount          = 5
)

type PunchResult struct {
	Success    bool
	Conn       net.Conn
	RemoteAddr string
}

func PunchHole(localPort int, remotePublicAddr string, timeout time.Duration) (PunchResult, error) {
	if timeout == 0 {
		timeout = DefaultPunchTimeout
	}

	laddr, err := net.ResolveUDPAddr("udp4", fmt.Sprintf(":%d", localPort))
	if err != nil {
		return PunchResult{}, fmt.Errorf("resolve local UDP addr: %w", err)
	}

	udpConn, err := net.ListenUDP("udp4", laddr)
	if err != nil {
		return PunchResult{}, fmt.Errorf("bind UDP :%d: %w", localPort, err)
	}
	defer udpConn.Close()

	raddr, err := net.ResolveUDPAddr("udp4", remotePublicAddr)
	if err != nil {
		return PunchResult{}, fmt.Errorf("resolve remote addr %s: %w", remotePublicAddr, err)
	}

	deadline := time.Now().Add(timeout)
	result := make(chan PunchResult, 1)

	go func() {
		probe := []byte(punchMsg)
		for time.Now().Before(deadline) {
			if _, err := udpConn.WriteToUDP(probe, raddr); err != nil {
				log.Printf("[NAT] punch send error: %v", err)
			}
			log.Printf("[NAT] punching → %s", remotePublicAddr)
			time.Sleep(punchInterval)
		}
	}()

	go func() {
		laddrTCP, err := net.ResolveTCPAddr("tcp4", fmt.Sprintf(":%d", localPort))
		if err != nil {
			select {
			case result <- PunchResult{Success: false}:
			default:
			}
			return
		}

		dialer := &net.Dialer{
			LocalAddr: laddrTCP,
			Deadline:  deadline,
			Control:   reuseAddrControl,
		}

		const retryDelay = 500 * time.Millisecond
		for time.Now().Before(deadline) {
			conn, err := dialer.Dial("tcp", remotePublicAddr)
			if err == nil {
				log.Printf("[NAT] TCP connection through hole → %s", remotePublicAddr)
				select {
				case result <- PunchResult{
					Success:    true,
					Conn:       conn,
					RemoteAddr: remotePublicAddr,
				}:
				default:
				}
				return
			}
			log.Printf("[NAT] TCP dial retry → %s: %v", remotePublicAddr, err)
			time.Sleep(retryDelay)
		}

		select {
		case result <- PunchResult{Success: false}:
		default:
		}
	}()

	select {
	case r := <-result:
		if r.Success {
			return r, nil
		}
		return r, fmt.Errorf("hole punch to %s failed after %v", remotePublicAddr, timeout)
	case <-time.After(timeout):
		return PunchResult{Success: false}, fmt.Errorf(
			"hole punch to %s timed out after %v — peer may be behind symmetric NAT "+
				"(use a relay instead)", remotePublicAddr, timeout)
	}
}

func SendProbe(localPort int, remoteAddr string) error {
	laddr, err := net.ResolveUDPAddr("udp4", fmt.Sprintf(":%d", localPort))
	if err != nil {
		return fmt.Errorf("resolve local UDP addr: %w", err)
	}

	conn, err := net.ListenUDP("udp4", laddr)
	if err != nil {
		return fmt.Errorf("bind UDP :%d: %w", localPort, err)
	}
	defer conn.Close()

	raddr, err := net.ResolveUDPAddr("udp4", remoteAddr)
	if err != nil {
		return fmt.Errorf("resolve remote addr %s: %w", remoteAddr, err)
	}

	probe := []byte(punchMsg)
	for i := 0; i < probeCount; i++ {
		if _, err := conn.WriteToUDP(probe, raddr); err != nil {
			log.Printf("[NAT] probe send error: %v", err)
		}
		log.Printf("[NAT] probe %d/%d → %s", i+1, probeCount, remoteAddr)
		time.Sleep(100 * time.Millisecond)
	}

	log.Printf("[NAT] probes sent to %s — NAT entry created", remoteAddr)
	return nil
}
