package nat

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
)

type STUNServer struct {
	listenAddr string
	conn       net.PacketConn
	quit       chan struct{}
}

func NewSTUNServer(listenAddr string) *STUNServer {
	return &STUNServer{
		listenAddr: listenAddr,
		quit:       make(chan struct{}),
	}
}

func (s *STUNServer) Start() error {
	conn, err := net.ListenPacket("udp4", s.listenAddr)
	if err != nil {
		return fmt.Errorf("stun server listen %s: %w", s.listenAddr, err)
	}
	s.conn = conn
	log.Printf("[STUN] server listening on %s", s.listenAddr)
	go s.serveLoop()
	return nil
}

func (s *STUNServer) Stop() {
	close(s.quit)
	if s.conn != nil {
		s.conn.Close()
	}
}

func (s *STUNServer) serveLoop() {
	buf := make([]byte, 1024)
	for {
		n, addr, err := s.conn.ReadFrom(buf)
		if err != nil {
			select {
			case <-s.quit:
				return
			default:
				log.Printf("[STUN] read error: %v", err)
				continue
			}
		}
		pkt := make([]byte, n)
		copy(pkt, buf[:n])
		go s.handle(pkt, addr)
	}
}

func (s *STUNServer) handle(req []byte, addr net.Addr) {
	if len(req) < 20 {
		return
	}

	msgType := binary.BigEndian.Uint16(req[0:2])
	if msgType != 0x0001 {
		return
	}

	udpAddr, ok := addr.(*net.UDPAddr)
	if !ok {
		return
	}

	resp := s.buildResponse(req, udpAddr)
	s.conn.WriteTo(resp, addr)

	log.Printf("[STUN] %s → public addr %s", addr, udpAddr)
}

func (s *STUNServer) buildResponse(req []byte, addr *net.UDPAddr) []byte {
	attr := buildXORMappedAddress(addr)

	resp := make([]byte, 20)
	binary.BigEndian.PutUint16(resp[0:2], 0x0101)
	binary.BigEndian.PutUint16(resp[2:4], uint16(len(attr)))
	copy(resp[4:8], []byte{0x21, 0x12, 0xA4, 0x42})
	copy(resp[8:20], req[8:20])

	return append(resp, attr...)
}

func buildXORMappedAddress(addr *net.UDPAddr) []byte {
	const magicCookie = uint16(0x2112)
	magic := []byte{0x21, 0x12, 0xA4, 0x42}

	attr := make([]byte, 12)
	binary.BigEndian.PutUint16(attr[0:2], 0x0020)
	binary.BigEndian.PutUint16(attr[2:4], 8)
	attr[4] = 0x00
	attr[5] = 0x01

	port := uint16(addr.Port) ^ magicCookie
	binary.BigEndian.PutUint16(attr[6:8], port)

	ip := addr.IP.To4()
	attr[8] = ip[0] ^ magic[0]
	attr[9] = ip[1] ^ magic[1]
	attr[10] = ip[2] ^ magic[2]
	attr[11] = ip[3] ^ magic[3]

	return attr
}
