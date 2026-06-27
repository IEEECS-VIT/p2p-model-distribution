package nat

import (
	"encoding/binary"
	"fmt"
	"net"
	"time"
)

const (
	DefaultSTUNTimeout = 5 * time.Second
	stunMagicCookie    = uint32(0x2112A442)
)

type PublicAddr struct {
	IP   string
	Port int
}

func (p PublicAddr) String() string {
	return fmt.Sprintf("%s:%d", p.IP, p.Port)
}

func (p PublicAddr) IsZero() bool {
	return p.IP == "" || p.Port == 0
}

func DiscoverPublicAddr(stunAddr string, localPort int, timeout time.Duration) (PublicAddr, error) {
	if timeout == 0 {
		timeout = DefaultSTUNTimeout
	}

	laddr, err := net.ResolveUDPAddr("udp4", fmt.Sprintf(":%d", localPort))
	if err != nil {
		return PublicAddr{}, fmt.Errorf("resolve local addr: %w", err)
	}

	conn, err := net.ListenUDP("udp4", laddr)
	if err != nil {
		return PublicAddr{}, fmt.Errorf(
			"bind UDP :%d — is the TCP server already using this port? "+
				"On Linux you may need SO_REUSEPORT: %w", localPort, err)
	}
	defer conn.Close()

	raddr, err := net.ResolveUDPAddr("udp4", stunAddr)
	if err != nil {
		return PublicAddr{}, fmt.Errorf("resolve STUN server %s: %w", stunAddr, err)
	}

	req := buildBindingRequest()
	conn.SetDeadline(time.Now().Add(timeout))

	if _, err := conn.WriteToUDP(req, raddr); err != nil {
		return PublicAddr{}, fmt.Errorf("send STUN request: %w", err)
	}

	buf := make([]byte, 512)
	n, _, err := conn.ReadFromUDP(buf)
	if err != nil {
		return PublicAddr{}, fmt.Errorf(
			"read STUN response (is the STUN server running at %s?): %w", stunAddr, err)
	}

	return parseBindingResponse(buf[:n])
}

func buildBindingRequest() []byte {
	msg := make([]byte, 20)

	binary.BigEndian.PutUint16(msg[0:2], 0x0001)
	binary.BigEndian.PutUint16(msg[2:4], 0)
	binary.BigEndian.PutUint32(msg[4:8], stunMagicCookie)

	for i := 8; i < 20; i++ {
		msg[i] = byte(i * 17)
	}

	return msg
}

func parseBindingResponse(data []byte) (PublicAddr, error) {
	if len(data) < 20 {
		return PublicAddr{}, fmt.Errorf("response too short: %d bytes", len(data))
	}

	msgType := binary.BigEndian.Uint16(data[0:2])
	if msgType == 0x0111 {
		return PublicAddr{}, fmt.Errorf("STUN server returned error response")
	}
	if msgType != 0x0101 {
		return PublicAddr{}, fmt.Errorf("unexpected message type: 0x%04x", msgType)
	}

	offset := 20
	for offset+4 <= len(data) {
		attrType := binary.BigEndian.Uint16(data[offset : offset+2])
		attrLen := int(binary.BigEndian.Uint16(data[offset+2 : offset+4]))
		offset += 4

		if offset+attrLen > len(data) {
			break
		}

		switch attrType {
		case 0x0020:
			return decodeXORMappedAddress(data[offset : offset+attrLen])
		case 0x0001:
			return decodeMappedAddress(data[offset : offset+attrLen])
		}

		offset += (attrLen + 3) &^ 3
	}

	return PublicAddr{}, fmt.Errorf("no mapped address attribute in STUN response")
}

func decodeXORMappedAddress(attr []byte) (PublicAddr, error) {
	if len(attr) < 8 {
		return PublicAddr{}, fmt.Errorf("XOR-MAPPED-ADDRESS too short")
	}

	if attr[1] != 0x01 {
		return PublicAddr{}, fmt.Errorf("IPv6 not supported yet")
	}

	rawPort := binary.BigEndian.Uint16(attr[2:4])
	port := int(rawPort ^ 0x2112)

	magic := []byte{0x21, 0x12, 0xA4, 0x42}
	ip := make([]byte, 4)
	for i := 0; i < 4; i++ {
		ip[i] = attr[4+i] ^ magic[i]
	}

	return PublicAddr{
		IP:   fmt.Sprintf("%d.%d.%d.%d", ip[0], ip[1], ip[2], ip[3]),
		Port: port,
	}, nil
}

func decodeMappedAddress(attr []byte) (PublicAddr, error) {
	if len(attr) < 8 {
		return PublicAddr{}, fmt.Errorf("MAPPED-ADDRESS too short")
	}

	if attr[1] != 0x01 {
		return PublicAddr{}, fmt.Errorf("IPv6 not supported yet")
	}

	port := int(binary.BigEndian.Uint16(attr[2:4]))
	ip := attr[4:8]

	return PublicAddr{
		IP:   fmt.Sprintf("%d.%d.%d.%d", ip[0], ip[1], ip[2], ip[3]),
		Port: port,
	}, nil
}
