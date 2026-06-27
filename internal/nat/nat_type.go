package nat

import (
	"fmt"
	"net"
	"time"
)

type NATType int

const (
	NATTypeUnknown NATType = iota
	NATTypeNone
	NATTypeFullCone
	NATTypeRestrictedCone
	NATTypeSymmetric
)

func (n NATType) String() string {
	switch n {
	case NATTypeNone:
		return "No NAT (public IP)"
	case NATTypeFullCone:
		return "Full Cone NAT (hole punch: yes)"
	case NATTypeRestrictedCone:
		return "Restricted Cone NAT (hole punch: yes)"
	case NATTypeSymmetric:
		return "Symmetric NAT (hole punch: unreliable, use relay)"
	default:
		return "Unknown"
	}
}

func (n NATType) HolePunchable() bool {
	return n == NATTypeNone || n == NATTypeFullCone || n == NATTypeRestrictedCone
}

func DetectNATType(stunHost string, localPort int) (NATType, error) {
	addr1, err := DiscoverPublicAddr(
		fmt.Sprintf("%s:3478", stunHost),
		localPort,
		DefaultSTUNTimeout,
	)
	if err != nil {
		return NATTypeUnknown, fmt.Errorf("STUN query 1 failed: %w", err)
	}

	localIP, err := getLocalIP()
	if err == nil && localIP == addr1.IP {
		return NATTypeNone, nil
	}

	addr2, err := DiscoverPublicAddr(
		fmt.Sprintf("%s:3479", stunHost),
		localPort,
		DefaultSTUNTimeout,
	)
	if err != nil {
		return NATTypeRestrictedCone, nil
	}

	if addr1.Port != addr2.Port {
		return NATTypeSymmetric, nil
	}

	return NATTypeFullCone, nil
}

func getLocalIP() (string, error) {
	conn, err := net.DialTimeout("udp4", "8.8.8.8:80", 2*time.Second)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).IP.String(), nil
}
