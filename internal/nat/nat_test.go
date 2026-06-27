package nat_test

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/IEEECS-VIT/p2p-model-distribution/internal/nat"
)

func startLocalSTUN(t *testing.T) (string, func()) {
	t.Helper()

	conn, err := net.ListenPacket("udp4", ":0")
	if err != nil {
		t.Fatalf("could not find free port: %v", err)
	}
	addr := conn.LocalAddr().String()
	conn.Close()

	port := conn.LocalAddr().(*net.UDPAddr).Port
	stunAddr := fmt.Sprintf("127.0.0.1:%d", port)

	srv := nat.NewSTUNServer(fmt.Sprintf(":%d", port))
	if err := srv.Start(); err != nil {
		t.Fatalf("start STUN server: %v", err)
	}

	_ = addr
	return stunAddr, srv.Stop
}

func TestSTUNServerResponds(t *testing.T) {
	stunAddr, stop := startLocalSTUN(t)
	defer stop()

	conn, err := net.ListenUDP("udp4", &net.UDPAddr{Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	localPort := conn.LocalAddr().(*net.UDPAddr).Port
	conn.Close()

	addr, err := nat.DiscoverPublicAddr(stunAddr, localPort, 3*time.Second)
	if err != nil {
		t.Fatalf("DiscoverPublicAddr: %v", err)
	}

	if addr.IP == "" {
		t.Error("returned empty IP")
	}
	if addr.Port == 0 {
		t.Error("returned zero port")
	}

	t.Logf("STUN response: %s", addr)
}

func TestSTUNResponseIsLoopback(t *testing.T) {
	stunAddr, stop := startLocalSTUN(t)
	defer stop()

	conn, _ := net.ListenUDP("udp4", &net.UDPAddr{Port: 0})
	localPort := conn.LocalAddr().(*net.UDPAddr).Port
	conn.Close()

	addr, err := nat.DiscoverPublicAddr(stunAddr, localPort, 3*time.Second)
	if err != nil {
		t.Fatalf("DiscoverPublicAddr: %v", err)
	}

	ip := net.ParseIP(addr.IP)
	if ip == nil {
		t.Fatalf("returned IP is not parseable: %q", addr.IP)
	}

	t.Logf("observed address: %s (loopback=%v, private=%v)",
		addr, ip.IsLoopback(), ip.IsPrivate())
}

func TestPublicAddrString(t *testing.T) {
	addr := nat.PublicAddr{IP: "203.45.67.89", Port: 7000}
	want := "203.45.67.89:7000"
	if addr.String() != want {
		t.Errorf("got %q, want %q", addr.String(), want)
	}
}

func TestPublicAddrIsZero(t *testing.T) {
	var zero nat.PublicAddr
	if !zero.IsZero() {
		t.Error("default PublicAddr should be zero")
	}

	nonZero := nat.PublicAddr{IP: "1.2.3.4", Port: 1234}
	if nonZero.IsZero() {
		t.Error("populated PublicAddr should not be zero")
	}
}

func TestSTUNTimeout(t *testing.T) {
	_, err := nat.DiscoverPublicAddr("127.0.0.1:19999", 29999, 500*time.Millisecond)
	if err == nil {
		t.Error("expected error for unreachable STUN server, got nil")
	}
	t.Logf("correctly failed: %v", err)
}

func TestNATTypeString(t *testing.T) {
	cases := []struct {
		natType nat.NATType
		want    string
	}{
		{nat.NATTypeNone, "No NAT (public IP)"},
		{nat.NATTypeFullCone, "Full Cone NAT (hole punch: yes)"},
		{nat.NATTypeRestrictedCone, "Restricted Cone NAT (hole punch: yes)"},
		{nat.NATTypeSymmetric, "Symmetric NAT (hole punch: unreliable, use relay)"},
	}

	for _, tc := range cases {
		if tc.natType.String() != tc.want {
			t.Errorf("NATType(%d).String() = %q, want %q", tc.natType, tc.natType.String(), tc.want)
		}
	}
}

func TestHolePunchableTypes(t *testing.T) {
	yes := []nat.NATType{nat.NATTypeNone, nat.NATTypeFullCone, nat.NATTypeRestrictedCone}
	no := []nat.NATType{nat.NATTypeSymmetric, nat.NATTypeUnknown}

	for _, n := range yes {
		if !n.HolePunchable() {
			t.Errorf("%s should be hole punchable", n)
		}
	}
	for _, n := range no {
		if n.HolePunchable() {
			t.Errorf("%s should NOT be hole punchable", n)
		}
	}
}
