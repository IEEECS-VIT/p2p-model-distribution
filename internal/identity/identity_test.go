package identity

import (
	"crypto/tls"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func mustGenerate(t *testing.T) *Identity {
	t.Helper()
	id, err := Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	return id
}

// handshake runs a TLS handshake between client and server over a
// loopback TCP connection and returns each side's view of the other's node
// ID. (net.Pipe is unbuffered, which deadlocks when one side rejects the
// handshake after the other has already finished.)
func handshake(t *testing.T, clientCfg, serverCfg *tls.Config) (serverSeen, clientSeen string, clientErr, serverErr error) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	s, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	deadline := time.Now().Add(5 * time.Second)
	c.SetDeadline(deadline)
	s.SetDeadline(deadline)

	type result struct {
		peer string
		err  error
	}
	srvDone := make(chan result, 1)
	go func() {
		conn := tls.Server(s, serverCfg)
		err := conn.Handshake()
		if err != nil {
			s.Close() // unblock the client
			srvDone <- result{err: err}
			return
		}
		peer, err := PeerID(conn.ConnectionState())
		srvDone <- result{peer, err}
	}()

	conn := tls.Client(c, clientCfg)
	if clientErr = conn.Handshake(); clientErr == nil {
		clientSeen, clientErr = PeerID(conn.ConnectionState())
	} else {
		c.Close() // unblock the server
	}
	r := <-srvDone
	return r.peer, clientSeen, clientErr, r.err
}

func TestMutualTLSAuthenticatesBothSides(t *testing.T) {
	alice, bob := mustGenerate(t), mustGenerate(t)

	serverSeen, clientSeen, cErr, sErr := handshake(t, alice.ClientTLSConfig(bob.ID()), bob.ServerTLSConfig())
	if cErr != nil || sErr != nil {
		t.Fatalf("handshake failed: client=%v server=%v", cErr, sErr)
	}
	if serverSeen != alice.ID() {
		t.Errorf("server saw peer %s, want %s", serverSeen, alice.ID())
	}
	if clientSeen != bob.ID() {
		t.Errorf("client saw peer %s, want %s", clientSeen, bob.ID())
	}
}

func TestClientRejectsUnexpectedPeer(t *testing.T) {
	alice, bob, mallory := mustGenerate(t), mustGenerate(t), mustGenerate(t)

	// Alice expects Bob but Mallory answers.
	_, _, cErr, _ := handshake(t, alice.ClientTLSConfig(bob.ID()), mallory.ServerTLSConfig())
	if cErr == nil || !strings.Contains(cErr.Error(), "identity mismatch") {
		t.Fatalf("client err = %v, want identity mismatch", cErr)
	}
}

func TestServerRejectsClientWithoutCertificate(t *testing.T) {
	bob := mustGenerate(t)
	anon := &tls.Config{MinVersion: tls.VersionTLS13, InsecureSkipVerify: true, NextProtos: []string{ALPN}}

	_, _, _, sErr := handshake(t, anon, bob.ServerTLSConfig())
	if sErr == nil {
		t.Fatal("server accepted a client without a certificate")
	}
}

func TestRejectsWrongALPN(t *testing.T) {
	alice, bob := mustGenerate(t), mustGenerate(t)
	cfg := alice.ClientTLSConfig(bob.ID())
	cfg.NextProtos = []string{"something-else/1"}

	_, _, cErr, sErr := handshake(t, cfg, bob.ServerTLSConfig())
	if cErr == nil && sErr == nil {
		t.Fatal("handshake succeeded with a mismatched ALPN")
	}
}

func TestRejectsTLS12(t *testing.T) {
	alice, bob := mustGenerate(t), mustGenerate(t)
	cfg := alice.ClientTLSConfig(bob.ID())
	cfg.MinVersion, cfg.MaxVersion = tls.VersionTLS12, tls.VersionTLS12

	_, _, cErr, sErr := handshake(t, cfg, bob.ServerTLSConfig())
	if cErr == nil && sErr == nil {
		t.Fatal("handshake succeeded over TLS 1.2")
	}
}

func TestLoadOrCreatePersistsIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sub", "node.key")

	first, err := LoadOrCreate(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("key file mode = %#o, want 0600", perm)
	}

	second, err := LoadOrCreate(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if first.ID() != second.ID() {
		t.Fatalf("reloaded identity has ID %s, want %s", second.ID(), first.ID())
	}
	if !ValidID(first.ID()) {
		t.Fatalf("ID %q is not a valid node ID", first.ID())
	}
}

func TestLoadOrCreateRejectsInsecurePermissions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.key")
	if _, err := LoadOrCreate(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadOrCreate(path); err == nil {
		t.Fatal("loaded a world-readable identity key")
	}
}

func TestValidID(t *testing.T) {
	if ValidID("abc") || ValidID(strings.Repeat("G", 64)) || ValidID(strings.Repeat("A", 64)) {
		t.Fatal("ValidID accepted a malformed ID")
	}
	if !ValidID(strings.Repeat("a1", 32)) {
		t.Fatal("ValidID rejected a well-formed ID")
	}
}
