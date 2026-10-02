// Package identity manages a node's long-term cryptographic identity.
//
// Each node owns an ed25519 key pair. Its node ID is the hex SHA-256 of the
// raw public key, so IDs cannot be chosen freely: claiming an ID requires
// the matching private key. Connections use mutual TLS 1.3 with
// self-signed certificates carrying that key (the same model as libp2p);
// each side derives the other's node ID from the certificate it presents,
// so peer identities are authenticated end to end without a CA.
package identity

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"time"
)

// ALPN is the application protocol negotiated on every connection. Bump the
// version on incompatible wire changes so mismatched nodes fail the TLS
// handshake cleanly instead of exchanging garbage.
const ALPN = "p2p-model-distribution/2"

// certValidity is how long generated certificates are valid. Certificates
// are regenerated on every start, so this only needs to outlive a process.
const certValidity = 365 * 24 * time.Hour

// clockSkew backdates NotBefore so peers with slightly slow clocks still
// accept the certificate.
const clockSkew = time.Hour

// Identity is a node's key pair, derived node ID and TLS certificate.
type Identity struct {
	priv ed25519.PrivateKey
	id   string
	cert tls.Certificate
}

// Generate creates a fresh, ephemeral identity.
func Generate() (*Identity, error) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate key: %w", err)
	}
	return fromPrivateKey(priv)
}

// LoadOrCreate loads the identity key stored at path, creating it (with
// mode 0600) if it does not exist. Like OpenSSH, it refuses to use a key
// file that is readable by group or others.
func LoadOrCreate(path string) (*Identity, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return create(path)
	}
	if err != nil {
		return nil, fmt.Errorf("read identity key: %w", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("stat identity key: %w", err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("identity key %s has permissions %#o; it must not be accessible by group or others (chmod 600)", path, info.Mode().Perm())
	}

	block, _ := pem.Decode(data)
	if block == nil || block.Type != "PRIVATE KEY" {
		return nil, fmt.Errorf("identity key %s: no PEM PRIVATE KEY block", path)
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("identity key %s: %w", path, err)
	}
	priv, ok := key.(ed25519.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("identity key %s: not an ed25519 key", path)
	}
	return fromPrivateKey(priv)
}

func create(path string) (*Identity, error) {
	id, err := Generate()
	if err != nil {
		return nil, err
	}
	der, err := x509.MarshalPKCS8PrivateKey(id.priv)
	if err != nil {
		return nil, fmt.Errorf("marshal identity key: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create identity dir: %w", err)
	}
	// O_EXCL: never clobber a key that appeared concurrently.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create identity key: %w", err)
	}
	if err := pem.Encode(f, &pem.Block{Type: "PRIVATE KEY", Bytes: der}); err != nil {
		f.Close()
		os.Remove(path)
		return nil, fmt.Errorf("write identity key: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(path)
		return nil, fmt.Errorf("sync identity key: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(path)
		return nil, fmt.Errorf("close identity key: %w", err)
	}
	return id, nil
}

func fromPrivateKey(priv ed25519.PrivateKey) (*Identity, error) {
	pub := priv.Public().(ed25519.PublicKey)
	id := IDFromPublicKey(pub)

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("generate serial: %w", err)
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: id},
		NotBefore:             now.Add(-clockSkew),
		NotAfter:              now.Add(certValidity),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	if err != nil {
		return nil, fmt.Errorf("create certificate: %w", err)
	}

	return &Identity{
		priv: priv,
		id:   id,
		cert: tls.Certificate{Certificate: [][]byte{der}, PrivateKey: priv},
	}, nil
}

// ID returns the node ID: hex SHA-256 of the raw ed25519 public key.
func (i *Identity) ID() string { return i.id }

// IDFromPublicKey derives a node ID from an ed25519 public key.
func IDFromPublicKey(pub ed25519.PublicKey) string {
	sum := sha256.Sum256(pub)
	return hex.EncodeToString(sum[:])
}

// ValidID reports whether s has the shape of a node ID.
func ValidID(s string) bool {
	if len(s) != sha256.Size*2 {
		return false
	}
	for _, r := range s {
		if !(r >= '0' && r <= '9' || r >= 'a' && r <= 'f') {
			return false
		}
	}
	return true
}

// ServerTLSConfig returns the TLS config for accepting connections. Clients
// must present a valid self-signed ed25519 certificate.
func (i *Identity) ServerTLSConfig() *tls.Config {
	return &tls.Config{
		MinVersion:            tls.VersionTLS13,
		Certificates:          []tls.Certificate{i.cert},
		ClientAuth:            tls.RequireAnyClientCert,
		NextProtos:            []string{ALPN},
		VerifyPeerCertificate: verifyPeerCertificate,
		VerifyConnection:      verifyConnection(i.id, ""),
	}
}

// ClientTLSConfig returns the TLS config for dialing a peer. If expectedID
// is non-empty, the handshake fails unless the peer proves it owns that
// node ID.
func (i *Identity) ClientTLSConfig(expectedID string) *tls.Config {
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{i.cert},
		NextProtos:   []string{ALPN},
		// There is no CA: the standard chain verification is replaced by
		// verifyPeerCertificate, which authenticates the peer's key, and
		// verifyConnection, which pins its node ID.
		InsecureSkipVerify:    true,
		VerifyPeerCertificate: verifyPeerCertificate,
		VerifyConnection:      verifyConnection(i.id, expectedID),
	}
}

// PeerID returns the authenticated node ID of the peer on a completed TLS
// connection.
func PeerID(cs tls.ConnectionState) (string, error) {
	if len(cs.PeerCertificates) != 1 {
		return "", fmt.Errorf("expected exactly one peer certificate, got %d", len(cs.PeerCertificates))
	}
	pub, ok := cs.PeerCertificates[0].PublicKey.(ed25519.PublicKey)
	if !ok {
		return "", errors.New("peer certificate key is not ed25519")
	}
	return IDFromPublicKey(pub), nil
}

// verifyPeerCertificate checks that the peer presented exactly one
// currently valid, self-signed ed25519 certificate. Possession of the
// private key is proven by the TLS 1.3 handshake signature itself.
func verifyPeerCertificate(rawCerts [][]byte, _ [][]*x509.Certificate) error {
	if len(rawCerts) != 1 {
		return fmt.Errorf("expected exactly one peer certificate, got %d", len(rawCerts))
	}
	cert, err := x509.ParseCertificate(rawCerts[0])
	if err != nil {
		return fmt.Errorf("parse peer certificate: %w", err)
	}
	if _, ok := cert.PublicKey.(ed25519.PublicKey); !ok {
		return errors.New("peer certificate key is not ed25519")
	}
	now := time.Now()
	if now.Before(cert.NotBefore) || now.After(cert.NotAfter) {
		return errors.New("peer certificate is expired or not yet valid")
	}
	if err := cert.CheckSignature(cert.SignatureAlgorithm, cert.RawTBSCertificate, cert.Signature); err != nil {
		return fmt.Errorf("peer certificate is not validly self-signed: %w", err)
	}
	return nil
}

func verifyConnection(selfID, expectedID string) func(tls.ConnectionState) error {
	return func(cs tls.ConnectionState) error {
		if cs.NegotiatedProtocol != ALPN {
			return fmt.Errorf("peer does not speak %s", ALPN)
		}
		peerID, err := PeerID(cs)
		if err != nil {
			return err
		}
		if peerID == selfID {
			return errors.New("connected to self")
		}
		if expectedID != "" && peerID != expectedID {
			return fmt.Errorf("peer identity mismatch: expected %s, got %s", expectedID, peerID)
		}
		return nil
	}
}
