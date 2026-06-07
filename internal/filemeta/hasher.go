package filemeta

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
)

func HashBytes(data []byte) string {
	h := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(h[:])
}

func HashReader(r io.Reader) (string, error) {
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return "", fmt.Errorf("hash reader: %w", err)
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}
