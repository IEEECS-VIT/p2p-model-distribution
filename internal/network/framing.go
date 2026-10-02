package network

import (
	"encoding/binary"
	"fmt"
	"io"
)

// MaxMessageSize prevents malicious peers from crashing the server with OOM
// attacks. It must exceed filemeta.MaxChunkSize plus envelope overhead.
const MaxMessageSize = 10 * 1024 * 1024

// WriteFrame writes data prefixed with its 4-byte big-endian length.
// Header and payload go out in a single Write so the frame is never split
// across TLS records or syscalls unnecessarily.
func WriteFrame(w io.Writer, data []byte) error {
	if len(data) > MaxMessageSize {
		return fmt.Errorf("message too large: %d bytes (max: %d)", len(data), MaxMessageSize)
	}

	frame := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(frame, uint32(len(data)))
	copy(frame[4:], data)

	_, err := w.Write(frame)
	return err
}

func ReadFrame(r io.Reader) ([]byte, error) {
	header := make([]byte, 4)
	// io.ReadFull blocks until exactly 4 bytes are read, solving the fragmentation problem!
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}

	length := binary.BigEndian.Uint32(header)

	// SECURITY: Block excessively large messages
	if length > MaxMessageSize {
		return nil, fmt.Errorf("message too large: %d bytes (max: %d)", length, MaxMessageSize)
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}

	return data, nil
}
