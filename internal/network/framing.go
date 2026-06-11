package network

import (
	"encoding/binary"
	"fmt"
	"io"
)

// MaxMessageSize prevents malicious peers from crashing the server with OOM attacks.
// Let's set it to 10MB for now (adjust based on your AI model chunk sizes).
const MaxMessageSize = 10 * 1024 * 1024 

func WriteFrame(w io.Writer, data []byte) error {
	// Optional: You could allocate a single buffer to do exactly 1 syscall,
	// but writing twice is usually fine as long as the caller holds a Mutex!
	
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(data)))

	if _, err := w.Write(header); err != nil {
		return err
	}

	if _, err := w.Write(data); err != nil {
		return err
	}
	return nil
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
