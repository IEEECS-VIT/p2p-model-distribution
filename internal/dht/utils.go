package dht

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
)

func Hash(data []byte) [32]byte {
	return sha256.Sum256(data)
}

func HashString(data []byte) string {
	sum := Hash(data)
	return hex.EncodeToString(sum[:])
}

func distanceBytes(a string, b string) []byte {
	left := Hash([]byte(a))
	right := Hash([]byte(b))
	result := make([]byte, len(left))
	for i := range left {
		result[i] = left[i] ^ right[i]
	}

	return result
}

func bucketIndex(selfID string, peerID string) int {
	distance := distanceBytes(selfID, peerID)
	for i, value := range distance {
		if value == 0 {
			continue
		}

		return i*8 + leadingBitOffset(value)
	}

	return 0
}

func leadingBitOffset(value byte) int {
	for bit := 0; bit < 8; bit++ {
		if value&(1<<(7-bit)) != 0 {
			return bit
		}
	}

	return 7
}

func itoa(value int) string {
	return strconv.Itoa(value)
}
