package storage

import (
	"crypto/sha256"
	"encoding/hex"
)

// plainHash is the hex SHA-256 recorded for a chunk's plaintext.
func plainHash(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
