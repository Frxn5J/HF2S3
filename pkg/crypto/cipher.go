package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
)

const NonceSize = 12

// DeriveKey is the legacy (v1) key derivation: a bare SHA-256 of the
// passphrase. It is kept only so data written by older versions stays readable;
// new keys come from the Keyring (random master key + HKDF).
func DeriveKey(passphrase string) []byte {
	hash := sha256.Sum256([]byte(passphrase))
	return hash[:]
}

func newGCM(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("create cipher block: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create gcm: %w", err)
	}
	return gcm, nil
}

// Encrypt seals plaintext with AES-256-GCM and no additional data (v1 format).
func Encrypt(plaintext []byte, key []byte) ([]byte, error) {
	return EncryptAAD(plaintext, key, nil)
}

// Decrypt opens a v1 blob produced by Encrypt.
func Decrypt(ciphertext []byte, key []byte) ([]byte, error) {
	return DecryptAAD(ciphertext, key, nil)
}

// EncryptAAD seals plaintext binding it to aad (e.g. the remote path of a
// chunk), so a blob cannot be swapped for another one undetected.
// Output layout: nonce || ciphertext || tag.
func EncryptAAD(plaintext, key, aad []byte) ([]byte, error) {
	gcm, err := newGCM(key)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, NonceSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("generate nonce: %w", err)
	}

	return gcm.Seal(nonce, nonce, plaintext, aad), nil
}

// DecryptAAD opens a blob produced by EncryptAAD with the same aad.
func DecryptAAD(ciphertext, key, aad []byte) ([]byte, error) {
	if len(ciphertext) < NonceSize {
		return nil, errors.New("ciphertext too short to contain nonce")
	}

	gcm, err := newGCM(key)
	if err != nil {
		return nil, err
	}

	plaintext, err := gcm.Open(nil, ciphertext[:NonceSize], ciphertext[NonceSize:], aad)
	if err != nil {
		return nil, fmt.Errorf("decrypt authenticate: %w", err)
	}
	return plaintext, nil
}
