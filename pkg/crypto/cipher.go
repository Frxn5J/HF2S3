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

func DeriveKey(passphrase string) []byte {
	hash := sha256.Sum256([]byte(passphrase))
	return hash[:]
}

func Encrypt(plaintext []byte, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("create cipher block: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create gcm: %w", err)
	}

	nonce := make([]byte, NonceSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("generate nonce: %w", err)
	}

	sealed := gcm.Seal(nonce, nonce, plaintext, nil)
	return sealed, nil
}

func Decrypt(ciphertext []byte, key []byte) ([]byte, error) {
	if len(ciphertext) < NonceSize {
		return nil, errors.New("ciphertext too short to contain nonce")
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("create cipher block: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create gcm: %w", err)
	}

	nonce := ciphertext[:NonceSize]
	actualCiphertext := ciphertext[NonceSize:]

	plaintext, err := gcm.Open(nil, nonce, actualCiphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt authenticate: %w", err)
	}

	return plaintext, nil
}
