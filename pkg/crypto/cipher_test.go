package crypto

import (
	"bytes"
	"testing"
)

func TestEncryptDecryptRoundTrip(t *testing.T) {
	key := DeriveKey("my-super-secret-master-key-1234")
	plaintext := []byte("Antigravity HF2S3 - High performance distributed storage across Hugging Face accounts")

	ciphertext, err := Encrypt(plaintext, key)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	if bytes.Equal(ciphertext, plaintext) {
		t.Fatalf("Ciphertext should not match plaintext")
	}

	decrypted, err := Decrypt(ciphertext, key)
	if err != nil {
		t.Fatalf("Decrypt failed: %v", err)
	}

	if !bytes.Equal(decrypted, plaintext) {
		t.Fatalf("Decrypted content does not match original plaintext. Got: %s", string(decrypted))
	}
}

func TestDecryptWithInvalidKeyFails(t *testing.T) {
	key1 := DeriveKey("correct-key")
	key2 := DeriveKey("wrong-key")
	plaintext := []byte("sensitive chunk data")

	ciphertext, err := Encrypt(plaintext, key1)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	_, err = Decrypt(ciphertext, key2)
	if err == nil {
		t.Fatalf("Expected decryption with wrong key to fail, but it succeeded")
	}
}

func TestDecryptCorruptedCiphertextFails(t *testing.T) {
	key := DeriveKey("key")
	plaintext := []byte("some payload")

	ciphertext, err := Encrypt(plaintext, key)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	// Corrupt a byte
	ciphertext[len(ciphertext)-1] ^= 0xFF

	_, err = Decrypt(ciphertext, key)
	if err == nil {
		t.Fatalf("Expected decryption of tampered ciphertext to fail")
	}
}
