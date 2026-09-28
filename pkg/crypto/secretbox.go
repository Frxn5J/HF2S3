package crypto

import (
	"encoding/base64"
	"errors"
	"strings"
)

const sealedPrefix = "enc:v1:"

// SecretBox encrypts short secrets (API tokens, S3 secret keys) that are stored
// in the database. A nil *SecretBox is a valid passthrough, used by tests and
// by the one-off migration that seals legacy plaintext values.
type SecretBox struct {
	key []byte
}

func NewSecretBox(key []byte) (*SecretBox, error) {
	if len(key) != 32 {
		return nil, errors.New("secret box key must be 32 bytes")
	}
	return &SecretBox{key: key}, nil
}

// IsSealed reports whether v carries the sealed-value prefix.
func IsSealed(v string) bool { return strings.HasPrefix(v, sealedPrefix) }

// Seal encrypts v. Empty strings and already sealed values are returned as-is.
func (s *SecretBox) Seal(v string) (string, error) {
	if s == nil || v == "" || IsSealed(v) {
		return v, nil
	}
	ct, err := EncryptAAD([]byte(v), s.key, []byte("hf2s3/secret"))
	if err != nil {
		return "", err
	}
	return sealedPrefix + base64.RawStdEncoding.EncodeToString(ct), nil
}

// Open decrypts a sealed value; values without the prefix are legacy plaintext
// and are returned unchanged.
func (s *SecretBox) Open(v string) (string, error) {
	if !IsSealed(v) {
		return v, nil
	}
	if s == nil {
		return "", errors.New("sealed secret found but no secret key is configured")
	}
	raw, err := base64.RawStdEncoding.DecodeString(strings.TrimPrefix(v, sealedPrefix))
	if err != nil {
		return "", err
	}
	pt, err := DecryptAAD(raw, s.key, []byte("hf2s3/secret"))
	if err != nil {
		return "", err
	}
	return string(pt), nil
}
