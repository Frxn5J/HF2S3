package crypto

import (
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// Chunk encryption formats.
const (
	EncV1 = 1 // key = SHA-256(passphrase), no AAD (legacy)
	EncV2 = 2 // key = HKDF(master), AAD = remote path

	// LegacyKeyID marks chunks written before key ids existed.
	LegacyKeyID = "legacy"

	MasterKeyBytes = 32
)

const (
	hkdfInfoChunk    = "hf2s3 chunk encryption v2"
	hkdfInfoSettings = "hf2s3 settings encryption v1"
)

// GenerateMasterKey returns a fresh random master key, base64 encoded.
func GenerateMasterKey() (string, error) {
	b := make([]byte, MasterKeyBytes)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

// ParseMasterKey decodes a master key given as base64 (std/URL/raw) or hex and
// requires at least 32 bytes of entropy.
func ParseMasterKey(s string) ([]byte, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, errors.New("master key is empty")
	}
	if len(s) == 64 {
		if b, err := hex.DecodeString(s); err == nil {
			return b, nil
		}
	}
	for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		if b, err := enc.DecodeString(s); err == nil && len(b) >= MasterKeyBytes {
			return b, nil
		}
	}
	return nil, errors.New("master key must be at least 32 random bytes encoded as base64 or hex (generate one with: hf2s3 keygen)")
}

type key struct {
	id    string
	bytes []byte
}

func keyID(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:8])
}

// Keyring holds the key used for new writes plus the legacy keys still needed
// to read (and re-encrypt) old data.
type Keyring struct {
	current  key
	settings []byte
	legacy   []key

	// Previous random master keys (rotation). Chunks written under one of them
	// carry its key id, so they stay readable until re-keyed, and the secrets
	// sealed under it can be re-sealed under the current key.
	prevChunk    map[string]key
	prevSettings [][]byte
}

// NewKeyring builds a keyring from a random master key and the legacy
// passphrases that older data may have been encrypted with.
func NewKeyring(master []byte, legacyPassphrases []string) (*Keyring, error) {
	if len(master) < MasterKeyBytes {
		return nil, errors.New("master key too short")
	}
	chunkKey, err := hkdf.Key(sha256.New, master, nil, hkdfInfoChunk, 32)
	if err != nil {
		return nil, fmt.Errorf("derive chunk key: %w", err)
	}
	settingsKey, err := hkdf.Key(sha256.New, master, nil, hkdfInfoSettings, 32)
	if err != nil {
		return nil, fmt.Errorf("derive settings key: %w", err)
	}

	k := &Keyring{
		current:   key{id: keyID(chunkKey), bytes: chunkKey},
		settings:  settingsKey,
		prevChunk: map[string]key{},
	}
	for _, p := range legacyPassphrases {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		// Every entry is a candidate v1 passphrase (SHA-256 of the text)...
		lk := DeriveKey(p)
		k.legacy = append(k.legacy, key{id: "legacy-" + keyID(lk), bytes: lk})

		// ...and, when it looks like a generated master key, a previous master
		// whose v2 chunks (and sealed secrets) must remain readable.
		if pm, err := ParseMasterKey(p); err == nil {
			ck, err1 := hkdf.Key(sha256.New, pm, nil, hkdfInfoChunk, 32)
			sk, err2 := hkdf.Key(sha256.New, pm, nil, hkdfInfoSettings, 32)
			if err1 == nil && err2 == nil {
				k.prevChunk[keyID(ck)] = key{id: keyID(ck), bytes: ck}
				k.prevSettings = append(k.prevSettings, sk)
			}
		}
	}
	return k, nil
}

// NewKeyringFromLegacyKey wraps an already derived 32-byte key (the historical
// SHA-256 of a passphrase). New writes use v2 keyed from it; old blobs keep
// decrypting. Used by tests and dev mode.
func NewKeyringFromLegacyKey(derived []byte) *Keyring {
	master := derived
	if len(master) < MasterKeyBytes {
		s := sha256.Sum256(derived)
		master = s[:]
	}
	chunkKey, _ := hkdf.Key(sha256.New, master, nil, hkdfInfoChunk, 32)
	settingsKey, _ := hkdf.Key(sha256.New, master, nil, hkdfInfoSettings, 32)
	return &Keyring{
		current:  key{id: keyID(chunkKey), bytes: chunkKey},
		settings: settingsKey,
		legacy:   []key{{id: "legacy-" + keyID(derived), bytes: derived}},
	}
}

func (k *Keyring) CurrentKeyID() string { return k.current.id }

// SettingsKey is the key used to encrypt secrets stored in the database.
func (k *Keyring) SettingsKey() []byte { return k.settings }

// EncryptChunk seals a chunk in the current (v2) format.
func (k *Keyring) EncryptChunk(plain []byte, remotePath string) ([]byte, string, error) {
	ct, err := EncryptAAD(plain, k.current.bytes, []byte(remotePath))
	if err != nil {
		return nil, "", err
	}
	return ct, k.current.id, nil
}

// DecryptChunk opens a chunk of the given format. v1 blobs are tried against
// every legacy key, since the row only records "legacy".
func (k *Keyring) DecryptChunk(ct []byte, remotePath string, encVersion int, keyID string) ([]byte, error) {
	switch encVersion {
	case EncV2:
		if keyID == "" || keyID == k.current.id {
			return DecryptAAD(ct, k.current.bytes, []byte(remotePath))
		}
		if prev, ok := k.prevChunk[keyID]; ok {
			return DecryptAAD(ct, prev.bytes, []byte(remotePath))
		}
		return nil, fmt.Errorf("chunk was encrypted with unknown key %s (current is %s); add the previous master key to HF2S3_LEGACY_MASTER_KEYS", keyID, k.current.id)
	case EncV1, 0:
		var lastErr error
		for _, lk := range k.legacy {
			pt, err := Decrypt(ct, lk.bytes)
			if err == nil {
				return pt, nil
			}
			lastErr = err
		}
		if lastErr == nil {
			lastErr = errors.New("no legacy keys configured (set HF2S3_LEGACY_MASTER_KEYS)")
		}
		return nil, lastErr
	}
	return nil, fmt.Errorf("unsupported chunk encryption version %d", encVersion)
}

// NeedsRekey reports whether a chunk is not in the current format/key.
func (k *Keyring) NeedsRekey(encVersion int, keyID string) bool {
	return encVersion != EncV2 || keyID != k.current.id
}

// HasLegacyKeys reports whether any legacy or previous key is configured.
func (k *Keyring) HasLegacyKeys() bool { return len(k.legacy) > 0 || len(k.prevChunk) > 0 }

// PreviousSettingsKeys returns the secret-box keys of previous master keys.
func (k *Keyring) PreviousSettingsKeys() [][]byte { return k.prevSettings }
