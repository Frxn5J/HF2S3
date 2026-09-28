package crypto

import (
	"bytes"
	"strings"
	"testing"
)

func testMaster(t *testing.T) []byte {
	t.Helper()
	s, err := GenerateMasterKey()
	if err != nil {
		t.Fatal(err)
	}
	b, err := ParseMasterKey(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestParseMasterKeyRejectsWeak(t *testing.T) {
	for _, bad := range []string{"", "hf2s3-aes-master-passphrase-2026", "short", "YWJj"} {
		if _, err := ParseMasterKey(bad); err == nil {
			t.Errorf("ParseMasterKey(%q) should fail", bad)
		}
	}
	hexKey := strings.Repeat("ab", 32)
	if b, err := ParseMasterKey(hexKey); err != nil || len(b) != 32 {
		t.Errorf("hex master key should parse: %v", err)
	}
}

func TestChunkV2BindsRemotePath(t *testing.T) {
	kr, err := NewKeyring(testMaster(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	plain := []byte("chunk payload")
	ct, id, err := kr.EncryptChunk(plain, "data/aaa.enc")
	if err != nil {
		t.Fatal(err)
	}
	got, err := kr.DecryptChunk(ct, "data/aaa.enc", EncV2, id)
	if err != nil || !bytes.Equal(got, plain) {
		t.Fatalf("round trip failed: %v", err)
	}
	if _, err := kr.DecryptChunk(ct, "data/bbb.enc", EncV2, id); err == nil {
		t.Fatal("a chunk swapped to another path must fail authentication")
	}
	if kr.NeedsRekey(EncV2, id) {
		t.Fatal("current-format chunk should not need re-keying")
	}
}

func TestLegacyV1ChunksStayReadable(t *testing.T) {
	const oldPassphrase = "hf2s3-aes-master-passphrase-2026"
	legacyCT, err := Encrypt([]byte("old data"), DeriveKey(oldPassphrase))
	if err != nil {
		t.Fatal(err)
	}

	kr, err := NewKeyring(testMaster(t), []string{oldPassphrase})
	if err != nil {
		t.Fatal(err)
	}
	got, err := kr.DecryptChunk(legacyCT, "data/x.enc", EncV1, LegacyKeyID)
	if err != nil || string(got) != "old data" {
		t.Fatalf("legacy chunk not readable: %v", err)
	}
	if !kr.NeedsRekey(EncV1, LegacyKeyID) {
		t.Fatal("legacy chunk must be flagged for re-keying")
	}

	noLegacy, _ := NewKeyring(testMaster(t), nil)
	if _, err := noLegacy.DecryptChunk(legacyCT, "data/x.enc", EncV1, LegacyKeyID); err == nil {
		t.Fatal("without the legacy key the chunk must not decrypt")
	}
}

func TestSecretBox(t *testing.T) {
	kr, _ := NewKeyring(testMaster(t), nil)
	box, err := NewSecretBox(kr.SettingsKey())
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := box.Seal("hf_supersecret")
	if err != nil {
		t.Fatal(err)
	}
	if !IsSealed(sealed) || strings.Contains(sealed, "supersecret") {
		t.Fatalf("value not sealed: %s", sealed)
	}
	if again, _ := box.Seal(sealed); again != sealed {
		t.Fatal("sealing must be idempotent")
	}
	opened, err := box.Open(sealed)
	if err != nil || opened != "hf_supersecret" {
		t.Fatalf("open failed: %v %q", err, opened)
	}
	if legacy, _ := box.Open("plaintext-legacy"); legacy != "plaintext-legacy" {
		t.Fatal("legacy plaintext must pass through Open")
	}
	other, _ := NewKeyring(testMaster(t), nil)
	otherBox, _ := NewSecretBox(other.SettingsKey())
	if _, err := otherBox.Open(sealed); err == nil {
		t.Fatal("a different key must not open the value")
	}
}

func TestPreviousMasterKeyKeepsV2ChunksReadable(t *testing.T) {
	oldMaster, _ := GenerateMasterKey()
	newMaster, _ := GenerateMasterKey()
	oldBytes, _ := ParseMasterKey(oldMaster)
	newBytes, _ := ParseMasterKey(newMaster)

	oldKR, _ := NewKeyring(oldBytes, nil)
	ct, oldID, err := oldKR.EncryptChunk([]byte("written before rotation"), "data/p.enc")
	if err != nil {
		t.Fatal(err)
	}

	rotated, err := NewKeyring(newBytes, []string{oldMaster})
	if err != nil {
		t.Fatal(err)
	}
	got, err := rotated.DecryptChunk(ct, "data/p.enc", EncV2, oldID)
	if err != nil || string(got) != "written before rotation" {
		t.Fatalf("v2 chunk from the previous master must stay readable: %v", err)
	}
	if !rotated.NeedsRekey(EncV2, oldID) {
		t.Fatal("a chunk under the previous master must be flagged for re-keying")
	}
	if len(rotated.PreviousSettingsKeys()) != 1 {
		t.Fatal("previous settings key missing (secrets could not be re-sealed)")
	}

	alone, _ := NewKeyring(newBytes, nil)
	if _, err := alone.DecryptChunk(ct, "data/p.enc", EncV2, oldID); err == nil {
		t.Fatal("without the previous key the chunk must not decrypt")
	}
}
