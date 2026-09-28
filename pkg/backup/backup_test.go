package backup

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"hf2s3/pkg/crypto"
	"hf2s3/pkg/db"
	"hf2s3/pkg/models"
)

type memRemote struct {
	mu    sync.Mutex
	blobs map[string][]byte
}

func (m *memRemote) Put(_ context.Context, name string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.blobs[name] = data
	return nil
}

func (m *memRemote) Delete(_ context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.blobs, name)
	return nil
}

func newKeyring(t *testing.T) *crypto.Keyring {
	t.Helper()
	k, err := crypto.NewKeyring(bytes.Repeat([]byte{7}, 32), nil)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func TestEncryptedBackupRoundTrip(t *testing.T) {
	kr := newKeyring(t)
	plain := []byte("SQLite format 3\x00 pretend database")
	blob, err := Encrypt(kr, plain)
	if err != nil {
		t.Fatal(err)
	}
	if !IsEncrypted(blob) || bytes.Contains(blob, plain) {
		t.Fatal("blob must be opaque")
	}
	got, err := Decrypt(kr, blob)
	if err != nil || !bytes.Equal(got, plain) {
		t.Fatalf("round trip: %v", err)
	}
	other, _ := crypto.NewKeyring(bytes.Repeat([]byte{9}, 32), nil)
	if _, err := Decrypt(other, blob); err == nil {
		t.Fatal("a different master key must not open the backup")
	}
	if _, err := Decrypt(kr, plain); err == nil {
		t.Fatal("plaintext must not be accepted as an encrypted backup")
	}
}

func TestRunOncePrunesAndUploadsEncryptedCopy(t *testing.T) {
	dir := t.TempDir()
	database, err := db.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	_ = database.CreateBucket(ctx, "important-bucket")
	_ = database.CreateAccount(ctx, &models.Account{Name: "a", Username: "u", Token: "hf_secret_token", RepoName: "u/r", IsActive: true})

	remote := &memRemote{blobs: map[string][]byte{}}
	kr := newKeyring(t)
	m := &Manager{
		DB: database, Dir: filepath.Join(dir, "backups"), Keep: 2, Keyring: kr,
		Remote: func(context.Context) Remote { return remote },
	}

	// Existing old snapshots and several runs: only the newest two survive.
	_ = os.MkdirAll(m.Dir, 0o700)
	for _, n := range []string{"hf2s3-backup-20200101-000000.db", "hf2s3-backup-20200102-000000.db"} {
		_ = os.WriteFile(filepath.Join(m.Dir, n), []byte("old"), 0o600)
	}
	var last string
	for i := 0; i < 3; i++ {
		last, err = m.RunOnce(ctx)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	entries, _ := os.ReadDir(m.Dir)
	if len(entries) != 2 {
		t.Fatalf("expected 2 local backups after pruning, got %d", len(entries))
	}

	// The last snapshot is a usable database containing our data.
	restored, err := db.Open(last)
	if err != nil {
		t.Fatalf("snapshot is not a valid database: %v", err)
	}
	defer restored.Close()
	if ok, _ := restored.BucketExists(ctx, "important-bucket"); !ok {
		t.Fatal("snapshot is missing data")
	}

	// The remote copies are encrypted, bounded to Keep, and decrypt with the key.
	if len(remote.blobs) > 2 || len(remote.blobs) == 0 {
		t.Fatalf("remote copies = %d, want 1..2", len(remote.blobs))
	}
	for name, blob := range remote.blobs {
		if !strings.HasPrefix(name, "_hf2s3/backups/") || !IsEncrypted(blob) {
			t.Fatalf("bad remote copy %s", name)
		}
		if bytes.Contains(blob, []byte("important-bucket")) || bytes.Contains(blob, []byte("SQLite format 3")) {
			t.Fatal("remote copy leaks plaintext database content")
		}
		plain, err := Decrypt(kr, blob)
		if err != nil || !bytes.HasPrefix(plain, []byte("SQLite format 3")) {
			t.Fatalf("remote copy does not decrypt to a database: %v", err)
		}
	}
}
