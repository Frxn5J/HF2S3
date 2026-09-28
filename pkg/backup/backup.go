// Package backup takes scheduled snapshots of the metadata database. The
// database is the only map from object keys to encrypted chunks, so losing it
// means losing everything even though the chunks survive on Hugging Face.
package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"hf2s3/pkg/crypto"
	"hf2s3/pkg/db"
	"hf2s3/pkg/hfstorage"
)

const (
	filePrefix     = "hf2s3-backup-"
	fileSuffix     = ".db"
	remoteSuffix   = ".db.enc"
	remotePrefix   = "_hf2s3/backups/"
	remoteListKey  = "remote_backups"
	encryptedMagic = "HF2SBAK1"
	backupAAD      = "hf2s3/db-backup/v1"
)

// Remote stores encrypted backup copies off the server.
type Remote interface {
	Put(ctx context.Context, name string, data []byte) error
	Delete(ctx context.Context, name string) error
}

// S3Remote keeps encrypted backups in a cache bucket.
type S3Remote struct{ Client *hfstorage.S3Client }

func (r S3Remote) Put(ctx context.Context, name string, data []byte) error {
	return r.Client.PutObject(ctx, r.Client.Bucket(), name, bytes.NewReader(data), int64(len(data)), "application/octet-stream")
}

func (r S3Remote) Delete(ctx context.Context, name string) error {
	return r.Client.DeleteObject(ctx, r.Client.Bucket(), name)
}

type Manager struct {
	DB      *db.DB
	Dir     string
	Keep    int
	Keyring *crypto.Keyring
	// Remote returns the destination for the encrypted copy, or nil when none
	// is available (no cache bucket configured).
	Remote func(ctx context.Context) Remote
}

// Encrypt seals a database snapshot with a key derived from the master key.
func Encrypt(kr *crypto.Keyring, plain []byte) ([]byte, error) {
	ct, err := crypto.EncryptAAD(plain, kr.SettingsKey(), []byte(backupAAD))
	if err != nil {
		return nil, err
	}
	return append([]byte(encryptedMagic), ct...), nil
}

// IsEncrypted reports whether data is an encrypted backup produced by Encrypt.
func IsEncrypted(data []byte) bool { return bytes.HasPrefix(data, []byte(encryptedMagic)) }

// DecryptAny opens an encrypted backup made under the current master key or any
// previous one the keyring knows about (after a key rotation).
func DecryptAny(kr *crypto.Keyring, blob []byte) ([]byte, error) {
	if !IsEncrypted(blob) {
		return nil, errors.New("not an encrypted HF2S3 backup")
	}
	keys := append([][]byte{kr.SettingsKey()}, kr.PreviousSettingsKeys()...)
	var lastErr error
	for _, k := range keys {
		plain, err := crypto.DecryptAAD(blob[len(encryptedMagic):], k, []byte(backupAAD))
		if err == nil {
			return plain, nil
		}
		lastErr = err
	}
	return nil, lastErr
}

// Decrypt opens an encrypted backup with the master key it was made with.
func Decrypt(kr *crypto.Keyring, blob []byte) ([]byte, error) {
	if !IsEncrypted(blob) {
		return nil, errors.New("not an encrypted HF2S3 backup")
	}
	return crypto.DecryptAAD(blob[len(encryptedMagic):], kr.SettingsKey(), []byte(backupAAD))
}

// RunOnce writes a local snapshot, prunes old ones and uploads an encrypted copy
// when a remote is available. The local snapshot is kept even if the upload fails.
func (m *Manager) RunOnce(ctx context.Context) (string, error) {
	if m.Dir == "" {
		return "", errors.New("no backup directory configured")
	}
	stamp := time.Now().UTC().Format("20060102-150405.000")
	path := filepath.Join(m.Dir, filePrefix+stamp+fileSuffix)
	if err := m.DB.BackupToFile(ctx, path); err != nil {
		return "", err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		slog.Debug("could not tighten backup permissions", "err", err)
	}
	// Upload first: pruning must never remove the snapshot being sent.
	var uploadErr error
	if m.Remote != nil {
		if remote := m.Remote(ctx); remote != nil {
			uploadErr = m.uploadRemote(ctx, remote, path, stamp)
		}
	}
	if err := m.prune(); err != nil {
		slog.Warn("pruning old backups failed", "err", err)
	}
	if uploadErr != nil {
		return path, fmt.Errorf("local backup written but remote copy failed: %w", uploadErr)
	}
	return path, nil
}

func (m *Manager) prune() error {
	if m.Keep <= 0 {
		return nil
	}
	entries, err := os.ReadDir(m.Dir)
	if err != nil {
		return err
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasPrefix(e.Name(), filePrefix) && strings.HasSuffix(e.Name(), fileSuffix) {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names) // timestamps sort chronologically
	for len(names) > m.Keep {
		if err := os.Remove(filepath.Join(m.Dir, names[0])); err != nil {
			return err
		}
		names = names[1:]
	}
	return nil
}

func (m *Manager) uploadRemote(ctx context.Context, remote Remote, localPath, stamp string) error {
	plain, err := os.ReadFile(localPath)
	if err != nil {
		return err
	}
	blob, err := Encrypt(m.Keyring, plain)
	if err != nil {
		return err
	}
	name := remotePrefix + filePrefix + stamp + remoteSuffix
	if err := remote.Put(ctx, name, blob); err != nil {
		return err
	}

	// Track uploaded names so old remote copies can be removed.
	var names []string
	if raw, err := m.DB.GetSetting(ctx, remoteListKey); err == nil && raw != "" {
		_ = json.Unmarshal([]byte(raw), &names)
	}
	names = append(names, name)
	keep := m.Keep
	if keep <= 0 {
		keep = len(names)
	}
	for len(names) > keep {
		if err := remote.Delete(ctx, names[0]); err != nil {
			slog.Warn("could not delete an old remote backup", "name", names[0], "err", err)
			break
		}
		names = names[1:]
	}
	b, _ := json.Marshal(names)
	return m.DB.SetSetting(ctx, remoteListKey, string(b))
}

// Loop runs RunOnce every interval until ctx is cancelled. A first snapshot is
// taken shortly after start-up when none exists yet.
func (m *Manager) Loop(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	first := time.NewTimer(2 * time.Minute)
	defer first.Stop()
	tick := time.NewTicker(interval)
	defer tick.Stop()

	run := func() {
		path, err := m.RunOnce(ctx)
		switch {
		case ctx.Err() != nil:
		case err != nil:
			slog.Error("database backup failed", "err", err)
		default:
			slog.Info("database backup written", "path", path)
		}
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-first.C:
			if !m.hasBackup() {
				run()
			}
		case <-tick.C:
			run()
		}
	}
}

func (m *Manager) hasBackup() bool {
	entries, err := os.ReadDir(m.Dir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), filePrefix) {
			return true
		}
	}
	return false
}
