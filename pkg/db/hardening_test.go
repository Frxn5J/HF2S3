package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"hf2s3/pkg/crypto"
	"hf2s3/pkg/models"
)

func mustOpen(t *testing.T) *DB {
	t.Helper()
	d, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	return d
}

func seedAccountAndBucket(t *testing.T, d *DB) *models.Account {
	t.Helper()
	ctx := context.Background()
	acc := &models.Account{Name: "a", Username: "u", Token: "hf_plain_token", RepoName: "u/r", IsActive: true}
	if err := d.CreateAccount(ctx, acc); err != nil {
		t.Fatal(err)
	}
	if err := d.CreateBucket(ctx, "bkt"); err != nil {
		t.Fatal(err)
	}
	return acc
}

func TestMigrationFromLegacyUnversionedDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	ctx := context.Background()

	// Build a database exactly as the pre-migration release would have left it.
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.ExecContext(ctx, baselineSchema); err != nil {
		t.Fatal(err)
	}
	for _, c := range legacyAccountColumns {
		if _, err := raw.ExecContext(ctx, c.ddl); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now().UTC()
	mustExec := func(q string, args ...any) {
		t.Helper()
		if _, err := raw.ExecContext(ctx, q, args...); err != nil {
			t.Fatal(err)
		}
	}
	mustExec(`INSERT INTO accounts (name, username, token, repo_name, created_at, updated_at) VALUES ('a','u','hf_old_token','u/r',?,?)`, now, now)
	mustExec(`INSERT INTO buckets (name, created_at) VALUES ('bkt', ?)`, now)
	mustExec(`INSERT INTO objects (bucket, key, size, etag, content_type, created_at, updated_at) VALUES ('bkt','k',10,'"e"','text/plain',?,?)`, now, now)
	mustExec(`INSERT INTO chunks (object_id, chunk_index, offset_bytes, size_bytes, cipher_size_bytes, account_id, remote_path, sha256_hash, created_at) VALUES (1,0,0,10,38,1,'data/x.enc','h',?)`, now)
	mustExec(`INSERT INTO system_settings (key, value) VALUES ('master_key','old-master')`)
	_ = raw.Close()

	d, err := Open(path)
	if err != nil {
		t.Fatalf("open legacy db: %v", err)
	}
	defer d.Close()

	var version int
	if err := d.db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil || version != SchemaVersion() {
		t.Fatalf("user_version = %d (%v), want %d", version, err, SchemaVersion())
	}

	_, chunks, err := d.GetObjectWithChunks(ctx, "bkt", "k")
	if err != nil || len(chunks) != 1 {
		t.Fatalf("legacy chunk lost: %v %d", err, len(chunks))
	}
	if chunks[0].EncVersion != crypto.EncV1 || chunks[0].KeyID != crypto.LegacyKeyID {
		t.Fatalf("legacy chunk must default to v1/legacy, got v%d/%s", chunks[0].EncVersion, chunks[0].KeyID)
	}
	if v, _ := d.GetSetting(ctx, "master_key"); v != "old-master" {
		t.Fatalf("legacy setting must survive migration, got %q", v)
	}

	// Re-opening must be a no-op.
	d.Close()
	d2, err := Open(path)
	if err != nil {
		t.Fatalf("second open: %v", err)
	}
	d2.Close()
}

func TestSecretsAreSealedAtRest(t *testing.T) {
	d := mustOpen(t)
	ctx := context.Background()

	// A plaintext row written before encryption was enabled.
	acc := seedAccountAndBucket(t, d)

	kr, err := crypto.NewKeyring(bytes32(), nil)
	if err != nil {
		t.Fatal(err)
	}
	box, _ := crypto.NewSecretBox(kr.SettingsKey())
	d.SetSecretBox(box)

	// Plaintext still readable until migrated.
	got, _ := d.GetAccountByID(ctx, acc.ID)
	if got.Token != "hf_plain_token" {
		t.Fatalf("legacy plaintext token unreadable: %q", got.Token)
	}

	if err := d.SetSetting(ctx, "secret_access_key", "s3-secret"); err != nil {
		t.Fatal(err)
	}
	n, err := d.SealPlaintextSecrets(ctx)
	if err != nil || n == 0 {
		t.Fatalf("SealPlaintextSecrets = %d, %v", n, err)
	}

	var rawToken, rawSetting string
	_ = d.db.QueryRow(`SELECT token FROM accounts WHERE id = ?`, acc.ID).Scan(&rawToken)
	_ = d.db.QueryRow(`SELECT value FROM system_settings WHERE key = 'secret_access_key'`).Scan(&rawSetting)
	if !crypto.IsSealed(rawToken) || strings.Contains(rawToken, "hf_plain_token") {
		t.Fatalf("token stored in clear: %s", rawToken)
	}
	if !crypto.IsSealed(rawSetting) {
		t.Fatalf("setting stored in clear: %s", rawSetting)
	}

	got, _ = d.GetAccountByID(ctx, acc.ID)
	if got.Token != "hf_plain_token" {
		t.Fatalf("token not transparently decrypted: %q", got.Token)
	}
	if v, _ := d.GetSetting(ctx, "secret_access_key"); v != "s3-secret" {
		t.Fatalf("setting not transparently decrypted: %q", v)
	}

	// New writes are sealed too, and a second run seals nothing.
	acc2 := &models.Account{Name: "b", Username: "u", Token: "hf_second", RepoName: "u/r2", IsActive: true}
	_ = d.CreateAccount(ctx, acc2)
	_ = d.db.QueryRow(`SELECT token FROM accounts WHERE id = ?`, acc2.ID).Scan(&rawToken)
	if !crypto.IsSealed(rawToken) {
		t.Fatal("new account token must be sealed on insert")
	}
	if n, _ := d.SealPlaintextSecrets(ctx); n != 0 {
		t.Fatalf("second seal pass changed %d values", n)
	}
}

func bytes32() []byte {
	b := make([]byte, 32)
	for i := range b {
		b[i] = byte(i + 1)
	}
	return b
}

func chunkFor(acc *models.Account, idx int, path string) models.Chunk {
	return models.Chunk{
		PartNumber: 1, ChunkIndex: idx, OffsetBytes: int64(idx) * 10, SizeBytes: 10,
		CipherSizeBytes: 38, AccountID: acc.ID, RemotePath: path, Sha256Hash: "h",
		EncVersion: crypto.EncV2, KeyID: "k1",
	}
}

func TestOverwriteAndDeleteQueueRemoteCopiesAtomically(t *testing.T) {
	d := mustOpen(t)
	ctx := context.Background()
	acc := seedAccountAndBucket(t, d)
	_ = d.IncrementAccountUsage(ctx, acc.ID, 76)

	obj := &models.Object{Bucket: "bkt", Key: "movie.mp4", Size: 20, ETag: `"e1"`, ContentType: "video/mp4"}
	locs := []models.ObjectLocation{
		{Tier: models.TierCold, AccountID: acc.ID, RemotePath: "data/a.enc", IsEncrypted: true},
		{Tier: models.TierCache, AccountID: 7, RemotePath: "v/1/bkt/movie.mp4", SizeBytes: 20},
	}
	if err := d.SaveObjectWithChunksAndLocations(ctx, obj, []models.Chunk{chunkFor(acc, 0, "data/a.enc"), chunkFor(acc, 1, "data/b.enc")}, locs); err != nil {
		t.Fatal(err)
	}
	if n, _ := d.CountPendingDeletions(ctx); n != 0 {
		t.Fatalf("first save must not queue anything, got %d", n)
	}

	// Overwrite: the previous chunks and cache copy must be queued in the same tx.
	obj2 := &models.Object{Bucket: "bkt", Key: "movie.mp4", Size: 10, ETag: `"e2"`, ContentType: "video/mp4"}
	if err := d.SaveObjectWithChunks(ctx, obj2, []models.Chunk{chunkFor(acc, 0, "data/c.enc")}); err != nil {
		t.Fatal(err)
	}
	due, err := d.ListDueDeletions(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	paths := map[string]string{}
	for _, p := range due {
		paths[p.RemotePath] = p.Kind
	}
	for path, kind := range map[string]string{"data/a.enc": "chunk", "data/b.enc": "chunk", "v/1/bkt/movie.mp4": "cache"} {
		if paths[path] != kind {
			t.Fatalf("expected %s queued as %s, queue=%v", path, kind, paths)
		}
	}
	if _, ok := paths["data/c.enc"]; ok {
		t.Fatal("the new version's chunk must not be queued")
	}

	// Delete: the current chunk is queued as well.
	if _, err := d.DeleteObject(ctx, "bkt", "movie.mp4"); err != nil {
		t.Fatal(err)
	}
	if n, _ := d.CountPendingDeletions(ctx); n != 4 {
		t.Fatalf("queue length = %d, want 4", n)
	}

	// Completing releases the accounted usage, never below zero.
	if err := d.CompleteDeletions(ctx, due); err != nil {
		t.Fatal(err)
	}
	got, _ := d.GetAccountByID(ctx, acc.ID)
	if got.UsedBytes < 0 || got.UsedBytes > 76 {
		t.Fatalf("used_bytes = %d", got.UsedBytes)
	}
}

func TestSaveIntoDeletedBucketFails(t *testing.T) {
	d := mustOpen(t)
	ctx := context.Background()
	seedAccountAndBucket(t, d)
	if err := d.DeleteBucket(ctx, "bkt"); err != nil {
		t.Fatal(err)
	}
	err := d.SaveObjectWithChunks(ctx, &models.Object{Bucket: "bkt", Key: "k", ETag: `"e"`, ContentType: "x"}, nil)
	if !errors.Is(err, ErrBucketMissing) {
		t.Fatalf("expected ErrBucketMissing, got %v", err)
	}
}

func TestDeleteAccountRefusedWhileItOwnsData(t *testing.T) {
	d := mustOpen(t)
	ctx := context.Background()
	acc := seedAccountAndBucket(t, d)
	obj := &models.Object{Bucket: "bkt", Key: "k", Size: 10, ETag: `"e"`, ContentType: "x"}
	if err := d.SaveObjectWithChunks(ctx, obj, []models.Chunk{chunkFor(acc, 0, "data/a.enc")}); err != nil {
		t.Fatal(err)
	}
	if err := d.DeleteAccount(ctx, acc.ID); !errors.Is(err, ErrAccountInUse) {
		t.Fatalf("expected ErrAccountInUse, got %v", err)
	}

	// After the object is gone its chunks are still queued: the account (and its
	// token) must survive until the remote copies are really deleted.
	_, _ = d.DeleteObject(ctx, "bkt", "k")
	if err := d.DeleteAccount(ctx, acc.ID); !errors.Is(err, ErrAccountInUse) {
		t.Fatalf("expected ErrAccountInUse with pending deletions, got %v", err)
	}
	due, _ := d.ListDueDeletions(ctx, 10)
	_ = d.CompleteDeletions(ctx, due)
	if err := d.DeleteAccount(ctx, acc.ID); err != nil {
		t.Fatalf("delete of an empty account failed: %v", err)
	}
}

func TestDeleteCacheBucketDropsItsLocations(t *testing.T) {
	d := mustOpen(t)
	ctx := context.Background()
	acc := seedAccountAndBucket(t, d)
	cb := &models.CacheBucket{Name: "c", AccessKey: "ak", SecretKey: "sk", BucketName: "b", IsActive: true}
	if err := d.CreateCacheBucket(ctx, cb); err != nil {
		t.Fatal(err)
	}
	obj := &models.Object{Bucket: "bkt", Key: "k", Size: 10, ETag: `"e"`, ContentType: "x"}
	locs := []models.ObjectLocation{{Tier: models.TierCache, AccountID: cb.ID, RemotePath: "v/1/bkt/k", SizeBytes: 10}}
	if err := d.SaveObjectWithChunksAndLocations(ctx, obj, []models.Chunk{chunkFor(acc, 0, "data/a.enc")}, locs); err != nil {
		t.Fatal(err)
	}

	if err := d.DeleteCacheBucket(ctx, cb.ID); err != nil {
		t.Fatal(err)
	}
	got, _ := d.HeadObject(ctx, "bkt", "k")
	if got.HasCache {
		t.Fatal("dangling cache location survived bucket deletion")
	}
	// The bucket's credentials are gone, so nothing may stay queued against it.
	if n, _ := d.CountPendingDeletions(ctx); n != 0 {
		t.Fatalf("deletions against a removed bucket must not stay queued, got %d", n)
	}
}

func TestListObjectsPrefixIsLiteralAndCaseSensitive(t *testing.T) {
	d := mustOpen(t)
	ctx := context.Background()
	acc := seedAccountAndBucket(t, d)
	for _, k := range []string{"a_b/1", "aXb/2", "A_b/3", "a%b/4", "a_b/5", "z"} {
		obj := &models.Object{Bucket: "bkt", Key: k, Size: 1, ETag: `"e"`, ContentType: "x"}
		if err := d.SaveObjectWithChunks(ctx, obj, []models.Chunk{chunkFor(acc, 0, "data/"+strings.ReplaceAll(k, "/", "_"))}); err != nil {
			t.Fatal(err)
		}
	}

	keys := func(prefix, after string, inclusive bool, limit int) []string {
		objs, err := d.ListObjectsPage(ctx, "bkt", prefix, after, inclusive, limit)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, o := range objs {
			out = append(out, o.Key)
		}
		return out
	}

	if got := keys("a_b/", "", false, 10); fmt.Sprint(got) != "[a_b/1 a_b/5]" {
		t.Fatalf("underscore must be literal, got %v", got)
	}
	if got := keys("a%", "", false, 10); fmt.Sprint(got) != "[a%b/4]" {
		t.Fatalf("percent must be literal, got %v", got)
	}
	if got := keys("a_b/", "a_b/1", false, 10); fmt.Sprint(got) != "[a_b/5]" {
		t.Fatalf("exclusive start-after, got %v", got)
	}
	if got := keys("a_b/", "a_b/1", true, 10); fmt.Sprint(got) != "[a_b/1 a_b/5]" {
		t.Fatalf("inclusive start-after, got %v", got)
	}
	all := keys("", "", false, 3)
	if len(all) != 3 {
		t.Fatalf("limit not applied: %v", all)
	}

	objs, _ := d.ListObjectsPage(ctx, "bkt", "", "", false, 10)
	for _, o := range objs {
		if !o.HasCold {
			t.Fatalf("batch tier population missed %s", o.Key)
		}
	}
}

func TestRekeyBookkeeping(t *testing.T) {
	d := mustOpen(t)
	ctx := context.Background()
	acc := seedAccountAndBucket(t, d)
	legacy := models.Chunk{PartNumber: 1, ChunkIndex: 0, SizeBytes: 10, CipherSizeBytes: 38, AccountID: acc.ID, RemotePath: "data/old.enc", Sha256Hash: "h"}
	obj := &models.Object{Bucket: "bkt", Key: "k", Size: 10, ETag: `"e"`, ContentType: "x"}
	if err := d.SaveObjectWithChunks(ctx, obj, []models.Chunk{legacy}); err != nil {
		t.Fatal(err)
	}

	if n, _, _ := d.CountChunksToRekey(ctx, "k1"); n != 1 {
		t.Fatalf("expected 1 chunk to re-key, got %d", n)
	}
	todo, _ := d.ListChunksToRekey(ctx, "k1", 0, 10)
	if len(todo) != 1 || todo[0].EncVersion != crypto.EncV1 {
		t.Fatalf("unexpected rekey list: %+v", todo)
	}

	err := d.ApplyRekeyed(ctx, []RekeyedChunk{{
		ID: todo[0].ID, OldAccountID: acc.ID, OldRemotePath: "data/old.enc", OldCipherSize: 38,
		NewAccountID: acc.ID, NewRemotePath: "data/new.enc", NewCipherSize: 38, NewEncVersion: crypto.EncV2, NewKeyID: "k1",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if n, _, _ := d.CountChunksToRekey(ctx, "k1"); n != 0 {
		t.Fatalf("chunk still flagged after re-key: %d", n)
	}
	due, _ := d.ListDueDeletions(ctx, 10)
	if len(due) != 1 || due[0].RemotePath != "data/old.enc" {
		t.Fatalf("old copy not queued for deletion: %+v", due)
	}
}

func TestFutureSchemaVersionIsRefused(t *testing.T) {
	path := filepath.Join(t.TempDir(), "future.db")
	raw, _ := sql.Open("sqlite", path)
	_, _ = raw.Exec(fmt.Sprintf(`PRAGMA user_version = %d`, SchemaVersion()+5))
	_, _ = raw.Exec(`CREATE TABLE t (x)`)
	_ = raw.Close()
	if _, err := Open(path); err == nil {
		t.Fatal("a database from a newer release must be refused")
	}
}

func TestInspectBackup(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// A healthy database with data.
	good := filepath.Join(dir, "good.db")
	d, err := Open(good)
	if err != nil {
		t.Fatal(err)
	}
	acc := seedAccountAndBucketOn(t, d)
	obj := &models.Object{Bucket: "bkt", Key: "k", Size: 10, ETag: `"e"`, ContentType: "x"}
	if err := d.SaveObjectWithChunks(ctx, obj, []models.Chunk{chunkFor(acc, 0, "data/a.enc")}); err != nil {
		t.Fatal(err)
	}
	snap := filepath.Join(dir, "snap.db")
	if err := d.BackupToFile(ctx, snap); err != nil {
		t.Fatal(err)
	}
	d.Close()

	info, err := InspectBackup(ctx, snap)
	if err != nil {
		t.Fatalf("a valid snapshot must inspect fine: %v", err)
	}
	if info.Accounts != 1 || info.Buckets != 1 || info.Objects != 1 || info.Chunks != 1 || info.TotalBytes != 10 || info.SchemaVersion != SchemaVersion() {
		t.Fatalf("unexpected info: %+v", info)
	}
	if info.LatestObject.IsZero() {
		t.Fatal("latest object date missing")
	}

	// Not a database at all.
	junk := filepath.Join(dir, "junk.db")
	_ = os.WriteFile(junk, []byte("definitely not sqlite"), 0o600)
	if _, err := InspectBackup(ctx, junk); !errors.Is(err, ErrNotHF2SDatabase) {
		t.Fatalf("junk must be ErrNotHF2SDatabase, got %v", err)
	}

	// A valid SQLite file that is not ours.
	other := filepath.Join(dir, "other.db")
	raw, _ := sql.Open("sqlite", other)
	_, _ = raw.Exec(`CREATE TABLE t (x)`)
	raw.Close()
	if _, err := InspectBackup(ctx, other); !errors.Is(err, ErrNotHF2SDatabase) {
		t.Fatalf("a foreign sqlite file must be refused, got %v", err)
	}

	// Newer than this binary.
	future := filepath.Join(dir, "future.db")
	data, _ := os.ReadFile(snap)
	_ = os.WriteFile(future, data, 0o600)
	raw, _ = sql.Open("sqlite", future)
	_, _ = raw.Exec(fmt.Sprintf(`PRAGMA user_version = %d`, SchemaVersion()+3))
	raw.Close()
	if _, err := InspectBackup(ctx, future); err == nil || !errors.Is(err, ErrNewerSchema) {
		t.Fatalf("a newer schema must be refused, got %v", err)
	}

	// Truncated file.
	trunc := filepath.Join(dir, "trunc.db")
	_ = os.WriteFile(trunc, data[:len(data)/2], 0o600)
	if _, err := InspectBackup(ctx, trunc); err == nil {
		t.Fatal("a truncated database must be refused")
	}

	// An old, unversioned database (user_version 0) is accepted: migrations run on start-up.
	legacy := filepath.Join(dir, "legacy.db")
	_ = os.WriteFile(legacy, data, 0o600)
	raw, _ = sql.Open("sqlite", legacy)
	_, _ = raw.Exec(`PRAGMA user_version = 0`)
	raw.Close()
	if _, err := InspectBackup(ctx, legacy); err != nil {
		t.Fatalf("an unversioned legacy database must be accepted: %v", err)
	}
}

// seedAccountAndBucketOn is seedAccountAndBucket for a caller-owned database.
func seedAccountAndBucketOn(t *testing.T, d *DB) *models.Account {
	t.Helper()
	ctx := context.Background()
	acc := &models.Account{Name: "a", Username: "u", Token: "hf_plain_token", RepoName: "u/r", IsActive: true}
	if err := d.CreateAccount(ctx, acc); err != nil {
		t.Fatal(err)
	}
	if err := d.CreateBucket(ctx, "bkt"); err != nil {
		t.Fatal(err)
	}
	return acc
}
