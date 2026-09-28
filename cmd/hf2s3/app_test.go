package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"hf2s3/pkg/config"
	"hf2s3/pkg/crypto"
	"hf2s3/pkg/db"
	"hf2s3/pkg/hfclient"
	"hf2s3/pkg/hfclient/hftest"
	"hf2s3/pkg/models"
	"hf2s3/pkg/storage"
)

const (
	oldPassphrase = "hf2s3-aes-master-passphrase-2026" // the value shipped by earlier releases
	strongSecret  = "Zk3q9-very-long-unique-s3-secret-value"
)

func newMasterKey(t *testing.T) string {
	t.Helper()
	k, err := crypto.GenerateMasterKey()
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func cfgFrom(t *testing.T, dbPath string, extra map[string]string) *config.Config {
	t.Helper()
	env := map[string]string{
		"HF2S3_DB":         dbPath,
		"HF2S3_MASTER_KEY": newMasterKey(t),
		"HF2S3_ACCESS_KEY": "HF2SEXAMPLEKEY0001",
		"HF2S3_SECRET_KEY": strongSecret,
		"ADMIN_USERNAME":   "root",
		"ADMIN_PASSWORD":   "a-strong-admin-password",
	}
	for k, v := range extra {
		env[k] = v
	}
	cfg, err := config.FromEnvironment(func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func rawValue(t *testing.T, dbPath, query string, args ...any) string {
	t.Helper()
	raw, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	var v string
	if err := raw.QueryRow(query, args...).Scan(&v); err != nil {
		if err == sql.ErrNoRows {
			return ""
		}
		t.Fatal(err)
	}
	return v
}

// seedLegacyDeployment creates the database an older release would have left:
// plaintext master key, plaintext tokens/secrets, weak default credentials and
// chunks encrypted in the v1 format.
func seedLegacyDeployment(t *testing.T, dbPath string, hf *hftest.Fake) (plain []byte) {
	t.Helper()
	ctx := context.Background()
	old, err := db.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()

	for k, v := range map[string]string{
		"master_key": oldPassphrase, "access_key_id": "hf2s3-access-key", "secret_access_key": "hf2s3-secret-key",
		"admin_username": "admin", "admin_password": "admin123", "chunk_size_mb": "32",
	} {
		if err := old.SetSetting(ctx, k, v); err != nil {
			t.Fatal(err)
		}
	}
	acc := &models.Account{Name: "prod", Username: "me", Token: "hf_live_token_1234567890", RepoName: "me/vault", IsActive: true}
	if err := old.CreateAccount(ctx, acc); err != nil {
		t.Fatal(err)
	}
	if err := old.CreateBucket(ctx, "videos"); err != nil {
		t.Fatal(err)
	}

	plain = bytes.Repeat([]byte("legacy-video-bytes-"), 20)
	blob, err := crypto.Encrypt(plain, crypto.DeriveKey(oldPassphrase))
	if err != nil {
		t.Fatal(err)
	}
	hf.Put("me/vault/data/old-chunk.enc", blob)
	sum := sha256.Sum256(plain)
	obj := &models.Object{Bucket: "videos", Key: "holiday.mp4", Size: int64(len(plain)), ETag: `"x"`, ContentType: "video/mp4"}
	err = old.SaveObjectWithChunks(ctx, obj, []models.Chunk{{
		PartNumber: 1, SizeBytes: int64(len(plain)), CipherSizeBytes: int64(len(blob)), AccountID: acc.ID,
		RemotePath: "data/old-chunk.enc", Sha256Hash: hex.EncodeToString(sum[:]),
	}})
	if err != nil {
		t.Fatal(err)
	}
	return plain
}

func TestBootstrapMigratesALegacyDeployment(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	hf := hftest.New(t)
	plain := seedLegacyDeployment(t, dbPath, hf)

	cfg := cfgFrom(t, dbPath, nil)
	opts := BootstrapOptions{HFOptions: []hfclient.ClientOption{hfclient.WithBaseURL(hf.URL())}}
	app, err := Bootstrap(ctx, cfg, opts)
	if err != nil {
		t.Fatalf("bootstrap over a legacy database: %v", err)
	}

	if !app.LegacyAdopted {
		t.Error("the stored plaintext master key must be adopted as a legacy key")
	}
	if !app.Keyring.HasLegacyKeys() {
		t.Fatal("legacy key missing from the keyring: old data would be unreadable")
	}

	_ = app.Close()
	// --- What is on disk now -------------------------------------------------
	if v := rawValue(t, dbPath, `SELECT value FROM system_settings WHERE key = 'master_key'`); v != "" {
		t.Fatalf("the plaintext master key is still in the database: %q", v)
	}
	sealed := rawValue(t, dbPath, `SELECT value FROM system_settings WHERE key = 'legacy_master_keys'`)
	if !crypto.IsSealed(sealed) || strings.Contains(sealed, oldPassphrase) {
		t.Fatalf("the legacy key must be stored encrypted: %q", sealed)
	}
	if tok := rawValue(t, dbPath, `SELECT token FROM accounts WHERE id = 1`); !crypto.IsSealed(tok) || strings.Contains(tok, "hf_live") {
		t.Fatalf("the Hugging Face token must be encrypted at rest: %q", tok)
	}
	if v := rawValue(t, dbPath, `SELECT value FROM system_settings WHERE key = 'secret_access_key'`); strings.Contains(v, "hf2s3-secret-key") {
		t.Fatalf("stored S3 secret still readable: %q", v)
	}

	// --- Restart with the same configuration: nothing to adopt, data readable ---
	app2, err := Bootstrap(ctx, cfg, opts)
	if err != nil {
		t.Fatalf("second start: %v", err)
	}
	defer app2.Close()
	if app2.LegacyAdopted {
		t.Error("the legacy key must not be adopted twice")
	}

	readAll := func(a *App) []byte {
		_, rc, err := a.Pool.GetObject(ctx, "videos", "holiday.mp4", nil)
		if err != nil {
			t.Fatalf("GetObject: %v", err)
		}
		defer rc.Close()
		b, err := io.ReadAll(rc)
		if err != nil {
			t.Fatalf("legacy data unreadable after the upgrade: %v", err)
		}
		return b
	}
	if !bytes.Equal(readAll(app2), plain) {
		t.Fatal("legacy content changed")
	}

	// --- Re-key to the new format ----------------------------------------------
	rep, err := app2.Pool.Rekey(ctx, storage.RekeyOptions{})
	if err != nil || rep.Done != 1 || rep.Failed != 0 {
		t.Fatalf("rekey: %+v %v", rep, err)
	}
	app2.Pool.ProcessPendingDeletions(ctx)
	if _, ok := hf.Get("me/vault/data/old-chunk.enc"); ok {
		t.Fatal("the old ciphertext must be deleted after re-keying")
	}
	if !bytes.Equal(readAll(app2), plain) {
		t.Fatal("content changed by re-keying")
	}
	if n, _, _ := app2.DB.CountChunksToRekey(ctx, app2.Keyring.CurrentKeyID()); n != 0 {
		t.Fatalf("%d chunks still need re-keying", n)
	}
}

func TestBootstrapRefusesInsecureProduction(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	t.Run("no master key", func(t *testing.T) {
		cfg := cfgFrom(t, filepath.Join(dir, "a.db"), map[string]string{"HF2S3_MASTER_KEY": ""})
		if _, err := Bootstrap(ctx, cfg, BootstrapOptions{}); err == nil || !strings.Contains(err.Error(), "HF2S3_MASTER_KEY") {
			t.Fatalf("a missing master key must be refused, got %v", err)
		}
	})

	t.Run("legacy default master key passphrase", func(t *testing.T) {
		cfg := cfgFrom(t, filepath.Join(dir, "b.db"), map[string]string{"HF2S3_MASTER_KEY": oldPassphrase})
		if _, err := Bootstrap(ctx, cfg, BootstrapOptions{}); err == nil {
			t.Fatal("the well-known default master key must be refused")
		}
	})

	t.Run("default credentials from env", func(t *testing.T) {
		cfg := cfgFrom(t, filepath.Join(dir, "c.db"), map[string]string{"HF2S3_SECRET_KEY": "hf2s3-secret-key", "ADMIN_PASSWORD": "admin123"})
		_, err := Bootstrap(ctx, cfg, BootstrapOptions{})
		if err == nil || !strings.Contains(err.Error(), "insecure credentials") {
			t.Fatalf("default credentials must be refused, got %v", err)
		}
	})

	t.Run("default credentials left in an old database", func(t *testing.T) {
		dbPath := filepath.Join(dir, "d.db")
		seedLegacyDeployment(t, dbPath, hftest.New(t))
		cfg := cfgFrom(t, dbPath, map[string]string{"HF2S3_ACCESS_KEY": "", "HF2S3_SECRET_KEY": "", "ADMIN_USERNAME": "", "ADMIN_PASSWORD": ""})
		_, err := Bootstrap(ctx, cfg, BootstrapOptions{})
		if err == nil || !strings.Contains(err.Error(), "insecure credentials") {
			t.Fatalf("an old database holding default credentials must not start in production, got %v", err)
		}
	})

	t.Run("dev mode allows defaults", func(t *testing.T) {
		cfg := cfgFrom(t, filepath.Join(dir, "e.db"), map[string]string{
			"HF2S3_DEV": "1", "HF2S3_MASTER_KEY": "", "HF2S3_ACCESS_KEY": "", "HF2S3_SECRET_KEY": "", "ADMIN_USERNAME": "", "ADMIN_PASSWORD": "",
		})
		app, err := Bootstrap(ctx, cfg, BootstrapOptions{})
		if err != nil {
			t.Fatalf("dev mode: %v", err)
		}
		app.Close()
	})

	t.Run("maintenance commands do not need credentials", func(t *testing.T) {
		cfg := cfgFrom(t, filepath.Join(dir, "f.db"), map[string]string{"HF2S3_ACCESS_KEY": "", "HF2S3_SECRET_KEY": "", "ADMIN_USERNAME": "", "ADMIN_PASSWORD": ""})
		app, err := Bootstrap(ctx, cfg, BootstrapOptions{SkipCredentialChecks: true})
		if err != nil {
			t.Fatalf("maintenance bootstrap: %v", err)
		}
		app.Close()
	})
}

func TestWrongMasterKeyCannotOpenSealedSettings(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	cfg := cfgFrom(t, dbPath, nil)
	app, err := Bootstrap(ctx, cfg, BootstrapOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_ = app.DB.SetSetting(ctx, "hf_storage_secret_key", "cache-secret")
	_ = app.DB.CreateAccount(ctx, &models.Account{Name: "a", Username: "u", Token: "hf_token_abcdef", RepoName: "u/r", IsActive: true})
	app.Close()

	// A different master key must not read (or silently reset) the secrets.
	other := cfgFrom(t, dbPath, nil) // fresh random master key
	other.MasterKey = newMasterKey(t)
	if _, err := Bootstrap(ctx, other, BootstrapOptions{}); err == nil {
		t.Fatal("starting with the wrong master key must fail instead of ignoring sealed data")
	}
}

func TestEnvironmentOverridesDatabaseSettings(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "meta.db")

	first, err := Bootstrap(ctx, cfgFrom(t, dbPath, map[string]string{"HF2S3_MASTER_KEY": "", "HF2S3_DEV": "1", "HF2S3_ACCESS_KEY": "", "HF2S3_SECRET_KEY": "", "ADMIN_PASSWORD": ""}), BootstrapOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_ = first.DB.SetSetting(ctx, "s3_region", "eu-west-3")
	_ = first.DB.SetSetting(ctx, "chunk_size_mb", "64")
	first.Close()

	// No env override: database values apply.
	cfg := cfgFrom(t, dbPath, map[string]string{"HF2S3_MASTER_KEY": "", "HF2S3_DEV": "1", "HF2S3_ACCESS_KEY": "", "HF2S3_SECRET_KEY": "", "ADMIN_PASSWORD": ""})
	app, err := Bootstrap(ctx, cfg, BootstrapOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if app.Settings.S3Region != "eu-west-3" || app.Settings.ChunkSizeMB != 64 {
		t.Fatalf("database settings ignored: %+v", app.Settings)
	}
	app.Close()

	// With env values, the environment wins and is read-only in the console.
	cfg = cfgFrom(t, dbPath, map[string]string{
		"HF2S3_MASTER_KEY": "", "HF2S3_DEV": "1", "HF2S3_ACCESS_KEY": "", "HF2S3_SECRET_KEY": "", "ADMIN_PASSWORD": "",
		"HF2S3_REGION": "ap-south-1", "HF2S3_CHUNK_SIZE_MB": "16",
	})
	app, err = Bootstrap(ctx, cfg, BootstrapOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	if app.Settings.S3Region != "ap-south-1" || app.Settings.ChunkSizeMB != 16 {
		t.Fatalf("environment must override the database: %+v", app.Settings)
	}
	if app.Pool.ChunkSizeBytes() != 16*1024*1024 {
		t.Fatalf("pool chunk size = %d", app.Pool.ChunkSizeBytes())
	}
}

func TestRouterDispatchAndAccessLogHidesSignatures(t *testing.T) {
	ctx := context.Background()
	cfg := cfgFrom(t, filepath.Join(t.TempDir(), "meta.db"), map[string]string{"HF2S3_METRICS_TOKEN": "metrics-token-123"})
	app, err := Bootstrap(ctx, cfg, BootstrapOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	h := app.router()

	do := func(method, target string, mutate ...func(*http.Request)) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, target, nil)
		for _, m := range mutate {
			m(req)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	if rec := do(http.MethodGet, "/api/health"); rec.Code != http.StatusOK {
		t.Fatalf("/api/health = %d", rec.Code)
	}
	rec := do(http.MethodGet, "/", func(r *http.Request) { r.Header.Set("Accept", "text/html") })
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "HF2S3") {
		t.Fatalf("browser at / must get the console, got %d", rec.Code)
	}
	if rec.Header().Get("Content-Security-Policy") == "" {
		t.Fatal("console responses must carry the CSP")
	}
	// An S3 client at / (no HTML accept, no credentials) gets an S3 error, not the console.
	if rec := do(http.MethodGet, "/"); rec.Code != http.StatusForbidden || !strings.Contains(rec.Body.String(), "<Error>") {
		t.Fatalf("anonymous S3 request at / = %d %s", rec.Code, rec.Body.String())
	}
	// A presigned URL opened in a browser tab must still reach the S3 API.
	if rec := do(http.MethodGet, "/?X-Amz-Algorithm=AWS4-HMAC-SHA256", func(r *http.Request) { r.Header.Set("Accept", "text/html") }); strings.Contains(rec.Body.String(), "<!DOCTYPE html>") {
		t.Fatal("a presigned request must not be answered with the console page")
	}

	// Metrics are opt-in and token protected.
	if rec := do(http.MethodGet, "/metrics"); rec.Code != http.StatusUnauthorized {
		t.Fatalf("/metrics without a token = %d", rec.Code)
	}
	rec = do(http.MethodGet, "/metrics", func(r *http.Request) { r.Header.Set("Authorization", "Bearer metrics-token-123") })
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "hf2s3_http_requests_total") {
		t.Fatalf("/metrics with the token = %d %s", rec.Code, rec.Body.String())
	}

	// The access log records paths but never query strings (presigned signatures).
	var logs bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo})))
	defer slog.SetDefault(prev)
	do(http.MethodGet, "/some-bucket/some-key?X-Amz-Signature=SUPERSECRETSIGNATURE&X-Amz-Credential=AKIDLEAK")
	out := logs.String()
	if !strings.Contains(out, "/some-bucket/some-key") {
		t.Fatalf("request not logged: %q", out)
	}
	if strings.Contains(out, "SUPERSECRETSIGNATURE") || strings.Contains(out, "AKIDLEAK") {
		t.Fatalf("the access log leaked query parameters: %s", out)
	}
}

func TestRunShutsDownGracefully(t *testing.T) {
	cfg := cfgFrom(t, filepath.Join(t.TempDir(), "meta.db"), map[string]string{"PORT": "18089", "HF2S3_BACKUP_INTERVAL_HOURS": "0"})
	ctx, cancel := context.WithCancel(context.Background())
	app, err := Bootstrap(ctx, cfg, BootstrapOptions{})
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- app.Run(ctx) }()

	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := http.Get("http://127.0.0.1:18089/api/health")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("server never came up: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after the context was cancelled")
	}
	if _, err := http.Get("http://127.0.0.1:18089/api/health"); err == nil {
		t.Fatal("the port must be closed after shutdown")
	}
}

// A generated master key can be rotated without losing data: the new key goes
// in HF2S3_MASTER_KEY and the previous one in HF2S3_LEGACY_MASTER_KEYS.
func TestMasterKeyRotation(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	hf := hftest.New(t)
	opts := BootstrapOptions{HFOptions: []hfclient.ClientOption{hfclient.WithBaseURL(hf.URL())}}

	keyA, keyB := newMasterKey(t), newMasterKey(t)
	cfgA := cfgFrom(t, dbPath, map[string]string{"HF2S3_MASTER_KEY": keyA})
	appA, err := Bootstrap(ctx, cfgA, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := appA.DB.CreateBucket(ctx, "docs"); err != nil {
		t.Fatal(err)
	}
	if err := appA.DB.CreateAccount(ctx, &models.Account{Name: "a", Username: "u", Token: "hf_rotation_token", RepoName: "u/r", IsActive: true}); err != nil {
		t.Fatal(err)
	}
	if err := appA.DB.SetSetting(ctx, "hf_storage_secret_key", "cache-secret-value"); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("rotate me "), 100)
	if _, err := appA.Pool.PutObject(ctx, "docs", "a.txt", "text/plain", bytes.NewReader(payload), int64(len(payload)), nil); err != nil {
		t.Fatal(err)
	}
	oldKeyID := appA.Keyring.CurrentKeyID()
	appA.Close()

	// Without telling the gateway about the old key, the new one is refused.
	cfgNoLegacy := cfgFrom(t, dbPath, map[string]string{"HF2S3_MASTER_KEY": keyB})
	if _, err := Bootstrap(ctx, cfgNoLegacy, opts); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("a rotated key without the previous one must be refused, got %v", err)
	}

	// With the previous key listed, secrets are re-sealed and data stays readable.
	cfgB := cfgFrom(t, dbPath, map[string]string{"HF2S3_MASTER_KEY": keyB, "HF2S3_LEGACY_MASTER_KEYS": keyA})
	appB, err := Bootstrap(ctx, cfgB, opts)
	if err != nil {
		t.Fatalf("rotation start: %v", err)
	}
	acc, err := appB.DB.GetAccountByID(ctx, 1)
	if err != nil || acc.Token != "hf_rotation_token" {
		t.Fatalf("token lost during rotation: %v %v", acc, err)
	}
	if v, _ := appB.DB.GetSetting(ctx, "hf_storage_secret_key"); v != "cache-secret-value" {
		t.Fatalf("setting lost during rotation: %q", v)
	}
	read := func(a *App) []byte {
		_, rc, err := a.Pool.GetObject(ctx, "docs", "a.txt", nil)
		if err != nil {
			t.Fatal(err)
		}
		defer rc.Close()
		b, err := io.ReadAll(rc)
		if err != nil {
			t.Fatalf("data unreadable after rotation: %v", err)
		}
		return b
	}
	if !bytes.Equal(read(appB), payload) {
		t.Fatal("content changed by rotation")
	}
	if appB.Keyring.CurrentKeyID() == oldKeyID {
		t.Fatal("the current key id must change")
	}

	rep, err := appB.Pool.Rekey(ctx, storage.RekeyOptions{})
	if err != nil || rep.Failed != 0 || rep.Done < 1 {
		t.Fatalf("rekey after rotation: %+v %v", rep, err)
	}
	appB.Pool.ProcessPendingDeletions(ctx)
	appB.Close()

	// After re-keying, the previous key is no longer needed.
	appC, err := Bootstrap(ctx, cfgNoLegacy, opts)
	if err != nil {
		t.Fatalf("start with only the new key after re-keying: %v", err)
	}
	defer appC.Close()
	if !bytes.Equal(read(appC), payload) {
		t.Fatal("content unreadable with only the new key")
	}
	if n, _, _ := appC.DB.CountChunksToRekey(ctx, appC.Keyring.CurrentKeyID()); n != 0 {
		t.Fatalf("%d chunks still under the old key", n)
	}
}

func newAccount() *models.Account {
	return &models.Account{Name: "acc", Username: "me", Token: "hf_test_token_123456", RepoName: "me/vault", IsActive: true}
}
