package main

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"hf2s3/pkg/backup"
	"hf2s3/pkg/config"
	"hf2s3/pkg/dashboard"
	"hf2s3/pkg/db"
	"hf2s3/pkg/hfclient"
	"hf2s3/pkg/hfclient/hftest"
)

// restoreRig is a running deployment plus everything needed to restore into it.
type restoreRig struct {
	t      *testing.T
	ctx    context.Context
	hf     *hftest.Fake
	opts   BootstrapOptions
	master string
	dir    string
	dbPath string
	cfg    *config.Config
	app    *App
	m      *restoreManager
}

func newRig(t *testing.T, hf *hftest.Fake, master string) *restoreRig {
	t.Helper()
	if hf == nil {
		hf = hftest.New(t)
	}
	if master == "" {
		master = newMasterKey(t)
	}
	dir := t.TempDir()
	r := &restoreRig{
		t: t, ctx: context.Background(), hf: hf, master: master, dir: dir, dbPath: filepath.Join(dir, "meta.db"),
		opts: BootstrapOptions{HFOptions: []hfclient.ClientOption{hfclient.WithBaseURL(hf.URL())}},
	}
	r.cfg = cfgFrom(t, r.dbPath, map[string]string{"HF2S3_MASTER_KEY": master})
	r.start()
	return r
}

func (r *restoreRig) start() {
	r.t.Helper()
	app, err := Bootstrap(r.ctx, r.cfg, r.opts)
	if err != nil {
		r.t.Fatalf("bootstrap: %v", err)
	}
	r.app = app
	r.m = newRestoreManager(app)
	r.t.Cleanup(func() { _ = app.Close() }) // Windows cannot delete an open database
}

// addData creates a bucket, an account and one object; it returns the payload.
func (r *restoreRig) addData(bucket, key string) []byte {
	r.t.Helper()
	if err := r.app.DB.CreateBucket(r.ctx, bucket); err != nil {
		r.t.Fatal(err)
	}
	accounts, _ := r.app.DB.ListAccounts(r.ctx)
	if len(accounts) == 0 {
		if err := r.app.DB.CreateAccount(r.ctx, newAccount()); err != nil {
			r.t.Fatal(err)
		}
	}
	payload := bytes.Repeat([]byte(key+" "), 60)
	if _, err := r.app.Pool.PutObject(r.ctx, bucket, key, "text/plain", bytes.NewReader(payload), int64(len(payload)), nil); err != nil {
		r.t.Fatal(err)
	}
	return payload
}

func (r *restoreRig) read(bucket, key string) ([]byte, error) {
	_, rc, err := r.app.Pool.GetObject(r.ctx, bucket, key, nil)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

// snapshot writes a plain backup and returns its path.
func (r *restoreRig) snapshot(name string) string {
	r.t.Helper()
	path := filepath.Join(r.dir, name)
	if err := r.app.DB.BackupToFile(r.ctx, path); err != nil {
		r.t.Fatal(err)
	}
	return path
}

// upload copies a file into the manager's upload directory, as the HTTP handler does.
func (r *restoreRig) upload(src string) string {
	r.t.Helper()
	data, err := os.ReadFile(src)
	if err != nil {
		r.t.Fatal(err)
	}
	f, err := os.CreateTemp(r.m.UploadDir(), "restore-upload-*.tmp")
	if err != nil {
		r.t.Fatal(err)
	}
	_, _ = f.Write(data)
	f.Close()
	return f.Name()
}

func (r *restoreRig) leftovers() []string {
	entries, _ := os.ReadDir(r.dir)
	var out []string
	for _, e := range entries {
		n := e.Name()
		if strings.HasPrefix(n, "restore-upload-") || n == stagingName || strings.HasSuffix(n, pendingSuffix) || strings.HasSuffix(n, pendingMetaSuffix) {
			out = append(out, n)
		}
	}
	return out
}

func (r *restoreRig) mustRejectWith(err error, want string) {
	r.t.Helper()
	var re *dashboard.RestoreError
	if !errors.As(err, &re) {
		r.t.Fatalf("expected a user-facing RestoreError, got %v", err)
	}
	if !strings.Contains(strings.ToLower(re.Message), strings.ToLower(want)) {
		r.t.Fatalf("error %q should mention %q", re.Message, want)
	}
	if r.m.Pending() != nil {
		r.t.Fatal("a rejected upload must not leave a pending restore")
	}
	if l := r.leftovers(); len(l) != 0 {
		r.t.Fatalf("a rejected upload left files behind: %v", l)
	}
}

func TestRestoreFlowEndToEnd(t *testing.T) {
	hf := hftest.New(t)
	master := newMasterKey(t)

	// Deployment A holds the data we want back.
	a := newRig(t, hf, master)
	payload := a.addData("docs", "report.txt")
	snap := a.snapshot("snap.db")
	a.app.Close()

	// Deployment B (same master key, separate database) has different content.
	b := newRig(t, hf, master)
	b.addData("other", "only-in-b.txt")

	summary, err := b.m.Stage(b.ctx, b.upload(snap), "snap.db")
	if err != nil {
		t.Fatalf("Stage: %v", err)
	}
	if summary.Objects != 1 || summary.Accounts != 1 || summary.CurrentObjects != 1 || summary.Encrypted {
		t.Fatalf("unexpected summary: %+v", summary)
	}
	if p := b.m.Pending(); p == nil || p.Filename != "snap.db" {
		t.Fatalf("Pending() = %+v", p)
	}
	if _, err := os.Stat(b.dbPath + pendingSuffix); err != nil {
		t.Fatalf("staged file missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(b.dir, stagingName)); err == nil {
		t.Fatal("the working copy must not remain after staging")
	}

	// Staging changes nothing in the running database.
	if ok, _ := b.app.DB.BucketExists(b.ctx, "docs"); ok {
		t.Fatal("staging must not touch the live database")
	}
	if ok, _ := b.app.DB.BucketExists(b.ctx, "other"); !ok {
		t.Fatal("live data disappeared while only staging")
	}

	// Confirm: the process is asked to restart and stops accepting new work.
	if err := b.m.Apply(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-b.app.restartCh:
	default:
		t.Fatal("Apply must request a restart")
	}
	if !b.app.draining.Load() {
		t.Fatal("the app must drain before restarting")
	}
	b.app.Close()

	// What start-up does after the restart.
	applied, previous, err := applyPendingRestore(b.dbPath)
	if err != nil || !applied || previous == "" {
		t.Fatalf("applyPendingRestore = %v %q %v", applied, previous, err)
	}
	if _, err := os.Stat(previous); err != nil {
		t.Fatalf("the replaced database must be kept: %v", err)
	}
	if l := b.leftovers(); len(l) != 0 {
		t.Fatalf("pending files not cleaned: %v", l)
	}

	b.start()
	defer b.app.Close()
	if ok, _ := b.app.DB.BucketExists(b.ctx, "docs"); !ok {
		t.Fatal("restored bucket missing")
	}
	if ok, _ := b.app.DB.BucketExists(b.ctx, "other"); ok {
		t.Fatal("the previous content must be gone from the live database")
	}
	got, err := b.read("docs", "report.txt")
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("restored object unreadable: %v", err)
	}
	if res := b.m.LastResult(); res == nil || !res.OK || res.PreviousBackup == "" {
		t.Fatalf("LastResult = %+v", res)
	}
	if b.m.Pending() != nil {
		t.Fatal("nothing should be pending after applying")
	}

	// The replaced database is a usable backup of the state before the restore.
	info, err := db.InspectBackup(b.ctx, previous)
	if err != nil || info.Buckets != 1 || info.Objects != 1 {
		t.Fatalf("the kept database is not the pre-restore state: %+v %v", info, err)
	}
}

func TestRestoreStageAcceptsTheEncryptedBackupFromTheBucket(t *testing.T) {
	hf := hftest.New(t)
	master := newMasterKey(t)
	a := newRig(t, hf, master)
	a.addData("docs", "a.txt")
	plain, _ := os.ReadFile(a.snapshot("snap.db"))
	blob, err := backup.Encrypt(a.app.Keyring, plain)
	if err != nil {
		t.Fatal(err)
	}
	enc := filepath.Join(a.dir, "snap.db.enc")
	_ = os.WriteFile(enc, blob, 0o600)
	a.app.Close()

	b := newRig(t, hf, master)
	summary, err := b.m.Stage(b.ctx, b.upload(enc), "snap.db.enc")
	if err != nil {
		t.Fatalf("Stage encrypted: %v", err)
	}
	if !summary.Encrypted || summary.Objects != 1 {
		t.Fatalf("unexpected summary: %+v", summary)
	}
}

func TestRestoreStageRejectsWhatCannotWork(t *testing.T) {
	hf := hftest.New(t)
	target := newRig(t, hf, "")
	target.addData("keepme", "live.txt")

	stage := func(path, name string) error {
		_, err := target.m.Stage(target.ctx, target.upload(path), name)
		return err
	}

	t.Run("not a database", func(t *testing.T) {
		junk := filepath.Join(target.dir, "junk.db")
		_ = os.WriteFile(junk, []byte("this is not sqlite"), 0o600)
		target.mustRejectWith(stage(junk, "junk.db"), "no es una base de datos de HF2S3")
	})

	t.Run("a backup sealed under another master key", func(t *testing.T) {
		other := newRig(t, hf, "") // different random master key
		other.addData("docs", "x.txt")
		snap := other.snapshot("other.db")
		other.app.Close()
		target.mustRejectWith(stage(snap, "other.db"), "no es compatible")
	})

	t.Run("an encrypted backup from another master key", func(t *testing.T) {
		other := newRig(t, hf, "")
		other.addData("docs", "x.txt")
		plain, _ := os.ReadFile(other.snapshot("o2.db"))
		blob, _ := backup.Encrypt(other.app.Keyring, plain)
		enc := filepath.Join(other.dir, "o2.db.enc")
		_ = os.WriteFile(enc, blob, 0o600)
		other.app.Close()
		target.mustRejectWith(stage(enc, "o2.db.enc"), "descifrar")
	})

	t.Run("a database from a newer release", func(t *testing.T) {
		snap := target.snapshot("future.db")
		raw, _ := sql.Open("sqlite", snap)
		_, _ = raw.Exec(fmt.Sprintf(`PRAGMA user_version = %d`, db.SchemaVersion()+4))
		raw.Close()
		target.mustRejectWith(stage(snap, "future.db"), "versión más nueva")
	})

	t.Run("a truncated database", func(t *testing.T) {
		snap := target.snapshot("cut.db")
		data, _ := os.ReadFile(snap)
		_ = os.WriteFile(snap, data[:len(data)/3], 0o600)
		if err := stage(snap, "cut.db"); err == nil {
			t.Fatal("a truncated database must be refused")
		} else {
			target.mustRejectWith(err, "dañada")
		}
	})

	// After all the rejections the live database is untouched and still works.
	if ok, _ := target.app.DB.BucketExists(target.ctx, "keepme"); !ok {
		t.Fatal("live data was affected by rejected uploads")
	}
	if _, err := target.read("keepme", "live.txt"); err != nil {
		t.Fatalf("live object unreadable after rejections: %v", err)
	}
}

func TestRestoreOfABackupFromThePreviousRelease(t *testing.T) {
	hf := hftest.New(t)
	// A database as the previous release left it: plaintext master key and tokens,
	// default credentials, data encrypted with the old passphrase.
	legacyDir := t.TempDir()
	legacyPath := filepath.Join(legacyDir, "old.db")
	plain := seedLegacyDeployment(t, legacyPath, hf)

	target := newRig(t, hf, "")
	summary, err := target.m.Stage(target.ctx, target.upload(legacyPath), "old.db")
	if err != nil {
		t.Fatalf("staging a previous-release backup: %v", err)
	}
	joined := strings.Join(summary.Warnings, " | ")
	if !strings.Contains(joined, "versión anterior") {
		t.Fatalf("the previous-release backup must be flagged: %v", summary.Warnings)
	}
	if !strings.Contains(joined, "rekey") {
		t.Fatalf("the warnings must tell the admin to re-key: %v", summary.Warnings)
	}

	_ = target.m.Apply()
	target.app.Close()
	if applied, _, err := applyPendingRestore(target.dbPath); err != nil || !applied {
		t.Fatalf("apply: %v %v", applied, err)
	}
	target.start()
	defer target.app.Close()

	got, err := target.read("videos", "holiday.mp4")
	if err != nil || !bytes.Equal(got, plain) {
		t.Fatalf("data from the old backup must be readable through the adopted legacy key: %v", err)
	}
	// The plaintext master key from the old backup must not survive in the live database.
	raw, _ := sql.Open("sqlite", target.dbPath)
	defer raw.Close()
	var n int
	_ = raw.QueryRow(`SELECT COUNT(*) FROM system_settings WHERE key = 'master_key'`).Scan(&n)
	if n != 0 {
		t.Fatal("the old plaintext master key must be removed after restoring")
	}
}

func TestRestoreCancelRemovesThePendingCopy(t *testing.T) {
	hf := hftest.New(t)
	master := newMasterKey(t)
	a := newRig(t, hf, master)
	a.addData("docs", "a.txt")
	snap := a.snapshot("s.db")
	a.app.Close()

	b := newRig(t, hf, master)
	if _, err := b.m.Stage(b.ctx, b.upload(snap), "s.db"); err != nil {
		t.Fatal(err)
	}
	if err := b.m.Cancel(); err != nil {
		t.Fatal(err)
	}
	if b.m.Pending() != nil || len(b.leftovers()) != 0 {
		t.Fatalf("cancel left state behind: %v", b.leftovers())
	}
	if err := b.m.Apply(); err == nil {
		t.Fatal("Apply without a pending restore must fail")
	}
	if applied, _, _ := applyPendingRestore(b.dbPath); applied {
		t.Fatal("a cancelled restore must never be applied at start-up")
	}
}

// If a restored database passes validation but still fails to start, start-up
// puts the previous database back.
func TestRollbackPutsThePreviousDatabaseBack(t *testing.T) {
	hf := hftest.New(t)
	master := newMasterKey(t)
	a := newRig(t, hf, master)
	a.addData("docs", "a.txt")
	snap := a.snapshot("s.db")
	a.app.Close()

	b := newRig(t, hf, master)
	b.addData("mine", "m.txt")
	if _, err := b.m.Stage(b.ctx, b.upload(snap), "s.db"); err != nil {
		t.Fatal(err)
	}
	b.app.Close()

	applied, previous, err := applyPendingRestore(b.dbPath)
	if err != nil || !applied {
		t.Fatal(err)
	}
	if err := rollbackRestore(b.dbPath, previous, errors.New("boom")); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	b.start()
	defer b.app.Close()
	if ok, _ := b.app.DB.BucketExists(b.ctx, "mine"); !ok {
		t.Fatal("the previous database was not put back")
	}
	if ok, _ := b.app.DB.BucketExists(b.ctx, "docs"); ok {
		t.Fatal("the rejected database must not be live")
	}
	res := b.m.LastResult()
	if res == nil || res.OK || !strings.Contains(res.Message, "volvió automáticamente") {
		t.Fatalf("the rollback must be reported: %+v", res)
	}
	if rejected, _ := filepath.Glob(b.dbPath + ".rejected-*"); len(rejected) == 0 {
		t.Fatal("the rejected database should be kept for inspection")
	}
}

func TestOnlyTheNewestReplacedDatabasesAreKept(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "meta.db")
	for i := 0; i < 6; i++ {
		_ = os.WriteFile(dbPath, []byte(fmt.Sprintf("db %d", i)), 0o600)
		_ = os.WriteFile(dbPath+pendingSuffix, []byte(fmt.Sprintf("restored %d", i)), 0o600)
		if applied, _, err := applyPendingRestore(dbPath); err != nil || !applied {
			t.Fatalf("round %d: %v %v", i, applied, err)
		}
		time.Sleep(3 * time.Millisecond) // distinct millisecond stamps
	}
	kept, _ := filepath.Glob(dbPath + ".pre-restore-*")
	if len(kept) != keepPreRestore {
		t.Fatalf("kept %d replaced databases, want %d: %v", len(kept), keepPreRestore, kept)
	}
	if b, _ := os.ReadFile(dbPath); string(b) != "restored 5" {
		t.Fatalf("live database = %q", b)
	}
}

func TestDrainingRefusesNewWorkButAnswersProbes(t *testing.T) {
	r := newRig(t, nil, "")
	defer r.app.Close()
	h := r.app.router()

	get := func(path string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		return rec
	}
	if get("/api/ready").Code != http.StatusOK {
		t.Fatal("precondition: ready")
	}
	r.app.RequestRestart()

	if rec := get("/api/ready"); rec.Code != http.StatusOK {
		t.Fatalf("probes must keep answering while draining, got %d", rec.Code)
	}
	for _, path := range []string{"/some-bucket/some-key", "/api/stats", "/"} {
		rec := get(path)
		if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") == "" {
			t.Errorf("GET %s while draining = %d, want 503 with Retry-After", path, rec.Code)
		}
	}
}

func TestRunReturnsErrRestartWhenARestartIsRequested(t *testing.T) {
	cfg := cfgFrom(t, filepath.Join(t.TempDir(), "meta.db"), map[string]string{"PORT": "18091", "HF2S3_BACKUP_INTERVAL_HOURS": "0"})
	app, err := Bootstrap(context.Background(), cfg, BootstrapOptions{})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- app.Run(context.Background()) }()

	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := http.Get("http://127.0.0.1:18091/api/health")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("server never came up: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}

	app.RequestRestart()
	select {
	case err := <-done:
		if !errors.Is(err, ErrRestart) {
			t.Fatalf("Run returned %v, want ErrRestart", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after a restart was requested")
	}
	if _, err := http.Get("http://127.0.0.1:18091/api/health"); err == nil {
		t.Fatal("the port must be released before re-executing")
	}
}
