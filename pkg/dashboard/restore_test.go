package dashboard

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeRestore records what the handlers ask of the manager.
type fakeRestore struct {
	mu        sync.Mutex
	dir       string
	max       int64
	stageErr  error
	staged    []byte
	filename  string
	pending   *RestoreSummary
	applied   int
	cancelled int
	last      *RestoreResult
}

func (f *fakeRestore) UploadDir() string          { return f.dir }
func (f *fakeRestore) MaxUploadBytes() int64      { return f.max }
func (f *fakeRestore) LastResult() *RestoreResult { return f.last }

func (f *fakeRestore) Stage(_ context.Context, path, filename string) (*RestoreSummary, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, _ := os.ReadFile(path)
	_ = os.Remove(path) // the manager owns the upload
	if f.stageErr != nil {
		return nil, f.stageErr
	}
	f.staged, f.filename = data, filename
	f.pending = &RestoreSummary{Filename: filename, StagedAt: time.Now(), Objects: 7, Accounts: 2}
	return f.pending, nil
}

func (f *fakeRestore) Pending() *RestoreSummary {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pending
}

func (f *fakeRestore) Cancel() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancelled++
	f.pending = nil
	return nil
}

func (f *fakeRestore) Apply() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applied++
	return nil
}

func restoreEnv(t *testing.T) (*dashEnv, *fakeRestore) {
	t.Helper()
	env := setupTestDashboard(t)
	fr := &fakeRestore{dir: t.TempDir(), max: 1 << 20}
	env.h.SetRestoreManager(fr)
	return env, fr
}

// restoreBody builds a multipart body whose file field is named "database".
func restoreBody(t *testing.T, name string, content []byte) ([]byte, string) {
	t.Helper()
	body, ctype := multipartBody(t, nil, name, "application/octet-stream", content)
	return bytes.Replace(body.Bytes(), []byte(`name="file"`), []byte(`name="database"`), 1), ctype
}

func uploadBackup(t *testing.T, env *dashEnv, name string, content []byte, mutate ...func(*http.Request)) *httptest.ResponseRecorder {
	t.Helper()
	raw, ctype := restoreBody(t, name, content)
	all := append([]func(*http.Request){func(r *http.Request) { r.Header.Set("Content-Type", ctype) }}, mutate...)
	return env.do(http.MethodPost, "/api/admin/restore", raw, all...)
}

func TestRestoreUploadStagesTheFileWithoutApplyingIt(t *testing.T) {
	env, fr := restoreEnv(t)

	rec := uploadBackup(t, env, "backup.db", []byte("SQLite format 3 pretend"))
	if rec.Code != http.StatusOK {
		t.Fatalf("upload = %d %s", rec.Code, rec.Body.String())
	}
	if got := decode[RestoreSummary](t, rec); got.Objects != 7 || got.Filename != "backup.db" {
		t.Fatalf("unexpected summary: %+v", got)
	}
	if string(fr.staged) != "SQLite format 3 pretend" {
		t.Fatalf("the manager received %q", fr.staged)
	}
	if fr.applied != 0 {
		t.Fatal("uploading must never apply the restore")
	}
	if entries, _ := os.ReadDir(fr.dir); len(entries) != 0 {
		t.Fatalf("upload leftovers: %v", entries)
	}

	rec = env.do(http.MethodGet, "/api/admin/restore", nil)
	st := decode[map[string]any](t, rec)
	if st["pending"] == nil || st["max_upload_mb"] != float64(1) {
		t.Fatalf("status = %v", st)
	}
}

func TestRestoreUploadReportsValidationErrorsAndHidesInternalOnes(t *testing.T) {
	env, fr := restoreEnv(t)

	fr.stageErr = &RestoreError{Message: "La copia no es compatible con la configuración actual: clave maestra distinta"}
	rec := uploadBackup(t, env, "x.db", []byte("data"))
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "clave maestra distinta") {
		t.Fatalf("validation error = %d %s", rec.Code, rec.Body.String())
	}

	fr.stageErr = errors.New("open /data/restore-staging.db: permission denied (internal path)")
	rec = uploadBackup(t, env, "x.db", []byte("data"))
	if rec.Code != http.StatusInternalServerError || strings.Contains(rec.Body.String(), "/data/") || strings.Contains(rec.Body.String(), "permission") {
		t.Fatalf("internal error leaked: %d %s", rec.Code, rec.Body.String())
	}
}

func TestRestoreUploadLimitsAndPreconditions(t *testing.T) {
	env, fr := restoreEnv(t)

	big := bytes.Repeat([]byte("x"), int(fr.max)+100)
	if rec := uploadBackup(t, env, "big.db", big); rec.Code == http.StatusOK {
		t.Fatalf("a file over the limit must be refused, got %d", rec.Code)
	}
	if fr.pending != nil {
		t.Fatal("an oversized upload must not be staged")
	}
	if entries, _ := os.ReadDir(fr.dir); len(entries) != 0 {
		t.Fatalf("oversized upload left files behind: %v", entries)
	}

	// No "database" part at all.
	noFile := "--x\r\nContent-Disposition: form-data; name=\"other\"\r\n\r\nv\r\n--x--\r\n"
	rec := env.do(http.MethodPost, "/api/admin/restore", []byte(noFile),
		func(r *http.Request) { r.Header.Set("Content-Type", "multipart/form-data; boundary=x") })
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("no file = %d", rec.Code)
	}

	// Requires an admin session and a same-origin request.
	raw, ctype := restoreBody(t, "b.db", []byte("x"))
	req := httptest.NewRequest(http.MethodPost, "/api/admin/restore", bytes.NewReader(raw))
	req.Header.Set("Content-Type", ctype)
	anon := httptest.NewRecorder()
	env.mux.ServeHTTP(anon, req)
	if anon.Code != http.StatusUnauthorized {
		t.Fatalf("anonymous upload = %d, want 401", anon.Code)
	}
	cross := uploadBackup(t, env, "b.db", []byte("x"), func(r *http.Request) { r.Header.Set("Origin", "https://evil.example") })
	if cross.Code != http.StatusForbidden {
		t.Fatalf("cross-site upload = %d, want 403", cross.Code)
	}
}

func TestRestoreApplyNeedsPasswordAndAPendingCopy(t *testing.T) {
	env, fr := restoreEnv(t)

	rec := env.json(t, http.MethodPost, "/api/admin/restore/apply", map[string]string{"current_password": testAdminPassword})
	if rec.Code != http.StatusConflict {
		t.Fatalf("apply without a pending copy = %d, want 409", rec.Code)
	}

	uploadBackup(t, env, "b.db", []byte("data"))

	rec = env.json(t, http.MethodPost, "/api/admin/restore/apply", map[string]string{"current_password": "wrong"})
	if rec.Code != http.StatusForbidden || fr.applied != 0 {
		t.Fatalf("apply with a wrong password = %d (applied %d), want 403 and no restart", rec.Code, fr.applied)
	}
	rec = env.json(t, http.MethodPost, "/api/admin/restore/apply", map[string]string{})
	if rec.Code != http.StatusForbidden || fr.applied != 0 {
		t.Fatalf("apply without a password = %d", rec.Code)
	}

	rec = env.json(t, http.MethodPost, "/api/admin/restore/apply", map[string]string{"current_password": testAdminPassword})
	if rec.Code != http.StatusAccepted || fr.applied != 1 {
		t.Fatalf("confirmed apply = %d (applied %d)", rec.Code, fr.applied)
	}
	if got := decode[map[string]any](t, rec); got["restarting"] != true {
		t.Fatalf("response = %v", got)
	}
}

func TestRestoreCancel(t *testing.T) {
	env, fr := restoreEnv(t)
	uploadBackup(t, env, "b.db", []byte("data"))
	rec := env.do(http.MethodDelete, "/api/admin/restore", nil)
	if rec.Code != http.StatusOK || fr.cancelled != 1 || fr.pending != nil {
		t.Fatalf("cancel = %d cancelled=%d", rec.Code, fr.cancelled)
	}
}

func TestRestoreUnavailableWithoutAManager(t *testing.T) {
	env := setupTestDashboard(t) // no manager installed
	cases := []struct{ method, path string }{
		{http.MethodGet, "/api/admin/restore"}, {http.MethodPost, "/api/admin/restore"},
		{http.MethodPost, "/api/admin/restore/apply"}, {http.MethodDelete, "/api/admin/restore"},
	}
	for _, c := range cases {
		if rec := env.do(c.method, c.path, []byte("{}")); rec.Code != http.StatusNotImplemented {
			t.Errorf("%s %s = %d, want 501", c.method, c.path, rec.Code)
		}
	}
}

func TestRestoreLastResultIsReported(t *testing.T) {
	env, fr := restoreEnv(t)
	fr.last = &RestoreResult{At: time.Now(), OK: false, Message: "se volvió a la base anterior"}
	st := decode[map[string]any](t, env.do(http.MethodGet, "/api/admin/restore", nil))
	lr, _ := st["last_result"].(map[string]any)
	if lr == nil || lr["ok"] != false || !strings.Contains(lr["message"].(string), "anterior") {
		t.Fatalf("last result not reported: %v", st)
	}
}

// The restore UI shows file names and server messages that are untrusted: it must
// use textContent, never innerHTML.
func TestRestoreUIDoesNotInjectHTML(t *testing.T) {
	js := readStatic(t, "app.js")
	start := strings.Index(js, "// --- DATABASE RESTORE")
	end := strings.Index(js, "// Global Refresh")
	if start < 0 || end < start {
		t.Fatal("restore block not found in app.js")
	}
	block := js[start:end]
	for _, banned := range []string{"innerHTML", "insertAdjacentHTML", "outerHTML", "document.write"} {
		if strings.Contains(block, banned) {
			t.Errorf("the restore UI uses %s with untrusted data", banned)
		}
	}
	if !strings.Contains(block, "textContent") || !strings.Contains(block, "current_password") {
		t.Error("the restore UI must render with textContent and ask for the admin password")
	}
	page := readStatic(t, "index.html")
	if !strings.Contains(page, `id="btnApplyRestore"`) || !strings.Contains(page, `id="restoreConfirmPass"`) {
		t.Error("index.html lacks the restore confirmation controls")
	}
}
