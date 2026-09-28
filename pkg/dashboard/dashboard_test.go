package dashboard

import (
	"bytes"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"hf2s3/pkg/crypto"
	"hf2s3/pkg/db"
	"hf2s3/pkg/hfclient"
	"hf2s3/pkg/hfclient/hftest"
	"hf2s3/pkg/models"
	"hf2s3/pkg/s3api"
	"hf2s3/pkg/storage"
)

func init() {
	passwordIterations = 1000 // keep PBKDF2 fast in tests
}

const testAdminPassword = "correct-horse-battery"

type dashEnv struct {
	h      *DashboardHandler
	mux    *http.ServeMux
	root   http.Handler // with security headers
	token  string
	pool   *storage.PoolManager
	db     *db.DB
	hf     *hftest.Fake
	auth   *AdminAuthManager
	secret string
}

func setupTestDashboard(t *testing.T) *dashEnv {
	t.Helper()
	database, err := db.Open(":memory:")
	if err != nil {
		t.Fatalf("Open test db failed: %v", err)
	}
	t.Cleanup(func() { database.Close() })

	hf := hftest.New(t)
	client := hfclient.NewClient(hfclient.WithBaseURL(hf.URL()))
	kr := crypto.NewKeyringFromLegacyKey(crypto.DeriveKey("dashboard-test-key"))
	pool := storage.NewPoolManagerWithKeyring(database, client, kr, 1024*1024)
	t.Cleanup(func() {
		_ = pool.Shutdown(t.Context())
	})

	settings := &models.SystemSettings{
		AccessKeyID:     "dash-key",
		SecretAccessKey: "dash-secret-dash-secret-dash-secret",
		ChunkSizeMB:     32,
		S3Region:        "us-east-1",
	}

	adminAuth := NewAdminAuthManager("admin", testAdminPassword)
	handler := NewDashboardHandler(pool, settings, 8080, adminAuth)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	env := &dashEnv{h: handler, mux: mux, root: handler.Handler(), pool: pool, db: database, hf: hf, auth: adminAuth, secret: settings.SecretAccessKey}
	env.token = env.login(t, "admin", testAdminPassword, "203.0.113.5:1234")
	return env
}

func (e *dashEnv) login(t *testing.T, user, pass, remote string) string {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"username": user, "password": pass})
	req := httptest.NewRequest(http.MethodPost, "/api/auth/login", bytes.NewReader(body))
	req.RemoteAddr = remote
	rec := httptest.NewRecorder()
	e.mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("login failed: %d %s", rec.Code, rec.Body.String())
	}
	for _, c := range rec.Result().Cookies() {
		if c.Name == sessionCookieName {
			return c.Value
		}
	}
	t.Fatal("login did not set the session cookie")
	return ""
}

func (e *dashEnv) do(method, target string, body []byte, mutate ...func(*http.Request)) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, bytes.NewReader(body))
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: e.token})
	for _, m := range mutate {
		m(req)
	}
	rec := httptest.NewRecorder()
	e.mux.ServeHTTP(rec, req)
	return rec
}

func (e *dashEnv) json(t *testing.T, method, target string, payload any) *httptest.ResponseRecorder {
	t.Helper()
	b, _ := json.Marshal(payload)
	return e.do(method, target, b)
}

func decode[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.NewDecoder(rec.Body).Decode(&v); err != nil {
		t.Fatalf("decode: %v (body %q)", err, rec.Body.String())
	}
	return v
}

func TestDashboardAuthenticationFlow(t *testing.T) {
	env := setupTestDashboard(t)

	// Unauthenticated access to a protected endpoint.
	rec := httptest.NewRecorder()
	env.mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/stats", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without a session, got %d", rec.Code)
	}

	// Wrong password.
	body, _ := json.Marshal(map[string]string{"username": "admin", "password": "wrong"})
	rec = httptest.NewRecorder()
	env.mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/auth/login", bytes.NewReader(body)))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for a bad password, got %d", rec.Code)
	}

	// Good login: HttpOnly + SameSite=Strict cookie, and no token in the body.
	body, _ = json.Marshal(map[string]string{"username": "admin", "password": testAdminPassword})
	rec = httptest.NewRecorder()
	env.mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/auth/login", bytes.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var cookie *http.Cookie
	for _, c := range rec.Result().Cookies() {
		if c.Name == sessionCookieName {
			cookie = c
		}
	}
	if cookie == nil || !cookie.HttpOnly || cookie.SameSite != http.SameSiteStrictMode || cookie.Path != "/" {
		t.Fatalf("session cookie must be HttpOnly, SameSite=Strict, Path=/: %+v", cookie)
	}
	if strings.Contains(rec.Body.String(), cookie.Value) {
		t.Fatal("the session token must never appear in the response body (JavaScript-readable)")
	}

	// The cookie authorises; check endpoint reports it.
	rec = env.do(http.MethodGet, "/api/auth/check", nil)
	if got := decode[map[string]any](t, rec); got["authenticated"] != true {
		t.Fatalf("auth check: %v", got)
	}

	// Header, bearer and query-string tokens are no longer accepted.
	for name, mutate := range map[string]func(*http.Request){
		"X-Admin-Token": func(r *http.Request) { r.Header.Set("X-Admin-Token", env.token) },
		"Bearer":        func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+env.token) },
		"query":         func(r *http.Request) { r.URL.RawQuery = "token=" + env.token },
	} {
		req := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
		mutate(req)
		rec := httptest.NewRecorder()
		env.mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("%s token must not authenticate, got %d", name, rec.Code)
		}
	}

	// Logout revokes the session server-side.
	req := httptest.NewRequest(http.MethodPost, "/api/auth/logout", nil)
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: env.token})
	env.mux.ServeHTTP(httptest.NewRecorder(), req)
	if rec := env.do(http.MethodGet, "/api/stats", nil); rec.Code != http.StatusUnauthorized {
		t.Fatalf("a logged-out session must be rejected, got %d", rec.Code)
	}
}

func TestLoginIsRateLimited(t *testing.T) {
	env := setupTestDashboard(t)
	attempt := func(pass, remote string) *httptest.ResponseRecorder {
		body, _ := json.Marshal(map[string]string{"username": "admin", "password": pass})
		req := httptest.NewRequest(http.MethodPost, "/api/auth/login", bytes.NewReader(body))
		req.RemoteAddr = remote
		rec := httptest.NewRecorder()
		env.mux.ServeHTTP(rec, req)
		return rec
	}

	for i := 0; i < 5; i++ {
		if rec := attempt("nope", "198.51.100.7:4000"); rec.Code != http.StatusUnauthorized {
			t.Fatalf("attempt %d = %d", i, rec.Code)
		}
	}
	rec := attempt("nope", "198.51.100.7:4000")
	if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("6th failure must be throttled with Retry-After, got %d", rec.Code)
	}
	// Even the right password is refused while throttled...
	if rec := attempt(testAdminPassword, "198.51.100.7:4000"); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("throttled client with the right password = %d", rec.Code)
	}
	// ...but other addresses are unaffected.
	if rec := attempt(testAdminPassword, "198.51.100.99:4000"); rec.Code != http.StatusOK {
		t.Fatalf("another client must still be able to log in, got %d", rec.Code)
	}
}

func TestCrossSiteWritesAreRefused(t *testing.T) {
	env := setupTestDashboard(t)
	payload := []byte(`{"name":"csrf-bucket"}`)

	rec := env.do(http.MethodPost, "/api/buckets", payload, func(r *http.Request) { r.Header.Set("Origin", "https://evil.example") })
	if rec.Code != http.StatusForbidden {
		t.Fatalf("cross-origin POST = %d, want 403", rec.Code)
	}
	rec = env.do(http.MethodPost, "/api/buckets", payload, func(r *http.Request) { r.Header.Set("Sec-Fetch-Site", "cross-site") })
	if rec.Code != http.StatusForbidden {
		t.Fatalf("cross-site fetch metadata = %d, want 403", rec.Code)
	}
	rec = env.do(http.MethodPost, "/api/buckets", payload, func(r *http.Request) { r.Header.Set("Origin", "http://example.com") }) // httptest host
	if rec.Code != http.StatusCreated {
		t.Fatalf("same-origin POST = %d, want 201: %s", rec.Code, rec.Body.String())
	}
}

func TestPasswordIsStoredHashedAndVerifies(t *testing.T) {
	h, err := HashPassword("s3cret-value")
	if err != nil {
		t.Fatal(err)
	}
	if !IsPasswordHash(h) || strings.Contains(h, "s3cret-value") {
		t.Fatalf("not a hash: %s", h)
	}
	if !VerifyPassword(h, "s3cret-value") || VerifyPassword(h, "s3cret-valuE") || VerifyPassword("plain", "plain") {
		t.Fatal("verification broken")
	}
	h2, _ := HashPassword("s3cret-value")
	if h == h2 {
		t.Fatal("hashes must be salted")
	}
	if a := NewAdminAuthManager("u", h); a.PasswordHash() != h {
		t.Fatal("a stored hash must be adopted as-is, not re-hashed")
	}
}

func TestDashboardDatabaseBackupDownload(t *testing.T) {
	env := setupTestDashboard(t)

	rec := env.do(http.MethodGet, "/api/admin/backup", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("backup = %d %s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Content-Type") != "application/vnd.sqlite3" || rec.Body.Len() == 0 {
		t.Fatalf("unexpected backup response: %s (%d bytes)", rec.Header().Get("Content-Type"), rec.Body.Len())
	}
	if !bytes.HasPrefix(rec.Body.Bytes(), []byte("SQLite format 3")) {
		t.Fatal("backup is not a SQLite file")
	}

	// The in-place restore endpoint was removed (it swapped the live connection).
	rec = env.do(http.MethodPost, "/api/admin/restore", []byte("x"))
	if rec.Code == http.StatusOK {
		t.Fatal("hot restore must not exist any more")
	}
}

func TestDashboardStatsAndBucketsAPI(t *testing.T) {
	env := setupTestDashboard(t)

	rec := env.do(http.MethodGet, "/api/stats", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("stats = %d", rec.Code)
	}
	if stats := decode[models.PoolStats](t, rec); stats.TotalBuckets != 0 {
		t.Fatalf("expected 0 buckets, got %d", stats.TotalBuckets)
	}

	if rec := env.json(t, http.MethodPost, "/api/buckets", map[string]string{"name": "production-data"}); rec.Code != http.StatusCreated {
		t.Fatalf("create bucket = %d %s", rec.Code, rec.Body.String())
	}
	for _, bad := range []string{"media", "api", "AB", "x"} {
		if rec := env.json(t, http.MethodPost, "/api/buckets", map[string]string{"name": bad}); rec.Code != http.StatusBadRequest {
			t.Errorf("bucket %q accepted (%d)", bad, rec.Code)
		}
	}

	rec = env.do(http.MethodGet, "/api/buckets", nil)
	if buckets := decode[[]map[string]any](t, rec); len(buckets) != 1 || buckets[0]["object_count"] != float64(0) {
		t.Fatalf("buckets = %v", buckets)
	}
}

func TestSettingsNeverExposeSecrets(t *testing.T) {
	env := setupTestDashboard(t)
	rec := env.do(http.MethodGet, "/api/settings", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("settings = %d", rec.Code)
	}
	raw := rec.Body.String()
	for _, secret := range []string{env.secret, "master_key", testAdminPassword} {
		if strings.Contains(raw, secret) {
			t.Fatalf("settings response leaks %q: %s", secret, raw)
		}
	}
	got := decode[map[string]any](t, rec)
	if got["secret_access_key_set"] != true || got["access_key_id"] != "dash-key" {
		t.Fatalf("unexpected settings: %v", got)
	}
	snips := got["snippets"].(map[string]any)
	for name, s := range snips {
		if !strings.Contains(s.(string), "<SECRET_ACCESS_KEY>") {
			t.Errorf("snippet %s must use a placeholder for the secret", name)
		}
	}
}

func TestUpdateSettingsRules(t *testing.T) {
	env := setupTestDashboard(t)

	var gotKey, gotSecret string
	env.h.SetCredentialsUpdater(func(k, s string) {
		if k != "" {
			gotKey = k
		}
		if s != "" {
			gotSecret = s
		}
	})

	// Changing the password needs the current one and a minimum length.
	if rec := env.json(t, http.MethodPost, "/api/settings", map[string]any{"admin_password": "another-long-pass"}); rec.Code != http.StatusForbidden {
		t.Fatalf("password change without the current password = %d, want 403", rec.Code)
	}
	if rec := env.json(t, http.MethodPost, "/api/settings", map[string]any{"admin_password": "short", "current_password": testAdminPassword}); rec.Code != http.StatusBadRequest {
		t.Fatalf("weak password = %d, want 400", rec.Code)
	}

	rec := env.json(t, http.MethodPost, "/api/settings", map[string]any{
		"admin_username":        "superadmin",
		"admin_password":        "brand-new-password-1",
		"current_password":      testAdminPassword,
		"access_key_id":         "new-s3-key",
		"s3_region":             "eu-west-1",
		"chunk_size_mb":         64,
		"hf_storage_bucket":     "my-cache",
		"hf_storage_endpoint":   "https://s3.hf.co",
		"hf_storage_access_key": "HFAKnew",
		"hf_storage_secret_key": "storage-secret",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("update = %d %s", rec.Code, rec.Body.String())
	}
	if gotKey != "new-s3-key" {
		t.Fatalf("live credentials updater not invoked: %q", gotKey)
	}

	// The stored admin password is a hash, never the plaintext.
	stored, _ := env.db.GetSetting(t.Context(), "admin_password")
	if !IsPasswordHash(stored) || strings.Contains(stored, "brand-new") {
		t.Fatalf("admin password persisted as %q", stored)
	}
	// The stored secrets are readable by the app but no secret is echoed back.
	if v, _ := env.db.GetSetting(t.Context(), "hf_storage_secret_key"); v != "storage-secret" {
		t.Fatalf("hf storage secret not persisted: %q", v)
	}

	// The password change revoked the old session; the new credentials work.
	if rec := env.do(http.MethodGet, "/api/stats", nil); rec.Code != http.StatusUnauthorized {
		t.Fatalf("old session survived a password change: %d", rec.Code)
	}
	env.token = env.login(t, "superadmin", "brand-new-password-1", "203.0.113.9:1")

	rec = env.do(http.MethodGet, "/api/settings", nil)
	if strings.Contains(rec.Body.String(), "storage-secret") {
		t.Fatal("cache storage secret leaked by GET /api/settings")
	}
	got := decode[map[string]any](t, rec)
	if got["s3_region"] != "eu-west-1" || got["chunk_size_mb"] != float64(64) || got["hf_storage_bucket"] != "my-cache" {
		t.Fatalf("settings not applied: %v", got)
	}

	// Rotating the S3 secret returns the new value once and applies it live.
	rec = env.json(t, http.MethodPost, "/api/settings/rotate-s3-secret", map[string]any{})
	if rec.Code != http.StatusOK {
		t.Fatalf("rotate = %d", rec.Code)
	}
	rot := decode[map[string]string](t, rec)
	if len(rot["secret_access_key"]) < 40 || rot["secret_access_key"] != gotSecret {
		t.Fatalf("rotated secret not applied to the live credentials: %v vs %q", rot, gotSecret)
	}
	if v, _ := env.db.GetSetting(t.Context(), "secret_access_key"); v != rot["secret_access_key"] {
		t.Fatal("rotated secret not persisted")
	}
}

func TestEnvManagedSettingsAreReadOnly(t *testing.T) {
	env := setupTestDashboard(t)
	env.h.SetEnvManaged("access_key_id", "secret_access_key", "admin_password")

	if rec := env.json(t, http.MethodPost, "/api/settings", map[string]any{"access_key_id": "hijack"}); rec.Code != http.StatusConflict {
		t.Fatalf("editing an env-managed setting = %d, want 409", rec.Code)
	}
	if rec := env.json(t, http.MethodPost, "/api/settings/rotate-s3-secret", map[string]any{}); rec.Code != http.StatusConflict {
		t.Fatalf("rotating an env-managed secret = %d, want 409", rec.Code)
	}
	got := decode[map[string]any](t, env.do(http.MethodGet, "/api/settings", nil))
	managed := got["env_managed"].(map[string]any)
	if managed["access_key_id"] != true {
		t.Fatalf("env_managed not reported: %v", managed)
	}
}

func TestStaticAssetsAndSecurityHeaders(t *testing.T) {
	env := setupTestDashboard(t)

	rec := httptest.NewRecorder()
	env.root.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusOK || !bytes.Contains(rec.Body.Bytes(), []byte("HF2S3")) {
		t.Fatalf("index = %d", rec.Code)
	}
	csp := rec.Header().Get("Content-Security-Policy")
	for _, want := range []string{"default-src 'self'", "script-src 'self'", "frame-ancestors 'none'", "object-src 'none'"} {
		if !strings.Contains(csp, want) {
			t.Errorf("CSP lacks %q: %s", want, csp)
		}
	}
	if strings.Contains(csp, "script-src 'self' 'unsafe") || strings.Contains(strings.Split(csp, "style-src")[0], "unsafe-inline") {
		t.Errorf("scripts must not allow unsafe-inline: %s", csp)
	}
	if rec.Header().Get("X-Content-Type-Options") != "nosniff" || rec.Header().Get("X-Frame-Options") != "DENY" {
		t.Error("missing nosniff / frame protection")
	}

	// The page must not carry inline scripts or handlers, or the CSP would break it.
	page := rec.Body.String()
	for _, banned := range []string{"onclick=", "onchange=", "onsubmit=", "javascript:"} {
		if strings.Contains(page, banned) {
			t.Errorf("index.html uses %s, incompatible with the CSP", banned)
		}
	}
	if strings.Contains(strings.ReplaceAll(page, `<script src=`, ""), "<script>") {
		t.Error("index.html has an inline <script>")
	}

	rec = httptest.NewRecorder()
	env.root.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/style.css", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("style.css = %d", rec.Code)
	}
}

func TestStaticScriptDoesNotStoreTokensOrTrustHTML(t *testing.T) {
	js := readStatic(t, "app.js")
	for _, banned := range []string{"localStorage.setItem('hf2s3_admin_token'", "X-Admin-Token", "'Bearer '"} {
		if strings.Contains(js, banned) {
			t.Errorf("app.js still handles a JavaScript-readable session token: %s", banned)
		}
	}
	if !strings.Contains(js, "escapeHtml") {
		t.Error("app.js must escape dynamic values before writing HTML")
	}
}

func readStatic(t *testing.T, name string) string {
	t.Helper()
	b, err := staticFS.ReadFile("static/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestUpdateAccountDetails(t *testing.T) {
	env := setupTestDashboard(t)
	ctx := t.Context()
	acc := &models.Account{
		Name:       "Original Name",
		Username:   "testuser",
		Token:      "hf_sampletoken_12345678",
		RepoName:   "testuser/original-repo",
		QuotaBytes: 50 * 1024 * 1024 * 1024,
		IsActive:   true,
	}
	if err := env.db.CreateAccount(ctx, acc); err != nil {
		t.Fatalf("Failed to create test account: %v", err)
	}

	rec := env.json(t, http.MethodPut, "/api/accounts/1", map[string]any{
		"name": "Renamed Account", "repo_name": "testuser/original-repo", "quota_gb": 0, "is_active": false,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("update account = %d: %s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "hf_sampletoken_12345678") {
		t.Fatal("update response leaks the full token")
	}

	updated, _ := env.db.GetAccountByID(ctx, 1)
	if updated.Name != "Renamed Account" || updated.QuotaBytes != 0 || updated.IsActive {
		t.Fatalf("account not updated: %+v", updated)
	}

	rec = env.do(http.MethodGet, "/api/accounts", nil)
	list := decode[[]AccountResponse](t, rec)
	if len(list) != 1 || list[0].RateLimit.APILimit != 1000 {
		t.Fatalf("unexpected list: %+v", list)
	}
	if strings.Contains(rec.Body.String(), "hf_sampletoken_12345678") || list[0].Token != "hf_s...5678" {
		t.Fatalf("token not masked in the list: %q", list[0].Token)
	}
}

func TestAccountWithDataCannotBeDeleted(t *testing.T) {
	env := setupTestDashboard(t)
	ctx := t.Context()
	acc := &models.Account{Name: "a", Username: "u", Token: "tok", RepoName: "u/r", IsActive: true}
	_ = env.db.CreateAccount(ctx, acc)
	_ = env.db.CreateBucket(ctx, "bkt")

	// Store one object through the pool so the account owns a chunk.
	if _, err := env.pool.PutObject(ctx, "bkt", "k", "text/plain", strings.NewReader("data"), 4, nil); err != nil {
		t.Fatal(err)
	}
	rec := env.do(http.MethodDelete, "/api/accounts/1", nil)
	if rec.Code != http.StatusConflict {
		t.Fatalf("deleting an account that stores data = %d, want 409", rec.Code)
	}
}

func multipartBody(t *testing.T, fields [][2]string, fileName, fileType string, content []byte) (*bytes.Buffer, string) {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	for _, f := range fields {
		_ = mw.WriteField(f[0], f[1])
	}
	if fileName != "" {
		hdr := make(map[string][]string)
		hdr["Content-Disposition"] = []string{`form-data; name="file"; filename="` + fileName + `"`}
		hdr["Content-Type"] = []string{fileType}
		part, _ := mw.CreatePart(hdr)
		_, _ = part.Write(content)
	}
	_ = mw.Close()
	return &buf, mw.FormDataContentType()
}

func TestUploadStreamsAndDownloadIsAlwaysAnAttachment(t *testing.T) {
	env := setupTestDashboard(t)
	ctx := t.Context()
	_ = env.db.CreateAccount(ctx, &models.Account{Name: "a", Username: "u", Token: "tok", RepoName: "u/r", IsActive: true})
	_ = env.db.CreateBucket(ctx, "bkt")

	evil := []byte(`<html><script>fetch('/api/settings')</script></html>`)
	body, ctype := multipartBody(t, [][2]string{{"bucket", "bkt"}, {"key", "page.html"}}, "page.html", "text/html", evil)
	rec := env.do(http.MethodPost, "/api/objects/upload", body.Bytes(), func(r *http.Request) { r.Header.Set("Content-Type", ctype) })
	if rec.Code != http.StatusCreated {
		t.Fatalf("upload = %d %s", rec.Code, rec.Body.String())
	}

	// The bucket field must precede the file so the upload can stream.
	body, ctype = multipartBody(t, nil, "late.bin", "application/octet-stream", []byte("x"))
	rec = env.do(http.MethodPost, "/api/objects/upload", body.Bytes(), func(r *http.Request) { r.Header.Set("Content-Type", ctype) })
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("upload without a bucket = %d, want 400", rec.Code)
	}

	rec = env.do(http.MethodGet, "/api/objects/download?bucket=bkt&key="+url.QueryEscape("page.html"), nil)
	if rec.Code != http.StatusOK || !bytes.Equal(rec.Body.Bytes(), evil) {
		t.Fatalf("download = %d", rec.Code)
	}
	if rec.Header().Get("Content-Type") != "application/octet-stream" {
		t.Fatalf("uploaded content type must not be reflected: %s", rec.Header().Get("Content-Type"))
	}
	if !strings.HasPrefix(rec.Header().Get("Content-Disposition"), "attachment") ||
		rec.Header().Get("X-Content-Type-Options") != "nosniff" ||
		!strings.Contains(rec.Header().Get("Content-Security-Policy"), "sandbox") {
		t.Fatalf("download must be a sandboxed attachment: %v", rec.Header())
	}

	// A key with quotes/newlines cannot break out of the header.
	obj := &models.Object{Bucket: "bkt", Key: "a\"b\r\nX-Injected: 1.txt", Size: 0, ETag: `"e"`, ContentType: "x"}
	_ = env.db.SaveObjectWithChunks(ctx, obj, nil)
	rec = env.do(http.MethodGet, "/api/objects/download?bucket=bkt&key="+url.QueryEscape(obj.Key), nil)
	if strings.Contains(rec.Header().Get("Content-Disposition"), "\n") || rec.Header().Get("X-Injected") != "" {
		t.Fatalf("header injection through the object key: %q", rec.Header().Get("Content-Disposition"))
	}
}

func TestPresignedLinkWorksAgainstTheGateway(t *testing.T) {
	env := setupTestDashboard(t)
	ctx := t.Context()
	_ = env.db.CreateAccount(ctx, &models.Account{Name: "a", Username: "u", Token: "tok", RepoName: "u/r", IsActive: true})
	_ = env.db.CreateBucket(ctx, "vids")
	if _, err := env.pool.PutObject(ctx, "vids", "clip 1.mp4", "video/mp4", strings.NewReader("video-bytes"), 11, nil); err != nil {
		t.Fatal(err)
	}

	rec := env.json(t, http.MethodPost, "/api/objects/presign", map[string]any{"bucket": "vids", "key": "clip 1.mp4", "expires_seconds": 600})
	if rec.Code != http.StatusOK {
		t.Fatalf("presign = %d %s", rec.Code, rec.Body.String())
	}
	link := decode[map[string]string](t, rec)["url"]
	if !strings.Contains(link, "/media/vids/clip%201.mp4?") || strings.Contains(link, env.secret) {
		t.Fatalf("unexpected link: %s", link)
	}

	// The gateway (with the same credentials) must accept exactly this link.
	auth := s3api.NewAuthManager("dash-key", env.secret)
	srv := s3api.NewServer(env.pool, auth)
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, link, nil))
	if rec.Code != http.StatusOK || rec.Body.String() != "video-bytes" {
		t.Fatalf("gateway rejected the console's presigned link: %d %s", rec.Code, rec.Body.String())
	}

	// Tampering with the object in the link fails.
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, strings.Replace(link, "clip%201", "clip%202", 1), nil))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("a modified link must be rejected, got %d", rec.Code)
	}

	// The link's lifetime is capped.
	rec = env.json(t, http.MethodPost, "/api/objects/presign", map[string]any{"bucket": "vids", "key": "clip 1.mp4", "expires_seconds": 999999999})
	u, _ := url.Parse(decode[map[string]string](t, rec)["url"])
	if u.Query().Get("X-Amz-Expires") != "604800" {
		t.Fatalf("expiry not capped to 7 days: %s", u.Query().Get("X-Amz-Expires"))
	}
}

func TestMediaInfoRequiresAdmin(t *testing.T) {
	env := setupTestDashboard(t)
	rec := httptest.NewRecorder()
	env.mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/media/info?bucket=b&key=k", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("/api/media/info without a session = %d, want 401", rec.Code)
	}
}

func TestProbes(t *testing.T) {
	env := setupTestDashboard(t)
	for _, p := range []string{"/api/health", "/api/ready"} {
		rec := httptest.NewRecorder()
		env.mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, p, nil))
		if rec.Code != http.StatusOK {
			t.Errorf("%s = %d", p, rec.Code)
		}
	}
	_ = env.db.Close()
	rec := httptest.NewRecorder()
	env.mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/ready", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("readiness with a dead database = %d, want 503", rec.Code)
	}
}

func TestErrorsToClientsAreGeneric(t *testing.T) {
	env := setupTestDashboard(t)
	_ = env.db.Close()
	rec := env.do(http.MethodGet, "/api/accounts", nil)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rec.Code)
	}
	body := strings.ToLower(rec.Body.String())
	for _, needle := range []string{"sql", "closed", "database"} {
		if strings.Contains(body, needle) {
			t.Fatalf("internal detail %q reached the client: %s", needle, body)
		}
	}
}

var _ = time.Second
