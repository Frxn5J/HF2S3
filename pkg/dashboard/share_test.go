package dashboard

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"hf2s3/pkg/models"
)

func seedShareFixtures(t *testing.T, env *dashEnv) {
	t.Helper()
	ctx := t.Context()
	_ = env.db.CreateAccount(ctx, &models.Account{Name: "a", Username: "u", Token: "tok", RepoName: "u/r", IsActive: true})
	_ = env.db.CreateBucket(ctx, "media-b")
	for _, o := range []struct{ key, ctype, body string }{
		{"pelis/a.mp4", "video/mp4", "0123456789"},
		{"pelis/sub/b.txt", "text/plain", "bbb"},
		{"pelis/x.html", "text/html", "<script>alert(1)</script>"},
		{"otro/c.txt", "text/plain", "ccc"},
		{"top.txt", "text/plain", "top"},
	} {
		if _, err := env.pool.PutObject(ctx, "media-b", o.key, o.ctype, strings.NewReader(o.body), int64(len(o.body)), nil); err != nil {
			t.Fatal(err)
		}
	}
}

// publicDo sends a request without the admin session, like a visitor would.
func (e *dashEnv) publicDo(method, target string, body []byte, cookies ...*http.Cookie) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, bytes.NewReader(body))
	req.RemoteAddr = "198.51.100.7:4321"
	for _, c := range cookies {
		req.AddCookie(c)
	}
	rec := httptest.NewRecorder()
	e.mux.ServeHTTP(rec, req)
	return rec
}

func (e *dashEnv) createShare(t *testing.T, payload map[string]any) (token string, view map[string]any) {
	t.Helper()
	rec := e.json(t, http.MethodPost, "/api/shares", payload)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create share = %d %s", rec.Code, rec.Body.String())
	}
	view = decode[map[string]any](t, rec)
	url := view["url"].(string)
	i := strings.Index(url, "/s/")
	if i < 0 {
		t.Fatalf("share url = %q", url)
	}
	return url[i+3:], view
}

type listResp struct {
	Path    string
	Folders []shareFolderEntry
	Files   []shareFileEntry
	Next    string
}

func readZip(t *testing.T, rec *httptest.ResponseRecorder) map[string]string {
	t.Helper()
	zr, err := zip.NewReader(bytes.NewReader(rec.Body.Bytes()), int64(rec.Body.Len()))
	if err != nil {
		t.Fatalf("not a zip: %v", err)
	}
	got := map[string]string{}
	for _, f := range zr.File {
		rc, _ := f.Open()
		b, _ := io.ReadAll(rc)
		rc.Close()
		got[f.Name] = string(b)
	}
	return got
}

func TestShareFolderEndToEnd(t *testing.T) {
	env := setupTestDashboard(t)
	seedShareFixtures(t, env)

	token, view := env.createShare(t, map[string]any{
		"bucket": "media-b", "title": "Pelis", "expires_in_seconds": 3600,
		"items": []map[string]any{{"key": "pelis/", "is_prefix": true}},
	})
	if view["has_password"] != false || view["expired"] != false || len(token) < 20 {
		t.Fatalf("unexpected share view: %+v", view)
	}

	// The page itself is public HTML and must not be cached or indexed.
	rec := env.publicDo(http.MethodGet, "/s/"+token, nil)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "<html") || rec.Header().Get("Cache-Control") != "no-store" ||
		!strings.Contains(rec.Header().Get("X-Robots-Tag"), "noindex") || rec.Header().Get("Referrer-Policy") != "no-referrer" {
		t.Fatalf("share page = %d headers %v", rec.Code, rec.Header())
	}

	info := decode[map[string]any](t, env.publicDo(http.MethodGet, "/s/"+token+"/info", nil))
	if info["unlocked"] != true || info["root"] != "pelis/" || info["title"] != "Pelis" {
		t.Fatalf("info = %+v", info)
	}

	// A single shared folder opens directly inside it.
	list := decode[listResp](t, env.publicDo(http.MethodGet, "/s/"+token+"/list", nil))
	if list.Path != "pelis/" || len(list.Folders) != 1 || list.Folders[0].Path != "pelis/sub/" || len(list.Files) != 2 {
		t.Fatalf("list = %+v", list)
	}
	kinds := map[string]string{}
	for _, f := range list.Files {
		kinds[f.Name] = f.Kind
	}
	if kinds["a.mp4"] != "video" || kinds["x.html"] != "file" {
		t.Fatalf("kinds = %v (html must never be previewable)", kinds)
	}

	// Subfolders are reachable, other folders are not.
	if rec = env.publicDo(http.MethodGet, "/s/"+token+"/list?path=pelis/sub/", nil); rec.Code != http.StatusOK {
		t.Fatalf("subfolder = %d", rec.Code)
	}
	for _, p := range []string{"otro/", "pelis", "pel/"} {
		if rec = env.publicDo(http.MethodGet, "/s/"+token+"/list?path="+p, nil); rec.Code != http.StatusNotFound {
			t.Fatalf("list %q = %d, want 404", p, rec.Code)
		}
	}

	// Keys are literal: "../" does not climb out of the shared folder, it just matches nothing.
	dots := decode[listResp](t, env.publicDo(http.MethodGet, "/s/"+token+"/list?path=pelis/../otro/", nil))
	if len(dots.Files) != 0 || len(dots.Folders) != 0 {
		t.Fatalf("path with .. escaped the share: %+v", dots)
	}

	// A player can stream with Range.
	req := httptest.NewRequest(http.MethodGet, "/s/"+token+"/file?k=pelis/a.mp4", nil)
	req.Header.Set("Range", "bytes=2-5")
	rec = httptest.NewRecorder()
	env.mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusPartialContent || rec.Body.String() != "2345" || rec.Header().Get("Content-Range") != "bytes 2-5/10" ||
		rec.Header().Get("Content-Type") != "video/mp4" || !strings.HasPrefix(rec.Header().Get("Content-Disposition"), "inline") {
		t.Fatalf("range = %d %q %v", rec.Code, rec.Body.String(), rec.Header())
	}

	// dl=1 forces a download even for media.
	rec = env.publicDo(http.MethodGet, "/s/"+token+"/file?k=pelis/a.mp4&dl=1", nil)
	if rec.Code != http.StatusOK || rec.Body.String() != "0123456789" || !strings.HasPrefix(rec.Header().Get("Content-Disposition"), "attachment") ||
		rec.Header().Get("Content-Type") != "application/octet-stream" {
		t.Fatalf("download = %d %v", rec.Code, rec.Header())
	}

	// Stored HTML is never rendered on this origin.
	rec = env.publicDo(http.MethodGet, "/s/"+token+"/file?k=pelis/x.html", nil)
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != "application/octet-stream" ||
		!strings.HasPrefix(rec.Header().Get("Content-Disposition"), "attachment") || rec.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Fatalf("html must be an opaque attachment: %v", rec.Header())
	}

	// Objects outside the share stay out of reach, however the key is spelled.
	for _, k := range []string{"otro/c.txt", "top.txt", "pelis", "pelis/../top.txt", ""} {
		if rec = env.publicDo(http.MethodGet, "/s/"+token+"/file?k="+k, nil); rec.Code != http.StatusNotFound {
			t.Fatalf("file %q = %d, want 404", k, rec.Code)
		}
	}

	// Unsatisfiable ranges are refused.
	req = httptest.NewRequest(http.MethodGet, "/s/"+token+"/file?k=pelis/a.mp4", nil)
	req.Header.Set("Range", "bytes=50-60")
	rec = httptest.NewRecorder()
	env.mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("bad range = %d", rec.Code)
	}

	// The whole folder as a ZIP, with the folder as top level.
	rec = env.publicDo(http.MethodGet, "/s/"+token+"/zip", nil)
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != "application/zip" || !strings.Contains(rec.Header().Get("Content-Disposition"), "Pelis.zip") {
		t.Fatalf("zip = %d %v", rec.Code, rec.Header())
	}
	got := readZip(t, rec)
	if len(got) != 3 || got["pelis/a.mp4"] != "0123456789" || got["pelis/sub/b.txt"] != "bbb" {
		t.Fatalf("zip entries = %v", got)
	}

	// A subfolder zip contains just that folder.
	got = readZip(t, env.publicDo(http.MethodGet, "/s/"+token+"/zip?path=pelis/sub/", nil))
	if len(got) != 1 || got["sub/b.txt"] != "bbb" {
		t.Fatalf("subfolder zip = %v", got)
	}

	// Downloads are counted once per file/zip, not per range request.
	deadline := time.Now().Add(2 * time.Second)
	for {
		shares, _ := env.db.ListShares(t.Context())
		if len(shares) == 1 && shares[0].Downloads == 4 { // dl, html, zip, subfolder zip; the range read starts mid-file
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("downloads not counted as expected: %+v", shares)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Revoking kills the link at once.
	id := int64(view["id"].(float64))
	if rec = env.do(http.MethodDelete, fmt.Sprintf("/api/shares/%d", id), nil); rec.Code != http.StatusOK {
		t.Fatalf("revoke = %d", rec.Code)
	}
	if rec = env.publicDo(http.MethodGet, "/s/"+token+"/list", nil); rec.Code != http.StatusNotFound {
		t.Fatalf("revoked share = %d, want 404", rec.Code)
	}
}

func TestShareMixedSelection(t *testing.T) {
	env := setupTestDashboard(t)
	seedShareFixtures(t, env)
	token, _ := env.createShare(t, map[string]any{
		"bucket": "media-b",
		"items":  []map[string]any{{"key": "top.txt"}, {"key": "otro/", "is_prefix": true}},
	})

	list := decode[listResp](t, env.publicDo(http.MethodGet, "/s/"+token+"/list", nil))
	if list.Path != "" || len(list.Folders) != 1 || list.Folders[0].Name != "otro" || len(list.Files) != 1 || list.Files[0].Key != "top.txt" {
		t.Fatalf("virtual root = %+v", list)
	}
	if rec := env.publicDo(http.MethodGet, "/s/"+token+"/file?k=otro/c.txt", nil); rec.Body.String() != "ccc" {
		t.Fatalf("file inside shared folder = %d %q", rec.Code, rec.Body.String())
	}
	if rec := env.publicDo(http.MethodGet, "/s/"+token+"/file?k=pelis/a.mp4", nil); rec.Code != http.StatusNotFound {
		t.Fatalf("unshared file = %d, want 404", rec.Code)
	}

	got := readZip(t, env.publicDo(http.MethodGet, "/s/"+token+"/zip", nil))
	if len(got) != 2 || got["top.txt"] != "top" || got["otro/c.txt"] != "ccc" {
		t.Fatalf("zip of a mixed selection = %v", got)
	}
}

func TestSharePasswordAndRateLimit(t *testing.T) {
	env := setupTestDashboard(t)
	seedShareFixtures(t, env)
	token, view := env.createShare(t, map[string]any{
		"bucket": "media-b", "password": "secreto-123",
		"items": []map[string]any{{"key": "top.txt"}},
	})
	listing := env.do(http.MethodGet, "/api/shares", nil).Body.String()
	if view["has_password"] != true || strings.Contains(listing, "secreto-123") || strings.Contains(listing, "pbkdf2") {
		t.Fatalf("password material leaked in the console: %s", listing)
	}

	info := decode[map[string]any](t, env.publicDo(http.MethodGet, "/s/"+token+"/info", nil))
	if info["unlocked"] != false || info["has_password"] != true || info["title"] != nil {
		t.Fatalf("locked info must not reveal content: %+v", info)
	}
	for _, p := range []string{"/list", "/file?k=top.txt", "/zip"} {
		if rec := env.publicDo(http.MethodGet, "/s/"+token+p, nil); rec.Code != http.StatusUnauthorized {
			t.Fatalf("%s without password = %d, want 401", p, rec.Code)
		}
	}

	body := func(pw string) []byte { b, _ := json.Marshal(map[string]string{"password": pw}); return b }
	for i := 0; i < 5; i++ {
		if rec := env.publicDo(http.MethodPost, "/s/"+token+"/unlock", body("mal")); rec.Code != http.StatusForbidden {
			t.Fatalf("wrong password #%d = %d, want 403", i, rec.Code)
		}
	}
	// Too many failures: even the right password is refused for a while.
	rec := env.publicDo(http.MethodPost, "/s/"+token+"/unlock", body("secreto-123"))
	if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("after 5 failures = %d, want 429", rec.Code)
	}

	// Another visitor is not affected and gets a scoped, HttpOnly cookie.
	req := httptest.NewRequest(http.MethodPost, "/s/"+token+"/unlock", bytes.NewReader(body("secreto-123")))
	req.RemoteAddr = "203.0.113.99:1"
	rec = httptest.NewRecorder()
	env.mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unlock = %d %s", rec.Code, rec.Body.String())
	}
	var cookie *http.Cookie
	for _, c := range rec.Result().Cookies() {
		if c.Name == shareCookieName {
			cookie = c
		}
	}
	if cookie == nil || !cookie.HttpOnly || cookie.Path != "/s/"+token || cookie.MaxAge <= 0 {
		t.Fatalf("unlock cookie = %+v", cookie)
	}
	if rec = env.publicDo(http.MethodGet, "/s/"+token+"/file?k=top.txt", nil, cookie); rec.Code != http.StatusOK || rec.Body.String() != "top" {
		t.Fatalf("download with the cookie = %d %q", rec.Code, rec.Body.String())
	}
	if rec = env.publicDo(http.MethodGet, "/s/"+token+"/file?k=top.txt", nil, &http.Cookie{Name: shareCookieName, Value: "forged"}); rec.Code != http.StatusUnauthorized {
		t.Fatalf("forged cookie = %d, want 401", rec.Code)
	}

	// The cookie of one share is useless on another.
	other, _ := env.createShare(t, map[string]any{"bucket": "media-b", "password": "otra-clave-9", "items": []map[string]any{{"key": "top.txt"}}})
	if rec = env.publicDo(http.MethodGet, "/s/"+other+"/file?k=top.txt", nil, cookie); rec.Code != http.StatusUnauthorized {
		t.Fatalf("cookie reused on another share = %d, want 401", rec.Code)
	}

	// Revoking a share drops the sessions that unlocked it.
	id := int64(view["id"].(float64))
	env.do(http.MethodDelete, fmt.Sprintf("/api/shares/%d", id), nil)
	if env.h.shareSess.valid(cookie.Value, id, time.Now()) {
		t.Fatal("session survived the revocation of its share")
	}
}

func TestShareExpiry(t *testing.T) {
	env := setupTestDashboard(t)
	seedShareFixtures(t, env)

	past := time.Now().Add(-time.Minute).UTC()
	if err := env.db.CreateShare(t.Context(), &models.Share{Bucket: "media-b", ExpiresAt: &past, Items: []models.ShareItem{{Key: "top.txt"}}}, "expired-token"); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"/info", "/list", "/file?k=top.txt", "/zip"} {
		if rec := env.publicDo(http.MethodGet, "/s/expired-token"+p, nil); rec.Code != http.StatusGone {
			t.Fatalf("%s on an expired link = %d, want 410", p, rec.Code)
		}
	}
	// It still shows up for the admin, flagged as expired.
	var list []map[string]any
	_ = json.Unmarshal(env.do(http.MethodGet, "/api/shares", nil).Body.Bytes(), &list)
	if len(list) != 1 || list[0]["expired"] != true {
		t.Fatalf("admin list = %+v", list)
	}

	if rec := env.publicDo(http.MethodGet, "/s/no-such-token/info", nil); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown token = %d, want 404", rec.Code)
	}
}

func TestShareCreationIsValidated(t *testing.T) {
	env := setupTestDashboard(t)
	seedShareFixtures(t, env)
	item := func(k string, prefix bool) []map[string]any { return []map[string]any{{"key": k, "is_prefix": prefix}} }

	cases := []struct {
		name    string
		payload map[string]any
		want    int
	}{
		{"no items", map[string]any{"bucket": "media-b", "items": []any{}}, http.StatusBadRequest},
		{"unknown bucket", map[string]any{"bucket": "nope", "items": item("top.txt", false)}, http.StatusNotFound},
		{"missing file", map[string]any{"bucket": "media-b", "items": item("ghost.txt", false)}, http.StatusNotFound},
		{"folder without slash", map[string]any{"bucket": "media-b", "items": item("pelis", true)}, http.StatusBadRequest},
		{"empty folder", map[string]any{"bucket": "media-b", "items": item("nada/", true)}, http.StatusBadRequest},
		{"short password", map[string]any{"bucket": "media-b", "password": "123", "items": item("top.txt", false)}, http.StatusBadRequest},
		{"negative duration", map[string]any{"bucket": "media-b", "expires_in_seconds": -5, "items": item("top.txt", false)}, http.StatusBadRequest},
		{"too short", map[string]any{"bucket": "media-b", "expires_in_seconds": 5, "items": item("top.txt", false)}, http.StatusBadRequest},
		{"never expires", map[string]any{"bucket": "media-b", "expires_in_seconds": 0, "items": item("top.txt", false)}, http.StatusCreated},
	}
	for _, c := range cases {
		if rec := env.json(t, http.MethodPost, "/api/shares", c.payload); rec.Code != c.want {
			t.Errorf("%s: status %d, want %d (%s)", c.name, rec.Code, c.want, rec.Body.String())
		}
	}

	// Creating links needs the admin session.
	b, _ := json.Marshal(map[string]any{"bucket": "media-b", "items": item("top.txt", false)})
	if rec := env.publicDo(http.MethodPost, "/api/shares", b); rec.Code != http.StatusUnauthorized {
		t.Fatalf("anonymous create = %d, want 401", rec.Code)
	}
	if rec := env.publicDo(http.MethodGet, "/api/shares", nil); rec.Code != http.StatusUnauthorized {
		t.Fatalf("anonymous list = %d, want 401", rec.Code)
	}
}

func TestZipNamesStayInsideTheArchive(t *testing.T) {
	for in, want := range map[string]string{
		"a/b.txt":       "a/b.txt",
		"../../evil.sh": "evil.sh",
		"/abs/path.txt": "abs/path.txt",
		"a/./b//c.txt":  "a/b/c.txt",
		"..":            "file",
		"a/../../../x":  "a/x",
	} {
		if got := safeZipName(in); got != want {
			t.Errorf("safeZipName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestInlineTypeAllowlist(t *testing.T) {
	for _, c := range []struct{ ctype, key, want, kind string }{
		{"video/mp4", "a.bin", "video/mp4", "video"},
		{"VIDEO/MP4; codecs=avc1", "a", "video/mp4", "video"},
		{"application/octet-stream", "clip.webm", "video/webm", "video"},
		{"", "song.mp3", "audio/mpeg", "audio"},
		{"image/svg+xml", "logo.svg", "", ""},
		{"text/html", "page.html", "", ""},
		{"image/svg+xml", "x.png", "image/png", "image"}, // the extension wins only when the stored type is not allowed
		{"application/pdf", "doc.pdf", "", ""},
	} {
		got, kind := inlineType(c.ctype, c.key)
		if got != c.want || kind != c.kind {
			t.Errorf("inlineType(%q, %q) = %q, %q; want %q, %q", c.ctype, c.key, got, kind, c.want, c.kind)
		}
	}
}

func TestFolderListingForTheConsole(t *testing.T) {
	env := setupTestDashboard(t)
	seedShareFixtures(t, env)
	rec := env.do(http.MethodGet, "/api/folders?bucket=media-b", nil)
	page := decode[struct {
		Folders []string
		Objects []models.Object
		Next    string
	}](t, rec)
	if rec.Code != http.StatusOK || strings.Join(page.Folders, ",") != "otro/,pelis/" || len(page.Objects) != 1 || page.Objects[0].Key != "top.txt" {
		t.Fatalf("folders = %+v", page)
	}
}
