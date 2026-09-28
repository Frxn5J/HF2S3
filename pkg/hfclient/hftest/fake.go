// Package hftest provides an in-memory fake of the Hugging Face Hub endpoints
// HF2S3 talks to (Git LFS batch/upload, commit, resolve). It exists for tests.
package hftest

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"

	"hf2s3/pkg/hfclient"
)

type Fake struct {
	ts *httptest.Server

	mu      sync.Mutex
	lfs     map[string][]byte // oid -> blob
	files   map[string][]byte // "owner/repo/path" -> blob
	commits int
}

func New(t testing.TB) *Fake {
	t.Helper()
	f := &Fake{lfs: map[string][]byte{}, files: map[string][]byte{}}
	f.ts = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.ts.Close)
	return f
}

// URL is the base URL to give to hfclient.WithBaseURL.
func (f *Fake) URL() string { return f.ts.URL }

// Files returns the committed "owner/repo/path" keys, sorted.
func (f *Fake) Files() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.files))
	for k := range f.files {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func (f *Fake) FileCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.files)
}

func (f *Fake) Commits() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.commits
}

func repoOf(path, prefix string) string {
	parts := strings.SplitN(strings.TrimPrefix(path, prefix), "/", 3)
	if len(parts) < 2 {
		return ""
	}
	return parts[0] + "/" + parts[1]
}

func (f *Fake) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	switch {
	case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/info/lfs/objects/batch"):
		var req hfclient.LfsBatchRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		resp := hfclient.LfsBatchResponse{Transfer: "basic"}
		for _, o := range req.Objects {
			resp.Objects = append(resp.Objects, hfclient.LfsObjectResp{
				Oid: o.Oid, Size: o.Size,
				Actions: map[string]hfclient.LfsAction{"upload": {Href: f.ts.URL + "/lfs-upload/" + o.Oid}},
			})
		}
		w.Header().Set("Content-Type", "application/vnd.git-lfs+json")
		_ = json.NewEncoder(w).Encode(resp)

	case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/lfs-upload/"):
		data, _ := io.ReadAll(r.Body)
		f.lfs[strings.TrimPrefix(r.URL.Path, "/lfs-upload/")] = data
		w.WriteHeader(http.StatusOK)

	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/commit/main"):
		var payload struct {
			LfsFiles       []hfclient.CommitLfsFile `json:"lfsFiles"`
			DeletedEntries []struct {
				Path string `json:"path"`
			} `json:"deletedEntries"`
		}
		_ = json.NewDecoder(r.Body).Decode(&payload)
		repo := repoOf(r.URL.Path, "/api/datasets/")
		for _, lf := range payload.LfsFiles {
			f.files[repo+"/"+lf.Path] = f.lfs[lf.Oid]
		}
		for _, d := range payload.DeletedEntries {
			delete(f.files, repo+"/"+d.Path)
		}
		f.commits++
		w.WriteHeader(http.StatusOK)

	case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/resolve/main/"):
		repo := repoOf(r.URL.Path, "/datasets/")
		key := repo + "/" + r.URL.Path[strings.Index(r.URL.Path, "/resolve/main/")+len("/resolve/main/"):]
		data, ok := f.files[key]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write(data)

	default:
		w.WriteHeader(http.StatusOK)
	}
}

// Put stores a file as if it had been committed to the repository, so tests can
// seed data written by older releases. path is "owner/repo/relative/path".
func (f *Fake) Put(path string, data []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.files[path] = append([]byte(nil), data...)
}

// Get returns a committed file.
func (f *Fake) Get(path string) ([]byte, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	d, ok := f.files[path]
	return d, ok
}
