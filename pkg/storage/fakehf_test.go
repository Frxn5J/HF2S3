package storage

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"hf2s3/pkg/crypto"
	"hf2s3/pkg/db"
	"hf2s3/pkg/hfclient"
	"hf2s3/pkg/models"
)

// fakeHF is an in-memory Hugging Face Hub (LFS + commit + resolve) that counts
// what the gateway does to it.
type fakeHF struct {
	t  *testing.T
	ts *httptest.Server

	mu        sync.Mutex
	lfs       map[string][]byte // oid -> blob
	files     map[string][]byte // "repo/path" -> blob (committed files)
	commits   map[string]int    // token -> commit count (uploads and deletes)
	downloads map[string]int    // "repo/path" -> GET count

	// failCommitFor makes commits by these tokens fail with HTTP 500.
	failCommitFor map[string]bool
	// failDeletes makes delete commits fail with HTTP 500.
	failDeletes bool
}

func newFakeHF(t *testing.T) *fakeHF {
	t.Helper()
	f := &fakeHF{
		t:             t,
		lfs:           map[string][]byte{},
		files:         map[string][]byte{},
		commits:       map[string]int{},
		downloads:     map[string]int{},
		failCommitFor: map[string]bool{},
	}
	f.ts = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.ts.Close)
	return f
}

func repoOf(path, prefix string) string {
	rest := strings.TrimPrefix(path, prefix)
	parts := strings.SplitN(rest, "/", 3)
	if len(parts) < 2 {
		return ""
	}
	return parts[0] + "/" + parts[1]
}

func (f *fakeHF) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")

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
		isDelete := len(payload.DeletedEntries) > 0
		if f.failCommitFor[token] || (isDelete && f.failDeletes) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"boom"}`))
			return
		}
		repo := repoOf(r.URL.Path, "/api/datasets/")
		for _, lf := range payload.LfsFiles {
			f.files[repo+"/"+lf.Path] = f.lfs[lf.Oid]
		}
		for _, d := range payload.DeletedEntries {
			delete(f.files, repo+"/"+d.Path)
		}
		f.commits[token]++
		w.WriteHeader(http.StatusOK)

	case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/resolve/main/"):
		repo := repoOf(r.URL.Path, "/datasets/")
		key := repo + "/" + r.URL.Path[strings.Index(r.URL.Path, "/resolve/main/")+len("/resolve/main/"):]
		f.downloads[key]++
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

func (f *fakeHF) fileCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.files)
}

func (f *fakeHF) totalCommits() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.commits {
		n += c
	}
	return n
}

func (f *fakeHF) commitsFor(token string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.commits[token]
}

// fileKeys lists committed "repo/path" keys.
func (f *fakeHF) fileKeys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for k := range f.files {
		out = append(out, k)
	}
	return out
}

func (f *fakeHF) maxDownloadsOfAnyChunk() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	max := 0
	for _, n := range f.downloads {
		if n > max {
			max = n
		}
	}
	return max
}

type testEnv struct {
	pool     *PoolManager
	db       *db.DB
	hf       *fakeHF
	accounts []*models.Account
}

// newTestEnv builds a pool over nAccounts accounts (tokens tok1, tok2, ...).
func newTestEnv(t *testing.T, chunkSize int64, nAccounts int) *testEnv {
	t.Helper()
	database, err := db.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })

	hf := newFakeHF(t)
	client := hfclient.NewClient(hfclient.WithBaseURL(hf.ts.URL))

	ctx := context.Background()
	env := &testEnv{db: database, hf: hf}
	for i := 1; i <= nAccounts; i++ {
		acc := &models.Account{
			Name: "acc" + string(rune('0'+i)), Username: "u", Token: "tok" + string(rune('0'+i)),
			RepoName: "user/repo" + string(rune('0'+i)), IsActive: true,
		}
		if err := database.CreateAccount(ctx, acc); err != nil {
			t.Fatal(err)
		}
		env.accounts = append(env.accounts, acc)
	}
	if err := database.CreateBucket(ctx, "bkt"); err != nil {
		t.Fatal(err)
	}

	kr, err := crypto.NewKeyring(bytes32(), []string{"old-passphrase"})
	if err != nil {
		t.Fatal(err)
	}
	env.pool = NewPoolManagerWithKeyring(database, client, kr, chunkSize)
	t.Cleanup(func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5e9)
		defer cancel()
		_ = env.pool.Shutdown(sctx)
	})
	return env
}

func bytes32() []byte {
	b := make([]byte, 32)
	for i := range b {
		b[i] = byte(i*7 + 3)
	}
	return b
}

func pattern(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}
