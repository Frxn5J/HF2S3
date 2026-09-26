package hfclient

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestVerifyToken(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer hf_valid_test_token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		resp := WhoamiResponse{
			Name:     "hugginguser",
			Type:     "user",
			Email:    "user@example.com",
			Fullname: "Hugging Face User",
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer ts.Close()

	client := NewClient(WithBaseURL(ts.URL))
	ctx := context.Background()

	whoami, err := client.VerifyToken(ctx, "hf_valid_test_token")
	if err != nil {
		t.Fatalf("VerifyToken failed: %v", err)
	}
	if whoami.Name != "hugginguser" {
		t.Fatalf("Expected username hugginguser, got %s", whoami.Name)
	}

	_, err = client.VerifyToken(ctx, "hf_invalid")
	if err == nil {
		t.Fatalf("Expected error for invalid token, got nil")
	}
}

func TestEnsureDatasetRepo(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/repos/create" {
			var body map[string]interface{}
			_ = json.NewDecoder(r.Body).Decode(&body)
			if body["name"] == "existing-repo" {
				w.WriteHeader(http.StatusConflict)
				return
			}
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	client := NewClient(WithBaseURL(ts.URL))
	ctx := context.Background()

	err := client.EnsureDatasetRepo(ctx, "hf_tok", "new-repo")
	if err != nil {
		t.Fatalf("EnsureDatasetRepo failed for new repo: %v", err)
	}

	err = client.EnsureDatasetRepo(ctx, "hf_tok", "existing-repo")
	if err != nil {
		t.Fatalf("EnsureDatasetRepo should tolerate existing repo (409): %v", err)
	}
}

func TestUploadAndDownloadChunkWithLFS(t *testing.T) {
	storedLFSBlobs := make(map[string][]byte)
	committedLFSFiles := make(map[string]string) // path -> oid

	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		// 1. LFS Batch endpoint
		case r.Method == http.MethodPost && r.URL.Path == "/datasets/myuser/myrepo.git/info/lfs/objects/batch":
			var batchReq LfsBatchRequest
			if err := json.NewDecoder(r.Body).Decode(&batchReq); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}

			resp := LfsBatchResponse{
				Transfer: "basic",
				Objects: []LfsObjectResp{
					{
						Oid:  batchReq.Objects[0].Oid,
						Size: batchReq.Objects[0].Size,
						Actions: map[string]LfsAction{
							"upload": {
								Href: ts.URL + "/lfs-upload/" + batchReq.Objects[0].Oid,
							},
						},
					},
				},
			}
			w.Header().Set("Content-Type", "application/vnd.git-lfs+json")
			_ = json.NewEncoder(w).Encode(resp)

		// 2. Storage PUT endpoint
		case r.Method == http.MethodPut && len(r.URL.Path) > len("/lfs-upload/"):
			oid := r.URL.Path[len("/lfs-upload/"):]
			data, _ := io.ReadAll(r.Body)
			storedLFSBlobs[oid] = data
			w.WriteHeader(http.StatusOK)

		// 3. Commit endpoint with lfsFiles
		case r.Method == http.MethodPost && r.URL.Path == "/api/datasets/myuser/myrepo/commit/main":
			var payload struct {
				Summary  string          `json:"summary"`
				LfsFiles []CommitLfsFile `json:"lfsFiles"`
			}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			for _, f := range payload.LfsFiles {
				committedLFSFiles[f.Path] = f.Oid
			}
			w.WriteHeader(http.StatusOK)

		// 4. Resolve download endpoint
		case r.Method == http.MethodGet && r.URL.Path == "/datasets/myuser/myrepo/resolve/main/data/chunk_1.bin":
			oid, ok := committedLFSFiles["data/chunk_1.bin"]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			data, ok := storedLFSBlobs[oid]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(data)

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	client := NewClient(WithBaseURL(ts.URL))
	ctx := context.Background()

	chunkPayload := []byte("Encrypted binary data blob for chunk 1")
	err := client.UploadChunk(ctx, "hf_tok", "myuser/myrepo", "data/chunk_1.bin", chunkPayload)
	if err != nil {
		t.Fatalf("UploadChunk failed: %v", err)
	}

	downloaded, err := client.DownloadChunk(ctx, "hf_tok", "myuser/myrepo", "data/chunk_1.bin")
	if err != nil {
		t.Fatalf("DownloadChunk failed: %v", err)
	}
	if string(downloaded) != string(chunkPayload) {
		t.Fatalf("Downloaded chunk mismatch. Got: %s", string(downloaded))
	}
}
