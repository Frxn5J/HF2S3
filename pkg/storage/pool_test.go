package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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

func setupTestStoragePool(t *testing.T, chunkSizeBytes int64) (*PoolManager, *db.DB, func()) {
	t.Helper()
	database, err := db.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open test db: %v", err)
	}

	var mu sync.Mutex
	fakeHFStore := make(map[string][]byte)

	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		switch {
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/info/lfs/objects/batch"):
			var batchReq hfclient.LfsBatchRequest
			_ = json.NewDecoder(r.Body).Decode(&batchReq)
			resp := hfclient.LfsBatchResponse{
				Transfer: "basic",
				Objects: []hfclient.LfsObjectResp{
					{
						Oid:  batchReq.Objects[0].Oid,
						Size: batchReq.Objects[0].Size,
						Actions: map[string]hfclient.LfsAction{
							"upload": {
								Href: ts.URL + "/lfs-upload/" + batchReq.Objects[0].Oid,
							},
						},
					},
				},
			}
			w.Header().Set("Content-Type", "application/vnd.git-lfs+json")
			_ = json.NewEncoder(w).Encode(resp)

		case r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/lfs-upload/"):
			oid := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
			data, _ := io.ReadAll(r.Body)
			fakeHFStore[oid] = data
			w.WriteHeader(http.StatusOK)

		case r.Method == http.MethodPost && (r.URL.Path == "/api/datasets/user1/repo1/commit/main" || r.URL.Path == "/api/datasets/user2/repo2/commit/main"):
			var payload struct {
				LfsFiles       []hfclient.CommitLfsFile `json:"lfsFiles"`
				DeletedEntries []struct {
					Path string `json:"path"`
				} `json:"deletedEntries"`
			}
			_ = json.NewDecoder(r.Body).Decode(&payload)
			for _, f := range payload.LfsFiles {
				fakeHFStore[f.Path] = fakeHFStore[f.Oid]
			}
			for _, d := range payload.DeletedEntries {
				delete(fakeHFStore, d.Path)
			}
			w.WriteHeader(http.StatusOK)

		case r.Method == http.MethodGet:
			// /datasets/{repo}/resolve/main/{path...}
			path := r.URL.Path
			prefix := "/datasets/"
			if len(path) > len(prefix) {
				sub := path[len(prefix):]
				// sub is user1/repo1/resolve/main/data/...
				parts := bytes.Split([]byte(sub), []byte("/resolve/main/"))
				if len(parts) == 2 {
					remotePath := string(parts[1])
					if data, ok := fakeHFStore[remotePath]; ok {
						w.WriteHeader(http.StatusOK)
						_, _ = w.Write(data)
						return
					}
				}
			}
			w.WriteHeader(http.StatusNotFound)

		default:
			w.WriteHeader(http.StatusOK)
		}
	}))

	client := hfclient.NewClient(hfclient.WithBaseURL(ts.URL))
	masterKey := crypto.DeriveKey("test-master-key")

	pool := NewPoolManager(database, client, masterKey, chunkSizeBytes)

	// Add 2 active accounts
	ctx := context.Background()
	_ = database.CreateBucket(ctx, "test-bucket")
	_ = database.CreateAccount(ctx, &models.Account{
		Name:       "HF Account 1",
		Username:   "user1",
		Token:      "hf_token1",
		RepoName:   "user1/repo1",
		QuotaBytes: 10 * 1024 * 1024,
		IsActive:   true,
	})
	_ = database.CreateAccount(ctx, &models.Account{
		Name:       "HF Account 2",
		Username:   "user2",
		Token:      "hf_token2",
		RepoName:   "user2/repo2",
		QuotaBytes: 10 * 1024 * 1024,
		IsActive:   true,
	})

	cleanup := func() {
		ts.Close()
		_ = database.Close()
	}

	return pool, database, cleanup
}

func TestPutAndGetObjectRoundTrip(t *testing.T) {
	// Chunk size 100 bytes to force multi-chunk distribution
	pool, _, cleanup := setupTestStoragePool(t, 100)
	defer cleanup()

	ctx := context.Background()
	content := bytes.Repeat([]byte("1234567890"), 35) // 350 bytes -> should create 4 chunks
	reader := bytes.NewReader(content)

	obj, err := pool.PutObject(ctx, "test-bucket", "documents/report.txt", "text/plain", reader, int64(len(content)), nil)
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	if obj.Size != int64(len(content)) {
		t.Fatalf("Expected size %d, got %d", len(content), obj.Size)
	}

	// Fetch object
	retrievedObj, readCloser, err := pool.GetObject(ctx, "test-bucket", "documents/report.txt", nil)
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer readCloser.Close()

	if retrievedObj.Key != "documents/report.txt" {
		t.Fatalf("Retrieved object key mismatch: %s", retrievedObj.Key)
	}

	fetchedData, err := io.ReadAll(readCloser)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if !bytes.Equal(fetchedData, content) {
		t.Fatalf("Retrieved content does not match original! Got len %d, expected %d", len(fetchedData), len(content))
	}
}

func TestGetObjectByteRange(t *testing.T) {
	pool, _, cleanup := setupTestStoragePool(t, 50)
	defer cleanup()

	ctx := context.Background()
	content := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz") // 62 bytes
	reader := bytes.NewReader(content)

	_, err := pool.PutObject(ctx, "test-bucket", "data/test.bin", "application/octet-stream", reader, int64(len(content)), nil)
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Request range bytes=10-25
	start := int64(10)
	end := int64(25)
	_, readCloser, err := pool.GetObject(ctx, "test-bucket", "data/test.bin", &ByteRange{Start: start, End: end})
	if err != nil {
		t.Fatalf("GetObject range failed: %v", err)
	}
	defer readCloser.Close()

	rangedData, err := io.ReadAll(readCloser)
	if err != nil {
		t.Fatalf("ReadAll range failed: %v", err)
	}

	expected := content[start : end+1]
	if !bytes.Equal(rangedData, expected) {
		t.Fatalf("Ranged data mismatch. Expected '%s', got '%s'", string(expected), string(rangedData))
	}
}

func TestDeleteObject(t *testing.T) {
	pool, database, cleanup := setupTestStoragePool(t, 100)
	defer cleanup()

	ctx := context.Background()
	content := []byte("Temporary data to delete")
	_, err := pool.PutObject(ctx, "test-bucket", "temp.txt", "text/plain", bytes.NewReader(content), int64(len(content)), nil)
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	err = pool.DeleteObject(ctx, "test-bucket", "temp.txt")
	if err != nil {
		t.Fatalf("DeleteObject failed: %v", err)
	}

	// Verify object gone
	_, _, err = pool.GetObject(ctx, "test-bucket", "temp.txt", nil)
	if err != db.ErrNotFound {
		t.Fatalf("Expected ErrNotFound after deletion, got %v", err)
	}

	stats, _ := database.GetStats(ctx)
	if stats.TotalObjects != 0 {
		t.Fatalf("Expected 0 objects in stats, got %d", stats.TotalObjects)
	}
}

func TestHighConcurrencyParallelUsersAndReads(t *testing.T) {
	// Chunk size 128 bytes to test multi-chunk distribution and prefetch pipeline under high concurrency
	pool, _, cleanup := setupTestStoragePool(t, 128)
	defer cleanup()

	ctx := context.Background()
	const numUsers = 15
	var wg sync.WaitGroup
	errCh := make(chan error, numUsers*2)

	// Phase 1: 15 concurrent users uploading unique files simultaneously
	for i := 0; i < numUsers; i++ {
		wg.Add(1)
		go func(userID int) {
			defer wg.Done()
			key := "users/user_" + strings.Repeat("A", userID+1) + "/data.txt"
			payload := bytes.Repeat([]byte("HighConcurrencyTestData_User_"), 20) // ~580 bytes -> ~5 chunks
			_, err := pool.PutObject(ctx, "test-bucket", key, "text/plain", bytes.NewReader(payload), int64(len(payload)), nil)
			if err != nil {
				errCh <- fmt.Errorf("user %d put object failed: %w", userID, err)
			}
		}(i)
	}

	wg.Wait()

	select {
	case err := <-errCh:
		t.Fatalf("Concurrent upload error: %v", err)
	default:
	}

	// Phase 2: 15 concurrent users reading their uploaded files simultaneously and verifying byte integrity
	for i := 0; i < numUsers; i++ {
		wg.Add(1)
		go func(userID int) {
			defer wg.Done()
			key := "users/user_" + strings.Repeat("A", userID+1) + "/data.txt"
			expected := bytes.Repeat([]byte("HighConcurrencyTestData_User_"), 20)

			obj, reader, err := pool.GetObject(ctx, "test-bucket", key, nil)
			if err != nil {
				errCh <- fmt.Errorf("user %d get object failed: %w", userID, err)
				return
			}
			defer reader.Close()

			if obj.Size != int64(len(expected)) {
				errCh <- fmt.Errorf("user %d size mismatch: expected %d, got %d", userID, len(expected), obj.Size)
				return
			}

			data, err := io.ReadAll(reader)
			if err != nil {
				errCh <- fmt.Errorf("user %d read all failed: %w", userID, err)
				return
			}

			if !bytes.Equal(data, expected) {
				errCh <- fmt.Errorf("user %d data mismatch", userID)
				return
			}
		}(i)
	}

	wg.Wait()

	select {
	case err := <-errCh:
		t.Fatalf("Concurrent download error: %v", err)
	default:
	}
}

func TestConcurrentMultipartUploads(t *testing.T) {
	pool, _, cleanup := setupTestStoragePool(t, 256)
	defer cleanup()

	ctx := context.Background()
	uploadID, err := pool.InitiateMultipartUpload(ctx, "test-bucket", "large-file.bin", "application/octet-stream")
	if err != nil {
		t.Fatalf("InitiateMultipartUpload failed: %v", err)
	}

	// Upload 4 parts concurrently
	const numParts = 4
	var wg sync.WaitGroup
	errCh := make(chan error, numParts)

	for p := 1; p <= numParts; p++ {
		wg.Add(1)
		go func(partNum int) {
			defer wg.Done()
			partData := bytes.Repeat([]byte{byte(partNum)}, 500)
			etag, err := pool.UploadPart(ctx, uploadID, partNum, bytes.NewReader(partData))
			if err != nil {
				errCh <- fmt.Errorf("upload part %d failed: %w", partNum, err)
				return
			}
			if etag == "" {
				errCh <- fmt.Errorf("upload part %d empty etag", partNum)
			}
		}(p)
	}

	wg.Wait()

	select {
	case err := <-errCh:
		t.Fatalf("Concurrent part upload error: %v", err)
	default:
	}

	obj, err := pool.CompleteMultipartUpload(ctx, uploadID)
	if err != nil {
		t.Fatalf("CompleteMultipartUpload failed: %v", err)
	}

	if obj.Size != numParts*500 {
		t.Fatalf("Expected completed size %d, got %d", numParts*500, obj.Size)
	}
}

