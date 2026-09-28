package s3api

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"hf2s3/pkg/crypto"
	"hf2s3/pkg/db"
	"hf2s3/pkg/hfclient"
	"hf2s3/pkg/hfstorage"
	"hf2s3/pkg/models"
	"hf2s3/pkg/sigv4"
	"hf2s3/pkg/storage"
)

func setupTestS3Server(t *testing.T) (*Server, func()) {
	t.Helper()
	database, err := db.Open(":memory:")
	if err != nil {
		t.Fatalf("Open test db failed: %v", err)
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
			return

		case r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/lfs-upload/"):
			oid := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
			data, _ := io.ReadAll(r.Body)
			fakeHFStore[oid] = data
			w.WriteHeader(http.StatusOK)
			return

		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/commit/main"):
			var payload struct {
				LfsFiles []hfclient.CommitLfsFile `json:"lfsFiles"`
			}
			_ = json.NewDecoder(r.Body).Decode(&payload)
			for _, f := range payload.LfsFiles {
				fakeHFStore[f.Path] = fakeHFStore[f.Oid]
			}
			w.WriteHeader(http.StatusOK)
			return

		case r.Method == http.MethodGet:
			for path, data := range fakeHFStore {
				if bytes.HasSuffix([]byte(r.URL.Path), []byte(path)) {
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write(data)
					return
				}
			}
			w.WriteHeader(http.StatusNotFound)
			return

		default:
			w.WriteHeader(http.StatusOK)
		}
	}))

	client := hfclient.NewClient(hfclient.WithBaseURL(ts.URL))
	masterKey := crypto.DeriveKey("s3-test-key")
	pool := storage.NewPoolManager(database, client, masterKey, 1024*1024)

	ctx := context.Background()
	_ = database.CreateAccount(ctx, &models.Account{
		Name:       "Test Acc",
		Username:   "testuser",
		Token:      "hf_token",
		RepoName:   "testuser/vault",
		QuotaBytes: 100 * 1024 * 1024,
		IsActive:   true,
	})

	auth := NewAuthManager("test-access-key", "test-secret-key")
	server := NewServer(pool, auth)

	cleanup := func() {
		ts.Close()
		_ = database.Close()
	}

	return server, cleanup
}

func TestS3BucketAndObjectWorkflow(t *testing.T) {
	server, cleanup := setupTestS3Server(t)
	defer cleanup()

	// 1. Create Bucket
	req := httptest.NewRequest(http.MethodPut, "/my-bucket", nil)
	signTest(req)
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("CreateBucket failed with code %d: %s", rec.Code, rec.Body.String())
	}

	// 2. Put Object
	bodyContent := []byte("Hello S3 R2 compatible world with Hugging Face multi-account backend!")
	putReq := httptest.NewRequest(http.MethodPut, "/my-bucket/hello.txt", bytes.NewReader(bodyContent))
	signTest(putReq)
	putReq.Header.Set("Content-Type", "text/plain")
	putRec := httptest.NewRecorder()
	server.ServeHTTP(putRec, putReq)

	if putRec.Code != http.StatusOK {
		t.Fatalf("PutObject failed with code %d: %s", putRec.Code, putRec.Body.String())
	}
	etag := putRec.Header().Get("ETag")
	if etag == "" {
		t.Fatalf("Expected ETag header on PutObject response")
	}

	// 3. Head Object
	headReq := httptest.NewRequest(http.MethodHead, "/my-bucket/hello.txt", nil)
	signTest(headReq)
	headRec := httptest.NewRecorder()
	server.ServeHTTP(headRec, headReq)

	if headRec.Code != http.StatusOK {
		t.Fatalf("HeadObject failed with code %d", headRec.Code)
	}
	if headRec.Header().Get("Content-Type") != "text/plain" {
		t.Fatalf("Expected text/plain content type, got: %s", headRec.Header().Get("Content-Type"))
	}

	// 4. Get Object
	getReq := httptest.NewRequest(http.MethodGet, "/my-bucket/hello.txt", nil)
	signTest(getReq)
	getRec := httptest.NewRecorder()
	server.ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("GetObject failed with code %d: %s", getRec.Code, getRec.Body.String())
	}
	fetchedData, _ := io.ReadAll(getRec.Body)
	if string(fetchedData) != string(bodyContent) {
		t.Fatalf("GetObject content mismatch. Got: %s", string(fetchedData))
	}

	// 5. List Objects
	listReq := httptest.NewRequest(http.MethodGet, "/my-bucket?list-type=2", nil)
	signTest(listReq)
	listRec := httptest.NewRecorder()
	server.ServeHTTP(listRec, listReq)

	if listRec.Code != http.StatusOK {
		t.Fatalf("ListObjects failed with code %d", listRec.Code)
	}

	var listResult ListBucketResult
	err := xml.NewDecoder(listRec.Body).Decode(&listResult)
	if err != nil {
		t.Fatalf("Decode ListBucketResult failed: %v", err)
	}
	if len(listResult.Contents) != 1 || listResult.Contents[0].Key != "hello.txt" {
		t.Fatalf("Expected 1 object hello.txt in bucket, got: %+v", listResult.Contents)
	}
}

func TestS3AuthRejection(t *testing.T) {
	server, cleanup := setupTestS3Server(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	signAs(req, "wrong-key", "test-secret-key", sigv4.UnsignedPayload)
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("Expected StatusForbidden (403), got: %d", rec.Code)
	}
}

func TestS3ConcurrentUsersBenchmark(t *testing.T) {
	server, cleanup := setupTestS3Server(t)
	defer cleanup()

	// 1. Ensure test bucket exists
	bReq := httptest.NewRequest(http.MethodPut, "/test", nil)
	signTest(bReq)
	bRec := httptest.NewRecorder()
	server.ServeHTTP(bRec, bReq)
	if bRec.Code != http.StatusOK && bRec.Code != http.StatusConflict {
		t.Fatalf("Create bucket test failed: code %d", bRec.Code)
	}

	// 2. Run 20 concurrent users uploading and downloading files through S3 API
	const numUsers = 20
	const opsPerUser = 5
	var wg sync.WaitGroup
	errCh := make(chan error, numUsers*opsPerUser*2)

	payload := bytes.Repeat([]byte("BenchTestData_"), 10) // 140 bytes

	startTime := time.Now()
	for u := 0; u < numUsers; u++ {
		wg.Add(1)
		go func(userID int) {
			defer wg.Done()
			for op := 0; op < opsPerUser; op++ {
				key := strings.TrimLeft(strings.ReplaceAll(t.Name(), "/", "_"), "") + "_" + string(rune('A'+userID)) + "_" + string(rune('0'+op)) + ".bin"

				// PUT Object
				putReq := httptest.NewRequest(http.MethodPut, "/test/"+key, bytes.NewReader(payload))
				signTest(putReq)
				putReq.Header.Set("Content-Type", "application/octet-stream")
				putRec := httptest.NewRecorder()
				server.ServeHTTP(putRec, putReq)

				if putRec.Code != http.StatusOK {
					errCh <- fmt.Errorf("user %d put %s failed with code %d", userID, key, putRec.Code)
					return
				}

				// GET Object
				getReq := httptest.NewRequest(http.MethodGet, "/test/"+key, nil)
				signTest(getReq)
				getRec := httptest.NewRecorder()
				server.ServeHTTP(getRec, getReq)

				if getRec.Code != http.StatusOK {
					errCh <- fmt.Errorf("user %d get %s failed with code %d", userID, key, getRec.Code)
					return
				}

				body, _ := io.ReadAll(getRec.Body)
				if !bytes.Equal(body, payload) {
					errCh <- fmt.Errorf("user %d get %s data mismatch", userID, key)
					return
				}
			}
		}(u)
	}

	wg.Wait()
	duration := time.Since(startTime)

	select {
	case err := <-errCh:
		t.Fatalf("S3 concurrency benchmark error: %v", err)
	default:
	}

	totalOps := numUsers * opsPerUser * 2 // 1 PUT + 1 GET
	opsPerSec := float64(totalOps) / duration.Seconds()
	t.Logf("S3 Concurrency Benchmark Passed: %d ops across %d users in %v (%.1f ops/sec)", totalOps, numUsers, duration, opsPerSec)
}

func TestMultiTierDirectDownloadRedirect(t *testing.T) {
	database, err := db.Open(":memory:")
	if err != nil {
		t.Fatalf("Open db failed: %v", err)
	}
	defer database.Close()

	ctx := context.Background()
	_ = database.CreateBucket(ctx, "videobucket")
	testAcc := &models.Account{
		Name:     "Test",
		Username: "user",
		Token:    "tok",
		RepoName: "user/repo",
		IsActive: true,
	}
	_ = database.CreateAccount(ctx, testAcc)

	masterPass := "testpass"
	derivedKey := crypto.DeriveKey(masterPass)
	hfCli := hfclient.NewClient()
	cacheCli := hfstorage.NewS3Client(hfstorage.S3ClientConfig{
		Endpoint:  "https://s3.hf.co",
		Region:    "us-east-1",
		AccessKey: "HFAKTEST",
		SecretKey: "secret",
		Bucket:    "cache-bucket",
	})

	pool := storage.NewPoolManager(database, hfCli, derivedKey, 32*1024*1024)
	pool.SetCacheClient(cacheCli)

	server := NewServer(pool, nil) // No auth required for test

	// Create object with TierCache location
	obj := &models.Object{
		Bucket:      "videobucket",
		Key:         "movies/sample.mp4",
		Size:        1000,
		ETag:        `"etag-sample"`,
		ContentType: "video/mp4",
	}
	locations := []models.ObjectLocation{
		{
			Tier:       models.TierCache,
			AccountID:  0, // 0 = the default (legacy) cache client
			RemotePath: "videobucket/movies/sample.mp4",
			SizeBytes:  1000,
		},
	}
	_ = database.SaveObjectWithChunksAndLocations(ctx, obj, nil, locations)

	// 1. Test GET /videobucket/movies/sample.mp4 -> Expect HTTP 302 Found redirect with presigned Location
	req := httptest.NewRequest(http.MethodGet, "/videobucket/movies/sample.mp4", nil)
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusFound {
		t.Fatalf("Expected HTTP 302 Found redirect, got: %d", rec.Code)
	}
	locHeader := rec.Header().Get("Location")
	if locHeader == "" || !strings.Contains(locHeader, "s3.hf.co") {
		t.Errorf("Expected redirect Location to s3.hf.co, got: %s", locHeader)
	}
	if rec.Header().Get("X-HF2S3-Cache-Status") != "HIT" {
		t.Errorf("Expected Cache HIT header")
	}

	// 2. Test GET /media/videobucket/movies/sample.mp4 -> Dedicated media route
	mediaReq := httptest.NewRequest(http.MethodGet, "/media/videobucket/movies/sample.mp4", nil)
	mediaRec := httptest.NewRecorder()
	server.ServeHTTP(mediaRec, mediaReq)

	if mediaRec.Code != http.StatusFound {
		t.Fatalf("Expected HTTP 302 Found on /media route, got: %d", mediaRec.Code)
	}
	if mediaRec.Header().Get("Location") != locHeader {
		t.Errorf("Location mismatch on /media route: %s vs %s", mediaRec.Header().Get("Location"), locHeader)
	}

	// 3. Test HEAD /videobucket/movies/sample.mp4
	headReq := httptest.NewRequest(http.MethodHead, "/videobucket/movies/sample.mp4", nil)
	headRec := httptest.NewRecorder()
	server.ServeHTTP(headRec, headReq)

	if headRec.Code != http.StatusOK {
		t.Fatalf("Expected 200 OK on HEAD, got: %d", headRec.Code)
	}
	if headRec.Header().Get("X-HF2S3-Cache-Status") != "HIT" {
		t.Errorf("Expected Cache-Status HIT on HEAD")
	}
}
