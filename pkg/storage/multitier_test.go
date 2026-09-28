package storage

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"hf2s3/pkg/crypto"
	"hf2s3/pkg/db"
	"hf2s3/pkg/hfclient"
	"hf2s3/pkg/hfstorage"
	"hf2s3/pkg/models"
)

func TestMultiTierLifecycle(t *testing.T) {
	// Setup test database
	database, err := db.Open(":memory:")
	if err != nil {
		t.Fatalf("Open test DB: %v", err)
	}
	defer database.Close()

	ctx := context.Background()

	// 1. Mock S3 Cache Server (Tier 1 - HF Storage Bucket)
	cacheStorage := make(map[string][]byte)
	var cachePutCount, cacheDeleteCount int

	s3Server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/test-cache-bucket/")
		switch r.Method {
		case http.MethodPut:
			cachePutCount++
			data, _ := io.ReadAll(r.Body)
			cacheStorage[key] = data
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if data, ok := cacheStorage[key]; ok {
				w.Header().Set("Content-Type", "text/plain")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(data)
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
		case http.MethodDelete:
			cacheDeleteCount++
			delete(cacheStorage, key)
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer s3Server.Close()

	// 2. Mock HF Datasets Server (Tier 2 - Public Datasets Hub)
	hfStorage := make(map[string][]byte)
	hfServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		// Resolve endpoint: /datasets/<repo>/resolve/main/<remotePath>
		if strings.Contains(path, "/resolve/main/") {
			parts := strings.Split(path, "/resolve/main/")
			if len(parts) == 2 {
				remotePath := parts[1]
				if data, ok := hfStorage[remotePath]; ok {
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write(data)
					return
				}
			}
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer hfServer.Close()

	masterPass := "test-multitier-master-key-2026"
	derivedKey := crypto.DeriveKey(masterPass)

	hfCli := hfclient.NewClient(
		hfclient.WithBaseURL(hfServer.URL),
		hfclient.WithHTTPClient(hfServer.Client()),
	)

	cacheCli := hfstorage.NewS3Client(hfstorage.S3ClientConfig{
		Endpoint:   s3Server.URL,
		Region:     "us-east-1",
		AccessKey:  "HFAKTESTKEY",
		SecretKey:  "testsecret",
		Bucket:     "test-cache-bucket",
		HTTPClient: s3Server.Client(),
	})

	pool := NewPoolManager(database, hfCli, derivedKey, 32*1024*1024)
	pool.SetCacheClient(cacheCli)

	bucketName := "videos"
	if err := database.CreateBucket(ctx, bucketName); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	testAcc := &models.Account{
		Name:       "Public Media Account",
		Username:   "hfuser",
		Token:      "", // Public dataset: no token needed for reads!
		RepoName:   "hfuser/public-vault",
		QuotaBytes: 100 * 1024 * 1024 * 1024,
		IsActive:   true,
		IsPublic:   true,
	}
	if err := database.CreateAccount(ctx, testAcc); err != nil {
		t.Fatalf("CreateAccount: %v", err)
	}

	// 3. Prepare an encrypted cold object in Tier 2
	plaintext := []byte("Cold tier video content that should be auto-promoted to cache")
	encryptedBlob, err := crypto.Encrypt(plaintext, derivedKey)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	remoteColdPath := "data/video123.enc"
	hfStorage[remoteColdPath] = encryptedBlob

	obj := &models.Object{
		Bucket:      bucketName,
		Key:         "trailer.mp4",
		Size:        int64(len(plaintext)),
		ETag:        `"etag-cold-1"`,
		ContentType: "video/mp4",
	}

	chunks := []models.Chunk{
		{
			PartNumber:      1,
			ChunkIndex:      0,
			OffsetBytes:     0,
			SizeBytes:       int64(len(plaintext)),
			CipherSizeBytes: int64(len(encryptedBlob)),
			AccountID:       testAcc.ID,
			RemotePath:      remoteColdPath,
			Sha256Hash:      "dummyhash",
			CreatedAt:       time.Now(),
		},
	}

	if err := database.SaveObjectWithChunks(ctx, obj, chunks); err != nil {
		t.Fatalf("SaveObjectWithChunks: %v", err)
	}

	// 4. Test Cache MISS: GetObjectOrPresigned should decrypt on-the-fly and return stream
	presignedURL, fetchedObj, reader, err := pool.GetObjectOrPresigned(ctx, bucketName, "trailer.mp4")
	if err != nil {
		t.Fatalf("GetObjectOrPresigned (cold miss): %v", err)
	}
	if presignedURL != "" {
		t.Fatalf("Expected empty presigned URL on cold miss, got: %s", presignedURL)
	}
	if reader == nil {
		t.Fatalf("Expected valid reader on cold miss")
	}
	readBytes, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		t.Fatalf("Read decrypted stream: %v", err)
	}
	if string(readBytes) != string(plaintext) {
		t.Errorf("Decrypted content mismatch: got %q, want %q", string(readBytes), string(plaintext))
	}
	_ = fetchedObj

	// 5. Explicitly promote to cache (or wait briefly for async promotion)
	err = pool.PromoteToCache(ctx, bucketName, "trailer.mp4")
	if err != nil {
		t.Fatalf("PromoteToCache: %v", err)
	}

	// Verify unencrypted content is stored in cache
	cachedData, existsInCache := cacheStorage["videos/trailer.mp4"]
	if !existsInCache {
		t.Fatalf("Expected object in cacheStorage at 'videos/trailer.mp4'")
	}
	if string(cachedData) != string(plaintext) {
		t.Errorf("Cache data should be unencrypted plaintext: got %q", string(cachedData))
	}

	// 6. Test Cache HIT: Next GetObjectOrPresigned MUST return a Presigned URL for zero-bandwidth VPS download!
	presignedURL, _, cacheReader, err := pool.GetObjectOrPresigned(ctx, bucketName, "trailer.mp4")
	if err != nil {
		t.Fatalf("GetObjectOrPresigned (cache hit): %v", err)
	}
	if cacheReader != nil {
		_ = cacheReader.Close()
		t.Fatalf("Expected nil reader on cache hit (should redirect via presigned URL)")
	}
	if presignedURL == "" {
		t.Fatalf("Expected presigned URL on cache hit, got empty string")
	}
	if !strings.Contains(presignedURL, "X-Amz-Signature") {
		t.Errorf("Presigned URL missing signature: %s", presignedURL)
	}

	// 7. Test Cache Eviction: Delete from cache, verify golden copy in public dataset is untouched!
	err = pool.EvictCache(ctx, bucketName, "trailer.mp4")
	if err != nil {
		t.Fatalf("EvictCache: %v", err)
	}

	// Verify evicted from cache
	if _, ok := cacheStorage["videos/trailer.mp4"]; ok {
		t.Errorf("Object still present in cacheStorage after eviction")
	}

	// Verify golden copy in public dataset is still 100% present and intact
	if _, ok := hfStorage[remoteColdPath]; !ok {
		t.Errorf("CRITICAL: Cold tier encrypted original was deleted during cache eviction!")
	}

	// Verify object in DB now reports HasCache=false and HasCold=true
	headAfter, err := database.HeadObject(ctx, bucketName, "trailer.mp4")
	if err != nil {
		t.Fatalf("HeadObject after eviction: %v", err)
	}
	if headAfter.HasCache {
		t.Errorf("Expected HasCache=false after eviction")
	}
	if !headAfter.HasCold {
		t.Errorf("Expected HasCold=true (golden copy intact)")
	}
}

func TestMultiBucketCacheRouting(t *testing.T) {
	database, err := db.Open(":memory:")
	if err != nil {
		t.Fatalf("Open test DB: %v", err)
	}
	defer database.Close()

	ctx := context.Background()

	// 1. Mock Two Separate S3 Cache Servers (representing 2 accounts with 100GB each)
	cacheStorage1 := make(map[string][]byte)
	s3Server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/bucket-account1/")
		switch r.Method {
		case http.MethodPut:
			data, _ := io.ReadAll(r.Body)
			cacheStorage1[key] = data
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if data, ok := cacheStorage1[key]; ok {
				_, _ = w.Write(data)
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
		case http.MethodDelete:
			delete(cacheStorage1, key)
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer s3Server1.Close()

	cacheStorage2 := make(map[string][]byte)
	s3Server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/bucket-account2/")
		switch r.Method {
		case http.MethodPut:
			data, _ := io.ReadAll(r.Body)
			cacheStorage2[key] = data
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if data, ok := cacheStorage2[key]; ok {
				_, _ = w.Write(data)
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
		case http.MethodDelete:
			delete(cacheStorage2, key)
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer s3Server2.Close()

	// Register the two cache buckets in DB
	cb1 := &models.CacheBucket{
		Name:       "Cache Account 1",
		Endpoint:   s3Server1.URL,
		Region:     "us-east-1",
		AccessKey:  "KEY1",
		SecretKey:  "SEC1",
		BucketName: "bucket-account1",
		QuotaBytes: 100 * 1024 * 1024 * 1024,
		UsedBytes:  0,
		IsActive:   true,
	}
	if err := database.CreateCacheBucket(ctx, cb1); err != nil {
		t.Fatalf("CreateCacheBucket 1: %v", err)
	}

	cb2 := &models.CacheBucket{
		Name:       "Cache Account 2",
		Endpoint:   s3Server2.URL,
		Region:     "us-east-1",
		AccessKey:  "KEY2",
		SecretKey:  "SEC2",
		BucketName: "bucket-account2",
		QuotaBytes: 100 * 1024 * 1024 * 1024,
		UsedBytes:  10 * 1024 * 1024 * 1024, // 10 GB already used -> cb1 should be picked first
		IsActive:   true,
	}
	if err := database.CreateCacheBucket(ctx, cb2); err != nil {
		t.Fatalf("CreateCacheBucket 2: %v", err)
	}

	// 2. Mock HF Datasets cold storage
	hfStorage := make(map[string][]byte)
	hfServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if strings.Contains(path, "/resolve/main/") {
			parts := strings.Split(path, "/resolve/main/")
			if len(parts) == 2 {
				remotePath := parts[1]
				if data, ok := hfStorage[remotePath]; ok {
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write(data)
					return
				}
			}
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer hfServer.Close()

	masterPass := "test-multibucket-key-2026"
	derivedKey := crypto.DeriveKey(masterPass)
	hfCli := hfclient.NewClient(
		hfclient.WithBaseURL(hfServer.URL),
		hfclient.WithHTTPClient(hfServer.Client()),
	)

	pool := NewPoolManager(database, hfCli, derivedKey, 32*1024*1024)

	bucketName := "assets"
	if err := database.CreateBucket(ctx, bucketName); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	testAcc := &models.Account{
		Name:       "Media Node",
		Username:   "hfmedia",
		RepoName:   "hfmedia/vault",
		QuotaBytes: 100 * 1024 * 1024 * 1024,
		IsActive:   true,
		IsPublic:   true,
	}
	_ = database.CreateAccount(ctx, testAcc)

	// Prepare encrypted cold object
	plainText := []byte("Multi-bucket cached media stream")
	encryptedBlob, _ := crypto.Encrypt(plainText, derivedKey)
	remoteCold := "data/doc.enc"
	hfStorage[remoteCold] = encryptedBlob

	obj := &models.Object{
		Bucket:      bucketName,
		Key:         "doc.pdf",
		Size:        int64(len(plainText)),
		ETag:        `"doc-etag"`,
		ContentType: "application/pdf",
	}
	chunks := []models.Chunk{
		{
			PartNumber:      1,
			ChunkIndex:      0,
			OffsetBytes:     0,
			SizeBytes:       int64(len(plainText)),
			CipherSizeBytes: int64(len(encryptedBlob)),
			AccountID:       testAcc.ID,
			RemotePath:      remoteCold,
			Sha256Hash:      "doc-sha256",
			CreatedAt:       time.Now(),
		},
	}
	_ = database.SaveObjectWithChunks(ctx, obj, chunks)

	// Promote: should select cb1 (0% usage vs cb2 10% usage)
	if err := pool.PromoteToCache(ctx, bucketName, "doc.pdf"); err != nil {
		t.Fatalf("PromoteToCache: %v", err)
	}

	// Verify cb1 received the unencrypted file
	if _, ok := cacheStorage1["assets/doc.pdf"]; !ok {
		t.Errorf("Expected object in cacheStorage1 (least used bucket)")
	}
	if _, ok := cacheStorage2["assets/doc.pdf"]; ok {
		t.Errorf("Object unexpectedly found in cacheStorage2")
	}

	// Verify presigned URL directs to bucket-account1
	presignedURL, _, _, err := pool.GetObjectOrPresigned(ctx, bucketName, "doc.pdf")
	if err != nil {
		t.Fatalf("GetObjectOrPresigned: %v", err)
	}
	if !strings.Contains(presignedURL, "bucket-account1") {
		t.Errorf("Expected presigned URL to target bucket-account1, got: %s", presignedURL)
	}

	// Verify cb1 used_bytes was incremented in DB
	cb1After, _ := database.GetCacheBucketByID(ctx, cb1.ID)
	if cb1After.UsedBytes != int64(len(plainText)) {
		t.Errorf("Expected used_bytes %d for cb1, got %d", len(plainText), cb1After.UsedBytes)
	}

	// Evict doc.pdf
	if err := pool.EvictCache(ctx, bucketName, "doc.pdf"); err != nil {
		t.Fatalf("EvictCache: %v", err)
	}

	if _, ok := cacheStorage1["assets/doc.pdf"]; ok {
		t.Errorf("Object still in cacheStorage1 after eviction")
	}
	cb1Evicted, _ := database.GetCacheBucketByID(ctx, cb1.ID)
	if cb1Evicted.UsedBytes != 0 {
		t.Errorf("Expected 0 used_bytes for cb1 after eviction, got %d", cb1Evicted.UsedBytes)
	}
}

