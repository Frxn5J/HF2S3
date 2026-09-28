package db

import (
	"context"
	"testing"
	"time"

	"hf2s3/pkg/models"
)

func setupTestDB(t *testing.T) *DB {
	t.Helper()
	database, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open test database: %v", err)
	}
	t.Cleanup(func() {
		_ = database.Close()
	})
	return database
}

func TestAccountCRUD(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()

	acc := &models.Account{
		Name:       "Primary Account",
		Username:   "hftestuser",
		Token:      "hf_test_token_12345",
		RepoName:   "hftestuser/vault-1",
		QuotaBytes: 100 * 1024 * 1024 * 1024,
		UsedBytes:  0,
		IsActive:   true,
	}

	err := db.CreateAccount(ctx, acc)
	if err != nil {
		t.Fatalf("CreateAccount failed: %v", err)
	}
	if acc.ID == 0 {
		t.Fatalf("Expected non-zero ID for created account")
	}

	fetched, err := db.GetAccountByID(ctx, acc.ID)
	if err != nil {
		t.Fatalf("GetAccountByID failed: %v", err)
	}
	if fetched.Username != "hftestuser" || fetched.RepoName != "hftestuser/vault-1" {
		t.Fatalf("Fetched account mismatch: %+v", fetched)
	}

	// Update used bytes
	err = db.UpdateAccountUsage(ctx, acc.ID, 50*1024*1024)
	if err != nil {
		t.Fatalf("UpdateAccountUsage failed: %v", err)
	}

	updated, err := db.GetAccountByID(ctx, acc.ID)
	if err != nil {
		t.Fatalf("GetAccountByID failed: %v", err)
	}
	if updated.UsedBytes != 50*1024*1024 {
		t.Fatalf("Expected used bytes 52428800, got: %d", updated.UsedBytes)
	}

	accounts, err := db.ListActiveAccounts(ctx)
	if err != nil {
		t.Fatalf("ListActiveAccounts failed: %v", err)
	}
	if len(accounts) != 1 {
		t.Fatalf("Expected 1 active account, got %d", len(accounts))
	}
}

func TestBucketAndObjectCRUD(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()

	bucketName := "my-data-bucket"
	err := db.CreateBucket(ctx, bucketName)
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	exists, err := db.BucketExists(ctx, bucketName)
	if err != nil || !exists {
		t.Fatalf("Expected bucket to exist, err: %v", err)
	}

	// Create parent account for foreign key constraint
	testAcc := &models.Account{
		Name:       "Test Account",
		Username:   "tester",
		Token:      "hf_dummy",
		RepoName:   "tester/vault",
		QuotaBytes: 100 * 1024 * 1024 * 1024,
		IsActive:   true,
	}
	if err := db.CreateAccount(ctx, testAcc); err != nil {
		t.Fatalf("CreateAccount failed: %v", err)
	}

	// Create test object with 2 chunks
	obj := &models.Object{
		Bucket:      bucketName,
		Key:         "photos/summer2026.zip",
		Size:        1000,
		ETag:        `"etag-test-123"`,
		ContentType: "application/zip",
	}

	chunks := []models.Chunk{
		{
			PartNumber:      1,
			ChunkIndex:      0,
			OffsetBytes:     0,
			SizeBytes:       500,
			CipherSizeBytes: 528,
			AccountID:       testAcc.ID,
			RemotePath:      "data/chunk_0.enc",
			Sha256Hash:      "hash0",
			CreatedAt:       time.Now(),
		},
		{
			PartNumber:      1,
			ChunkIndex:      1,
			OffsetBytes:     500,
			SizeBytes:       500,
			CipherSizeBytes: 528,
			AccountID:       testAcc.ID,
			RemotePath:      "data/chunk_1.enc",
			Sha256Hash:      "hash1",
			CreatedAt:       time.Now(),
		},
	}

	err = db.SaveObjectWithChunks(ctx, obj, chunks)
	if err != nil {
		t.Fatalf("SaveObjectWithChunks failed: %v", err)
	}
	if obj.ID == 0 {
		t.Fatalf("Expected object ID to be assigned")
	}

	retrievedObj, retrievedChunks, err := db.GetObjectWithChunks(ctx, bucketName, "photos/summer2026.zip")
	if err != nil {
		t.Fatalf("GetObjectWithChunks failed: %v", err)
	}
	if retrievedObj.Key != "photos/summer2026.zip" || len(retrievedChunks) != 2 {
		t.Fatalf("Retrieved object or chunks mismatch: obj=%+v, chunks=%d", retrievedObj, len(retrievedChunks))
	}

	// Test ListObjects
	list, err := db.ListObjects(ctx, bucketName, "photos/", "", 10)
	if err != nil {
		t.Fatalf("ListObjects failed: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("Expected 1 object in list, got %d", len(list))
	}

	// Test DeleteObject
	deletedChunks, err := db.DeleteObject(ctx, bucketName, "photos/summer2026.zip")
	if err != nil {
		t.Fatalf("DeleteObject failed: %v", err)
	}
	if len(deletedChunks) != 2 {
		t.Fatalf("Expected 2 deleted chunks returned, got %d", len(deletedChunks))
	}

	_, _, err = db.GetObjectWithChunks(ctx, bucketName, "photos/summer2026.zip")
	if err == nil {
		t.Fatalf("Expected object to be deleted, but found it")
	}
}

func TestMultiTierObjectLocations(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()

	bucketName := "media-bucket"
	if err := db.CreateBucket(ctx, bucketName); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	testAcc := &models.Account{
		Name:       "HF Account 1",
		Username:   "hftest",
		Token:      "hf_token",
		RepoName:   "hftest/public-media",
		QuotaBytes: 100 * 1024 * 1024 * 1024,
		IsActive:   true,
		IsPublic:   true,
	}
	if err := db.CreateAccount(ctx, testAcc); err != nil {
		t.Fatalf("CreateAccount failed: %v", err)
	}

	// 1. Create object with both Cold (Dataset) and Cache (Storage Bucket) locations
	obj := &models.Object{
		Bucket:      bucketName,
		Key:         "videos/nature.mp4",
		Size:        5000000,
		ETag:        `"etag-nature-5mb"`,
		ContentType: "video/mp4",
	}

	locations := []models.ObjectLocation{
		{
			Tier:        models.TierCold,
			AccountID:   testAcc.ID,
			RemotePath:  "data/nature_encrypted.enc",
			IsEncrypted: true,
			SizeBytes:   5000028,
		},
		{
			Tier:        models.TierCache,
			AccountID:   testAcc.ID,
			RemotePath:  "cache/media-bucket/videos/nature.mp4",
			IsEncrypted: false,
			SizeBytes:   5000000,
		},
	}

	err := db.SaveObjectWithChunksAndLocations(ctx, obj, nil, locations)
	if err != nil {
		t.Fatalf("SaveObjectWithChunksAndLocations failed: %v", err)
	}

	// 2. Verify object tiers populated
	headObj, err := db.HeadObject(ctx, bucketName, "videos/nature.mp4")
	if err != nil {
		t.Fatalf("HeadObject failed: %v", err)
	}
	if !headObj.HasCache {
		t.Errorf("Expected HasCache to be true")
	}
	if !headObj.HasCold {
		t.Errorf("Expected HasCold to be true")
	}
	if len(headObj.Locations) != 2 {
		t.Errorf("Expected 2 locations, got %d", len(headObj.Locations))
	}

	// 3. Verify GetObjectLocationByTier
	cacheLoc, err := db.GetObjectLocationByTier(ctx, headObj.ID, models.TierCache)
	if err != nil {
		t.Fatalf("GetObjectLocationByTier(cache) failed: %v", err)
	}
	if cacheLoc.RemotePath != "cache/media-bucket/videos/nature.mp4" {
		t.Errorf("Unexpected cache remote path: %s", cacheLoc.RemotePath)
	}

	coldLoc, err := db.GetObjectLocationByTier(ctx, headObj.ID, models.TierCold)
	if err != nil {
		t.Fatalf("GetObjectLocationByTier(cold) failed: %v", err)
	}
	if !coldLoc.IsEncrypted {
		t.Errorf("Expected cold location to be encrypted")
	}

	// 4. Test Cache Eviction (deleting only TierCache location)
	err = db.DeleteObjectLocationByTier(ctx, headObj.ID, models.TierCache)
	if err != nil {
		t.Fatalf("DeleteObjectLocationByTier(cache) failed: %v", err)
	}

	// 5. Verify that cache is gone but cold golden copy persists
	headAfterEvict, err := db.HeadObject(ctx, bucketName, "videos/nature.mp4")
	if err != nil {
		t.Fatalf("HeadObject after eviction failed: %v", err)
	}
	if headAfterEvict.HasCache {
		t.Errorf("Expected HasCache to be false after eviction")
	}
	if !headAfterEvict.HasCold {
		t.Errorf("Expected HasCold to remain true after eviction (golden copy preserved!)")
	}

	// 6. Test re-adding cache location (promotion)
	newCacheLoc := &models.ObjectLocation{
		ObjectID:    headObj.ID,
		Tier:        models.TierCache,
		AccountID:   testAcc.ID,
		RemotePath:  "cache/media-bucket/videos/nature.mp4",
		IsEncrypted: false,
		SizeBytes:   5000000,
	}
	err = db.SaveObjectLocation(ctx, newCacheLoc)
	if err != nil {
		t.Fatalf("SaveObjectLocation (re-promotion) failed: %v", err)
	}

	headAfterPromote, err := db.HeadObject(ctx, bucketName, "videos/nature.mp4")
	if err != nil {
		t.Fatalf("HeadObject after promote failed: %v", err)
	}
	if !headAfterPromote.HasCache {
		t.Errorf("Expected HasCache to be true after promotion")
	}

	// 7. Test LRU ordering
	lruList, err := db.ListLRUCacheLocations(ctx, 10)
	if err != nil {
		t.Fatalf("ListLRUCacheLocations failed: %v", err)
	}
	if len(lruList) != 1 {
		t.Errorf("Expected 1 LRU cache location, got %d", len(lruList))
	}

	// 8. Stats check
	stats, err := db.GetStats(ctx)
	if err != nil {
		t.Fatalf("GetStats failed: %v", err)
	}
	if stats.CachedObjects != 1 || stats.ColdObjects != 1 {
		t.Errorf("Stats mismatch: cached=%d, cold=%d", stats.CachedObjects, stats.ColdObjects)
	}
}

