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
