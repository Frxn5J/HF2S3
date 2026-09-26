package db

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"hf2s3/pkg/models"
)

func TestExportAndImportBackupRoundTrip(t *testing.T) {
	tempDir := t.TempDir()
	dbPath1 := filepath.Join(tempDir, "test1.db")
	dbPath2 := filepath.Join(tempDir, "test2.db")

	ctx := context.Background()

	// 1. Setup source DB
	db1, err := Open(dbPath1)
	if err != nil {
		t.Fatalf("Open db1 failed: %v", err)
	}

	acc := &models.Account{
		Name:       "Backup Test Account",
		Username:   "testuser",
		Token:      "hf_token_secret_123",
		RepoName:   "testuser/vault",
		QuotaBytes: 100 * 1024 * 1024 * 1024,
		UsedBytes:  500,
		IsActive:   true,
	}
	if err := db1.CreateAccount(ctx, acc); err != nil {
		t.Fatalf("CreateAccount failed: %v", err)
	}

	if err := db1.CreateBucket(ctx, "vault-bucket"); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// 2. Export Backup to buffer
	var backupBuf bytes.Buffer
	if err := db1.ExportBackup(ctx, &backupBuf); err != nil {
		t.Fatalf("ExportBackup failed: %v", err)
	}
	_ = db1.Close()

	if backupBuf.Len() == 0 {
		t.Fatalf("ExportBackup produced 0 bytes")
	}

	// 3. Write backup to a second file and open it as a new instance
	if err := os.WriteFile(dbPath2, backupBuf.Bytes(), 0644); err != nil {
		t.Fatalf("WriteFile db2 failed: %v", err)
	}

	db2, err := Open(dbPath2)
	if err != nil {
		t.Fatalf("Open restored db2 failed: %v", err)
	}
	defer db2.Close()

	// 4. Verify data in restored instance
	accounts, err := db2.ListAccounts(ctx)
	if err != nil {
		t.Fatalf("ListAccounts on db2 failed: %v", err)
	}
	if len(accounts) != 1 || accounts[0].Name != "Backup Test Account" {
		t.Fatalf("Account mismatch in restored db: %+v", accounts)
	}

	exists, err := db2.BucketExists(ctx, "vault-bucket")
	if err != nil || !exists {
		t.Fatalf("Bucket 'vault-bucket' not found in restored db: %v", err)
	}
}
