package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"hf2s3/pkg/models"
	_ "modernc.org/sqlite"
)

var ErrNotFound = errors.New("record not found")

type DB struct {
	db          *sql.DB
	filePath    string
	writeMu     sync.Mutex
	bucketCache sync.Map
}

func Open(dataSourceName string) (*DB, error) {
	connStr := dataSourceName
	if connStr == ":memory:" {
		connStr = fmt.Sprintf("file:memdb_%d?mode=memory&cache=shared", time.Now().UnixNano())
	} else {
		if dir := filepath.Dir(dataSourceName); dir != "" && dir != "." && dir != "/" && dir != "\\" {
			if err := os.MkdirAll(dir, 0755); err != nil {
				dataSourceName = filepath.Base(dataSourceName)
				connStr = dataSourceName
			}
		}
	}
	pragmas := "_pragma=busy_timeout(30000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)&_pragma=synchronous(NORMAL)&_pragma=cache_size(-64000)"
	if !strings.Contains(connStr, "?") {
		connStr += "?" + pragmas
	} else {
		connStr += "&" + pragmas
	}

	database, err := sql.Open("sqlite", connStr)
	if err != nil {
		return nil, fmt.Errorf("open sqlite db: %w", err)
	}

	maxConns := runtime.NumCPU() * 4
	if maxConns < 8 {
		maxConns = 8
	}
	database.SetMaxOpenConns(maxConns)
	database.SetMaxIdleConns(maxConns / 2)
	database.SetConnMaxLifetime(1 * time.Hour)

	d := &DB{db: database, filePath: dataSourceName}
	if err := d.migrate(); err != nil {
		_ = database.Close()
		return nil, fmt.Errorf("migrate db: %w", err)
	}

	// Warm up bucket cache
	if buckets, err := d.ListBuckets(context.Background()); err == nil {
		for _, b := range buckets {
			d.bucketCache.Store(b.Name, true)
		}
	}

	return d, nil
}

func (d *DB) Close() error {
	return d.db.Close()
}

func (d *DB) migrate() error {
	schema := `
	CREATE TABLE IF NOT EXISTS accounts (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL,
		username TEXT NOT NULL,
		token TEXT NOT NULL,
		repo_name TEXT NOT NULL,
		quota_bytes INTEGER NOT NULL DEFAULT 107374182400,
		used_bytes INTEGER NOT NULL DEFAULT 0,
		is_active INTEGER NOT NULL DEFAULT 1,
		last_checked DATETIME,
		created_at DATETIME NOT NULL,
		updated_at DATETIME NOT NULL
	);

	CREATE TABLE IF NOT EXISTS buckets (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT UNIQUE NOT NULL,
		created_at DATETIME NOT NULL
	);

	CREATE TABLE IF NOT EXISTS objects (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		bucket TEXT NOT NULL,
		key TEXT NOT NULL,
		size INTEGER NOT NULL,
		etag TEXT NOT NULL,
		content_type TEXT NOT NULL,
		custom_metadata TEXT,
		created_at DATETIME NOT NULL,
		updated_at DATETIME NOT NULL,
		UNIQUE(bucket, key)
	);

	CREATE TABLE IF NOT EXISTS chunks (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		object_id INTEGER NOT NULL,
		part_number INTEGER NOT NULL DEFAULT 1,
		chunk_index INTEGER NOT NULL,
		offset_bytes INTEGER NOT NULL,
		size_bytes INTEGER NOT NULL,
		cipher_size_bytes INTEGER NOT NULL,
		account_id INTEGER NOT NULL,
		remote_path TEXT NOT NULL,
		sha256_hash TEXT NOT NULL,
		created_at DATETIME NOT NULL,
		FOREIGN KEY (object_id) REFERENCES objects(id) ON DELETE CASCADE,
		FOREIGN KEY (account_id) REFERENCES accounts(id)
	);

	CREATE TABLE IF NOT EXISTS multipart_uploads (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		upload_id TEXT UNIQUE NOT NULL,
		bucket TEXT NOT NULL,
		key TEXT NOT NULL,
		content_type TEXT NOT NULL,
		initiated_at DATETIME NOT NULL
	);

	CREATE TABLE IF NOT EXISTS multipart_parts (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		upload_id TEXT NOT NULL,
		part_number INTEGER NOT NULL,
		etag TEXT NOT NULL,
		size_bytes INTEGER NOT NULL,
		chunks_json TEXT NOT NULL,
		uploaded_at DATETIME NOT NULL,
		UNIQUE(upload_id, part_number)
	);

	CREATE TABLE IF NOT EXISTS system_settings (
		key TEXT PRIMARY KEY,
		value TEXT NOT NULL
	);

	CREATE TABLE IF NOT EXISTS object_locations (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		object_id INTEGER NOT NULL,
		tier TEXT NOT NULL,
		account_id INTEGER NOT NULL DEFAULT 0,
		remote_path TEXT NOT NULL,
		is_encrypted INTEGER NOT NULL DEFAULT 0,
		size_bytes INTEGER NOT NULL DEFAULT 0,
		access_count INTEGER NOT NULL DEFAULT 0,
		last_accessed_at DATETIME NOT NULL,
		created_at DATETIME NOT NULL,
		FOREIGN KEY (object_id) REFERENCES objects(id) ON DELETE CASCADE,
		UNIQUE(object_id, tier, remote_path)
	);

	CREATE TABLE IF NOT EXISTS cache_buckets (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		account_id INTEGER NOT NULL DEFAULT 0,
		name TEXT NOT NULL,
		endpoint TEXT NOT NULL DEFAULT 'https://s3.hf.co',
		region TEXT NOT NULL DEFAULT 'us-east-1',
		access_key TEXT NOT NULL,
		secret_key TEXT NOT NULL,
		bucket_name TEXT NOT NULL,
		quota_bytes INTEGER NOT NULL DEFAULT 107374182400,
		used_bytes INTEGER NOT NULL DEFAULT 0,
		is_active INTEGER NOT NULL DEFAULT 1,
		created_at DATETIME NOT NULL,
		updated_at DATETIME NOT NULL
	);

	CREATE INDEX IF NOT EXISTS idx_objects_bucket_key ON objects(bucket, key);
	CREATE INDEX IF NOT EXISTS idx_chunks_object_id ON chunks(object_id);
	CREATE INDEX IF NOT EXISTS idx_obj_loc_tier ON object_locations(object_id, tier);
	CREATE INDEX IF NOT EXISTS idx_obj_loc_lru ON object_locations(tier, last_accessed_at);
	CREATE INDEX IF NOT EXISTS idx_cache_buckets_active ON cache_buckets(is_active);
	`
	if _, err := d.db.Exec(schema); err != nil {
		return err
	}

	// Safe column additions for accounts table
	_, _ = d.db.Exec(`ALTER TABLE accounts ADD COLUMN is_public INTEGER NOT NULL DEFAULT 1`)
	_, _ = d.db.Exec(`ALTER TABLE accounts ADD COLUMN s3_access_key TEXT NOT NULL DEFAULT ''`)
	_, _ = d.db.Exec(`ALTER TABLE accounts ADD COLUMN s3_secret_key TEXT NOT NULL DEFAULT ''`)
	_, _ = d.db.Exec(`ALTER TABLE accounts ADD COLUMN s3_endpoint TEXT NOT NULL DEFAULT ''`)
	_, _ = d.db.Exec(`ALTER TABLE accounts ADD COLUMN s3_bucket TEXT NOT NULL DEFAULT ''`)

	return nil
}

// --- Accounts Operations ---

func (d *DB) CreateAccount(ctx context.Context, acc *models.Account) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	now := time.Now().UTC()
	acc.CreatedAt = now
	acc.UpdatedAt = now
	acc.LastChecked = now

	res, err := d.db.ExecContext(ctx, `
		INSERT INTO accounts (name, username, token, repo_name, quota_bytes, used_bytes, is_active, last_checked, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, acc.Name, acc.Username, acc.Token, acc.RepoName, acc.QuotaBytes, acc.UsedBytes, acc.IsActive, acc.LastChecked, acc.CreatedAt, acc.UpdatedAt)
	if err != nil {
		return err
	}

	id, err := res.LastInsertId()
	if err != nil {
		return err
	}
	acc.ID = id
	return nil
}

func (d *DB) UpdateAccount(ctx context.Context, acc *models.Account) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	acc.UpdatedAt = time.Now().UTC()
	_, err := d.db.ExecContext(ctx, `
		UPDATE accounts
		SET name = ?, username = ?, token = ?, repo_name = ?, quota_bytes = ?, is_active = ?, updated_at = ?
		WHERE id = ?
	`, acc.Name, acc.Username, acc.Token, acc.RepoName, acc.QuotaBytes, acc.IsActive, acc.UpdatedAt, acc.ID)
	return err
}

func (d *DB) UpdateAccountUsage(ctx context.Context, accountID int64, usedBytes int64) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	now := time.Now().UTC()
	_, err := d.db.ExecContext(ctx, `
		UPDATE accounts
		SET used_bytes = ?, last_checked = ?, updated_at = ?
		WHERE id = ?
	`, usedBytes, now, now, accountID)
	return err
}

func (d *DB) IncrementAccountUsage(ctx context.Context, accountID int64, deltaBytes int64) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	now := time.Now().UTC()
	_, err := d.db.ExecContext(ctx, `
		UPDATE accounts
		SET used_bytes = used_bytes + ?, last_checked = ?, updated_at = ?
		WHERE id = ?
	`, deltaBytes, now, now, accountID)
	return err
}

func (d *DB) DeleteAccount(ctx context.Context, accountID int64) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	_, err := d.db.ExecContext(ctx, `DELETE FROM accounts WHERE id = ?`, accountID)
	return err
}

func (d *DB) GetAccountByID(ctx context.Context, accountID int64) (*models.Account, error) {
	row := d.db.QueryRowContext(ctx, `
		SELECT id, name, username, token, repo_name, quota_bytes, used_bytes, is_active, last_checked, created_at, updated_at
		FROM accounts WHERE id = ?
	`, accountID)

	var acc models.Account
	var isActive int
	err := row.Scan(&acc.ID, &acc.Name, &acc.Username, &acc.Token, &acc.RepoName, &acc.QuotaBytes, &acc.UsedBytes, &isActive, &acc.LastChecked, &acc.CreatedAt, &acc.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	acc.IsActive = isActive == 1
	return &acc, nil
}

func (d *DB) ListAccounts(ctx context.Context) ([]models.Account, error) {
	rows, err := d.db.QueryContext(ctx, `
		SELECT id, name, username, token, repo_name, quota_bytes, used_bytes, is_active, last_checked, created_at, updated_at
		FROM accounts ORDER BY id ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []models.Account
	for rows.Next() {
		var acc models.Account
		var isActive int
		if err := rows.Scan(&acc.ID, &acc.Name, &acc.Username, &acc.Token, &acc.RepoName, &acc.QuotaBytes, &acc.UsedBytes, &isActive, &acc.LastChecked, &acc.CreatedAt, &acc.UpdatedAt); err != nil {
			return nil, err
		}
		acc.IsActive = isActive == 1
		result = append(result, acc)
	}
	return result, rows.Err()
}

func (d *DB) ListActiveAccounts(ctx context.Context) ([]models.Account, error) {
	rows, err := d.db.QueryContext(ctx, `
		SELECT id, name, username, token, repo_name, quota_bytes, used_bytes, is_active, last_checked, created_at, updated_at
		FROM accounts WHERE is_active = 1 ORDER BY (used_bytes * 1.0 / quota_bytes) ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []models.Account
	for rows.Next() {
		var acc models.Account
		var isActive int
		if err := rows.Scan(&acc.ID, &acc.Name, &acc.Username, &acc.Token, &acc.RepoName, &acc.QuotaBytes, &acc.UsedBytes, &isActive, &acc.LastChecked, &acc.CreatedAt, &acc.UpdatedAt); err != nil {
			return nil, err
		}
		acc.IsActive = isActive == 1
		result = append(result, acc)
	}
	return result, rows.Err()
}

// --- Cache Buckets (Tier 1 S3 Cache) Operations ---

func (d *DB) CreateCacheBucket(ctx context.Context, cb *models.CacheBucket) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	now := time.Now().UTC()
	cb.CreatedAt = now
	cb.UpdatedAt = now
	if cb.Endpoint == "" {
		cb.Endpoint = "https://s3.hf.co"
	}
	if cb.Region == "" {
		cb.Region = "us-east-1"
	}
	if cb.QuotaBytes <= 0 {
		cb.QuotaBytes = 100 * 1024 * 1024 * 1024 // 100GB
	}

	res, err := d.db.ExecContext(ctx, `
		INSERT INTO cache_buckets (account_id, name, endpoint, region, access_key, secret_key, bucket_name, quota_bytes, used_bytes, is_active, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, cb.AccountID, cb.Name, cb.Endpoint, cb.Region, cb.AccessKey, cb.SecretKey, cb.BucketName, cb.QuotaBytes, cb.UsedBytes, cb.IsActive, cb.CreatedAt, cb.UpdatedAt)
	if err != nil {
		return err
	}

	id, err := res.LastInsertId()
	if err != nil {
		return err
	}
	cb.ID = id
	return nil
}

func (d *DB) UpdateCacheBucket(ctx context.Context, cb *models.CacheBucket) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	cb.UpdatedAt = time.Now().UTC()
	_, err := d.db.ExecContext(ctx, `
		UPDATE cache_buckets
		SET name = ?, endpoint = ?, region = ?, access_key = ?, secret_key = ?, bucket_name = ?, quota_bytes = ?, is_active = ?, updated_at = ?
		WHERE id = ?
	`, cb.Name, cb.Endpoint, cb.Region, cb.AccessKey, cb.SecretKey, cb.BucketName, cb.QuotaBytes, cb.IsActive, cb.UpdatedAt, cb.ID)
	return err
}

func (d *DB) UpdateCacheBucketUsage(ctx context.Context, id int64, usedBytes int64) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	now := time.Now().UTC()
	_, err := d.db.ExecContext(ctx, `
		UPDATE cache_buckets
		SET used_bytes = ?, updated_at = ?
		WHERE id = ?
	`, usedBytes, now, id)
	return err
}

func (d *DB) IncrementCacheBucketUsage(ctx context.Context, id int64, deltaBytes int64) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	now := time.Now().UTC()
	_, err := d.db.ExecContext(ctx, `
		UPDATE cache_buckets
		SET used_bytes = CASE WHEN used_bytes + ? < 0 THEN 0 ELSE used_bytes + ? END, updated_at = ?
		WHERE id = ?
	`, deltaBytes, deltaBytes, now, id)
	return err
}

func (d *DB) DeleteCacheBucket(ctx context.Context, id int64) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	_, err := d.db.ExecContext(ctx, `DELETE FROM cache_buckets WHERE id = ?`, id)
	return err
}

func (d *DB) ToggleCacheBucket(ctx context.Context, id int64) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	now := time.Now().UTC()
	_, err := d.db.ExecContext(ctx, `
		UPDATE cache_buckets
		SET is_active = CASE WHEN is_active = 1 THEN 0 ELSE 1 END, updated_at = ?
		WHERE id = ?
	`, now, id)
	return err
}

func (d *DB) GetCacheBucketByID(ctx context.Context, id int64) (*models.CacheBucket, error) {
	row := d.db.QueryRowContext(ctx, `
		SELECT id, account_id, name, endpoint, region, access_key, secret_key, bucket_name, quota_bytes, used_bytes, is_active, created_at, updated_at
		FROM cache_buckets WHERE id = ?
	`, id)

	var cb models.CacheBucket
	var isActive int
	err := row.Scan(&cb.ID, &cb.AccountID, &cb.Name, &cb.Endpoint, &cb.Region, &cb.AccessKey, &cb.SecretKey, &cb.BucketName, &cb.QuotaBytes, &cb.UsedBytes, &isActive, &cb.CreatedAt, &cb.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	cb.IsActive = isActive == 1
	return &cb, nil
}

func (d *DB) ListCacheBuckets(ctx context.Context) ([]models.CacheBucket, error) {
	rows, err := d.db.QueryContext(ctx, `
		SELECT id, account_id, name, endpoint, region, access_key, secret_key, bucket_name, quota_bytes, used_bytes, is_active, created_at, updated_at
		FROM cache_buckets ORDER BY id ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []models.CacheBucket
	for rows.Next() {
		var cb models.CacheBucket
		var isActive int
		if err := rows.Scan(&cb.ID, &cb.AccountID, &cb.Name, &cb.Endpoint, &cb.Region, &cb.AccessKey, &cb.SecretKey, &cb.BucketName, &cb.QuotaBytes, &cb.UsedBytes, &isActive, &cb.CreatedAt, &cb.UpdatedAt); err != nil {
			return nil, err
		}
		cb.IsActive = isActive == 1
		result = append(result, cb)
	}
	return result, rows.Err()
}

func (d *DB) ListActiveCacheBuckets(ctx context.Context) ([]models.CacheBucket, error) {
	rows, err := d.db.QueryContext(ctx, `
		SELECT id, account_id, name, endpoint, region, access_key, secret_key, bucket_name, quota_bytes, used_bytes, is_active, created_at, updated_at
		FROM cache_buckets WHERE is_active = 1
		ORDER BY (used_bytes * 1.0 / NULLIF(quota_bytes, 0)) ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []models.CacheBucket
	for rows.Next() {
		var cb models.CacheBucket
		var isActive int
		if err := rows.Scan(&cb.ID, &cb.AccountID, &cb.Name, &cb.Endpoint, &cb.Region, &cb.AccessKey, &cb.SecretKey, &cb.BucketName, &cb.QuotaBytes, &cb.UsedBytes, &isActive, &cb.CreatedAt, &cb.UpdatedAt); err != nil {
			return nil, err
		}
		cb.IsActive = isActive == 1
		result = append(result, cb)
	}
	return result, rows.Err()
}

// --- Buckets Operations ---

func (d *DB) CreateBucket(ctx context.Context, name string) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	now := time.Now().UTC()
	_, err := d.db.ExecContext(ctx, `INSERT INTO buckets (name, created_at) VALUES (?, ?)`, name, now)
	if err == nil {
		d.bucketCache.Store(name, true)
	}
	return err
}

func (d *DB) DeleteBucket(ctx context.Context, name string) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	// Check if empty
	var count int
	err := d.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM objects WHERE bucket = ?`, name).Scan(&count)
	if err != nil {
		return err
	}
	if count > 0 {
		return fmt.Errorf("bucket is not empty (%d objects)", count)
	}

	res, err := d.db.ExecContext(ctx, `DELETE FROM buckets WHERE name = ?`, name)
	if err != nil {
		return err
	}
	rowsAffected, _ := res.RowsAffected()
	if rowsAffected == 0 {
		return ErrNotFound
	}
	d.bucketCache.Delete(name)
	return nil
}

func (d *DB) BucketExists(ctx context.Context, name string) (bool, error) {
	if _, ok := d.bucketCache.Load(name); ok {
		return true, nil
	}

	var count int
	err := d.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM buckets WHERE name = ?`, name).Scan(&count)
	if err == nil && count > 0 {
		d.bucketCache.Store(name, true)
		return true, nil
	}
	return false, err
}

func (d *DB) ListBuckets(ctx context.Context) ([]models.Bucket, error) {
	rows, err := d.db.QueryContext(ctx, `SELECT id, name, created_at FROM buckets ORDER BY name ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var buckets []models.Bucket
	for rows.Next() {
		var b models.Bucket
		if err := rows.Scan(&b.ID, &b.Name, &b.CreatedAt); err != nil {
			return nil, err
		}
		buckets = append(buckets, b)
		d.bucketCache.Store(b.Name, true)
	}
	return buckets, rows.Err()
}

// --- Objects & Chunks Operations ---

func (d *DB) SaveObjectWithChunks(ctx context.Context, obj *models.Object, chunks []models.Chunk) error {
	return d.SaveObjectWithChunksAndLocations(ctx, obj, chunks, nil)
}

func (d *DB) SaveObjectWithChunksAndLocations(ctx context.Context, obj *models.Object, chunks []models.Chunk, locations []models.ObjectLocation) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	now := time.Now().UTC()
	obj.CreatedAt = now
	obj.UpdatedAt = now

	var customMetaStr string
	if len(obj.CustomMetadata) > 0 {
		metaBytes, _ := json.Marshal(obj.CustomMetadata)
		customMetaStr = string(metaBytes)
	}

	// Delete old object chunks and locations if object previously existed
	var existingID int64
	err = tx.QueryRowContext(ctx, `SELECT id FROM objects WHERE bucket = ? AND key = ?`, obj.Bucket, obj.Key).Scan(&existingID)
	if err == nil {
		if _, err := tx.ExecContext(ctx, `DELETE FROM chunks WHERE object_id = ?`, existingID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM object_locations WHERE object_id = ?`, existingID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM objects WHERE id = ?`, existingID); err != nil {
			return err
		}
	}

	res, err := tx.ExecContext(ctx, `
		INSERT INTO objects (bucket, key, size, etag, content_type, custom_metadata, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, obj.Bucket, obj.Key, obj.Size, obj.ETag, obj.ContentType, customMetaStr, obj.CreatedAt, obj.UpdatedAt)
	if err != nil {
		return err
	}

	objID, err := res.LastInsertId()
	if err != nil {
		return err
	}
	obj.ID = objID

	for i := range chunks {
		chunks[i].ObjectID = objID
		chunks[i].CreatedAt = now
		_, err := tx.ExecContext(ctx, `
			INSERT INTO chunks (object_id, part_number, chunk_index, offset_bytes, size_bytes, cipher_size_bytes, account_id, remote_path, sha256_hash, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, objID, chunks[i].PartNumber, chunks[i].ChunkIndex, chunks[i].OffsetBytes, chunks[i].SizeBytes, chunks[i].CipherSizeBytes, chunks[i].AccountID, chunks[i].RemotePath, chunks[i].Sha256Hash, chunks[i].CreatedAt)
		if err != nil {
			return err
		}
	}

	for i := range locations {
		loc := &locations[i]
		loc.ObjectID = objID
		if loc.CreatedAt.IsZero() {
			loc.CreatedAt = now
		}
		if loc.LastAccessedAt.IsZero() {
			loc.LastAccessedAt = now
		}
		isEnc := 0
		if loc.IsEncrypted {
			isEnc = 1
		}
		_, err := tx.ExecContext(ctx, `
			INSERT INTO object_locations (object_id, tier, account_id, remote_path, is_encrypted, size_bytes, access_count, last_accessed_at, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, objID, string(loc.Tier), loc.AccountID, loc.RemotePath, isEnc, loc.SizeBytes, loc.AccessCount, loc.LastAccessedAt, loc.CreatedAt)
		if err != nil {
			return err
		}
	}

	// Fallback: If no explicit cold location but chunks exist, add a cold location representation
	if len(locations) == 0 && len(chunks) > 0 {
		_, _ = tx.ExecContext(ctx, `
			INSERT INTO object_locations (object_id, tier, account_id, remote_path, is_encrypted, size_bytes, access_count, last_accessed_at, created_at)
			VALUES (?, ?, ?, ?, 1, ?, 0, ?, ?)
		`, objID, string(models.TierCold), chunks[0].AccountID, chunks[0].RemotePath, obj.Size, now, now)
	}

	return tx.Commit()
}

func (d *DB) GetObjectWithChunks(ctx context.Context, bucket, key string) (*models.Object, []models.Chunk, error) {
	var obj models.Object
	var customMetaStr sql.NullString

	row := d.db.QueryRowContext(ctx, `
		SELECT id, bucket, key, size, etag, content_type, custom_metadata, created_at, updated_at
		FROM objects WHERE bucket = ? AND key = ?
	`, bucket, key)

	err := row.Scan(&obj.ID, &obj.Bucket, &obj.Key, &obj.Size, &obj.ETag, &obj.ContentType, &customMetaStr, &obj.CreatedAt, &obj.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil, ErrNotFound
	}
	if err != nil {
		return nil, nil, err
	}

	if customMetaStr.Valid && customMetaStr.String != "" {
		_ = json.Unmarshal([]byte(customMetaStr.String), &obj.CustomMetadata)
	}

	rows, err := d.db.QueryContext(ctx, `
		SELECT id, object_id, part_number, chunk_index, offset_bytes, size_bytes, cipher_size_bytes, account_id, remote_path, sha256_hash, created_at
		FROM chunks WHERE object_id = ? ORDER BY offset_bytes ASC, chunk_index ASC
	`, obj.ID)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	var chunks []models.Chunk
	for rows.Next() {
		var c models.Chunk
		if err := rows.Scan(&c.ID, &c.ObjectID, &c.PartNumber, &c.ChunkIndex, &c.OffsetBytes, &c.SizeBytes, &c.CipherSizeBytes, &c.AccountID, &c.RemotePath, &c.Sha256Hash, &c.CreatedAt); err != nil {
			return nil, nil, err
		}
		chunks = append(chunks, c)
	}

	_ = d.PopulateObjectTiers(ctx, &obj)

	return &obj, chunks, rows.Err()
}

func (d *DB) HeadObject(ctx context.Context, bucket, key string) (*models.Object, error) {
	var obj models.Object
	var customMetaStr sql.NullString

	row := d.db.QueryRowContext(ctx, `
		SELECT id, bucket, key, size, etag, content_type, custom_metadata, created_at, updated_at
		FROM objects WHERE bucket = ? AND key = ?
	`, bucket, key)

	err := row.Scan(&obj.ID, &obj.Bucket, &obj.Key, &obj.Size, &obj.ETag, &obj.ContentType, &customMetaStr, &obj.CreatedAt, &obj.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	if customMetaStr.Valid && customMetaStr.String != "" {
		_ = json.Unmarshal([]byte(customMetaStr.String), &obj.CustomMetadata)
	}

	_ = d.PopulateObjectTiers(ctx, &obj)

	return &obj, nil
}

func (d *DB) DeleteObject(ctx context.Context, bucket, key string) ([]models.Chunk, error) {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	var objID int64
	err := d.db.QueryRowContext(ctx, `SELECT id FROM objects WHERE bucket = ? AND key = ?`, bucket, key).Scan(&objID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	// Fetch chunks before deletion so storage manager can remove them from HF
	rows, err := d.db.QueryContext(ctx, `
		SELECT id, object_id, part_number, chunk_index, offset_bytes, size_bytes, cipher_size_bytes, account_id, remote_path, sha256_hash, created_at
		FROM chunks WHERE object_id = ?
	`, objID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var chunks []models.Chunk
	for rows.Next() {
		var c models.Chunk
		if err := rows.Scan(&c.ID, &c.ObjectID, &c.PartNumber, &c.ChunkIndex, &c.OffsetBytes, &c.SizeBytes, &c.CipherSizeBytes, &c.AccountID, &c.RemotePath, &c.Sha256Hash, &c.CreatedAt); err != nil {
			return nil, err
		}
		chunks = append(chunks, c)
	}

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `DELETE FROM chunks WHERE object_id = ?`, objID); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM object_locations WHERE object_id = ?`, objID); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM objects WHERE id = ?`, objID); err != nil {
		return nil, err
	}

	return chunks, tx.Commit()
}

func (d *DB) ListObjects(ctx context.Context, bucket, prefix, startAfter string, limit int) ([]models.Object, error) {
	query := `
		SELECT id, bucket, key, size, etag, content_type, custom_metadata, created_at, updated_at
		FROM objects
		WHERE bucket = ? AND key LIKE ? AND key > ?
		ORDER BY key ASC
		LIMIT ?
	`
	prefixPattern := prefix + "%"
	rows, err := d.db.QueryContext(ctx, query, bucket, prefixPattern, startAfter, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var objects []models.Object
	for rows.Next() {
		var obj models.Object
		var customMetaStr sql.NullString
		if err := rows.Scan(&obj.ID, &obj.Bucket, &obj.Key, &obj.Size, &obj.ETag, &obj.ContentType, &customMetaStr, &obj.CreatedAt, &obj.UpdatedAt); err != nil {
			return nil, err
		}
		if customMetaStr.Valid && customMetaStr.String != "" {
			_ = json.Unmarshal([]byte(customMetaStr.String), &obj.CustomMetadata)
		}
		_ = d.PopulateObjectTiers(ctx, &obj)
		objects = append(objects, obj)
	}
	return objects, rows.Err()
}

// --- Multi-Tier Object Locations ---

func (d *DB) SaveObjectLocation(ctx context.Context, loc *models.ObjectLocation) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	now := time.Now().UTC()
	if loc.CreatedAt.IsZero() {
		loc.CreatedAt = now
	}
	if loc.LastAccessedAt.IsZero() {
		loc.LastAccessedAt = now
	}
	isEnc := 0
	if loc.IsEncrypted {
		isEnc = 1
	}

	res, err := d.db.ExecContext(ctx, `
		INSERT INTO object_locations (object_id, tier, account_id, remote_path, is_encrypted, size_bytes, access_count, last_accessed_at, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(object_id, tier, remote_path) DO UPDATE SET
			size_bytes = excluded.size_bytes,
			last_accessed_at = excluded.last_accessed_at,
			access_count = object_locations.access_count + 1
	`, loc.ObjectID, string(loc.Tier), loc.AccountID, loc.RemotePath, isEnc, loc.SizeBytes, loc.AccessCount, loc.LastAccessedAt, loc.CreatedAt)
	if err != nil {
		return err
	}
	id, err := res.LastInsertId()
	if err == nil && id > 0 {
		loc.ID = id
	}
	return nil
}

func (d *DB) GetObjectLocations(ctx context.Context, objectID int64) ([]models.ObjectLocation, error) {
	rows, err := d.db.QueryContext(ctx, `
		SELECT id, object_id, tier, account_id, remote_path, is_encrypted, size_bytes, access_count, last_accessed_at, created_at
		FROM object_locations WHERE object_id = ? ORDER BY id ASC
	`, objectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var locs []models.ObjectLocation
	for rows.Next() {
		var l models.ObjectLocation
		var tierStr string
		var isEnc int
		if err := rows.Scan(&l.ID, &l.ObjectID, &tierStr, &l.AccountID, &l.RemotePath, &isEnc, &l.SizeBytes, &l.AccessCount, &l.LastAccessedAt, &l.CreatedAt); err != nil {
			return nil, err
		}
		l.Tier = models.TierType(tierStr)
		l.IsEncrypted = isEnc == 1
		locs = append(locs, l)
	}
	return locs, rows.Err()
}

func (d *DB) GetObjectLocationByTier(ctx context.Context, objectID int64, tier models.TierType) (*models.ObjectLocation, error) {
	row := d.db.QueryRowContext(ctx, `
		SELECT id, object_id, tier, account_id, remote_path, is_encrypted, size_bytes, access_count, last_accessed_at, created_at
		FROM object_locations WHERE object_id = ? AND tier = ? LIMIT 1
	`, objectID, string(tier))

	var l models.ObjectLocation
	var tierStr string
	var isEnc int
	err := row.Scan(&l.ID, &l.ObjectID, &tierStr, &l.AccountID, &l.RemotePath, &isEnc, &l.SizeBytes, &l.AccessCount, &l.LastAccessedAt, &l.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	l.Tier = models.TierType(tierStr)
	l.IsEncrypted = isEnc == 1
	return &l, nil
}

func (d *DB) DeleteObjectLocationByTier(ctx context.Context, objectID int64, tier models.TierType) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	_, err := d.db.ExecContext(ctx, `DELETE FROM object_locations WHERE object_id = ? AND tier = ?`, objectID, string(tier))
	return err
}

func (d *DB) RecordLocationAccess(ctx context.Context, locationID int64) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	now := time.Now().UTC()
	_, err := d.db.ExecContext(ctx, `
		UPDATE object_locations
		SET access_count = access_count + 1, last_accessed_at = ?
		WHERE id = ?
	`, now, locationID)
	return err
}

func (d *DB) ListLRUCacheLocations(ctx context.Context, limit int) ([]models.ObjectLocation, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := d.db.QueryContext(ctx, `
		SELECT id, object_id, tier, account_id, remote_path, is_encrypted, size_bytes, access_count, last_accessed_at, created_at
		FROM object_locations
		WHERE tier = 'cache'
		ORDER BY last_accessed_at ASC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var locs []models.ObjectLocation
	for rows.Next() {
		var l models.ObjectLocation
		var tierStr string
		var isEnc int
		if err := rows.Scan(&l.ID, &l.ObjectID, &tierStr, &l.AccountID, &l.RemotePath, &isEnc, &l.SizeBytes, &l.AccessCount, &l.LastAccessedAt, &l.CreatedAt); err != nil {
			return nil, err
		}
		l.Tier = models.TierType(tierStr)
		l.IsEncrypted = isEnc == 1
		locs = append(locs, l)
	}
	return locs, rows.Err()
}

func (d *DB) PopulateObjectTiers(ctx context.Context, obj *models.Object) error {
	if obj == nil || obj.ID == 0 {
		return nil
	}
	locs, err := d.GetObjectLocations(ctx, obj.ID)
	if err != nil {
		return err
	}
	obj.Locations = locs
	for _, l := range locs {
		if l.Tier == models.TierCache {
			obj.HasCache = true
		}
		if l.Tier == models.TierCold {
			obj.HasCold = true
		}
	}
	if !obj.HasCold {
		var chunkCount int
		_ = d.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM chunks WHERE object_id = ?`, obj.ID).Scan(&chunkCount)
		if chunkCount > 0 {
			obj.HasCold = true
		}
	}
	return nil
}

// --- Multipart Uploads ---

func (d *DB) CreateMultipartUpload(ctx context.Context, mp *models.MultipartUpload) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	mp.InitiatedAt = time.Now().UTC()
	_, err := d.db.ExecContext(ctx, `
		INSERT INTO multipart_uploads (upload_id, bucket, key, content_type, initiated_at)
		VALUES (?, ?, ?, ?, ?)
	`, mp.UploadID, mp.Bucket, mp.Key, mp.ContentType, mp.InitiatedAt)
	return err
}

func (d *DB) GetMultipartUpload(ctx context.Context, uploadID string) (*models.MultipartUpload, error) {
	var mp models.MultipartUpload
	row := d.db.QueryRowContext(ctx, `
		SELECT id, upload_id, bucket, key, content_type, initiated_at
		FROM multipart_uploads WHERE upload_id = ?
	`, uploadID)
	err := row.Scan(&mp.ID, &mp.UploadID, &mp.Bucket, &mp.Key, &mp.ContentType, &mp.InitiatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return &mp, err
}

func (d *DB) SaveMultipartPart(ctx context.Context, part *models.MultipartPart) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	part.UploadedAt = time.Now().UTC()
	_, err := d.db.ExecContext(ctx, `
		INSERT OR REPLACE INTO multipart_parts (upload_id, part_number, etag, size_bytes, chunks_json, uploaded_at)
		VALUES (?, ?, ?, ?, ?, ?)
	`, part.UploadID, part.PartNumber, part.ETag, part.SizeBytes, part.ChunksJSON, part.UploadedAt)
	return err
}

func (d *DB) ListMultipartParts(ctx context.Context, uploadID string) ([]models.MultipartPart, error) {
	rows, err := d.db.QueryContext(ctx, `
		SELECT id, upload_id, part_number, etag, size_bytes, chunks_json, uploaded_at
		FROM multipart_parts WHERE upload_id = ? ORDER BY part_number ASC
	`, uploadID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var parts []models.MultipartPart
	for rows.Next() {
		var p models.MultipartPart
		if err := rows.Scan(&p.ID, &p.UploadID, &p.PartNumber, &p.ETag, &p.SizeBytes, &p.ChunksJSON, &p.UploadedAt); err != nil {
			return nil, err
		}
		parts = append(parts, p)
	}
	return parts, rows.Err()
}

func (d *DB) AbortMultipartUpload(ctx context.Context, uploadID string) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `DELETE FROM multipart_parts WHERE upload_id = ?`, uploadID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM multipart_uploads WHERE upload_id = ?`, uploadID); err != nil {
		return err
	}

	return tx.Commit()
}

// --- System Settings ---

func (d *DB) GetSetting(ctx context.Context, key string) (string, error) {
	var val string
	err := d.db.QueryRowContext(ctx, `SELECT value FROM system_settings WHERE key = ?`, key).Scan(&val)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrNotFound
	}
	return val, err
}

func (d *DB) SetSetting(ctx context.Context, key, value string) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	_, err := d.db.ExecContext(ctx, `
		INSERT OR REPLACE INTO system_settings (key, value) VALUES (?, ?)
	`, key, value)
	return err
}

// --- Overall Stats ---

func (d *DB) GetStats(ctx context.Context) (*models.PoolStats, error) {
	var stats models.PoolStats

	// Accounts aggregation
	row := d.db.QueryRowContext(ctx, `
		SELECT 
			COALESCE(SUM(quota_bytes), 0),
			COALESCE(SUM(used_bytes), 0),
			COUNT(*),
			COALESCE(SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END), 0)
		FROM accounts
	`)
	if err := row.Scan(&stats.TotalCapacityBytes, &stats.TotalUsedBytes, &stats.TotalAccounts, &stats.ActiveAccounts); err != nil {
		return nil, err
	}
	stats.TotalFreeBytes = stats.TotalCapacityBytes - stats.TotalUsedBytes
	if stats.TotalFreeBytes < 0 {
		stats.TotalFreeBytes = 0
	}

	// Buckets count
	_ = d.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM buckets`).Scan(&stats.TotalBuckets)

	// Objects count
	_ = d.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM objects`).Scan(&stats.TotalObjects)

	// Cached vs Cold objects count
	_ = d.db.QueryRowContext(ctx, `SELECT COUNT(DISTINCT object_id) FROM object_locations WHERE tier = 'cache'`).Scan(&stats.CachedObjects)
	_ = d.db.QueryRowContext(ctx, `SELECT COUNT(DISTINCT object_id) FROM object_locations WHERE tier = 'cold'`).Scan(&stats.ColdObjects)
	if stats.ColdObjects == 0 && stats.TotalObjects > 0 {
		stats.ColdObjects = stats.TotalObjects
	}

	// Cache Buckets aggregation (Tier 1 S3 Cache)
	rowCB := d.db.QueryRowContext(ctx, `
		SELECT 
			COUNT(*),
			COALESCE(SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(quota_bytes), 0),
			COALESCE(SUM(used_bytes), 0)
		FROM cache_buckets
	`)
	_ = rowCB.Scan(&stats.TotalCacheBuckets, &stats.ActiveCacheBuckets, &stats.TotalCacheCapacityBytes, &stats.TotalCacheUsedBytes)

	return &stats, nil
}

// ExportBackup creates a self-contained, fully checkpointed, consistent SQLite database backup and streams it to w.
func (d *DB) ExportBackup(ctx context.Context, w io.Writer) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	// Force WAL checkpoint to flush pending transactions
	_, _ = d.db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE);")

	// Vacuum into temporary file for a consistent snapshot
	tempFile, err := os.CreateTemp("", "hf2s3_backup_*.db")
	if err != nil {
		return fmt.Errorf("create temp backup file: %w", err)
	}
	tempPath := tempFile.Name()
	_ = tempFile.Close()
	_ = os.Remove(tempPath)
	defer os.Remove(tempPath)

	normalizedPath := filepath.ToSlash(tempPath)
	_, err = d.db.ExecContext(ctx, fmt.Sprintf("VACUUM INTO '%s';", normalizedPath))
	if err != nil {
		// Fallback: If VACUUM INTO fails, copy checkpointed source file directly if on disk
		if d.filePath != "" && d.filePath != ":memory:" && !strings.HasPrefix(d.filePath, "file:") {
			src, openErr := os.Open(d.filePath)
			if openErr != nil {
				return fmt.Errorf("fallback open db file %s: %w", d.filePath, openErr)
			}
			defer src.Close()
			_, copyErr := io.Copy(w, src)
			return copyErr
		}
		return fmt.Errorf("vacuum into: %w", err)
	}

	src, err := os.Open(tempPath)
	if err != nil {
		return fmt.Errorf("open vacuumed backup file: %w", err)
	}
	defer src.Close()

	_, err = io.Copy(w, src)
	return err
}

// ImportRestore replaces the database file with an uploaded backup and reinitializes connections.
func (d *DB) ImportRestore(ctx context.Context, r io.Reader) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	if d.filePath == "" || d.filePath == ":memory:" || strings.HasPrefix(d.filePath, "file:") {
		return errors.New("database restore is only supported for file-based databases")
	}

	tempFile, err := os.CreateTemp("", "hf2s3_restore_*.db")
	if err != nil {
		return fmt.Errorf("create temp restore file: %w", err)
	}
	tempPath := tempFile.Name()
	defer os.Remove(tempPath)

	if _, err := io.Copy(tempFile, r); err != nil {
		_ = tempFile.Close()
		return fmt.Errorf("write uploaded db: %w", err)
	}
	_ = tempFile.Close()

	// Verify the uploaded database is valid SQLite by checking accounts table
	testDB, err := sql.Open("sqlite", tempPath)
	if err != nil {
		return fmt.Errorf("invalid sqlite database: %w", err)
	}
	var testCount int
	err = testDB.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='accounts'").Scan(&testCount)
	_ = testDB.Close()
	if err != nil || testCount == 0 {
		return errors.New("uploaded file is not a valid HF2S3 database (missing accounts table)")
	}

	// Close current connection
	_ = d.db.Close()

	// Remove old WAL and SHM files
	_ = os.Remove(d.filePath + "-wal")
	_ = os.Remove(d.filePath + "-shm")

	src, err := os.Open(tempPath)
	if err != nil {
		return fmt.Errorf("open verified db: %w", err)
	}
	defer src.Close()

	dst, err := os.OpenFile(d.filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("open target db for writing: %w", err)
	}
	defer dst.Close()

	if _, err := io.Copy(dst, src); err != nil {
		return fmt.Errorf("copy restored db: %w", err)
	}

	// Reopen database connection
	connStr := d.filePath
	pragmas := "_pragma=busy_timeout(30000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)&_pragma=synchronous(NORMAL)&_pragma=cache_size(-64000)"
	if !strings.Contains(connStr, "?") {
		connStr += "?" + pragmas
	} else {
		connStr += "&" + pragmas
	}

	database, err := sql.Open("sqlite", connStr)
	if err != nil {
		return fmt.Errorf("re-open sqlite db: %w", err)
	}

	maxConns := runtime.NumCPU() * 4
	if maxConns < 8 {
		maxConns = 8
	}
	database.SetMaxOpenConns(maxConns)
	database.SetMaxIdleConns(maxConns / 2)
	database.SetConnMaxLifetime(1 * time.Hour)

	d.db = database

	// Reload bucket cache
	d.bucketCache.Range(func(key, value interface{}) bool {
		d.bucketCache.Delete(key)
		return true
	})
	if buckets, err := d.ListBuckets(ctx); err == nil {
		for _, b := range buckets {
			d.bucketCache.Store(b.Name, true)
		}
	}

	return nil
}

