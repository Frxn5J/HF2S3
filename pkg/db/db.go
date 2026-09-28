package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"hf2s3/pkg/crypto"
	"hf2s3/pkg/models"
	_ "modernc.org/sqlite"
)

var ErrNotFound = errors.New("record not found")

type DB struct {
	db          *sql.DB
	filePath    string
	writeMu     sync.Mutex
	bucketCache sync.Map
	box         *crypto.SecretBox // seals tokens/secrets at rest; nil = plaintext (tests, pre-migration)
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

	var metaJSON string
	if len(mp.Metadata) > 0 {
		b, _ := json.Marshal(mp.Metadata)
		metaJSON = string(b)
	}

	mp.InitiatedAt = time.Now().UTC()
	_, err := d.db.ExecContext(ctx, `
		INSERT INTO multipart_uploads (upload_id, bucket, key, content_type, metadata_json, initiated_at)
		VALUES (?, ?, ?, ?, ?, ?)
	`, mp.UploadID, mp.Bucket, mp.Key, mp.ContentType, metaJSON, mp.InitiatedAt)
	return err
}

func (d *DB) GetMultipartUpload(ctx context.Context, uploadID string) (*models.MultipartUpload, error) {
	var mp models.MultipartUpload
	var metaJSON string
	row := d.db.QueryRowContext(ctx, `
		SELECT id, upload_id, bucket, key, content_type, metadata_json, initiated_at
		FROM multipart_uploads WHERE upload_id = ?
	`, uploadID)
	err := row.Scan(&mp.ID, &mp.UploadID, &mp.Bucket, &mp.Key, &mp.ContentType, &metaJSON, &mp.InitiatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if metaJSON != "" {
		_ = json.Unmarshal([]byte(metaJSON), &mp.Metadata)
	}
	return &mp, nil
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

// sensitiveSettings are encrypted at rest when a SecretBox is configured.
// admin_password is not listed: it is stored as a salted hash instead.
var sensitiveSettings = map[string]bool{
	"secret_access_key":     true,
	"legacy_master_keys":    true,
	"key_check":             true,
	"hf_storage_secret_key": true,
}

func (d *DB) GetSetting(ctx context.Context, key string) (string, error) {
	var val string
	err := d.db.QueryRowContext(ctx, `SELECT value FROM system_settings WHERE key = ?`, key).Scan(&val)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", err
	}
	if sensitiveSettings[key] {
		return d.open(val)
	}
	return val, nil
}

func (d *DB) SetSetting(ctx context.Context, key, value string) error {
	if sensitiveSettings[key] {
		sealed, err := d.seal(value)
		if err != nil {
			return err
		}
		value = sealed
	}

	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	_, err := d.db.ExecContext(ctx, `
		INSERT OR REPLACE INTO system_settings (key, value) VALUES (?, ?)
	`, key, value)
	return err
}

// DeleteSetting removes a setting (used to purge the legacy plaintext master key).
func (d *DB) DeleteSetting(ctx context.Context, key string) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	_, err := d.db.ExecContext(ctx, `DELETE FROM system_settings WHERE key = ?`, key)
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

	_ = d.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pending_deletions`).Scan(&stats.PendingDeletions)

	return &stats, nil
}
