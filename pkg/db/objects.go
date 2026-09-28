package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"hf2s3/pkg/crypto"
	"hf2s3/pkg/models"
)

// ErrBucketMissing is returned when an object is saved into a bucket that no
// longer exists (e.g. it was deleted while the upload was in flight).
var ErrBucketMissing = errors.New("bucket does not exist")

// --- Objects & Chunks Operations ---

func (d *DB) SaveObjectWithChunks(ctx context.Context, obj *models.Object, chunks []models.Chunk) error {
	return d.SaveObjectWithChunksAndLocations(ctx, obj, chunks, nil)
}

const chunkColumns = `id, object_id, part_number, chunk_index, offset_bytes, size_bytes, cipher_size_bytes, account_id, remote_path, sha256_hash, enc_version, key_id, created_at`

func scanChunk(sc interface{ Scan(...any) error }) (models.Chunk, error) {
	var c models.Chunk
	err := sc.Scan(&c.ID, &c.ObjectID, &c.PartNumber, &c.ChunkIndex, &c.OffsetBytes, &c.SizeBytes, &c.CipherSizeBytes, &c.AccountID, &c.RemotePath, &c.Sha256Hash, &c.EncVersion, &c.KeyID, &c.CreatedAt)
	return c, err
}

// enqueueObjectRemotesTx queues every remote copy of objectID (HF chunks and
// cache copies) for deletion. It runs inside the caller's transaction so the
// rows and the queue entries change atomically: a crash can never orphan them.
func enqueueObjectRemotesTx(ctx context.Context, tx *sql.Tx, objectID int64, now time.Time) error {
	if _, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO pending_deletions (kind, account_id, remote_path, size_bytes, next_attempt_at, created_at)
		SELECT 'chunk', account_id, remote_path, cipher_size_bytes, ?, ? FROM chunks WHERE object_id = ?
	`, now, now, objectID); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO pending_deletions (kind, account_id, remote_path, size_bytes, next_attempt_at, created_at)
		SELECT 'cache', account_id, remote_path, size_bytes, ?, ? FROM object_locations WHERE object_id = ? AND tier = 'cache'
	`, now, now, objectID)
	return err
}

// SaveObjectWithChunksAndLocations stores (or atomically replaces) an object.
// If the key already existed, its old chunks and cache copies are queued in
// pending_deletions within the same transaction.
func (d *DB) SaveObjectWithChunksAndLocations(ctx context.Context, obj *models.Object, chunks []models.Chunk, locations []models.ObjectLocation) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// The bucket may have been deleted while a long upload was in flight.
	var bucketID int64
	if err := tx.QueryRowContext(ctx, `SELECT id FROM buckets WHERE name = ?`, obj.Bucket).Scan(&bucketID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrBucketMissing
		}
		return err
	}

	now := time.Now().UTC()
	obj.CreatedAt = now
	obj.UpdatedAt = now

	var customMetaStr string
	if len(obj.CustomMetadata) > 0 {
		metaBytes, _ := json.Marshal(obj.CustomMetadata)
		customMetaStr = string(metaBytes)
	}

	// Replace any previous version, queueing its remote copies for deletion.
	var existingID int64
	err = tx.QueryRowContext(ctx, `SELECT id FROM objects WHERE bucket = ? AND key = ?`, obj.Bucket, obj.Key).Scan(&existingID)
	if err == nil {
		if err := enqueueObjectRemotesTx(ctx, tx, existingID, now); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM chunks WHERE object_id = ?`, existingID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM object_locations WHERE object_id = ?`, existingID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM objects WHERE id = ?`, existingID); err != nil {
			return err
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
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
		c := &chunks[i]
		c.ObjectID = objID
		c.CreatedAt = now
		if c.EncVersion == 0 {
			c.EncVersion = crypto.EncV1
			c.KeyID = crypto.LegacyKeyID
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO chunks (object_id, part_number, chunk_index, offset_bytes, size_bytes, cipher_size_bytes, account_id, remote_path, sha256_hash, enc_version, key_id, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, objID, c.PartNumber, c.ChunkIndex, c.OffsetBytes, c.SizeBytes, c.CipherSizeBytes, c.AccountID, c.RemotePath, c.Sha256Hash, c.EncVersion, c.KeyID, c.CreatedAt); err != nil {
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
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO object_locations (object_id, tier, account_id, remote_path, is_encrypted, size_bytes, access_count, last_accessed_at, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, objID, string(loc.Tier), loc.AccountID, loc.RemotePath, isEnc, loc.SizeBytes, loc.AccessCount, loc.LastAccessedAt, loc.CreatedAt); err != nil {
			return err
		}
	}

	// If no explicit cold location was given but chunks exist, record one.
	if len(locations) == 0 && len(chunks) > 0 {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO object_locations (object_id, tier, account_id, remote_path, is_encrypted, size_bytes, access_count, last_accessed_at, created_at)
			VALUES (?, ?, ?, ?, 1, ?, 0, ?, ?)
		`, objID, string(models.TierCold), chunks[0].AccountID, chunks[0].RemotePath, obj.Size, now, now); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (d *DB) GetObjectWithChunks(ctx context.Context, bucket, key string) (*models.Object, []models.Chunk, error) {
	obj, err := d.HeadObject(ctx, bucket, key)
	if err != nil {
		return nil, nil, err
	}

	rows, err := d.db.QueryContext(ctx, `
		SELECT `+chunkColumns+`
		FROM chunks WHERE object_id = ? ORDER BY offset_bytes ASC, chunk_index ASC
	`, obj.ID)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	var chunks []models.Chunk
	for rows.Next() {
		c, err := scanChunk(rows)
		if err != nil {
			return nil, nil, err
		}
		chunks = append(chunks, c)
	}
	return obj, chunks, rows.Err()
}

const objectColumns = `id, bucket, key, size, etag, content_type, custom_metadata, created_at, updated_at`

func scanObject(sc interface{ Scan(...any) error }) (models.Object, error) {
	var obj models.Object
	var customMetaStr sql.NullString
	if err := sc.Scan(&obj.ID, &obj.Bucket, &obj.Key, &obj.Size, &obj.ETag, &obj.ContentType, &customMetaStr, &obj.CreatedAt, &obj.UpdatedAt); err != nil {
		return obj, err
	}
	if customMetaStr.Valid && customMetaStr.String != "" {
		_ = json.Unmarshal([]byte(customMetaStr.String), &obj.CustomMetadata)
	}
	return obj, nil
}

func (d *DB) HeadObject(ctx context.Context, bucket, key string) (*models.Object, error) {
	obj, err := scanObject(d.db.QueryRowContext(ctx, `SELECT `+objectColumns+` FROM objects WHERE bucket = ? AND key = ?`, bucket, key))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	_ = d.PopulateObjectTiers(ctx, &obj)
	return &obj, nil
}

// DeleteObject removes an object and queues its remote copies for deletion in
// the same transaction. It returns the removed chunks for informational use;
// the actual remote deletion is performed by the pending-deletion worker.
func (d *DB) DeleteObject(ctx context.Context, bucket, key string) ([]models.Chunk, error) {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var objID int64
	err = tx.QueryRowContext(ctx, `SELECT id FROM objects WHERE bucket = ? AND key = ?`, bucket, key).Scan(&objID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	rows, err := tx.QueryContext(ctx, `SELECT `+chunkColumns+` FROM chunks WHERE object_id = ?`, objID)
	if err != nil {
		return nil, err
	}
	var chunks []models.Chunk
	for rows.Next() {
		c, err := scanChunk(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		chunks = append(chunks, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if err := enqueueObjectRemotesTx(ctx, tx, objID, time.Now().UTC()); err != nil {
		return nil, err
	}
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

// ListObjects returns up to limit objects with key > startAfter under prefix.
func (d *DB) ListObjects(ctx context.Context, bucket, prefix, startAfter string, limit int) ([]models.Object, error) {
	return d.ListObjectsPage(ctx, bucket, prefix, startAfter, false, limit)
}

// prefixUpperBound returns the smallest string greater than every string that
// starts with prefix (ok=false when there is none, i.e. prefix is empty/0xFF..).
func prefixUpperBound(prefix string) (string, bool) {
	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xFF {
			b[i]++
			return string(b[:i+1]), true
		}
	}
	return "", false
}

// ListObjectsPage lists keys in binary order. The prefix filter is a range
// scan (not LIKE), so '%' and '_' are literal and matching is case-sensitive.
// after is exclusive unless inclusive is true.
func (d *DB) ListObjectsPage(ctx context.Context, bucket, prefix, after string, inclusive bool, limit int) ([]models.Object, error) {
	q := `SELECT ` + objectColumns + ` FROM objects WHERE bucket = ?`
	args := []any{bucket}
	if prefix != "" {
		q += ` AND key >= ?`
		args = append(args, prefix)
		if upper, ok := prefixUpperBound(prefix); ok {
			q += ` AND key < ?`
			args = append(args, upper)
		}
	}
	if after != "" {
		if inclusive {
			q += ` AND key >= ?`
		} else {
			q += ` AND key > ?`
		}
		args = append(args, after)
	}
	q += ` ORDER BY key ASC LIMIT ?`
	args = append(args, limit)

	rows, err := d.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var objects []models.Object
	for rows.Next() {
		obj, err := scanObject(rows)
		if err != nil {
			return nil, err
		}
		objects = append(objects, obj)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()

	_ = d.populateTiersBatch(ctx, objects)
	return objects, nil
}

// populateTiersBatch fills Locations/HasCache/HasCold for many objects with a
// constant number of queries (no N+1).
func (d *DB) populateTiersBatch(ctx context.Context, objs []models.Object) error {
	if len(objs) == 0 {
		return nil
	}
	idx := make(map[int64]int, len(objs))
	ph := make([]string, len(objs))
	args := make([]any, len(objs))
	for i := range objs {
		idx[objs[i].ID] = i
		ph[i] = "?"
		args[i] = objs[i].ID
	}
	in := strings.Join(ph, ",")

	rows, err := d.db.QueryContext(ctx, `
		SELECT id, object_id, tier, account_id, remote_path, is_encrypted, size_bytes, access_count, last_accessed_at, created_at
		FROM object_locations WHERE object_id IN (`+in+`) ORDER BY id ASC`, args...)
	if err != nil {
		return err
	}
	for rows.Next() {
		var l models.ObjectLocation
		var tierStr string
		var isEnc int
		if err := rows.Scan(&l.ID, &l.ObjectID, &tierStr, &l.AccountID, &l.RemotePath, &isEnc, &l.SizeBytes, &l.AccessCount, &l.LastAccessedAt, &l.CreatedAt); err != nil {
			rows.Close()
			return err
		}
		l.Tier = models.TierType(tierStr)
		l.IsEncrypted = isEnc == 1
		o := &objs[idx[l.ObjectID]]
		o.Locations = append(o.Locations, l)
		switch l.Tier {
		case models.TierCache:
			o.HasCache = true
		case models.TierCold:
			o.HasCold = true
		}
	}
	rows.Close()

	crows, err := d.db.QueryContext(ctx, `SELECT DISTINCT object_id FROM chunks WHERE object_id IN (`+in+`)`, args...)
	if err != nil {
		return err
	}
	defer crows.Close()
	for crows.Next() {
		var id int64
		if err := crows.Scan(&id); err != nil {
			return err
		}
		objs[idx[id]].HasCold = true
	}
	return crows.Err()
}

// PrefixUpperBound returns the smallest string greater than every string that
// starts with prefix (ok=false when there is none, i.e. the prefix is empty).
func PrefixUpperBound(prefix string) (string, bool) {
	return prefixUpperBound(prefix)
}

// BucketUsage is the object count and total size of one bucket.
type BucketUsage struct {
	Objects int64
	Bytes   int64
}

// BucketUsages aggregates every bucket in a single query.
func (d *DB) BucketUsages(ctx context.Context) (map[string]BucketUsage, error) {
	rows, err := d.db.QueryContext(ctx, `SELECT bucket, COUNT(*), COALESCE(SUM(size), 0) FROM objects GROUP BY bucket`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[string]BucketUsage{}
	for rows.Next() {
		var name string
		var u BucketUsage
		if err := rows.Scan(&name, &u.Objects, &u.Bytes); err != nil {
			return nil, err
		}
		out[name] = u
	}
	return out, rows.Err()
}

// Ping verifies the database answers queries (readiness probe).
func (d *DB) Ping(ctx context.Context) error {
	return d.db.PingContext(ctx)
}
