package db

import (
	"context"
	"encoding/json"
	"time"

	"hf2s3/pkg/models"
)

// --- Pending deletions (durable garbage-collection queue) ---

// EnqueueDeletion queues a remote object for deletion. It is idempotent.
func (d *DB) EnqueueDeletion(ctx context.Context, kind string, accountID int64, remotePath string, sizeBytes int64) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	now := time.Now().UTC()
	_, err := d.db.ExecContext(ctx, `
		INSERT OR IGNORE INTO pending_deletions (kind, account_id, remote_path, size_bytes, next_attempt_at, created_at)
		VALUES (?, ?, ?, ?, ?, ?)
	`, kind, accountID, remotePath, sizeBytes, now, now)
	return err
}

// ListDueDeletions returns queued deletions whose retry time has come, oldest first.
func (d *DB) ListDueDeletions(ctx context.Context, limit int) ([]models.PendingDeletion, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := d.db.QueryContext(ctx, `
		SELECT id, kind, account_id, remote_path, size_bytes, attempts, next_attempt_at, last_error, created_at
		FROM pending_deletions
		WHERE next_attempt_at <= ?
		ORDER BY account_id ASC, id ASC
		LIMIT ?
	`, time.Now().UTC(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []models.PendingDeletion
	for rows.Next() {
		var p models.PendingDeletion
		if err := rows.Scan(&p.ID, &p.Kind, &p.AccountID, &p.RemotePath, &p.SizeBytes, &p.Attempts, &p.NextAttemptAt, &p.LastError, &p.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// CompleteDeletions removes finished entries and releases the storage they
// accounted for, in one transaction (usage is only decremented on success).
func (d *DB) CompleteDeletions(ctx context.Context, done []models.PendingDeletion) error {
	if len(done) == 0 {
		return nil
	}
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	now := time.Now().UTC()
	for _, p := range done {
		if _, err := tx.ExecContext(ctx, `DELETE FROM pending_deletions WHERE id = ?`, p.ID); err != nil {
			return err
		}
		if p.SizeBytes <= 0 {
			continue
		}
		switch p.Kind {
		case models.DeletionChunk:
			_, err = tx.ExecContext(ctx, `
				UPDATE accounts SET used_bytes = CASE WHEN used_bytes - ? < 0 THEN 0 ELSE used_bytes - ? END, updated_at = ?
				WHERE id = ?`, p.SizeBytes, p.SizeBytes, now, p.AccountID)
		case models.DeletionCache:
			if p.AccountID > 0 {
				_, err = tx.ExecContext(ctx, `
					UPDATE cache_buckets SET used_bytes = CASE WHEN used_bytes - ? < 0 THEN 0 ELSE used_bytes - ? END, updated_at = ?
					WHERE id = ?`, p.SizeBytes, p.SizeBytes, now, p.AccountID)
			}
		}
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

// FailDeletion records a failed attempt and schedules the retry.
func (d *DB) FailDeletion(ctx context.Context, id int64, cause string, retryAfter time.Duration) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	if len(cause) > 500 {
		cause = cause[:500]
	}
	_, err := d.db.ExecContext(ctx, `
		UPDATE pending_deletions SET attempts = attempts + 1, last_error = ?, next_attempt_at = ? WHERE id = ?
	`, cause, time.Now().UTC().Add(retryAfter), id)
	return err
}

// DropDeletion discards an entry that can never succeed (e.g. its account is gone).
func (d *DB) DropDeletion(ctx context.Context, id int64) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	_, err := d.db.ExecContext(ctx, `DELETE FROM pending_deletions WHERE id = ?`, id)
	return err
}

// CountPendingDeletions returns the queue length.
func (d *DB) CountPendingDeletions(ctx context.Context) (int, error) {
	var n int
	err := d.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pending_deletions`).Scan(&n)
	return n, err
}

// --- Stale multipart uploads ---

// ListStaleMultipartUploads returns uploads started before cutoff.
func (d *DB) ListStaleMultipartUploads(ctx context.Context, cutoff time.Time) ([]string, error) {
	rows, err := d.db.QueryContext(ctx, `SELECT upload_id FROM multipart_uploads WHERE initiated_at < ?`, cutoff.UTC())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// --- Re-keying ---

// ListChunksToRekey returns chunks not in the given format/key, after afterID.
func (d *DB) ListChunksToRekey(ctx context.Context, currentKeyID string, afterID int64, limit int) ([]models.Chunk, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := d.db.QueryContext(ctx, `
		SELECT `+chunkColumns+`
		FROM chunks
		WHERE (enc_version != 2 OR key_id != ?) AND id > ?
		ORDER BY id ASC LIMIT ?
	`, currentKeyID, afterID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []models.Chunk
	for rows.Next() {
		c, err := scanChunk(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// CountChunksToRekey returns how many chunks and ciphertext bytes still need re-keying.
func (d *DB) CountChunksToRekey(ctx context.Context, currentKeyID string) (chunks int64, bytes int64, err error) {
	err = d.db.QueryRowContext(ctx, `
		SELECT COUNT(*), COALESCE(SUM(cipher_size_bytes), 0) FROM chunks WHERE enc_version != 2 OR key_id != ?
	`, currentKeyID).Scan(&chunks, &bytes)
	return
}

// RekeyedChunk describes a chunk that has been re-encrypted and re-uploaded.
type RekeyedChunk struct {
	ID            int64
	OldAccountID  int64
	OldRemotePath string
	OldCipherSize int64
	NewAccountID  int64
	NewRemotePath string
	NewCipherSize int64
	NewEncVersion int
	NewKeyID      string
}

// ApplyRekeyed points chunks at their new remote copy and queues the old copy
// for deletion, atomically. Used bytes of the new account are charged here.
func (d *DB) ApplyRekeyed(ctx context.Context, batch []RekeyedChunk) error {
	if len(batch) == 0 {
		return nil
	}
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	now := time.Now().UTC()
	for _, r := range batch {
		if _, err := tx.ExecContext(ctx, `
			UPDATE chunks SET account_id = ?, remote_path = ?, cipher_size_bytes = ?, enc_version = ?, key_id = ? WHERE id = ?
		`, r.NewAccountID, r.NewRemotePath, r.NewCipherSize, r.NewEncVersion, r.NewKeyID, r.ID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE accounts SET used_bytes = used_bytes + ?, updated_at = ? WHERE id = ?
		`, r.NewCipherSize, now, r.NewAccountID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT OR IGNORE INTO pending_deletions (kind, account_id, remote_path, size_bytes, next_attempt_at, created_at)
			VALUES ('chunk', ?, ?, ?, ?, ?)
		`, r.OldAccountID, r.OldRemotePath, r.OldCipherSize, now, now); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// ChunkCountForAccount returns how many chunks live in an account.
func (d *DB) ChunkCountForAccount(ctx context.Context, accountID int64) (int64, error) {
	var n int64
	err := d.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM chunks WHERE account_id = ?`, accountID).Scan(&n)
	return n, err
}

// AbortMultipartUploadAndQueue deletes an upload's bookkeeping and queues the
// chunks of its parts for remote deletion, atomically. Chunks that already
// belong to a committed object are never queued (safety net for an upload whose
// completion crashed half way).
func (d *DB) AbortMultipartUploadAndQueue(ctx context.Context, uploadID string) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `SELECT chunks_json FROM multipart_parts WHERE upload_id = ?`, uploadID)
	if err != nil {
		return err
	}
	var blobs []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			rows.Close()
			return err
		}
		blobs = append(blobs, s)
	}
	rows.Close()

	now := time.Now().UTC()
	for _, blob := range blobs {
		var chunks []models.Chunk
		if err := json.Unmarshal([]byte(blob), &chunks); err != nil {
			continue
		}
		for _, c := range chunks {
			if _, err := tx.ExecContext(ctx, `
				INSERT OR IGNORE INTO pending_deletions (kind, account_id, remote_path, size_bytes, next_attempt_at, created_at)
				SELECT 'chunk', ?, ?, ?, ?, ? WHERE NOT EXISTS (SELECT 1 FROM chunks WHERE account_id = ? AND remote_path = ?)
			`, c.AccountID, c.RemotePath, c.CipherSizeBytes, now, now, c.AccountID, c.RemotePath); err != nil {
				return err
			}
		}
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM multipart_parts WHERE upload_id = ?`, uploadID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM multipart_uploads WHERE upload_id = ?`, uploadID); err != nil {
		return err
	}
	return tx.Commit()
}

// DetachCacheLocation removes a cache location and queues its remote copy for
// deletion in one transaction. The cold (golden) copy is never touched.
func (d *DB) DetachCacheLocation(ctx context.Context, loc models.ObjectLocation) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	now := time.Now().UTC()
	if _, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO pending_deletions (kind, account_id, remote_path, size_bytes, next_attempt_at, created_at)
		VALUES ('cache', ?, ?, ?, ?, ?)
	`, loc.AccountID, loc.RemotePath, loc.SizeBytes, now, now); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM object_locations WHERE id = ? AND tier = 'cache'`, loc.ID); err != nil {
		return err
	}
	return tx.Commit()
}

// ListLRUCacheLocationsForBucket returns the least recently used cache
// locations stored in one cache bucket (0 = the legacy default bucket).
func (d *DB) ListLRUCacheLocationsForBucket(ctx context.Context, bucketID int64, limit int) ([]models.ObjectLocation, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := d.db.QueryContext(ctx, `
		SELECT id, object_id, tier, account_id, remote_path, is_encrypted, size_bytes, access_count, last_accessed_at, created_at
		FROM object_locations
		WHERE tier = 'cache' AND account_id = ?
		ORDER BY last_accessed_at ASC
		LIMIT ?
	`, bucketID, limit)
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

// ListChunksByAccount returns chunks stored in accountID with id > afterID.
func (d *DB) ListChunksByAccount(ctx context.Context, accountID, afterID int64, limit int) ([]models.Chunk, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := d.db.QueryContext(ctx, `
		SELECT `+chunkColumns+` FROM chunks WHERE account_id = ? AND id > ? ORDER BY id ASC LIMIT ?
	`, accountID, afterID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []models.Chunk
	for rows.Next() {
		c, err := scanChunk(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// RetryDeletionsNow makes every queued deletion due immediately (admin "retry now").
func (d *DB) RetryDeletionsNow(ctx context.Context) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	_, err := d.db.ExecContext(ctx, `UPDATE pending_deletions SET next_attempt_at = ?`, time.Now().UTC().Add(-time.Second))
	return err
}
