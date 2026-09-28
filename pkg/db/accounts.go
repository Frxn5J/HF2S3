package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"hf2s3/pkg/crypto"
	"hf2s3/pkg/models"
)

// ErrAccountInUse is returned when deleting an account that still owns data.
var ErrAccountInUse = errors.New("account still stores chunks or has pending deletions; drain it first")

// SetSecretBox enables encryption of tokens and secrets at rest. Values that
// are still plaintext keep working until SealPlaintextSecrets runs.
func (d *DB) SetSecretBox(box *crypto.SecretBox) {
	d.box = box
}

func (d *DB) seal(v string) (string, error) {
	return d.box.Seal(v)
}

func (d *DB) open(v string) (string, error) {
	return d.box.Open(v)
}

// SealPlaintextSecrets encrypts every secret that is still stored in plaintext.
// It is idempotent and safe to run at each startup.
func (d *DB) SealPlaintextSecrets(ctx context.Context) (int, error) {
	return d.rewriteSecrets(ctx, func(val string) (string, bool, error) {
		if val == "" || crypto.IsSealed(val) {
			return val, false, nil
		}
		s, err := d.box.Seal(val)
		return s, true, err
	})
}

// ResealSecrets re-encrypts secrets that were sealed with the previous
// secret box (master key rotation) under the current one. Values already under
// the current key are left alone; plaintext values are sealed.
func (d *DB) ResealSecrets(ctx context.Context, from *crypto.SecretBox) (int, error) {
	return d.rewriteSecrets(ctx, func(val string) (string, bool, error) {
		if val == "" {
			return val, false, nil
		}
		if !crypto.IsSealed(val) {
			s, err := d.box.Seal(val)
			return s, true, err
		}
		if _, err := d.box.Open(val); err == nil {
			return val, false, nil // already under the current key
		}
		plain, err := from.Open(val)
		if err != nil {
			return "", false, fmt.Errorf("a stored secret is sealed with neither the current nor the previous master key: %w", err)
		}
		s, err := d.box.Seal(plain)
		return s, true, err
	})
}

// rewriteSecrets applies transform to every secret column and setting inside one
// transaction, counting the values it changed.
func (d *DB) rewriteSecrets(ctx context.Context, transform func(string) (string, bool, error)) (int, error) {
	if d.box == nil {
		return 0, errors.New("no secret box configured")
	}
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	sealed := 0
	type target struct{ table, idCol, col string }
	for _, t := range []target{
		{"accounts", "id", "token"},
		{"accounts", "id", "s3_secret_key"},
		{"cache_buckets", "id", "secret_key"},
	} {
		rows, err := tx.QueryContext(ctx, fmt.Sprintf(`SELECT %s, %s FROM %s`, t.idCol, t.col, t.table))
		if err != nil {
			return 0, err
		}
		type upd struct {
			id  int64
			val string
		}
		var todo []upd
		for rows.Next() {
			var id int64
			var val string
			if err := rows.Scan(&id, &val); err != nil {
				rows.Close()
				return 0, err
			}
			todo = append(todo, upd{id, val})
		}
		rows.Close()
		for _, u := range todo {
			s, changed, err := transform(u.val)
			if err != nil {
				return 0, err
			}
			if !changed {
				continue
			}
			if _, err := tx.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET %s = ? WHERE %s = ?`, t.table, t.col, t.idCol), s, u.id); err != nil {
				return 0, err
			}
			sealed++
		}
	}

	for key := range sensitiveSettings {
		var val string
		err := tx.QueryRowContext(ctx, `SELECT value FROM system_settings WHERE key = ?`, key).Scan(&val)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return 0, err
		}
		s, changed, err := transform(val)
		if err != nil {
			return 0, err
		}
		if changed {
			if _, err := tx.ExecContext(ctx, `UPDATE system_settings SET value = ? WHERE key = ?`, s, key); err != nil {
				return 0, err
			}
			sealed++
		}
	}

	return sealed, tx.Commit()
}

// --- Accounts Operations ---

const accountColumns = `id, name, username, token, repo_name, quota_bytes, used_bytes, is_active, last_checked, created_at, updated_at`

func (d *DB) scanAccount(sc interface{ Scan(...any) error }) (*models.Account, error) {
	var acc models.Account
	var isActive int
	var lastChecked sql.NullTime
	if err := sc.Scan(&acc.ID, &acc.Name, &acc.Username, &acc.Token, &acc.RepoName, &acc.QuotaBytes, &acc.UsedBytes, &isActive, &lastChecked, &acc.CreatedAt, &acc.UpdatedAt); err != nil {
		return nil, err
	}
	acc.IsActive = isActive == 1
	if lastChecked.Valid {
		acc.LastChecked = lastChecked.Time
	}
	tok, err := d.open(acc.Token)
	if err != nil {
		return nil, fmt.Errorf("decrypt token of account %d: %w", acc.ID, err)
	}
	acc.Token = tok
	return &acc, nil
}

func (d *DB) CreateAccount(ctx context.Context, acc *models.Account) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	now := time.Now().UTC()
	acc.CreatedAt = now
	acc.UpdatedAt = now
	acc.LastChecked = now

	token, err := d.seal(acc.Token)
	if err != nil {
		return err
	}

	res, err := d.db.ExecContext(ctx, `
		INSERT INTO accounts (name, username, token, repo_name, quota_bytes, used_bytes, is_active, last_checked, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, acc.Name, acc.Username, token, acc.RepoName, acc.QuotaBytes, acc.UsedBytes, acc.IsActive, acc.LastChecked, acc.CreatedAt, acc.UpdatedAt)
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

	token, err := d.seal(acc.Token)
	if err != nil {
		return err
	}

	acc.UpdatedAt = time.Now().UTC()
	_, err = d.db.ExecContext(ctx, `
		UPDATE accounts
		SET name = ?, username = ?, token = ?, repo_name = ?, quota_bytes = ?, is_active = ?, updated_at = ?
		WHERE id = ?
	`, acc.Name, acc.Username, token, acc.RepoName, acc.QuotaBytes, acc.IsActive, acc.UpdatedAt, acc.ID)
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
		SET used_bytes = CASE WHEN used_bytes + ? < 0 THEN 0 ELSE used_bytes + ? END, last_checked = ?, updated_at = ?
		WHERE id = ?
	`, deltaBytes, deltaBytes, now, now, accountID)
	return err
}

func (d *DB) UpdateAccountQuota(ctx context.Context, accountID int64, quotaBytes int64) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	now := time.Now().UTC()
	_, err := d.db.ExecContext(ctx, `
		UPDATE accounts
		SET quota_bytes = ?, updated_at = ?
		WHERE id = ?
	`, quotaBytes, now, accountID)
	return err
}

// DeleteAccount removes an account. It refuses while the account still owns
// chunks or queued deletions, since their tokens would be lost with it.
func (d *DB) DeleteAccount(ctx context.Context, accountID int64) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	var n int
	if err := d.db.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM chunks WHERE account_id = ?) +
		(SELECT COUNT(*) FROM pending_deletions WHERE kind = 'chunk' AND account_id = ?)`, accountID, accountID).Scan(&n); err != nil {
		return err
	}
	if n > 0 {
		return ErrAccountInUse
	}

	_, err := d.db.ExecContext(ctx, `DELETE FROM accounts WHERE id = ?`, accountID)
	return err
}

func (d *DB) GetAccountByID(ctx context.Context, accountID int64) (*models.Account, error) {
	acc, err := d.scanAccount(d.db.QueryRowContext(ctx, `SELECT `+accountColumns+` FROM accounts WHERE id = ?`, accountID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return acc, err
}

func (d *DB) queryAccounts(ctx context.Context, query string, args ...any) ([]models.Account, error) {
	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []models.Account
	for rows.Next() {
		acc, err := d.scanAccount(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *acc)
	}
	return result, rows.Err()
}

func (d *DB) ListAccounts(ctx context.Context) ([]models.Account, error) {
	return d.queryAccounts(ctx, `SELECT `+accountColumns+` FROM accounts ORDER BY id ASC`)
}

func (d *DB) ListActiveAccounts(ctx context.Context) ([]models.Account, error) {
	return d.queryAccounts(ctx, `
		SELECT `+accountColumns+` FROM accounts WHERE is_active = 1
		ORDER BY CASE WHEN quota_bytes > 0 THEN (used_bytes * 1.0 / quota_bytes) ELSE 0 END ASC, used_bytes ASC
	`)
}

// --- Cache Buckets (Tier 1 S3 Cache) Operations ---

const cacheBucketColumns = `id, account_id, name, endpoint, region, access_key, secret_key, bucket_name, quota_bytes, used_bytes, is_active, created_at, updated_at`

func (d *DB) scanCacheBucket(sc interface{ Scan(...any) error }) (*models.CacheBucket, error) {
	var cb models.CacheBucket
	var isActive int
	if err := sc.Scan(&cb.ID, &cb.AccountID, &cb.Name, &cb.Endpoint, &cb.Region, &cb.AccessKey, &cb.SecretKey, &cb.BucketName, &cb.QuotaBytes, &cb.UsedBytes, &isActive, &cb.CreatedAt, &cb.UpdatedAt); err != nil {
		return nil, err
	}
	cb.IsActive = isActive == 1
	secret, err := d.open(cb.SecretKey)
	if err != nil {
		return nil, fmt.Errorf("decrypt secret of cache bucket %d: %w", cb.ID, err)
	}
	cb.SecretKey = secret
	return &cb, nil
}

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

	secret, err := d.seal(cb.SecretKey)
	if err != nil {
		return err
	}

	res, err := d.db.ExecContext(ctx, `
		INSERT INTO cache_buckets (account_id, name, endpoint, region, access_key, secret_key, bucket_name, quota_bytes, used_bytes, is_active, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, cb.AccountID, cb.Name, cb.Endpoint, cb.Region, cb.AccessKey, secret, cb.BucketName, cb.QuotaBytes, cb.UsedBytes, cb.IsActive, cb.CreatedAt, cb.UpdatedAt)
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

	secret, err := d.seal(cb.SecretKey)
	if err != nil {
		return err
	}

	cb.UpdatedAt = time.Now().UTC()
	_, err = d.db.ExecContext(ctx, `
		UPDATE cache_buckets
		SET name = ?, endpoint = ?, region = ?, access_key = ?, secret_key = ?, bucket_name = ?, quota_bytes = ?, is_active = ?, updated_at = ?
		WHERE id = ?
	`, cb.Name, cb.Endpoint, cb.Region, cb.AccessKey, secret, cb.BucketName, cb.QuotaBytes, cb.IsActive, cb.UpdatedAt, cb.ID)
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

// DeleteCacheBucket removes the bucket and every cache location pointing at it,
// so objects fall back to the cold tier instead of keeping dangling references.
// The bucket's credentials disappear with its row, so the remote copies cannot
// be garbage-collected afterwards: they stay in the HF bucket until the bucket
// itself is deleted on Hugging Face.
func (d *DB) DeleteCacheBucket(ctx context.Context, id int64) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `DELETE FROM object_locations WHERE tier = 'cache' AND account_id = ?`, id); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM pending_deletions WHERE kind = 'cache' AND account_id = ?`, id); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM cache_buckets WHERE id = ?`, id); err != nil {
		return err
	}
	return tx.Commit()
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
	cb, err := d.scanCacheBucket(d.db.QueryRowContext(ctx, `SELECT `+cacheBucketColumns+` FROM cache_buckets WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return cb, err
}

func (d *DB) queryCacheBuckets(ctx context.Context, query string) ([]models.CacheBucket, error) {
	rows, err := d.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []models.CacheBucket
	for rows.Next() {
		cb, err := d.scanCacheBucket(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *cb)
	}
	return result, rows.Err()
}

func (d *DB) ListCacheBuckets(ctx context.Context) ([]models.CacheBucket, error) {
	return d.queryCacheBuckets(ctx, `SELECT `+cacheBucketColumns+` FROM cache_buckets ORDER BY id ASC`)
}

func (d *DB) ListActiveCacheBuckets(ctx context.Context) ([]models.CacheBucket, error) {
	return d.queryCacheBuckets(ctx, `
		SELECT `+cacheBucketColumns+` FROM cache_buckets WHERE is_active = 1
		ORDER BY (used_bytes * 1.0 / NULLIF(quota_bytes, 0)) ASC
	`)
}
