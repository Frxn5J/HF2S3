package db

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"time"

	"hf2s3/pkg/models"
)

func hashShareToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// CreateShare stores a new share under token and fills in its ID and CreatedAt.
func (d *DB) CreateShare(ctx context.Context, s *models.Share, token string) error {
	sealed, err := d.seal(token)
	if err != nil {
		return err
	}

	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	now := time.Now().UTC()
	var expires any
	if s.ExpiresAt != nil {
		expires = s.ExpiresAt.UTC()
	}
	res, err := tx.ExecContext(ctx, `INSERT INTO shares (token_hash, token_sealed, bucket, title, password_hash, expires_at, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`, hashShareToken(token), sealed, s.Bucket, s.Title, s.PasswordHash, expires, now)
	if err != nil {
		return err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return err
	}
	for _, it := range s.Items {
		if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO share_items (share_id, key, is_prefix) VALUES (?, ?, ?)`, id, it.Key, it.IsPrefix); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.ID, s.CreatedAt, s.Token, s.HasPassword = id, now, token, s.PasswordHash != ""
	return nil
}

const shareColumns = `id, token_sealed, bucket, title, password_hash, expires_at, downloads, created_at`

func (d *DB) scanShare(sc interface{ Scan(...any) error }) (*models.Share, error) {
	var (
		s       models.Share
		sealed  string
		expires sql.NullTime
	)
	if err := sc.Scan(&s.ID, &sealed, &s.Bucket, &s.Title, &s.PasswordHash, &expires, &s.Downloads, &s.CreatedAt); err != nil {
		return nil, err
	}
	if expires.Valid {
		t := expires.Time.UTC()
		s.ExpiresAt = &t
	}
	token, err := d.open(sealed)
	if err != nil {
		return nil, err
	}
	s.Token = token
	s.HasPassword = s.PasswordHash != ""
	return &s, nil
}

func (d *DB) loadShareItems(ctx context.Context, shares ...*models.Share) error {
	for _, s := range shares {
		rows, err := d.db.QueryContext(ctx, `SELECT key, is_prefix FROM share_items WHERE share_id = ? ORDER BY key`, s.ID)
		if err != nil {
			return err
		}
		s.Items = []models.ShareItem{}
		for rows.Next() {
			var it models.ShareItem
			if err := rows.Scan(&it.Key, &it.IsPrefix); err != nil {
				rows.Close()
				return err
			}
			s.Items = append(s.Items, it)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// GetShareByToken returns the share behind a link token, or ErrNotFound.
func (d *DB) GetShareByToken(ctx context.Context, token string) (*models.Share, error) {
	s, err := d.scanShare(d.db.QueryRowContext(ctx, `SELECT `+shareColumns+` FROM shares WHERE token_hash = ?`, hashShareToken(token)))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if err := d.loadShareItems(ctx, s); err != nil {
		return nil, err
	}
	return s, nil
}

// ListShares returns every share, newest first.
func (d *DB) ListShares(ctx context.Context) ([]*models.Share, error) {
	rows, err := d.db.QueryContext(ctx, `SELECT `+shareColumns+` FROM shares ORDER BY id DESC`)
	if err != nil {
		return nil, err
	}
	var shares []*models.Share
	for rows.Next() {
		s, err := d.scanShare(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		shares = append(shares, s)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	if err := d.loadShareItems(ctx, shares...); err != nil {
		return nil, err
	}
	return shares, nil
}

// DeleteShare revokes a share; its link stops working immediately.
func (d *DB) DeleteShare(ctx context.Context, id int64) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()
	res, err := d.db.ExecContext(ctx, `DELETE FROM shares WHERE id = ?`, id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

// AddShareDownloads adds n to the share's download counter.
func (d *DB) AddShareDownloads(ctx context.Context, id int64, n int64) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()
	_, err := d.db.ExecContext(ctx, `UPDATE shares SET downloads = downloads + ? WHERE id = ?`, n, id)
	return err
}
