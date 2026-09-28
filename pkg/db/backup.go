package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// BackupToFile writes a consistent, compacted snapshot of the live database to
// path (which must not exist). It does not block readers or writers.
func (d *DB) BackupToFile(ctx context.Context, path string) error {
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("backup target %s already exists", path)
	}
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
	}
	if _, err := d.db.ExecContext(ctx, `VACUUM INTO ?`, filepath.ToSlash(path)); err != nil {
		_ = os.Remove(path)
		return fmt.Errorf("vacuum into: %w", err)
	}
	return nil
}

// ExportBackup streams a consistent snapshot of the database to w.
func (d *DB) ExportBackup(ctx context.Context, w io.Writer) error {
	dir, err := os.MkdirTemp("", "hf2s3_backup_")
	if err != nil {
		return fmt.Errorf("create temp dir: %w", err)
	}
	defer os.RemoveAll(dir)

	tmp := filepath.Join(dir, "snapshot.db")
	if err := d.BackupToFile(ctx, tmp); err != nil {
		return err
	}

	src, err := os.Open(tmp)
	if err != nil {
		return err
	}
	defer src.Close()

	_, err = io.Copy(w, src)
	return err
}

// IntegrityCheck runs SQLite's integrity_check and fails on anything but "ok".
func (d *DB) IntegrityCheck(ctx context.Context) error {
	return integrityCheck(ctx, d.db)
}

func integrityCheck(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, `PRAGMA integrity_check`)
	if err != nil {
		return err
	}
	defer rows.Close()
	var problems []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return err
		}
		if line != "ok" {
			problems = append(problems, line)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(problems) > 0 {
		return fmt.Errorf("integrity_check failed: %v", problems)
	}
	return nil
}

// BackupInfo describes a database file offered for restoration.
type BackupInfo struct {
	SchemaVersion int
	Accounts      int64
	Buckets       int64
	Objects       int64
	Chunks        int64
	TotalBytes    int64
	LatestObject  time.Time // zero when the backup holds no objects
}

// Reasons InspectBackup can refuse a file; callers translate them for users.
var (
	ErrNotHF2SDatabase = errors.New("the file is not an HF2S3 database")
	ErrCorruptBackup   = errors.New("the database failed its integrity check")
	ErrNewerSchema     = errors.New("the backup comes from a newer release")
)

// InspectBackup opens a database file read-only and verifies it is a healthy
// HF2S3 database this binary can use: SQLite integrity, the expected tables, and
// a schema version that is not newer than the binary supports. It never modifies
// the file.
func InspectBackup(ctx context.Context, path string) (*BackupInfo, error) {
	src, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path)+"?mode=ro")
	if err != nil {
		return nil, fmt.Errorf("open backup: %w", err)
	}
	defer src.Close()

	var tables int
	if err := src.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('accounts','buckets','objects','chunks')`).Scan(&tables); err != nil {
		// "file is not a database" means the bytes are not SQLite at all; any other
		// failure (malformed image, short read) means a damaged SQLite file.
		if strings.Contains(err.Error(), "not a database") {
			return nil, fmt.Errorf("%w: %v", ErrNotHF2SDatabase, err)
		}
		return nil, fmt.Errorf("%w: %v", ErrCorruptBackup, err)
	}
	if tables != 4 {
		return nil, ErrNotHF2SDatabase
	}
	if err := integrityCheck(ctx, src); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCorruptBackup, err)
	}

	info := &BackupInfo{}
	if err := src.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&info.SchemaVersion); err != nil {
		return nil, err
	}
	if info.SchemaVersion > SchemaVersion() {
		return nil, fmt.Errorf("%w (schema %d, this binary supports up to %d)", ErrNewerSchema, info.SchemaVersion, SchemaVersion())
	}

	for query, dst := range map[string]*int64{
		`SELECT COUNT(*) FROM accounts`:              &info.Accounts,
		`SELECT COUNT(*) FROM buckets`:               &info.Buckets,
		`SELECT COUNT(*) FROM objects`:               &info.Objects,
		`SELECT COUNT(*) FROM chunks`:                &info.Chunks,
		`SELECT COALESCE(SUM(size), 0) FROM objects`: &info.TotalBytes,
	} {
		if err := src.QueryRowContext(ctx, query).Scan(dst); err != nil {
			return nil, err
		}
	}
	var latest sql.NullTime
	if err := src.QueryRowContext(ctx, `SELECT updated_at FROM objects ORDER BY updated_at DESC LIMIT 1`).Scan(&latest); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if latest.Valid {
		info.LatestObject = latest.Time
	}
	return info, nil
}

// RestoreFile replaces the database at dstPath with the backup at srcPath. It
// must run while the service is stopped: it validates the backup (integrity and
// schema), keeps the current file as dstPath+".pre-restore", and removes stale
// WAL/SHM files. Migrations are applied the next time the service opens it.
func RestoreFile(ctx context.Context, srcPath, dstPath string) error {
	if _, err := InspectBackup(ctx, srcPath); err != nil {
		return fmt.Errorf("invalid backup: %w", err)
	}

	if _, err := os.Stat(dstPath); err == nil {
		if err := os.Rename(dstPath, dstPath+".pre-restore"); err != nil {
			return fmt.Errorf("keep current database: %w", err)
		}
	}
	_ = os.Remove(dstPath + "-wal")
	_ = os.Remove(dstPath + "-shm")

	in, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dstPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}
