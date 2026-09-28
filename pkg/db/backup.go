package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
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

// RestoreFile replaces the database at dstPath with the backup at srcPath. It
// must run while the service is stopped: it validates the backup (integrity and
// schema), keeps the current file as dstPath+".pre-restore", and removes stale
// WAL/SHM files. Migrations are applied the next time the service opens it.
func RestoreFile(ctx context.Context, srcPath, dstPath string) error {
	src, err := sql.Open("sqlite", "file:"+filepath.ToSlash(srcPath)+"?mode=ro")
	if err != nil {
		return fmt.Errorf("open backup: %w", err)
	}
	var tables int
	err = src.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('accounts','objects','chunks')`).Scan(&tables)
	if err == nil {
		err = integrityCheck(ctx, src)
	}
	_ = src.Close()
	if err != nil {
		return fmt.Errorf("invalid backup: %w", err)
	}
	if tables != 3 {
		return errors.New("invalid backup: not an HF2S3 database")
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
