package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// baselineSchema is the schema as it existed before versioned migrations. It is
// idempotent, so databases created by older releases (user_version = 0) go
// through it harmlessly before the numbered migrations run.
const baselineSchema = `
CREATE TABLE IF NOT EXISTS accounts (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	name TEXT NOT NULL,
	username TEXT NOT NULL,
	token TEXT NOT NULL,
	repo_name TEXT NOT NULL,
	quota_bytes INTEGER NOT NULL DEFAULT 0,
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

// legacyAccountColumns were added by older releases with error-swallowing
// ALTERs; they are still applied (only if missing) to baseline databases.
var legacyAccountColumns = []struct{ name, ddl string }{
	{"is_public", `ALTER TABLE accounts ADD COLUMN is_public INTEGER NOT NULL DEFAULT 1`},
	{"s3_access_key", `ALTER TABLE accounts ADD COLUMN s3_access_key TEXT NOT NULL DEFAULT ''`},
	{"s3_secret_key", `ALTER TABLE accounts ADD COLUMN s3_secret_key TEXT NOT NULL DEFAULT ''`},
	{"s3_endpoint", `ALTER TABLE accounts ADD COLUMN s3_endpoint TEXT NOT NULL DEFAULT ''`},
	{"s3_bucket", `ALTER TABLE accounts ADD COLUMN s3_bucket TEXT NOT NULL DEFAULT ''`},
}

type migration struct {
	version int
	stmts   []string
}

// migrations are applied in order, each in its own transaction, and recorded in
// PRAGMA user_version. Never edit a released migration; append a new one.
var migrations = []migration{
	{
		version: 2,
		stmts: []string{
			// Encryption format/key of each chunk, so old data can be re-keyed.
			`ALTER TABLE chunks ADD COLUMN enc_version INTEGER NOT NULL DEFAULT 1`,
			`ALTER TABLE chunks ADD COLUMN key_id TEXT NOT NULL DEFAULT 'legacy'`,
			`CREATE INDEX IF NOT EXISTS idx_chunks_account ON chunks(account_id)`,
			`CREATE INDEX IF NOT EXISTS idx_chunks_rekey ON chunks(enc_version, key_id)`,
			// Durable queue of remote objects (HF chunks / cache copies) to delete.
			`CREATE TABLE IF NOT EXISTS pending_deletions (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				kind TEXT NOT NULL,
				account_id INTEGER NOT NULL,
				remote_path TEXT NOT NULL,
				size_bytes INTEGER NOT NULL DEFAULT 0,
				attempts INTEGER NOT NULL DEFAULT 0,
				next_attempt_at DATETIME NOT NULL,
				last_error TEXT NOT NULL DEFAULT '',
				created_at DATETIME NOT NULL,
				UNIQUE(kind, account_id, remote_path)
			)`,
			`CREATE INDEX IF NOT EXISTS idx_pending_del_due ON pending_deletions(next_attempt_at)`,
			`CREATE INDEX IF NOT EXISTS idx_multipart_initiated ON multipart_uploads(initiated_at)`,
			`ALTER TABLE multipart_uploads ADD COLUMN metadata_json TEXT NOT NULL DEFAULT ''`,
		},
	},
}

// SchemaVersion is the newest schema version this binary understands.
func SchemaVersion() int {
	return migrations[len(migrations)-1].version
}

func (d *DB) migrate() error {
	ctx := context.Background()

	var current int
	if err := d.db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&current); err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}
	if current > SchemaVersion() {
		return fmt.Errorf("database schema version %d is newer than this binary supports (%d); upgrade hf2s3", current, SchemaVersion())
	}

	if current < 1 {
		if err := d.applyBaseline(ctx); err != nil {
			return fmt.Errorf("baseline schema: %w", err)
		}
		current = 1
	}

	for _, m := range migrations {
		if m.version <= current {
			continue
		}
		if err := d.applyMigration(ctx, m); err != nil {
			return fmt.Errorf("migration %d: %w", m.version, err)
		}
	}
	return nil
}

func (d *DB) applyBaseline(ctx context.Context) error {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, baselineSchema); err != nil {
		return err
	}
	for _, col := range legacyAccountColumns {
		has, err := columnExists(ctx, tx, "accounts", col.name)
		if err != nil {
			return err
		}
		if !has {
			if _, err := tx.ExecContext(ctx, col.ddl); err != nil {
				return err
			}
		}
	}
	// Historic fix-up: 100 GiB used to be the default quota for public datasets,
	// where quota is actually unknown (0 = dynamic).
	if _, err := tx.ExecContext(ctx, `UPDATE accounts SET quota_bytes = 0 WHERE quota_bytes = 107374182400`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `PRAGMA user_version = 1`); err != nil {
		return err
	}
	return tx.Commit()
}

func (d *DB) applyMigration(ctx context.Context, m migration) error {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, stmt := range m.stmts {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("%q: %w", firstLine(stmt), err)
		}
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`PRAGMA user_version = %d`, m.version)); err != nil {
		return err
	}
	return tx.Commit()
}

func columnExists(ctx context.Context, tx *sql.Tx, table, column string) (bool, error) {
	rows, err := tx.QueryContext(ctx, fmt.Sprintf(`PRAGMA table_info(%s)`, table))
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull, pk int
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return false, err
		}
		if name == column {
			return true, nil
		}
	}
	return false, rows.Err()
}

func firstLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}
