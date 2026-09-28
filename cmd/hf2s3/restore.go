package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"hf2s3/pkg/backup"
	"hf2s3/pkg/dashboard"
	"hf2s3/pkg/db"
)

// Files kept next to the database while a restore is in progress.
const (
	stagingName       = "restore-staging.db"
	pendingSuffix     = ".restore-pending"
	pendingMetaSuffix = ".restore-pending.json"
	resultSuffix      = ".restore-result.json"
	keepPreRestore    = 3

	// An encrypted backup is one AEAD blob, so it has to be decrypted in memory.
	maxEncryptedBackup = 256 << 20
)

var errNoPending = errors.New("no restore is pending")

func rmDB(path string) {
	_ = os.Remove(path)
	_ = os.Remove(path + "-wal")
	_ = os.Remove(path + "-shm")
}

// restoreManager implements dashboard.RestoreManager.
//
// A restore is two-phase. Stage validates the upload completely, including a
// full trial start-up on a private copy, so anything that would stop the service
// from starting (wrong master key, insecure credentials, unreadable secrets) is
// reported before anything is replaced. Apply then restarts the process, and
// applyPendingRestore swaps the file in while nothing has the database open.
type restoreManager struct {
	app *App
	mu  sync.Mutex
}

func newRestoreManager(a *App) *restoreManager { return &restoreManager{app: a} }

func (m *restoreManager) dbPath() string { return m.app.Cfg.DBPath }
func (m *restoreManager) dir() string    { return filepath.Dir(m.dbPath()) }

func (m *restoreManager) UploadDir() string {
	_ = os.MkdirAll(m.dir(), 0o700)
	return m.dir()
}

func (m *restoreManager) MaxUploadBytes() int64 {
	mb := m.app.Cfg.RestoreMaxMB
	if mb <= 0 {
		mb = 1024
	}
	return int64(mb) << 20
}

func userErr(format string, args ...any) error {
	return &dashboard.RestoreError{Message: fmt.Sprintf(format, args...)}
}

func (m *restoreManager) Stage(ctx context.Context, uploaded, filename string) (*dashboard.RestoreSummary, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	defer os.Remove(uploaded) // this manager owns the upload

	staging := filepath.Join(m.dir(), stagingName)
	rmDB(staging)
	defer rmDB(staging) // whatever happens, no half-staged file is left behind

	// 1. Put the plain database at the staging path (decrypting if needed).
	head := make([]byte, 8)
	f, err := os.Open(uploaded)
	if err != nil {
		return nil, err
	}
	n, _ := io.ReadFull(f, head)
	f.Close()
	encrypted := n == 8 && backup.IsEncrypted(head)

	if encrypted {
		st, err := os.Stat(uploaded)
		if err != nil {
			return nil, err
		}
		if st.Size() > maxEncryptedBackup {
			return nil, userErr("Las copias cifradas de más de %d MB no se pueden abrir desde el panel; usa `hf2s3 restore` en el servidor", maxEncryptedBackup>>20)
		}
		blob, err := os.ReadFile(uploaded)
		if err != nil {
			return nil, err
		}
		plain, err := backup.DecryptAny(m.app.Keyring, blob)
		if err != nil {
			return nil, userErr("No se pudo descifrar la copia: HF2S3_MASTER_KEY no coincide con la clave con la que se creó (si la rotaste, mantén la anterior en HF2S3_LEGACY_MASTER_KEYS)")
		}
		if err := os.WriteFile(staging, plain, 0o600); err != nil {
			return nil, err
		}
	} else if err := os.Rename(uploaded, staging); err != nil {
		return nil, err
	}
	_ = os.Chmod(staging, 0o600)

	// 2. Structural checks.
	info, err := db.InspectBackup(ctx, staging)
	switch {
	case errors.Is(err, db.ErrNotHF2SDatabase):
		return nil, userErr("El archivo no es una base de datos de HF2S3")
	case errors.Is(err, db.ErrCorruptBackup):
		return nil, userErr("La base de datos de la copia está dañada: no supera la comprobación de integridad")
	case errors.Is(err, db.ErrNewerSchema):
		return nil, userErr("La copia procede de una versión más nueva de HF2S3; actualiza el servicio antes de restaurarla")
	case err != nil:
		return nil, err
	}

	// 3. Trial start-up on the staged copy: the exact start-up the real service
	// will perform after the swap (key check, secret sealing, credential checks).
	cfg := *m.app.Cfg
	cfg.DBPath = staging
	pre, err := Bootstrap(ctx, &cfg, BootstrapOptions{})
	if err != nil {
		return nil, userErr("La copia no es compatible con la configuración actual: %v", err)
	}
	var warnings []string
	if pre.LegacyAdopted {
		warnings = append(warnings, "Es una copia de una versión anterior: su clave maestra antigua se conservará cifrada como clave legacy. Después ejecuta `hf2s3 rekey`.")
	}
	if n, _, err := pre.DB.CountChunksToRekey(ctx, pre.Keyring.CurrentKeyID()); err == nil && n > 0 {
		warnings = append(warnings, fmt.Sprintf("%d fragmentos usan un formato o clave de cifrado anterior; ejecuta `hf2s3 rekey` tras restaurar.", n))
	}
	if err := pre.Close(); err != nil {
		return nil, err
	}

	// 4. Compare with what is running now.
	summary := &dashboard.RestoreSummary{
		Filename:      filename,
		Encrypted:     encrypted,
		StagedAt:      time.Now().UTC(),
		SchemaVersion: info.SchemaVersion,
		Accounts:      info.Accounts,
		Buckets:       info.Buckets,
		Objects:       info.Objects,
		Chunks:        info.Chunks,
		TotalBytes:    info.TotalBytes,
	}
	if !info.LatestObject.IsZero() {
		t := info.LatestObject.UTC()
		summary.LatestObjectAt = &t
	}
	if cur, err := m.app.DB.GetStats(ctx); err == nil {
		summary.CurrentAccounts = int64(cur.TotalAccounts)
		summary.CurrentObjects = int64(cur.TotalObjects)
		if info.Objects < int64(cur.TotalObjects) {
			warnings = append(warnings, fmt.Sprintf("La copia tiene %d objetos menos que el estado actual: lo subido después de la copia no aparecerá y sus fragmentos quedarán huérfanos en Hugging Face.", int64(cur.TotalObjects)-info.Objects))
		}
		if info.Accounts < int64(cur.TotalAccounts) {
			warnings = append(warnings, "La copia tiene menos cuentas de Hugging Face que la configuración actual.")
		}
	}
	if info.Objects > 0 && summary.LatestObjectAt != nil && time.Since(*summary.LatestObjectAt) > 30*24*time.Hour {
		warnings = append(warnings, "El último objeto de la copia tiene más de 30 días.")
	}
	summary.Warnings = warnings

	// 5. Commit the staged file as pending (atomic: same directory).
	pending := m.dbPath() + pendingSuffix
	rmDB(pending)
	// The trial start-up may have left journal files next to the staged copy.
	_ = os.Remove(staging + "-wal")
	_ = os.Remove(staging + "-shm")
	if err := os.Rename(staging, pending); err != nil {
		return nil, fmt.Errorf("staging the validated backup: %w", err)
	}
	meta, _ := json.Marshal(summary)
	if err := os.WriteFile(m.dbPath()+pendingMetaSuffix, meta, 0o600); err != nil {
		_ = os.Remove(pending)
		return nil, err
	}
	return summary, nil
}

func (m *restoreManager) Pending() *dashboard.RestoreSummary {
	if _, err := os.Stat(m.dbPath() + pendingSuffix); err != nil {
		return nil
	}
	raw, err := os.ReadFile(m.dbPath() + pendingMetaSuffix)
	if err != nil {
		return &dashboard.RestoreSummary{Filename: "(desconocido)"}
	}
	var s dashboard.RestoreSummary
	if json.Unmarshal(raw, &s) != nil {
		return &dashboard.RestoreSummary{Filename: "(desconocido)"}
	}
	return &s
}

func (m *restoreManager) Cancel() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	rmDB(m.dbPath() + pendingSuffix)
	_ = os.Remove(m.dbPath() + pendingMetaSuffix)
	return nil
}

func (m *restoreManager) Apply() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, err := os.Stat(m.dbPath() + pendingSuffix); err != nil {
		return errNoPending
	}
	m.app.RequestRestart()
	return nil
}

func (m *restoreManager) LastResult() *dashboard.RestoreResult {
	raw, err := os.ReadFile(m.dbPath() + resultSuffix)
	if err != nil {
		return nil
	}
	var r dashboard.RestoreResult
	if json.Unmarshal(raw, &r) != nil {
		return nil
	}
	return &r
}

func writeResult(dbPath string, r dashboard.RestoreResult) {
	raw, _ := json.Marshal(r)
	if err := os.WriteFile(dbPath+resultSuffix, raw, 0o600); err != nil {
		slog.Warn("could not record the restore result", "err", err)
	}
}

// applyPendingRestore swaps in a staged restore. It must run before the
// database is opened. The database being replaced is kept as
// <db>.pre-restore-<timestamp> (the newest keepPreRestore are retained), and a
// failure leaves the current database in place. It reports whether a restore was
// applied and the path the replaced database was kept at ("" if there was none).
func applyPendingRestore(dbPath string) (applied bool, previous string, err error) {
	pending := dbPath + pendingSuffix
	if _, err := os.Stat(pending); err != nil {
		return false, "", nil
	}

	fail := func(err error) (bool, string, error) {
		writeResult(dbPath, dashboard.RestoreResult{At: time.Now().UTC(), OK: false, Message: "No se pudo aplicar la restauración: " + err.Error()})
		return false, "", err
	}

	prev := ""
	if _, err := os.Stat(dbPath); err == nil {
		prev = dbPath + ".pre-restore-" + time.Now().UTC().Format("20060102-150405.000")
		// The WAL may hold committed data that is not in the main file yet.
		for _, suffix := range []string{"", "-wal", "-shm"} {
			if err := os.Rename(dbPath+suffix, prev+suffix); err != nil && !os.IsNotExist(err) {
				if suffix != "" {
					continue
				}
				return fail(fmt.Errorf("keeping the current database: %w", err))
			}
		}
	}

	if err := os.Rename(pending, dbPath); err != nil {
		// Put the previous database back.
		if prev != "" {
			for _, suffix := range []string{"", "-wal", "-shm"} {
				_ = os.Rename(prev+suffix, dbPath+suffix)
			}
		}
		return fail(fmt.Errorf("installing the restored database: %w", err))
	}
	_ = os.Remove(pending + "-wal")
	_ = os.Remove(pending + "-shm")
	_ = os.Remove(dbPath + pendingMetaSuffix)

	pruneOldDatabases(dbPath)
	writeResult(dbPath, dashboard.RestoreResult{
		At: time.Now().UTC(), OK: true,
		Message:        "Base de datos restaurada correctamente.",
		PreviousBackup: filepath.Base(prev),
	})
	slog.Warn("database restored from the console upload", "previous", prev)
	return true, prev, nil
}

// rollbackRestore puts the database replaced by applyPendingRestore back and
// sets the restored one aside. It is the safety net for a restored database that
// passed validation but still fails to start.
func rollbackRestore(dbPath, previous string, cause error) error {
	if previous == "" {
		return errors.New("there is no previous database to roll back to")
	}
	rejected := dbPath + ".rejected-" + time.Now().UTC().Format("20060102-150405.000")
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if err := os.Rename(dbPath+suffix, rejected+suffix); err != nil && !os.IsNotExist(err) && suffix == "" {
			return err
		}
	}
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if err := os.Rename(previous+suffix, dbPath+suffix); err != nil && !os.IsNotExist(err) && suffix == "" {
			return err
		}
	}
	writeResult(dbPath, dashboard.RestoreResult{
		At: time.Now().UTC(), OK: false,
		Message: "La base restaurada no arrancó (" + cause.Error() + "); se volvió automáticamente a la anterior. La rechazada se conserva como " + filepath.Base(rejected) + ".",
	})
	return nil
}

// pruneOldDatabases keeps only the newest keepPreRestore replaced databases.
func pruneOldDatabases(dbPath string) {
	matches, _ := filepath.Glob(dbPath + ".pre-restore-*")
	var mains []string
	for _, m := range matches {
		if !strings.HasSuffix(m, "-wal") && !strings.HasSuffix(m, "-shm") {
			mains = append(mains, m)
		}
	}
	sort.Strings(mains)
	for len(mains) > keepPreRestore {
		rmDB(mains[0])
		mains = mains[1:]
	}
}
