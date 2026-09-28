package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"hf2s3/pkg/backup"
	"hf2s3/pkg/config"
	"hf2s3/pkg/crypto"
	"hf2s3/pkg/dashboard"
	"hf2s3/pkg/db"
	"hf2s3/pkg/hfclient"
	"hf2s3/pkg/hfstorage"
	"hf2s3/pkg/metrics"
	"hf2s3/pkg/models"
	"hf2s3/pkg/s3api"
	"hf2s3/pkg/storage"
)

// App is the fully wired gateway.
type App struct {
	Cfg      *config.Config
	DB       *db.DB
	Keyring  *crypto.Keyring
	HF       *hfclient.Client
	Pool     *storage.PoolManager
	Settings *models.SystemSettings
	S3Auth   *s3api.AuthManager
	Admin    *dashboard.AdminAuthManager
	S3       *s3api.Server
	Console  *dashboard.DashboardHandler
	Backups  *backup.Manager
	Metrics  *metrics.Registry

	// LegacyAdopted is true when a master key stored by an older release was
	// moved (sealed) into the legacy key list during this start.
	LegacyAdopted bool

	restartCh   chan struct{}
	restartOnce sync.Once
	draining    atomic.Bool // set while shutting down to restart: new work gets 503
}

// ErrRestart is returned by Run when the process must be restarted (a database
// restore was confirmed).
var ErrRestart = errors.New("restart requested")

// RequestRestart asks Run to drain, shut down and return ErrRestart.
func (a *App) RequestRestart() {
	a.restartOnce.Do(func() {
		a.draining.Store(true)
		close(a.restartCh)
	})
}

// BootstrapOptions tune Bootstrap for commands that do not serve traffic.
type BootstrapOptions struct {
	// SkipCredentialChecks lets maintenance commands (rekey, squash...) run
	// without valid S3/admin credentials.
	SkipCredentialChecks bool
	// HFOptions customise the Hugging Face client (tests point it at a fake).
	HFOptions []hfclient.ClientOption
}

const legacyKeysSetting = "legacy_master_keys"

// resolveKeyring builds the keyring, adopting the plaintext master key that
// older releases stored in the database as a legacy key so existing data stays
// readable until `hf2s3 rekey` migrates it.
func resolveKeyring(ctx context.Context, cfg *config.Config, database *db.DB) (*crypto.Keyring, bool, error) {
	if cfg.MasterKey == "" {
		if !cfg.Dev {
			return nil, false, errors.New("HF2S3_MASTER_KEY is required (generate one with: hf2s3 keygen)")
		}
		// Development convenience only: a passphrase-derived key, as older releases did.
		pass := "hf2s3-dev-passphrase"
		if v, err := database.GetSetting(ctx, "master_key"); err == nil && v != "" {
			pass = v
		}
		slog.Warn("HF2S3_DEV: running without HF2S3_MASTER_KEY; do not store real data this way")
		return crypto.NewKeyringFromLegacyKey(crypto.DeriveKey(pass)), false, nil
	}

	master, err := crypto.ParseMasterKey(cfg.MasterKey)
	if err != nil {
		return nil, false, fmt.Errorf("HF2S3_MASTER_KEY: %w", err)
	}

	// First pass: only the settings key, to be able to unseal stored legacy keys.
	base, err := crypto.NewKeyring(master, nil)
	if err != nil {
		return nil, false, err
	}
	box, err := crypto.NewSecretBox(base.SettingsKey())
	if err != nil {
		return nil, false, err
	}
	database.SetSecretBox(box)
	if err := verifyMasterKey(ctx, database); err != nil {
		if !errors.Is(err, errKeyMismatch) {
			return nil, false, err
		}
		// The key changed: is the database sealed under a previous master key we were given?
		withPrevious, kerr := crypto.NewKeyring(master, cfg.LegacyMasterKeys)
		if kerr != nil {
			return nil, false, kerr
		}
		if err := rotateIfNeeded(ctx, database, box, withPrevious); err != nil {
			return nil, false, err
		}
	}
	if n, err := database.SealPlaintextSecrets(ctx); err != nil {
		return nil, false, fmt.Errorf("encrypting stored secrets: %w", err)
	} else if n > 0 {
		slog.Info("encrypted stored secrets at rest", "values", n)
	}

	var stored []string
	if raw, err := database.GetSetting(ctx, legacyKeysSetting); err == nil && raw != "" {
		if err := json.Unmarshal([]byte(raw), &stored); err != nil {
			return nil, false, fmt.Errorf("stored legacy keys are unreadable (wrong HF2S3_MASTER_KEY?): %w", err)
		}
	} else if err != nil && !errors.Is(err, db.ErrNotFound) {
		return nil, false, fmt.Errorf("stored legacy keys are unreadable (wrong HF2S3_MASTER_KEY?): %w", err)
	}

	adopted := false
	if old, err := database.GetSetting(ctx, "master_key"); err == nil && old != "" {
		if !contains(stored, old) {
			stored = append(stored, old)
		}
		b, _ := json.Marshal(stored)
		if err := database.SetSetting(ctx, legacyKeysSetting, string(b)); err != nil {
			return nil, false, err
		}
		// Only after the sealed copy is safely stored, remove the plaintext.
		if err := database.DeleteSetting(ctx, "master_key"); err != nil {
			return nil, false, err
		}
		adopted = true
		slog.Warn("the previous master key was stored in plaintext in the database; it was moved (encrypted) to the legacy key list and the plaintext removed. Run `hf2s3 rekey` to migrate existing data, then rotate any other secret that was stored alongside it.")
	}

	legacy := append([]string(nil), cfg.LegacyMasterKeys...)
	for _, k := range stored {
		if !contains(legacy, k) {
			legacy = append(legacy, k)
		}
	}
	kr, err := crypto.NewKeyring(master, legacy)
	return kr, adopted, err
}

func contains(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

// setting resolves one setting: environment/flag value > database > default.
func setting(ctx context.Context, database *db.DB, cfg *config.Config, key, envVal, def string) (string, error) {
	if cfg.FromEnv[key] && envVal != "" {
		return envVal, nil
	}
	v, err := database.GetSetting(ctx, key)
	switch {
	case err == nil && v != "":
		return v, nil
	case err == nil, errors.Is(err, db.ErrNotFound):
		return def, nil
	}
	return "", fmt.Errorf("cannot read stored setting %q (is HF2S3_MASTER_KEY the key this database was encrypted with?): %w", key, err)
}

// keyCheckValue is stored encrypted on first start; failing to open it later
// proves the configured master key is not the one the data was sealed with.
const keyCheckValue = "hf2s3-key-check-v1"

var errKeyMismatch = errors.New("HF2S3_MASTER_KEY does not match the key this database was encrypted with; refusing to start (the stored tokens and secrets cannot be decrypted with it). To rotate the key, keep the previous one in HF2S3_LEGACY_MASTER_KEYS")

func verifyMasterKey(ctx context.Context, database *db.DB) error {
	v, err := database.GetSetting(ctx, "key_check")
	switch {
	case err == nil && v == keyCheckValue:
		return nil
	case err == nil, !errors.Is(err, db.ErrNotFound):
		return errKeyMismatch
	}
	return database.SetSetting(ctx, "key_check", keyCheckValue)
}

// rotateIfNeeded handles a changed master key: if the database was sealed under
// one of the previous master keys (HF2S3_LEGACY_MASTER_KEYS), every stored secret
// is re-sealed under the current key. Chunks stay readable through the previous
// key until `hf2s3 rekey` migrates them.
func rotateIfNeeded(ctx context.Context, database *db.DB, current *crypto.SecretBox, withPrevious *crypto.Keyring) error {
	for _, prevKey := range withPrevious.PreviousSettingsKeys() {
		prevBox, err := crypto.NewSecretBox(prevKey)
		if err != nil {
			continue
		}
		database.SetSecretBox(prevBox)
		v, err := database.GetSetting(ctx, "key_check")
		if err != nil || v != keyCheckValue {
			continue
		}
		database.SetSecretBox(current)
		n, err := database.ResealSecrets(ctx, prevBox)
		if err != nil {
			return fmt.Errorf("re-encrypting stored secrets under the new master key: %w", err)
		}
		if err := database.SetSetting(ctx, "key_check", keyCheckValue); err != nil {
			return err
		}
		slog.Warn("master key rotated: stored secrets were re-encrypted under the new key. Run `hf2s3 rekey` to migrate the chunks, then remove the previous key from HF2S3_LEGACY_MASTER_KEYS.", "values", n)
		return nil
	}
	database.SetSecretBox(current)
	return errKeyMismatch
}

// Bootstrap opens the database and wires every component.
func Bootstrap(ctx context.Context, cfg *config.Config, opts BootstrapOptions) (*App, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	database, err := db.Open(cfg.DBPath)
	if err != nil {
		return nil, fmt.Errorf("cannot open the database at %s: %w", cfg.DBPath, err)
	}
	ok := false
	defer func() {
		if !ok {
			_ = database.Close()
		}
	}()

	kr, adopted, err := resolveKeyring(ctx, cfg, database)
	if err != nil {
		return nil, err
	}

	// --- Resolved settings ---------------------------------------------------
	var settingErr error
	get := func(key, envVal, def string) string {
		v, err := setting(ctx, database, cfg, key, envVal, def)
		if err != nil && settingErr == nil {
			settingErr = err
		}
		return v
	}
	accessKey := get("access_key_id", cfg.AccessKey, "")
	secretKey := get("secret_access_key", cfg.SecretKey, "")
	region := get("s3_region", cfg.Region, "us-east-1")
	adminUser := get("admin_username", cfg.AdminUser, "")

	chunkMB := cfg.ChunkSizeMB
	if !cfg.FromEnv["chunk_size_mb"] {
		if v, err := database.GetSetting(ctx, "chunk_size_mb"); err == nil {
			if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 512 {
				chunkMB = n
			}
		}
	}

	// Admin password: an environment value is hashed in memory and never stored;
	// otherwise the stored hash is used (a legacy plaintext value is converted).
	adminPass := ""
	adminPassIsHash := false
	switch {
	case cfg.FromEnv["admin_password"] && cfg.AdminPass != "":
		adminPass = cfg.AdminPass
		adminPassIsHash = dashboard.IsPasswordHash(adminPass)
	default:
		if v, err := database.GetSetting(ctx, "admin_password"); err == nil && v != "" {
			adminPass = v
			adminPassIsHash = dashboard.IsPasswordHash(v)
			if !adminPassIsHash {
				h, herr := dashboard.HashPassword(v)
				if herr != nil {
					return nil, herr
				}
				if err := database.SetSetting(ctx, "admin_password", h); err != nil {
					return nil, err
				}
				slog.Info("converted the stored admin password to a salted hash")
				// The old plaintext is still checked against defaults below.
			}
		}
	}

	if cfg.Dev {
		accessKey = firstNonEmpty(accessKey, "hf2s3-access-key")
		secretKey = firstNonEmpty(secretKey, "hf2s3-secret-key")
		adminUser = firstNonEmpty(adminUser, "admin")
		adminPass = firstNonEmpty(adminPass, "admin123")
	}
	if !opts.SkipCredentialChecks {
		if err := config.CheckCredentials(cfg.Dev, accessKey, secretKey, adminUser, adminPassIsHash, adminPass); err != nil {
			return nil, err
		}
	}

	hfEndpoint := get("hf_storage_endpoint", cfg.HFStorageEndpoint, "https://s3.hf.co")
	hfRegion := get("hf_storage_region", cfg.HFStorageRegion, "us-east-1")
	hfAccess := get("hf_storage_access_key", cfg.HFStorageAccessKey, "")
	hfSecret := get("hf_storage_secret_key", cfg.HFStorageSecretKey, "")
	hfBucket := get("hf_storage_bucket", cfg.HFStorageBucket, "")
	if settingErr != nil {
		return nil, settingErr
	}

	settings := &models.SystemSettings{
		AccessKeyID:        accessKey,
		SecretAccessKey:    secretKey,
		ChunkSizeMB:        chunkMB,
		S3Region:           region,
		HFStorageEndpoint:  hfEndpoint,
		HFStorageRegion:    hfRegion,
		HFStorageAccessKey: hfAccess,
		HFStorageSecretKey: hfSecret,
		HFStorageBucket:    hfBucket,
	}

	// --- Components -----------------------------------------------------------
	hf := hfclient.NewClient(opts.HFOptions...)
	pool := storage.NewPoolManagerWithKeyring(database, hf, kr, int64(chunkMB)*1024*1024)
	pool.SetMaxCacheObjectSize(cfg.MaxCacheObjectMB * 1024 * 1024)

	if legacyCache := hfstorage.NewS3Client(hfstorage.S3ClientConfig{
		Endpoint: hfEndpoint, Region: hfRegion, AccessKey: hfAccess, SecretKey: hfSecret, Bucket: hfBucket,
	}); legacyCache.IsConfigured() {
		pool.SetCacheClient(legacyCache)
		slog.Info("tier 1 cache active", "bucket", hfBucket, "endpoint", hfEndpoint)
	}

	s3Auth := s3api.NewAuthManager(accessKey, secretKey)
	s3Server := s3api.NewServer(pool, s3Auth)
	s3Server.SetRegion(region)
	s3Server.SetRedirectMode(s3api.RedirectMode(cfg.RedirectMode))
	s3Server.SetCORSOrigins(cfg.CORSOrigins)

	admin := dashboard.NewAdminAuthManager(adminUser, adminPass)
	admin.SetTrustProxy(cfg.TrustProxy)
	console := dashboard.NewDashboardHandler(pool, settings, cfg.Port, admin)
	console.SetCredentialsUpdater(s3Auth.UpdateCredentials)
	console.SetPublicURL(cfg.PublicURL)
	var managed []string
	for k, v := range cfg.FromEnv {
		if v {
			managed = append(managed, k)
		}
	}
	console.SetEnvManaged(managed...)

	backupDir := cfg.BackupDir
	if backupDir == "" {
		backupDir = filepath.Join(filepath.Dir(cfg.DBPath), "backups")
	}
	backups := &backup.Manager{
		DB: database, Dir: backupDir, Keep: cfg.BackupKeep, Keyring: kr,
		Remote: func(ctx context.Context) backup.Remote {
			if client, _, err := pool.GetCacheClient(ctx, 0); err == nil && client != nil && client.IsConfigured() {
				return backup.S3Remote{Client: client}
			}
			return nil
		},
	}

	ok = true
	app := &App{
		Cfg: cfg, DB: database, Keyring: kr, HF: hf, Pool: pool, Settings: settings,
		S3Auth: s3Auth, Admin: admin, S3: s3Server, Console: console, Backups: backups,
		Metrics: metrics.New(), LegacyAdopted: adopted, restartCh: make(chan struct{}),
	}
	console.SetRestoreManager(newRestoreManager(app))
	return app, nil
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

// Close releases the database.
func (a *App) Close() error { return a.DB.Close() }
