// Package config resolves the gateway's settings from flags and environment
// variables and refuses to start with insecure or missing production values.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"hf2s3/pkg/crypto"
)

// Well-known values shipped by earlier releases; they must never protect a
// production gateway.
var knownDefaults = map[string]bool{
	"hf2s3-access-key":                 true,
	"hf2s3-secret-key":                 true,
	"hf2s3-aes-master-passphrase-2026": true,
	"admin123":                         true,
	"admin":                            true,
	"changeme":                         true,
	"password":                         true,
	"mi-secret-key-s3":                 true,
	"mi-access-key-s3":                 true,
	"clave-segura-panel-2026":          true,
}

// Config is the fully resolved runtime configuration.
type Config struct {
	Port             int
	DBPath           string
	ChunkSizeMB      int
	Region           string
	PublicURL        string
	CORSOrigins      []string
	RedirectMode     string // auto | always | never
	TrustProxy       bool
	Dev              bool
	LogFormat        string // json | text
	LogLevel         string
	ShutdownTimeout  time.Duration
	MetricsToken     string
	MaxCacheObjectMB int64

	// Secrets and identities that may come from the environment.
	AccessKey        string
	SecretKey        string
	MasterKey        string   // base64/hex random key (HF2S3_MASTER_KEY)
	LegacyMasterKeys []string // old passphrases, only to read and re-key old data
	AdminUser        string
	AdminPass        string

	HFStorageEndpoint  string
	HFStorageRegion    string
	HFStorageAccessKey string
	HFStorageSecretKey string
	HFStorageBucket    string

	BackupDir      string
	BackupInterval time.Duration
	BackupKeep     int

	// FromEnv records which settings were given explicitly (env or flag). They
	// take precedence over values stored in the database and are read-only in
	// the web console.
	FromEnv map[string]bool
}

// Getenv abstracts os.Getenv for tests.
type Getenv func(string) string

func firstEnv(get Getenv, names ...string) (string, string) {
	for _, n := range names {
		if v := strings.TrimSpace(get(n)); v != "" {
			return v, n
		}
	}
	return "", ""
}

func atoi(get Getenv, def int, names ...string) (int, error) {
	v, name := firstEnv(get, names...)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer, got %q", name, v)
	}
	return n, nil
}

func parseBool(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

func splitList(v string) []string {
	var out []string
	for _, p := range strings.Split(v, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// FromEnvironment builds a Config from environment variables. Flags (parsed by
// the caller) may override individual fields afterwards, marking them in FromEnv.
func FromEnvironment(get Getenv) (*Config, error) {
	c := &Config{FromEnv: map[string]bool{}}
	var err error

	if c.Port, err = atoi(get, 8080, "PORT"); err != nil {
		return nil, err
	}
	c.DBPath = firstOr(get, "hf2s3_metadata.db", "HF2S3_DB")
	if c.ChunkSizeMB, err = atoi(get, 32, "HF2S3_CHUNK_SIZE_MB"); err != nil {
		return nil, err
	}
	if _, name := firstEnv(get, "HF2S3_CHUNK_SIZE_MB"); name != "" {
		c.FromEnv["chunk_size_mb"] = true
	}
	c.Region = firstOr(get, "us-east-1", "HF2S3_REGION")
	if _, name := firstEnv(get, "HF2S3_REGION"); name != "" {
		c.FromEnv["s3_region"] = true
	}
	c.PublicURL = strings.TrimRight(firstOr(get, "", "HF2S3_PUBLIC_URL"), "/")
	c.CORSOrigins = splitList(get("HF2S3_CORS_ORIGINS"))
	c.RedirectMode = strings.ToLower(firstOr(get, "auto", "S3_GET_REDIRECT"))
	c.TrustProxy = parseBool(get("HF2S3_TRUST_PROXY"))
	c.Dev = parseBool(get("HF2S3_DEV"))
	c.LogFormat = strings.ToLower(firstOr(get, "json", "HF2S3_LOG_FORMAT"))
	c.LogLevel = strings.ToLower(firstOr(get, "info", "HF2S3_LOG_LEVEL"))
	c.MetricsToken = firstOr(get, "", "HF2S3_METRICS_TOKEN")
	if c.MaxCacheObjectMB, err = func() (int64, error) {
		n, e := atoi(get, 5120, "HF2S3_CACHE_MAX_OBJECT_MB")
		return int64(n), e
	}(); err != nil {
		return nil, err
	}

	secs, err := atoi(get, 30, "HF2S3_SHUTDOWN_TIMEOUT_SECONDS")
	if err != nil {
		return nil, err
	}
	c.ShutdownTimeout = time.Duration(secs) * time.Second

	if v, name := firstEnv(get, "HF2S3_ACCESS_KEY"); v != "" {
		c.AccessKey = v
		c.FromEnv["access_key_id"] = true
		_ = name
	}
	if v, _ := firstEnv(get, "HF2S3_SECRET_KEY"); v != "" {
		c.SecretKey = v
		c.FromEnv["secret_access_key"] = true
	}
	c.MasterKey = firstOr(get, "", "HF2S3_MASTER_KEY")
	c.LegacyMasterKeys = splitList(get("HF2S3_LEGACY_MASTER_KEYS"))
	if v, _ := firstEnv(get, "ADMIN_USERNAME", "HF2S3_ADMIN_USER"); v != "" {
		c.AdminUser = v
		c.FromEnv["admin_username"] = true
	}
	if v, _ := firstEnv(get, "ADMIN_PASSWORD", "HF2S3_ADMIN_PASS"); v != "" {
		c.AdminPass = v
		c.FromEnv["admin_password"] = true
	}

	c.HFStorageEndpoint = firstOr(get, "https://s3.hf.co", "HF_STORAGE_ENDPOINT")
	c.HFStorageRegion = firstOr(get, "us-east-1", "HF_STORAGE_REGION")
	c.HFStorageAccessKey = firstOr(get, "", "HF_STORAGE_ACCESS_KEY")
	c.HFStorageSecretKey = firstOr(get, "", "HF_STORAGE_SECRET_KEY")
	c.HFStorageBucket = firstOr(get, "", "HF_STORAGE_BUCKET")
	if c.HFStorageAccessKey != "" || c.HFStorageSecretKey != "" || c.HFStorageBucket != "" {
		c.FromEnv["hf_storage"] = true
	}

	c.BackupDir = firstOr(get, "", "HF2S3_BACKUP_DIR")
	hours, err := atoi(get, 24, "HF2S3_BACKUP_INTERVAL_HOURS")
	if err != nil {
		return nil, err
	}
	c.BackupInterval = time.Duration(hours) * time.Hour
	if c.BackupKeep, err = atoi(get, 7, "HF2S3_BACKUP_KEEP"); err != nil {
		return nil, err
	}
	return c, nil
}

func firstOr(get Getenv, def string, names ...string) string {
	if v, _ := firstEnv(get, names...); v != "" {
		return v
	}
	return def
}

// LoadEnvFile reads KEY=VALUE lines from the first readable file and exports
// the ones that are not already set. It reports the file used.
func LoadEnvFile(paths ...string) string {
	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			k, v, ok := strings.Cut(line, "=")
			if !ok {
				continue
			}
			k = strings.TrimSpace(k)
			v = strings.Trim(strings.TrimSpace(v), `"'`)
			if os.Getenv(k) == "" {
				_ = os.Setenv(k, v)
			}
		}
		return p
	}
	return ""
}

// IsWeak reports whether v is empty, a shipped default, or (when minLen > 0) too short.
func IsWeak(v string, minLen int) bool {
	return v == "" || knownDefaults[strings.ToLower(v)] || len(v) < minLen
}

// Validate checks the values that do not depend on the database. Production
// mode (Dev=false) additionally requires a strong master key.
func (c *Config) Validate() error {
	var problems []string
	add := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }

	switch c.RedirectMode {
	case "auto", "always", "never":
	default:
		add("S3_GET_REDIRECT must be auto, always or never (got %q)", c.RedirectMode)
	}
	if c.LogFormat != "json" && c.LogFormat != "text" {
		add("HF2S3_LOG_FORMAT must be json or text (got %q)", c.LogFormat)
	}
	if c.Port < 1 || c.Port > 65535 {
		add("PORT must be between 1 and 65535")
	}
	if c.ChunkSizeMB < 1 || c.ChunkSizeMB > 512 {
		add("HF2S3_CHUNK_SIZE_MB must be between 1 and 512")
	}
	if c.PublicURL != "" {
		if u, err := url.Parse(c.PublicURL); err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
			add("HF2S3_PUBLIC_URL must be an absolute http(s) URL")
		}
	}
	if c.MasterKey != "" {
		if _, err := crypto.ParseMasterKey(c.MasterKey); err != nil && !c.Dev {
			add("HF2S3_MASTER_KEY: %v", err)
		}
	}
	if !c.Dev && c.MasterKey == "" {
		add("HF2S3_MASTER_KEY is required (generate one with: hf2s3 keygen)")
	}
	if len(problems) > 0 {
		return errors.New("invalid configuration:\n  - " + strings.Join(problems, "\n  - "))
	}
	return nil
}

// CheckCredentials validates the final S3 and admin credentials once the
// database has been consulted. In production, missing or default values stop the
// process rather than silently exposing the gateway.
func CheckCredentials(dev bool, accessKey, secretKey, adminUser string, adminPassIsHash bool, adminPass string) error {
	if dev {
		return nil
	}
	var problems []string
	if IsWeak(accessKey, 8) {
		problems = append(problems, "the S3 access key ID is unset, too short or a well-known default (set HF2S3_ACCESS_KEY)")
	}
	if IsWeak(secretKey, 24) {
		problems = append(problems, "the S3 secret access key is unset, shorter than 24 characters or a well-known default (set HF2S3_SECRET_KEY)")
	}
	if adminUser == "" {
		problems = append(problems, "the admin user name is empty (set ADMIN_USERNAME)")
	}
	if !adminPassIsHash && IsWeak(adminPass, 12) {
		problems = append(problems, "the admin password is unset, shorter than 12 characters or a well-known default (set ADMIN_PASSWORD)")
	}
	if len(problems) > 0 {
		return errors.New("refusing to start with insecure credentials (set HF2S3_DEV=1 only for local development):\n  - " + strings.Join(problems, "\n  - "))
	}
	return nil
}

// ParseLevel maps a level name to an slog level value (debug=-4 ... error=8).
func ParseLevel(name string) int {
	switch name {
	case "debug":
		return -4
	case "warn", "warning":
		return 4
	case "error":
		return 8
	}
	return 0
}
