package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"flag"
	"fmt"
	"math/big"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"hf2s3/pkg/backup"
	"hf2s3/pkg/config"
	"hf2s3/pkg/crypto"
	"hf2s3/pkg/db"
	"hf2s3/pkg/storage"
)

// parseServeFlags applies command-line flags on top of the environment. Flags
// are kept for compatibility with earlier releases; environment variables are
// preferred because flags are visible in the process list.
func parseServeFlags(cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	port := fs.Int("port", cfg.Port, "port to listen on (PORT)")
	dbPath := fs.String("db", cfg.DBPath, "path of the SQLite database (HF2S3_DB)")
	chunk := fs.Int("chunk-size", cfg.ChunkSizeMB, "chunk size in MB (HF2S3_CHUNK_SIZE_MB)")
	access := fs.String("access-key", "", "S3 access key ID (HF2S3_ACCESS_KEY)")
	secret := fs.String("secret-key", "", "S3 secret access key (HF2S3_SECRET_KEY)")
	master := fs.String("master-key", "", "master key (HF2S3_MASTER_KEY); prefer the environment")
	region := fs.String("region", cfg.Region, "S3 region (HF2S3_REGION)")
	adminUser := fs.String("admin-user", "", "console admin user (ADMIN_USERNAME)")
	adminPass := fs.String("admin-pass", "", "console admin password (ADMIN_PASSWORD)")
	hfEndpoint := fs.String("hf-storage-endpoint", cfg.HFStorageEndpoint, "cache bucket endpoint (HF_STORAGE_ENDPOINT)")
	hfRegion := fs.String("hf-storage-region", cfg.HFStorageRegion, "cache bucket region (HF_STORAGE_REGION)")
	hfAccess := fs.String("hf-storage-access-key", "", "cache bucket access key (HF_STORAGE_ACCESS_KEY)")
	hfSecret := fs.String("hf-storage-secret-key", "", "cache bucket secret key (HF_STORAGE_SECRET_KEY)")
	hfBucket := fs.String("hf-storage-bucket", "", "cache bucket name (HF_STORAGE_BUCKET)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	fs.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "port":
			cfg.Port = *port
		case "db":
			cfg.DBPath = *dbPath
		case "chunk-size":
			cfg.ChunkSizeMB, cfg.FromEnv["chunk_size_mb"] = *chunk, true
		case "access-key":
			cfg.AccessKey, cfg.FromEnv["access_key_id"] = *access, true
		case "secret-key":
			cfg.SecretKey, cfg.FromEnv["secret_access_key"] = *secret, true
		case "master-key":
			cfg.MasterKey = *master
		case "region":
			cfg.Region, cfg.FromEnv["s3_region"] = *region, true
		case "admin-user":
			cfg.AdminUser, cfg.FromEnv["admin_username"] = *adminUser, true
		case "admin-pass":
			cfg.AdminPass, cfg.FromEnv["admin_password"] = *adminPass, true
		case "hf-storage-endpoint":
			cfg.HFStorageEndpoint, cfg.FromEnv["hf_storage"] = *hfEndpoint, true
		case "hf-storage-region":
			cfg.HFStorageRegion, cfg.FromEnv["hf_storage"] = *hfRegion, true
		case "hf-storage-access-key":
			cfg.HFStorageAccessKey, cfg.FromEnv["hf_storage"] = *hfAccess, true
		case "hf-storage-secret-key":
			cfg.HFStorageSecretKey, cfg.FromEnv["hf_storage"] = *hfSecret, true
		case "hf-storage-bucket":
			cfg.HFStorageBucket, cfg.FromEnv["hf_storage"] = *hfBucket, true
		}
	})
	return nil
}

func randomString(n int, alphabet string) string {
	out := make([]byte, n)
	max := big.NewInt(int64(len(alphabet)))
	for i := range out {
		v, err := rand.Int(rand.Reader, max)
		if err != nil {
			fatal("random: %v", err)
		}
		out[i] = alphabet[v.Int64()]
	}
	return string(out)
}

// runKeygen prints a ready-to-use configuration with fresh random secrets.
func runKeygen(args []string) {
	master, err := crypto.GenerateMasterKey()
	if err != nil {
		fatal("%v", err)
	}
	secretBytes := make([]byte, 32)
	if _, err := rand.Read(secretBytes); err != nil {
		fatal("%v", err)
	}
	const alnum = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

	fmt.Fprintln(os.Stderr, "# Fresh secrets for a new HF2S3 deployment. Store them in your secret manager;")
	fmt.Fprintln(os.Stderr, "# HF2S3_MASTER_KEY cannot be recovered: without it the stored data cannot be decrypted.")
	fmt.Println("HF2S3_MASTER_KEY=" + master)
	fmt.Println("HF2S3_ACCESS_KEY=HF2S" + randomString(16, "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"))
	fmt.Println("HF2S3_SECRET_KEY=" + base64.RawURLEncoding.EncodeToString(secretBytes))
	fmt.Println("ADMIN_USERNAME=admin")
	fmt.Println("ADMIN_PASSWORD=" + randomString(24, alnum))
	fmt.Println("HF2S3_METRICS_TOKEN=" + randomString(32, alnum))
}

func signalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

// maintenanceApp boots the app for offline commands.
func maintenanceApp(ctx context.Context) *App {
	cfg, err := loadConfig()
	if err != nil {
		fatal("%v", err)
	}
	cfg.LogFormat = "text"
	setupLogging(cfg)
	app, err := Bootstrap(ctx, cfg, BootstrapOptions{SkipCredentialChecks: true})
	if err != nil {
		fatal("%v", err)
	}
	return app
}

func runRekey(args []string) {
	fs := flag.NewFlagSet("rekey", flag.ExitOnError)
	dry := fs.Bool("dry-run", false, "only report how much work there is")
	batch := fs.Int("batch", 32, "chunks per commit group")
	workers := fs.Int("workers", 4, "parallel chunk transfers")
	purge := fs.Bool("purge-legacy-keys", false, "delete the stored legacy keys once nothing needs re-keying")
	_ = fs.Parse(args)

	ctx, stop := signalContext()
	defer stop()
	app := maintenanceApp(ctx)
	defer app.Close()

	rep, err := app.Pool.Rekey(ctx, storage.RekeyOptions{
		DryRun: *dry, BatchSize: *batch, Workers: *workers,
		Progress: func(p storage.RekeyProgress) {
			fmt.Printf("  re-keyed %d/%d chunks (%d failed)\n", p.Done, p.Total, p.Failed)
		},
	})
	if rep != nil {
		fmt.Printf("chunks needing work: %d (%.1f MiB of ciphertext)\n", rep.Total, float64(rep.Bytes)/(1<<20))
		if !*dry {
			fmt.Printf("re-keyed: %d   failed: %d\n", rep.Done, rep.Failed)
			for _, f := range rep.Failures {
				fmt.Printf("  FAILED chunk %d (%s): %s\n", f.ChunkID, f.Path, f.Err)
			}
		}
	}
	if err != nil {
		fatal("%v", err)
	}
	if *dry {
		if rep != nil && rep.Total > 0 && !app.Keyring.HasLegacyKeys() {
			fmt.Println("warning: no legacy keys are configured; old chunks cannot be decrypted (set HF2S3_LEGACY_MASTER_KEYS)")
		}
		return
	}

	// Old ciphertext is only deleted after the database points at the new copy.
	done, failed := app.Pool.ProcessPendingDeletions(ctx)
	left, _ := app.DB.CountPendingDeletions(ctx)
	fmt.Printf("deleted %d superseded remote object(s); %d failed; %d still queued\n", done, failed, left)

	remaining, _, _ := app.DB.CountChunksToRekey(ctx, app.Keyring.CurrentKeyID())
	if remaining > 0 {
		fmt.Printf("%d chunk(s) still need re-keying; fix the failures above and run again\n", remaining)
		os.Exit(1)
	}
	if *purge {
		if err := app.DB.DeleteSetting(ctx, legacyKeysSetting); err != nil {
			fatal("purging legacy keys: %v", err)
		}
		fmt.Println("stored legacy keys removed. Also remove HF2S3_LEGACY_MASTER_KEYS from your environment.")
	}
	fmt.Println("done. Next: `hf2s3 squash --yes` to purge the old ciphertext from the datasets' git history.")
}

func runSquash(args []string) {
	fs := flag.NewFlagSet("squash", flag.ExitOnError)
	account := fs.Int64("account", 0, "account id to squash (0 = all accounts)")
	yes := fs.Bool("yes", false, "confirm: this rewrites the dataset history and cannot be undone")
	_ = fs.Parse(args)

	ctx, stop := signalContext()
	defer stop()
	app := maintenanceApp(ctx)
	defer app.Close()

	app.Pool.ProcessPendingDeletions(ctx)
	if left, _ := app.DB.CountPendingDeletions(ctx); left > 0 {
		fatal("%d remote deletions are still queued; run `hf2s3 rekey` or wait for the queue to drain before squashing", left)
	}

	accounts, err := app.DB.ListAccounts(ctx)
	if err != nil {
		fatal("%v", err)
	}
	for _, acc := range accounts {
		if *account != 0 && acc.ID != *account {
			continue
		}
		if !*yes {
			fmt.Printf("would squash %s (account %d); re-run with --yes to proceed\n", acc.RepoName, acc.ID)
			continue
		}
		fmt.Printf("squashing %s ...\n", acc.RepoName)
		if err := app.HF.SuperSquash(ctx, acc.Token, acc.RepoName, "Squash history"); err != nil {
			fmt.Printf("  FAILED: %v\n", err)
			continue
		}
		if size, err := app.HF.GetRepoTreeSize(ctx, acc.Token, acc.RepoName); err == nil {
			_ = app.DB.UpdateAccountUsage(ctx, acc.ID, size)
		}
		fmt.Println("  done")
	}
	if !*yes {
		os.Exit(1)
	}
}

func runBackup(args []string) {
	if len(args) != 1 {
		fatal("usage: hf2s3 backup <file>")
	}
	cfg, err := loadConfig()
	if err != nil {
		fatal("%v", err)
	}
	database, err := db.Open(cfg.DBPath)
	if err != nil {
		fatal("%v", err)
	}
	defer database.Close()
	if err := database.BackupToFile(context.Background(), args[0]); err != nil {
		fatal("%v", err)
	}
	fmt.Println("backup written to", args[0])
}

func runRestore(args []string) {
	fs := flag.NewFlagSet("restore", flag.ExitOnError)
	dbPath := fs.String("db", "", "database to replace (default: HF2S3_DB)")
	_ = fs.Parse(args)
	if fs.NArg() != 1 {
		fatal("usage: hf2s3 restore [-db path] <backup.db | backup.db.enc>")
	}
	src := fs.Arg(0)

	cfg, err := loadConfig()
	if err != nil {
		fatal("%v", err)
	}
	target := cfg.DBPath
	if *dbPath != "" {
		target = *dbPath
	}

	if strings.HasSuffix(src, ".enc") {
		master, err := crypto.ParseMasterKey(cfg.MasterKey)
		if err != nil {
			fatal("HF2S3_MASTER_KEY is needed to open an encrypted backup: %v", err)
		}
		kr, err := crypto.NewKeyring(master, nil)
		if err != nil {
			fatal("%v", err)
		}
		blob, err := os.ReadFile(src)
		if err != nil {
			fatal("%v", err)
		}
		plain, err := backup.Decrypt(kr, blob)
		if err != nil {
			fatal("cannot decrypt the backup (wrong HF2S3_MASTER_KEY?): %v", err)
		}
		tmp := filepath.Join(os.TempDir(), "hf2s3-restore.db")
		if err := os.WriteFile(tmp, plain, 0o600); err != nil {
			fatal("%v", err)
		}
		defer os.Remove(tmp)
		src = tmp
	}

	if err := db.RestoreFile(context.Background(), src, target); err != nil {
		fatal("%v", err)
	}
	fmt.Printf("database restored to %s (the previous file, if any, was kept as %s.pre-restore)\n", target, target)
	fmt.Println("start the service again; pending schema migrations run on start-up.")
}
