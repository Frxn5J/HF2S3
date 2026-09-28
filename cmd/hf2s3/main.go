// Command hf2s3 is a multi-tier S3-compatible gateway over Hugging Face.
//
//	hf2s3 [serve] [flags]        run the gateway (default)
//	hf2s3 keygen                 print fresh secrets for a new deployment
//	hf2s3 rekey [flags]          re-encrypt old chunks with the current key
//	hf2s3 squash [flags]         drop old ciphertext from dataset git history
//	hf2s3 backup <file>          write a snapshot of the database
//	hf2s3 restore <file>         replace the database (service must be stopped)
//	hf2s3 version
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"hf2s3/pkg/config"
	"hf2s3/pkg/db"
)

// version is set at build time: -ldflags "-X main.version=1.2.3".
var version = "dev"

func dbSchemaVersion() int { return db.SchemaVersion() }

func setupLogging(cfg *config.Config) {
	level := slog.Level(config.ParseLevel(cfg.LogLevel))
	opts := &slog.HandlerOptions{Level: level}
	var h slog.Handler
	if cfg.LogFormat == "text" {
		h = slog.NewTextHandler(os.Stderr, opts)
	} else {
		h = slog.NewJSONHandler(os.Stderr, opts)
	}
	// Also routes the standard library's log package through the same handler.
	slog.SetDefault(slog.New(h))
}

func loadConfig() (*config.Config, error) {
	exeDir := ""
	if len(os.Args) > 0 {
		exeDir = filepath.Dir(os.Args[0])
	}
	config.LoadEnvFile(".env", filepath.Join(exeDir, ".env"), "../.env")
	return config.FromEnvironment(os.Getenv)
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "hf2s3: "+format+"\n", args...)
	os.Exit(1)
}

func main() {
	args := os.Args[1:]
	cmd := "serve"
	if len(args) > 0 && len(args[0]) > 0 && args[0][0] != '-' {
		cmd, args = args[0], args[1:]
	}

	switch cmd {
	case "serve":
		runServe(args)
	case "keygen":
		runKeygen(args)
	case "rekey":
		runRekey(args)
	case "squash":
		runSquash(args)
	case "backup":
		runBackup(args)
	case "restore":
		runRestore(args)
	case "version", "--version", "-v":
		fmt.Println("hf2s3", version)
	case "help", "-h", "--help":
		fmt.Print(usage)
	default:
		fmt.Fprintf(os.Stderr, "hf2s3: unknown command %q\n\n%s", cmd, usage)
		os.Exit(2)
	}
}

const usage = `Usage: hf2s3 [command] [flags]

Commands:
  serve            Run the gateway (default). Configuration comes from environment variables.
  keygen           Print a .env template with freshly generated secrets.
  rekey            Re-encrypt chunks that use an old format or key (--dry-run to preview).
  squash           Purge superseded ciphertext from the datasets' git history.
  backup <file>    Write a consistent snapshot of the metadata database.
  restore <file>   Replace the database with a backup (stop the service first).
  version          Print the version.
`

func runServe(args []string) {
	cfg, err := loadConfig()
	if err != nil {
		fatal("%v", err)
	}
	if err := parseServeFlags(cfg, args); err != nil {
		fatal("%v", err)
	}
	setupLogging(cfg)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// A restore confirmed in the console is applied here, before anything has the
	// database open. On failure the current database stays in place.
	applied, previous, rerr := applyPendingRestore(cfg.DBPath)
	if rerr != nil {
		slog.Error("could not apply the pending database restore; continuing with the current database", "err", rerr)
	}

	app, err := Bootstrap(ctx, cfg, BootstrapOptions{})
	if err != nil && applied {
		// The restored database passed validation but does not start: go back to the old one.
		slog.Error("the restored database does not start; rolling back", "err", err)
		if rb := rollbackRestore(cfg.DBPath, previous, err); rb != nil {
			slog.Error("rollback failed", "err", rb)
		} else {
			app, err = Bootstrap(ctx, cfg, BootstrapOptions{})
		}
	}
	if err != nil {
		slog.Error("cannot start", "err", err)
		fmt.Fprintf(os.Stderr, "\nhf2s3: %v\n", err)
		os.Exit(1)
	}
	if err := app.Run(ctx); err != nil {
		if errors.Is(err, ErrRestart) {
			// Everything is closed; start over so the restored database is opened cleanly.
			if rerr := reexec(); rerr != nil {
				slog.Error("could not restart in place; exiting so the supervisor restarts the service", "err", rerr)
				os.Exit(0)
			}
			return
		}
		os.Exit(1)
	}
}
