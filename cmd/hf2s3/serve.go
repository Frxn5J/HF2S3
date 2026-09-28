package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strings"
	"time"

	"hf2s3/pkg/storage"
)

// statusWriter records the status code and body size for the access log.
type statusWriter struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(p)
	w.bytes += int64(n)
	return n, err
}

func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func routeOf(path string) string {
	switch {
	case strings.HasPrefix(path, "/api/"):
		return "api"
	case strings.HasPrefix(path, "/media/"):
		return "media"
	case path == "/metrics":
		return "metrics"
	case path == "/style.css", path == "/app.js", path == "/favicon.ico", strings.HasPrefix(path, "/static/"):
		return "static"
	}
	return "s3"
}

func quietPath(path string) bool {
	return path == "/api/health" || path == "/api/ready" || routeOf(path) == "static"
}

// instrument adds panic recovery, an access log (never including the query
// string: it carries presigned signatures) and request metrics.
func (a *App) instrument(next http.Handler) http.Handler {
	a.Metrics.Describe("hf2s3_http_requests_total", "HTTP requests by route and status class.")
	a.Metrics.Describe("hf2s3_http_response_bytes_total", "Response body bytes by route.")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w}
		defer func() {
			if rec := recover(); rec != nil {
				if rec == http.ErrAbortHandler {
					panic(rec)
				}
				slog.Error("panic while serving request", "method", r.Method, "path", r.URL.Path, "panic", fmt.Sprint(rec), "stack", string(debug.Stack()))
				if sw.status == 0 {
					http.Error(sw, "internal server error", http.StatusInternalServerError)
				}
			}
			status := sw.status
			if status == 0 {
				status = http.StatusOK
			}
			route := routeOf(r.URL.Path)
			a.Metrics.Add("hf2s3_http_requests_total", fmt.Sprintf(`route=%q,class="%dxx"`, route, status/100), 1)
			a.Metrics.Add("hf2s3_http_response_bytes_total", fmt.Sprintf(`route=%q`, route), sw.bytes)
			if quietPath(r.URL.Path) && status < 500 {
				return
			}
			slog.Info("request",
				"request_id", sw.Header().Get("x-amz-request-id"),
				"method", r.Method,
				"path", r.URL.Path,
				"status", status,
				"bytes", sw.bytes,
				"duration_ms", time.Since(start).Milliseconds(),
				"remote", a.Admin.ClientIP(r),
			)
		}()
		next.ServeHTTP(sw, r)
	})
}

// router dispatches between the web console, metrics and the S3 API.
func (a *App) router() http.Handler {
	console := a.Console.Handler()
	metricsHandler := a.Metrics.Handler(a.Cfg.MetricsToken)

	return a.instrument(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path

		switch {
		case strings.HasPrefix(path, "/api/"):
			console.ServeHTTP(w, r)
		case path == "/style.css", path == "/app.js", path == "/favicon.ico", strings.HasPrefix(path, "/static/"):
			console.ServeHTTP(w, r)
		case path == "/metrics":
			metricsHandler.ServeHTTP(w, r)
		case path == "/" || path == "":
			// Browsers get the console; S3 clients (signed header or presigned
			// query) get ListBuckets.
			isBrowser := strings.Contains(r.Header.Get("Accept"), "text/html") &&
				!strings.HasPrefix(r.Header.Get("Authorization"), "AWS") &&
				r.URL.Query().Get("X-Amz-Algorithm") == ""
			if isBrowser {
				console.ServeHTTP(w, r)
				return
			}
			a.S3.ServeHTTP(w, r)
		default:
			a.S3.ServeHTTP(w, r)
		}
	}))
}

func (a *App) registerGauges() {
	ctx := context.Background()
	a.Metrics.Gauge("hf2s3_pending_deletions", "Remote objects queued for deletion.", func() float64 {
		n, _ := a.DB.CountPendingDeletions(ctx)
		return float64(n)
	})
	a.Metrics.Gauge("hf2s3_chunks_needing_rekey", "Chunks not yet in the current encryption format.", func() float64 {
		n, _, _ := a.DB.CountChunksToRekey(ctx, a.Keyring.CurrentKeyID())
		return float64(n)
	})
	a.Metrics.Gauge("hf2s3_objects", "Stored objects.", func() float64 {
		if s, err := a.DB.GetStats(ctx); err == nil {
			return float64(s.TotalObjects)
		}
		return 0
	})
	a.Metrics.Gauge("hf2s3_cached_objects", "Objects with a copy in the cache tier.", func() float64 {
		if s, err := a.DB.GetStats(ctx); err == nil {
			return float64(s.CachedObjects)
		}
		return 0
	})
	a.Metrics.Gauge("hf2s3_accounts_throttled", "Hugging Face accounts currently rate limited.", func() float64 {
		accounts, err := a.DB.ListAccounts(ctx)
		if err != nil {
			return 0
		}
		n := 0
		for _, acc := range accounts {
			if throttled, _ := a.HF.IsThrottled(acc.Token); throttled {
				n++
			}
		}
		return float64(n)
	})
}

// Run serves until ctx is cancelled, then shuts down gracefully: in-flight
// requests get Cfg.ShutdownTimeout to finish, background work is stopped and
// the database is closed last.
func (a *App) Run(ctx context.Context) error {
	a.registerGauges()

	a.Pool.StartBackground(storage.DefaultBackgroundConfig())
	bgCtx, stopBackups := context.WithCancel(ctx)
	defer stopBackups()
	backupsDone := make(chan struct{})
	go func() {
		defer close(backupsDone)
		a.Backups.Loop(bgCtx, a.Cfg.BackupInterval)
	}()

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", a.Cfg.Port),
		Handler:           a.router(),
		ReadHeaderTimeout: 30 * time.Second, // slowloris protection without cutting streaming bodies
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	a.printBanner()

	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	var serveErr error
	select {
	case <-ctx.Done():
		slog.Info("shutting down: draining in-flight requests", "timeout", a.Cfg.ShutdownTimeout.String())
	case serveErr = <-errCh:
		slog.Error("server stopped unexpectedly", "err", serveErr)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), a.Cfg.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Warn("some requests did not finish before the shutdown timeout", "err", err)
		_ = srv.Close()
	}
	stopBackups()
	<-backupsDone
	if err := a.Pool.Shutdown(shutdownCtx); err != nil {
		slog.Warn("background work did not stop in time", "err", err)
	}
	if err := a.Close(); err != nil {
		slog.Warn("closing the database", "err", err)
	}
	slog.Info("stopped")
	return serveErr
}

// printBanner logs the effective configuration. Secrets are never printed.
func (a *App) printBanner() {
	stats, _ := a.DB.GetStats(context.Background())
	attrs := []any{
		"port", a.Cfg.Port,
		"region", a.Settings.S3Region,
		"access_key_id", a.Settings.AccessKeyID,
		"encryption_key_id", a.Keyring.CurrentKeyID(),
		"cache_configured", a.Pool.HasCacheConfigured(context.Background()),
		"get_redirect", a.Cfg.RedirectMode,
		"backups_dir", a.Backups.Dir,
		"schema_version", dbSchemaVersion(),
	}
	if stats != nil {
		attrs = append(attrs, "objects", stats.TotalObjects, "cached_objects", stats.CachedObjects, "pending_deletions", stats.PendingDeletions)
	}
	if n, _, err := a.DB.CountChunksToRekey(context.Background(), a.Keyring.CurrentKeyID()); err == nil && n > 0 {
		attrs = append(attrs, "chunks_needing_rekey", n)
		slog.Warn("some chunks still use an old encryption format or key; run `hf2s3 rekey`", "chunks", n)
	}
	if a.Cfg.Dev {
		slog.Warn("HF2S3_DEV is set: insecure defaults are allowed")
	}
	slog.Info("HF2S3 gateway started", attrs...)
}
