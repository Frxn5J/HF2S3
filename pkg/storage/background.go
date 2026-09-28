package storage

import (
	"context"
	"log/slog"
	"time"
)

// BackgroundConfig tunes the maintenance loops started by StartBackground.
// A zero interval disables the corresponding loop.
type BackgroundConfig struct {
	GCInterval        time.Duration // retry pending remote deletions
	EvictInterval     time.Duration // LRU cache eviction sweep
	EvictHighWater    float64       // start evicting above this fill ratio
	EvictLowWater     float64       // evict down to this fill ratio
	MultipartTTL      time.Duration // abort multipart uploads older than this
	ReconcileInterval time.Duration // re-sync account usage with Hugging Face
}

func DefaultBackgroundConfig() BackgroundConfig {
	return BackgroundConfig{
		GCInterval:        time.Minute,
		EvictInterval:     5 * time.Minute,
		EvictHighWater:    0.90,
		EvictLowWater:     0.75,
		MultipartTTL:      24 * time.Hour,
		ReconcileInterval: time.Hour,
	}
}

// StartBackground launches the maintenance loops. They stop on Shutdown.
func (p *PoolManager) StartBackground(cfg BackgroundConfig) {
	if !p.trackBackground() {
		return
	}
	go func() {
		defer p.bgWG.Done()

		tick := func(d time.Duration) <-chan time.Time {
			if d <= 0 {
				return nil
			}
			return time.NewTicker(d).C
		}
		gcTick := tick(cfg.GCInterval)
		evictTick := tick(cfg.EvictInterval)
		mpTick := tick(time.Hour)
		reconcileTick := tick(cfg.ReconcileInterval)

		// Work left over from a previous run (or a crash) is picked up right away.
		p.ProcessPendingDeletions(p.bgCtx)

		for {
			select {
			case <-p.bgCtx.Done():
				return
			case <-p.gcWake:
				p.ProcessPendingDeletions(p.bgCtx)
			case <-gcTick:
				p.ProcessPendingDeletions(p.bgCtx)
			case <-evictTick:
				if _, err := p.EvictToWatermarks(p.bgCtx, cfg.EvictHighWater, cfg.EvictLowWater); err != nil && p.bgCtx.Err() == nil {
					slog.Warn("cache eviction sweep failed", "err", err)
				}
			case <-mpTick:
				if cfg.MultipartTTL > 0 {
					p.AbortStaleMultipartUploads(p.bgCtx, cfg.MultipartTTL)
				}
			case <-reconcileTick:
				p.ReconcileAccountUsage(p.bgCtx)
			}
		}
	}()
}

// Shutdown stops background work and waits (until ctx expires) for running
// promotions, deletions and sweeps to finish.
func (p *PoolManager) Shutdown(ctx context.Context) error {
	p.bgMu.Lock()
	p.bgClosed = true
	p.bgMu.Unlock()
	p.bgCancel()
	done := make(chan struct{})
	go func() {
		p.bgWG.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// AbortStaleMultipartUploads aborts uploads that were started more than ttl
// ago and never completed, releasing their chunks.
func (p *PoolManager) AbortStaleMultipartUploads(ctx context.Context, ttl time.Duration) int {
	ids, err := p.db.ListStaleMultipartUploads(ctx, time.Now().Add(-ttl))
	if err != nil {
		slog.Warn("listing stale multipart uploads failed", "err", err)
		return 0
	}
	n := 0
	for _, id := range ids {
		if err := p.db.AbortMultipartUploadAndQueue(ctx, id); err != nil {
			slog.Warn("aborting stale multipart upload failed", "upload_id", id, "err", err)
			continue
		}
		n++
	}
	if n > 0 {
		slog.Info("aborted stale multipart uploads", "count", n)
		p.wakeGC()
	}
	return n
}

// ReconcileAccountUsage re-reads each account's repository size from Hugging
// Face, correcting drift from failed or out-of-band operations. Accounts with
// uploads in flight are skipped so a racing write cannot be overwritten.
func (p *PoolManager) ReconcileAccountUsage(ctx context.Context) {
	accounts, err := p.db.ListAccounts(ctx)
	if err != nil {
		return
	}
	for _, acc := range accounts {
		if ctx.Err() != nil {
			return
		}
		p.inFlightMu.Lock()
		busy := p.inFlightCounts[acc.ID] > 0
		p.inFlightMu.Unlock()
		if busy {
			continue
		}
		size, err := p.hfClient.GetRepoTreeSize(ctx, acc.Token, acc.RepoName)
		if err != nil {
			slog.Debug("usage reconcile failed", "account", acc.ID, "err", err)
			continue
		}
		if size != acc.UsedBytes {
			slog.Info("account usage reconciled", "account", acc.ID, "was", acc.UsedBytes, "now", size)
			_ = p.db.UpdateAccountUsage(ctx, acc.ID, size)
		}
	}
}

// Go runs fn in a goroutine tied to the pool's lifetime: it receives a context
// that Shutdown cancels, and Shutdown waits for it to return.
func (p *PoolManager) Go(fn func(ctx context.Context)) {
	if !p.trackBackground() {
		return
	}
	go func() {
		defer p.bgWG.Done()
		fn(p.bgCtx)
	}()
}

// trackBackground registers one unit of background work. It reports false once
// Shutdown has begun, so no work can start after Wait.
func (p *PoolManager) trackBackground() bool {
	p.bgMu.Lock()
	defer p.bgMu.Unlock()
	if p.bgClosed {
		return false
	}
	p.bgWG.Add(1)
	return true
}
