package storage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"hf2s3/pkg/db"
	"hf2s3/pkg/hfclient"
	"hf2s3/pkg/models"
)

// wakeGC asks the background worker to look at the deletion queue soon.
func (p *PoolManager) wakeGC() {
	select {
	case p.gcWake <- struct{}{}:
	default:
	}
}

// gcNow processes the queue synchronously and best-effort, so a DELETE that
// succeeds against Hugging Face is complete when the API call returns. Entries
// that fail stay queued and are retried by the background worker.
func (p *PoolManager) gcNow(ctx context.Context) {
	// If the worker is already draining the queue it will also handle what we
	// just added; do not make an API request wait behind it.
	if !p.gcMu.TryLock() {
		return
	}
	defer p.gcMu.Unlock()
	bg, cancel := context.WithTimeout(context.WithoutCancel(ctx), 60*time.Second)
	defer cancel()
	p.processPendingLocked(bg)
}

func deletionBackoff(attempts int) time.Duration {
	d := 30 * time.Second
	for i := 0; i < attempts && d < 6*time.Hour; i++ {
		d *= 2
	}
	if d > 6*time.Hour {
		d = 6 * time.Hour
	}
	return d
}

func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "status 404") || strings.Contains(s, "not found") || strings.Contains(s, "does not exist")
}

type gcGroup struct {
	kind      string
	accountID int64
	items     []models.PendingDeletion
}

// ProcessPendingDeletions deletes queued remote objects (HF dataset chunks and
// cache copies), grouped so each account gets one commit per batch. Failures
// are retried later with exponential backoff; usage counters are only
// decremented for deletions that really happened.
func (p *PoolManager) ProcessPendingDeletions(ctx context.Context) (done, failed int) {
	p.gcMu.Lock()
	defer p.gcMu.Unlock()
	return p.processPendingLocked(ctx)
}

// processPendingLocked does the work of ProcessPendingDeletions; gcMu must be held.
func (p *PoolManager) processPendingLocked(ctx context.Context) (done, failed int) {
	for round := 0; round < 50 && ctx.Err() == nil; round++ {
		due, err := p.db.ListDueDeletions(ctx, 200)
		if err != nil {
			slog.Error("gc: list pending deletions", "err", err)
			return
		}
		if len(due) == 0 {
			return
		}

		groups := map[string]*gcGroup{}
		for _, item := range due {
			k := fmt.Sprintf("%s/%d", item.Kind, item.AccountID)
			g, ok := groups[k]
			if !ok {
				g = &gcGroup{kind: item.Kind, accountID: item.AccountID}
				groups[k] = g
			}
			g.items = append(g.items, item)
		}
		keys := make([]string, 0, len(groups))
		for k := range groups {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		roundDone := 0
		for _, k := range keys {
			g := groups[k]
			var d, f int
			switch g.kind {
			case models.DeletionChunk:
				d, f = p.gcChunks(ctx, g)
			case models.DeletionCache:
				d, f = p.gcCache(ctx, g)
			default:
				for _, it := range g.items {
					_ = p.db.DropDeletion(ctx, it.ID)
				}
			}
			roundDone += d
			done += d
			failed += f
		}
		if roundDone == 0 {
			return // everything left has been rescheduled with backoff
		}
	}
	return
}

func (p *PoolManager) failItems(ctx context.Context, items []models.PendingDeletion, cause error) {
	for _, it := range items {
		if err := p.db.FailDeletion(ctx, it.ID, cause.Error(), deletionBackoff(it.Attempts)); err != nil {
			slog.Error("gc: record failed deletion", "id", it.ID, "err", err)
		}
	}
}

func (p *PoolManager) gcChunks(ctx context.Context, g *gcGroup) (done, failed int) {
	acc, err := p.db.GetAccountByID(ctx, g.accountID)
	if errors.Is(err, db.ErrNotFound) {
		slog.Warn("gc: dropping deletions of a removed account", "account", g.accountID, "count", len(g.items))
		for _, it := range g.items {
			_ = p.db.DropDeletion(ctx, it.ID)
		}
		return len(g.items), 0
	}
	if err != nil {
		p.failItems(ctx, g.items, err)
		return 0, len(g.items)
	}

	paths := make([]string, len(g.items))
	for i, it := range g.items {
		paths[i] = it.RemotePath
	}

	err = p.hfClient.DeleteChunks(ctx, acc.Token, acc.RepoName, paths)
	if err == nil {
		if cerr := p.db.CompleteDeletions(ctx, g.items); cerr != nil {
			slog.Error("gc: complete deletions", "err", cerr)
			return 0, len(g.items)
		}
		return len(g.items), 0
	}

	var rle *hfclient.RateLimitError
	if errors.As(err, &rle) {
		p.hfClient.SetCooldown(acc.Token, rle.RetryAfter)
		p.failItems(ctx, g.items, err)
		return 0, len(g.items)
	}

	// A batch fails as a whole if one path is bad (e.g. already gone); retry the
	// items one by one so a single bad entry cannot block the rest.
	if len(g.items) == 1 {
		if isNotFound(err) {
			_ = p.db.CompleteDeletions(ctx, g.items)
			return 1, 0
		}
		p.failItems(ctx, g.items, err)
		return 0, 1
	}
	for _, it := range g.items {
		if ctx.Err() != nil {
			break
		}
		single := &gcGroup{kind: g.kind, accountID: g.accountID, items: []models.PendingDeletion{it}}
		d, f := p.gcChunks(ctx, single)
		done += d
		failed += f
	}
	return done, failed
}

func (p *PoolManager) gcCache(ctx context.Context, g *gcGroup) (done, failed int) {
	client, err := p.cacheClientStrict(ctx, g.accountID)
	if err != nil {
		// The bucket (and its credentials) no longer exist: nothing can be done.
		slog.Warn("gc: dropping cache deletions of an unavailable bucket", "bucket", g.accountID, "count", len(g.items), "err", err)
		for _, it := range g.items {
			_ = p.db.DropDeletion(ctx, it.ID)
		}
		return len(g.items), 0
	}

	var completed []models.PendingDeletion
	for _, it := range g.items {
		if ctx.Err() != nil {
			break
		}
		if err := client.DeleteObject(ctx, client.Bucket(), it.RemotePath); err != nil {
			p.failItems(ctx, []models.PendingDeletion{it}, err)
			failed++
			continue
		}
		completed = append(completed, it)
	}
	if err := p.db.CompleteDeletions(ctx, completed); err != nil {
		slog.Error("gc: complete cache deletions", "err", err)
		return 0, len(g.items)
	}
	return len(completed), failed
}
