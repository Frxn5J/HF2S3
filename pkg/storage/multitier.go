package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"hf2s3/pkg/db"
	"hf2s3/pkg/hfstorage"
	"hf2s3/pkg/models"
)

var (
	ErrNoCacheConfigured = errors.New("hugging face storage bucket cache is not configured")
	ErrObjectNotInCache  = errors.New("object is not currently in cache tier")
	ErrNoCacheSpace      = errors.New("no cache bucket has enough free space")
	ErrObjectTooLarge    = errors.New("object exceeds the maximum size promoted to the cache tier")
)

// cacheObjectPath is the key of an object's copy inside a cache bucket. It
// embeds the object's row id, so a promotion that finishes after the object was
// overwritten can never clobber (or be deleted together with) the new version.
func cacheObjectPath(obj *models.Object) string {
	return fmt.Sprintf("v/%d/%s/%s", obj.ID, obj.Bucket, obj.Key)
}

// SetCacheClient attaches a fallback or legacy Hugging Face Storage Bucket client (Bucket ID 0)
func (p *PoolManager) SetCacheClient(client *hfstorage.S3Client) {
	p.cacheClientsMu.Lock()
	defer p.cacheClientsMu.Unlock()
	p.cacheClients[0] = client
}

// CacheClient returns the fallback Hugging Face Storage Bucket client (ID 0)
func (p *PoolManager) CacheClient() *hfstorage.S3Client {
	p.cacheClientsMu.RLock()
	defer p.cacheClientsMu.RUnlock()
	return p.cacheClients[0]
}

// InvalidateCacheClient removes a cached S3 client instance from the pool manager
func (p *PoolManager) InvalidateCacheClient(bucketID int64) {
	p.cacheClientsMu.Lock()
	defer p.cacheClientsMu.Unlock()
	delete(p.cacheClients, bucketID)
}

func (p *PoolManager) clientForBucket(cb *models.CacheBucket) *hfstorage.S3Client {
	return hfstorage.NewS3Client(hfstorage.S3ClientConfig{
		Endpoint:  cb.Endpoint,
		Region:    cb.Region,
		AccessKey: cb.AccessKey,
		SecretKey: cb.SecretKey,
		Bucket:    cb.BucketName,
	})
}

// cacheClientStrict returns exactly the client of one cache bucket, with no
// fallback. Deleting or reading through the wrong bucket must never happen, so
// everything that acts on a stored location uses this.
func (p *PoolManager) cacheClientStrict(ctx context.Context, bucketID int64) (*hfstorage.S3Client, error) {
	if bucketID <= 0 {
		p.cacheClientsMu.RLock()
		fallback := p.cacheClients[0]
		p.cacheClientsMu.RUnlock()
		if fallback != nil && fallback.IsConfigured() {
			return fallback, nil
		}
		return nil, ErrNoCacheConfigured
	}

	p.cacheClientsMu.RLock()
	cli, ok := p.cacheClients[bucketID]
	p.cacheClientsMu.RUnlock()
	if ok && cli != nil {
		return cli, nil
	}

	cb, err := p.db.GetCacheBucketByID(ctx, bucketID)
	if err != nil {
		return nil, fmt.Errorf("cache bucket %d: %w", bucketID, err)
	}
	cli = p.clientForBucket(cb)
	p.cacheClientsMu.Lock()
	p.cacheClients[bucketID] = cli
	p.cacheClientsMu.Unlock()
	return cli, nil
}

// GetCacheClient retrieves or lazily instantiates an S3Client for a given cache bucket ID.
// If bucketID is 0 or not found in cache_buckets, it falls back to the default cache client or first active bucket.
func (p *PoolManager) GetCacheClient(ctx context.Context, bucketID int64) (*hfstorage.S3Client, *models.CacheBucket, error) {
	if bucketID > 0 {
		p.cacheClientsMu.RLock()
		cli, ok := p.cacheClients[bucketID]
		p.cacheClientsMu.RUnlock()
		if ok && cli != nil {
			return cli, nil, nil
		}

		cb, err := p.db.GetCacheBucketByID(ctx, bucketID)
		if err == nil && cb != nil {
			cli = p.clientForBucket(cb)
			p.cacheClientsMu.Lock()
			p.cacheClients[bucketID] = cli
			p.cacheClientsMu.Unlock()
			return cli, cb, nil
		}
		// If specific cache bucket not found in DB, fallback to default/legacy cache client below
	}

	// Fallback to default cache client (ID 0)
	p.cacheClientsMu.RLock()
	fallback, hasFallback := p.cacheClients[0]
	p.cacheClientsMu.RUnlock()
	if hasFallback && fallback != nil && fallback.IsConfigured() {
		return fallback, nil, nil
	}

	active, err := p.db.ListActiveCacheBuckets(ctx)
	if err == nil && len(active) > 0 {
		return p.GetCacheClient(ctx, active[0].ID)
	}

	return nil, nil, ErrNoCacheConfigured
}

// HasCacheConfigured returns true if any cache bucket or fallback client is ready
func (p *PoolManager) HasCacheConfigured(ctx context.Context) bool {
	p.cacheClientsMu.RLock()
	fallback := p.cacheClients[0]
	p.cacheClientsMu.RUnlock()
	if fallback != nil && fallback.IsConfigured() {
		return true
	}

	active, err := p.db.ListActiveCacheBuckets(ctx)
	return err == nil && len(active) > 0
}

// PresignedCacheURL returns a time-limited direct-download URL for the cached
// copy of obj, or "" when the object is not (usably) cached.
func (p *PoolManager) PresignedCacheURL(ctx context.Context, obj *models.Object, expires time.Duration) string {
	loc, err := p.db.GetObjectLocationByTier(ctx, obj.ID, models.TierCache)
	if err != nil || loc == nil {
		return ""
	}
	client, err := p.cacheClientStrict(ctx, loc.AccountID)
	if err != nil || client == nil || !client.IsConfigured() {
		return ""
	}
	url, err := client.PresignGetObject(client.Bucket(), loc.RemotePath, expires)
	if err != nil || url == "" {
		return ""
	}
	go func(locID int64) {
		_ = p.db.RecordLocationAccess(context.Background(), locID)
	}(loc.ID)
	return url
}

// GetObjectOrPresigned implements the intelligent multi-tier retrieval logic:
//  1. If in Tier 1 Cache: returns (presignedURL, obj, nil) for direct-to-client download without VPS bandwidth.
//  2. If in Tier 2 Cold: retrieves encrypted original from public dataset, decrypts on-the-fly,
//     returns ("", obj, reader), and schedules asynchronous promotion to Tier 1 Cache.
func (p *PoolManager) GetObjectOrPresigned(ctx context.Context, bucket, key string) (string, *models.Object, io.ReadCloser, error) {
	obj, chunks, err := p.db.GetObjectWithChunks(ctx, bucket, key)
	if err != nil {
		return "", nil, nil, err
	}

	if obj.HasCache {
		if url := p.PresignedCacheURL(ctx, obj, 15*time.Minute); url != "" {
			return url, obj, nil, nil
		}
	}

	reader, err := p.GetObjectStream(ctx, obj, chunks, nil)
	if err != nil {
		return "", nil, nil, fmt.Errorf("cold tier retrieval: %w", err)
	}

	p.promoteAsync(bucket, key)
	return "", obj, reader, nil
}

type cacheTarget struct {
	client     *hfstorage.S3Client
	bucketName string
	id         int64
}

// pickCacheTarget chooses the least-utilised active cache bucket that has room
// for size bytes, falling back to the legacy standalone client.
func (p *PoolManager) pickCacheTarget(ctx context.Context, size int64) (*cacheTarget, error) {
	if buckets, err := p.db.ListActiveCacheBuckets(ctx); err == nil && len(buckets) > 0 {
		var chosen *models.CacheBucket
		minRatio := 2.0
		for i := range buckets {
			b := &buckets[i]
			if b.QuotaBytes > 0 && b.QuotaBytes-b.UsedBytes < size {
				continue
			}
			ratio := 0.0
			if b.QuotaBytes > 0 {
				ratio = float64(b.UsedBytes) / float64(b.QuotaBytes)
			}
			if ratio < minRatio {
				minRatio = ratio
				chosen = b
			}
		}
		if chosen != nil {
			cli, err := p.cacheClientStrict(ctx, chosen.ID)
			if err != nil {
				return nil, fmt.Errorf("get cache client for bucket %d: %w", chosen.ID, err)
			}
			return &cacheTarget{client: cli, bucketName: chosen.BucketName, id: chosen.ID}, nil
		}
	}

	p.cacheClientsMu.RLock()
	fallback := p.cacheClients[0]
	p.cacheClientsMu.RUnlock()
	if fallback != nil && fallback.IsConfigured() {
		return &cacheTarget{client: fallback, bucketName: fallback.Bucket(), id: 0}, nil
	}

	if p.HasCacheConfigured(ctx) {
		return nil, ErrNoCacheSpace
	}
	return nil, ErrNoCacheConfigured
}

// promotion is one in-flight promotion of an object; concurrent requests for
// the same object share it instead of uploading the object twice.
type promotion struct {
	done chan struct{}
	err  error
}

// beginPromotion registers a promotion of id. When one is already running it
// returns that one and started=false.
func (p *PoolManager) beginPromotion(id string) (pr *promotion, started bool) {
	p.promoteMu.Lock()
	defer p.promoteMu.Unlock()
	if existing, busy := p.promoting[id]; busy {
		return existing, false
	}
	pr = &promotion{done: make(chan struct{})}
	p.promoting[id] = pr
	return pr, true
}

func (p *PoolManager) endPromotion(id string, pr *promotion, err error) {
	p.promoteMu.Lock()
	delete(p.promoting, id)
	p.promoteMu.Unlock()
	pr.err = err
	close(pr.done)
}

// PromoteToCache streams an object from cold storage (decrypting on the fly)
// into the best cache bucket, without buffering it in memory, and records the
// new cache location. If the same object is already being promoted (for
// example by a background promotion) it waits for that one and returns its
// result: two concurrent uploads would double-count the bucket usage.
func (p *PoolManager) PromoteToCache(ctx context.Context, bucket, key string) error {
	id := bucket + "\x00" + key
	pr, started := p.beginPromotion(id)
	if !started {
		select {
		case <-pr.done:
			return pr.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	err := p.promote(ctx, bucket, key)
	p.endPromotion(id, pr, err)
	return err
}

func (p *PoolManager) promote(ctx context.Context, bucket, key string) error {
	obj, chunks, err := p.db.GetObjectWithChunks(ctx, bucket, key)
	if err != nil {
		return fmt.Errorf("get object: %w", err)
	}
	if obj.HasCache || obj.Size == 0 || len(chunks) == 0 {
		return nil
	}
	if obj.Size > p.maxCacheObjectSize.Load() {
		return ErrObjectTooLarge
	}

	target, err := p.pickCacheTarget(ctx, obj.Size)
	if err != nil {
		return err
	}

	stream, err := p.GetObjectStream(ctx, obj, chunks, nil)
	if err != nil {
		return fmt.Errorf("open cold stream: %w", err)
	}
	defer stream.Close()

	remotePath := cacheObjectPath(obj)
	cType := obj.ContentType
	if cType == "" {
		cType = "application/octet-stream"
	}

	if err := target.client.PutObject(ctx, target.bucketName, remotePath, stream, obj.Size, cType); err != nil {
		return fmt.Errorf("upload to cache bucket %s: %w", target.bucketName, err)
	}

	now := time.Now().UTC()
	cacheLoc := &models.ObjectLocation{
		ObjectID:       obj.ID,
		Tier:           models.TierCache,
		AccountID:      target.id,
		RemotePath:     remotePath,
		IsEncrypted:    false,
		SizeBytes:      obj.Size,
		AccessCount:    1,
		LastAccessedAt: now,
		CreatedAt:      now,
	}
	if err := p.db.SaveObjectLocation(ctx, cacheLoc); err != nil {
		// The object was replaced or deleted while we uploaded (or the DB failed):
		// the copy we just wrote is unreferenced, so queue it for deletion.
		bg, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = p.db.EnqueueDeletion(bg, models.DeletionCache, target.id, remotePath, obj.Size)
		p.wakeGC()
		return fmt.Errorf("save cache location: %w", err)
	}

	if target.id > 0 {
		_ = p.db.IncrementCacheBucketUsage(ctx, target.id, obj.Size)
	}
	return nil
}

// promoteAsync schedules a background promotion. At most one promotion per
// object runs at a time and the number of concurrent promotions is bounded; if
// the workers are saturated the request is simply dropped (the next read of the
// object will try again).
func (p *PoolManager) promoteAsync(bucket, key string) {
	if p.bgCtx.Err() != nil || !p.HasCacheConfigured(p.bgCtx) {
		return
	}

	id := bucket + "\x00" + key
	select {
	case p.promoteSem <- struct{}{}:
	default:
		return // workers saturated; the next read will retry
	}
	pr, started := p.beginPromotion(id)
	if !started {
		<-p.promoteSem
		return
	}

	if !p.trackBackground() {
		<-p.promoteSem
		p.endPromotion(id, pr, context.Canceled)
		return
	}
	go func() {
		defer p.bgWG.Done()
		var err error
		defer func() {
			<-p.promoteSem
			p.endPromotion(id, pr, err)
		}()

		ctx, cancel := context.WithTimeout(p.bgCtx, 30*time.Minute)
		defer cancel()
		err = p.promote(ctx, bucket, key)
		switch {
		case err == nil:
			slog.Debug("promoted object to cache", "bucket", bucket, "key", key)
		case errors.Is(err, ErrNoCacheSpace), errors.Is(err, ErrObjectTooLarge), errors.Is(err, db.ErrNotFound), errors.Is(err, context.Canceled):
			// expected: eviction will make room / object too big / object gone
		default:
			slog.Warn("background promotion failed", "bucket", bucket, "key", key, "err", err)
		}
	}()
}

// EvictCache removes an object's copy from its cache bucket and its location
// record from SQLite. The original encrypted copy in the public dataset is NEVER deleted.
func (p *PoolManager) EvictCache(ctx context.Context, bucket, key string) error {
	obj, err := p.db.HeadObject(ctx, bucket, key)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return nil
		}
		return err
	}

	cacheLoc, err := p.db.GetObjectLocationByTier(ctx, obj.ID, models.TierCache)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return nil // Object was not in cache
		}
		return err
	}

	if err := p.db.DetachCacheLocation(ctx, *cacheLoc); err != nil {
		return fmt.Errorf("delete cache location from db: %w", err)
	}
	p.gcNow(ctx)

	slog.Info("evicted cache copy (cold golden copy intact)", "bucket", bucket, "key", key)
	return nil
}

// RunCacheEviction evicts up to maxEntries of the least recently accessed cache
// copies, guaranteeing that the cold tier original is never touched.
func (p *PoolManager) RunCacheEviction(ctx context.Context, maxEntries int) (int, error) {
	locs, err := p.db.ListLRUCacheLocations(ctx, maxEntries)
	if err != nil {
		return 0, err
	}

	evicted := 0
	for _, loc := range locs {
		if err := p.db.DetachCacheLocation(ctx, loc); err == nil {
			evicted++
		}
	}
	p.gcNow(ctx)
	return evicted, nil
}

// EvictToWatermarks keeps every cache bucket below its quota: when a bucket is
// more than `high` full it evicts least-recently-used copies until it is below
// `low`. It returns the number of copies evicted.
func (p *PoolManager) EvictToWatermarks(ctx context.Context, high, low float64) (int, error) {
	buckets, err := p.db.ListCacheBuckets(ctx)
	if err != nil {
		return 0, err
	}

	total := 0
	for _, b := range buckets {
		if b.QuotaBytes <= 0 || float64(b.UsedBytes) <= high*float64(b.QuotaBytes) {
			continue
		}
		target := int64(low * float64(b.QuotaBytes))
		used := b.UsedBytes
		for used > target {
			locs, err := p.db.ListLRUCacheLocationsForBucket(ctx, b.ID, 50)
			if err != nil {
				return total, err
			}
			if len(locs) == 0 {
				break
			}
			for _, loc := range locs {
				if used <= target {
					break
				}
				if err := p.db.DetachCacheLocation(ctx, loc); err != nil {
					return total, err
				}
				used -= loc.SizeBytes
				total++
			}
		}
	}
	if total > 0 {
		p.wakeGC()
		slog.Info("cache eviction sweep finished", "evicted", total)
	}
	return total, nil
}
