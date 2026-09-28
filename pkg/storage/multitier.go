package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	"hf2s3/pkg/db"
	"hf2s3/pkg/hfstorage"
	"hf2s3/pkg/models"
)

var (
	ErrNoCacheConfigured = errors.New("hugging face storage bucket cache is not configured")
	ErrObjectNotInCache  = errors.New("object is not currently in cache tier")
)

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
			p.cacheClientsMu.Lock()
			cli = hfstorage.NewS3Client(hfstorage.S3ClientConfig{
				Endpoint:  cb.Endpoint,
				Region:    cb.Region,
				AccessKey: cb.AccessKey,
				SecretKey: cb.SecretKey,
				Bucket:    cb.BucketName,
			})
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

// GetObjectOrPresigned implements the intelligent multi-tier retrieval logic:
// 1. If in Tier 1 Cache: returns (presignedURL, nil, nil) for direct-to-client download without VPS bandwidth.
// 2. If in Tier 2 Cold: retrieves encrypted original from public dataset, decrypts on-the-fly,
//    returns ("", reader, nil), and triggers asynchronous auto-promotion to Tier 1 Cache.
func (p *PoolManager) GetObjectOrPresigned(ctx context.Context, bucket, key string) (string, *models.Object, io.ReadCloser, error) {
	obj, chunks, err := p.db.GetObjectWithChunks(ctx, bucket, key)
	if err != nil {
		return "", nil, nil, err
	}

	// 1. Check if object has an active Tier 1 Cache location
	cacheLoc, err := p.db.GetObjectLocationByTier(ctx, obj.ID, models.TierCache)
	if err == nil && cacheLoc != nil {
		client, _, err := p.GetCacheClient(ctx, cacheLoc.AccountID)
		if err == nil && client != nil && client.IsConfigured() {
			presignedURL, err := client.PresignGetObject(client.Bucket(), cacheLoc.RemotePath, 15*time.Minute)
			if err == nil && presignedURL != "" {
				go func(locID int64) {
					_ = p.db.RecordLocationAccess(context.Background(), locID)
				}(cacheLoc.ID)

				return presignedURL, obj, nil, nil
			}
		}
	}

	// 2. Cache MISS: Retrieve from Tier 2 Cold Storage (Public Dataset), decrypt and stream
	reader, err := p.GetObjectStream(ctx, obj, chunks, nil)
	if err != nil {
		return "", nil, nil, fmt.Errorf("cold tier retrieval: %w", err)
	}

	// 3. Auto-promote cold object to Tier 1 Cache in the background if any cache is available
	if p.HasCacheConfigured(ctx) {
		go func(b, k string) {
			promoteCtx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()
			if err := p.PromoteToCache(promoteCtx, b, k); err != nil {
				log.Printf("[HF2S3 Multi-Tier] Background promotion failed for %s/%s: %v", b, k, err)
			} else {
				log.Printf("[HF2S3 Multi-Tier] Successfully promoted %s/%s to S3 Cache", b, k)
			}
		}(bucket, key)
	}

	return "", obj, reader, nil
}

// PromoteToCache fetches an object from cold storage, decrypts it, and uploads the unencrypted copy
// to the optimal Hugging Face Storage Bucket, updating the object's locations and bucket usage.
func (p *PoolManager) PromoteToCache(ctx context.Context, bucket, key string) error {
	obj, chunks, err := p.db.GetObjectWithChunks(ctx, bucket, key)
	if err != nil {
		return fmt.Errorf("get object: %w", err)
	}

	// Verify if already in cache
	if _, err := p.db.GetObjectLocationByTier(ctx, obj.ID, models.TierCache); err == nil {
		return nil
	}

	// Retrieve decrypted cold stream
	stream, err := p.GetObjectStream(ctx, obj, chunks, nil)
	if err != nil {
		return fmt.Errorf("open cold stream: %w", err)
	}
	defer stream.Close()

	// Read decrypted data into memory
	plainBytes, err := io.ReadAll(stream)
	if err != nil {
		return fmt.Errorf("read plain stream: %w", err)
	}
	plainSize := int64(len(plainBytes))

	// Select optimal cache bucket across active configured accounts
	activeBuckets, err := p.db.ListActiveCacheBuckets(ctx)
	var targetClient *hfstorage.S3Client
	var targetBucketName string
	var targetAccountID int64

	if err == nil && len(activeBuckets) > 0 {
		var chosenBucket *models.CacheBucket
		minRatio := 2.0

		for i := range activeBuckets {
			b := &activeBuckets[i]
			available := b.QuotaBytes - b.UsedBytes
			if available >= plainSize {
				ratio := float64(b.UsedBytes) / float64(b.QuotaBytes)
				if ratio < minRatio {
					minRatio = ratio
					chosenBucket = b
				}
			}
		}

		// Fallback to bucket with most free space if all are above quota threshold
		if chosenBucket == nil {
			var maxFree int64 = -1
			for i := range activeBuckets {
				b := &activeBuckets[i]
				free := b.QuotaBytes - b.UsedBytes
				if free > maxFree {
					maxFree = free
					chosenBucket = b
				}
			}
		}

		if chosenBucket != nil {
			cli, _, err := p.GetCacheClient(ctx, chosenBucket.ID)
			if err != nil {
				return fmt.Errorf("get cache client for bucket %d: %w", chosenBucket.ID, err)
			}
			targetClient = cli
			targetBucketName = chosenBucket.BucketName
			targetAccountID = chosenBucket.ID
		}
	}

	// Fallback to legacy/standalone client if no DB cache buckets are defined
	if targetClient == nil {
		p.cacheClientsMu.RLock()
		fallback := p.cacheClients[0]
		p.cacheClientsMu.RUnlock()
		if fallback != nil && fallback.IsConfigured() {
			targetClient = fallback
			targetBucketName = fallback.Bucket()
			targetAccountID = 0
		} else {
			return ErrNoCacheConfigured
		}
	}

	remoteCachePath := fmt.Sprintf("%s/%s", bucket, key)
	cType := obj.ContentType
	if cType == "" {
		cType = "application/octet-stream"
	}

	// Upload unencrypted plaintext to target Hugging Face Storage Bucket
	err = targetClient.PutObject(ctx, targetBucketName, remoteCachePath, bytes.NewReader(plainBytes), plainSize, cType)
	if err != nil {
		return fmt.Errorf("upload to cache bucket %s: %w", targetBucketName, err)
	}

	// Record TierCache location in database
	now := time.Now().UTC()
	cacheLoc := &models.ObjectLocation{
		ObjectID:       obj.ID,
		Tier:           models.TierCache,
		AccountID:      targetAccountID,
		RemotePath:     remoteCachePath,
		IsEncrypted:    false,
		SizeBytes:      plainSize,
		AccessCount:    1,
		LastAccessedAt: now,
		CreatedAt:      now,
	}

	if err := p.db.SaveObjectLocation(ctx, cacheLoc); err != nil {
		return fmt.Errorf("save cache location: %w", err)
	}

	if targetAccountID > 0 {
		_ = p.db.IncrementCacheBucketUsage(ctx, targetAccountID, plainSize)
	}

	return nil
}

// EvictCache removes an object from its Hugging Face Storage Bucket cache and removes its
// location record from SQLite. The original encrypted copy in the public dataset is NEVER deleted.
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

	// Delete from Hugging Face Storage Bucket
	client, _, _ := p.GetCacheClient(ctx, cacheLoc.AccountID)
	if client != nil && client.IsConfigured() {
		_ = client.DeleteObject(ctx, client.Bucket(), cacheLoc.RemotePath)
	}

	// Decrement bucket usage in DB
	if cacheLoc.AccountID > 0 {
		_ = p.db.IncrementCacheBucketUsage(ctx, cacheLoc.AccountID, -cacheLoc.SizeBytes)
	}

	// Remove cache location record from DB (TierCold remains completely untouched)
	if err := p.db.DeleteObjectLocationByTier(ctx, obj.ID, models.TierCache); err != nil {
		return fmt.Errorf("delete cache location from db: %w", err)
	}

	log.Printf("[HF2S3 Multi-Tier] Evicted cache for %s/%s (Golden copy in public dataset intact)", bucket, key)
	return nil
}

// RunCacheEviction performs an LRU sweep of the cache, evicting the least recently accessed items
// up to maxEntries, guaranteeing that the cold tier original is never touched.
func (p *PoolManager) RunCacheEviction(ctx context.Context, maxEntries int) (int, error) {
	locs, err := p.db.ListLRUCacheLocations(ctx, maxEntries)
	if err != nil {
		return 0, err
	}

	evictedCount := 0
	for _, loc := range locs {
		client, _, _ := p.GetCacheClient(ctx, loc.AccountID)
		if client != nil && client.IsConfigured() {
			_ = client.DeleteObject(ctx, client.Bucket(), loc.RemotePath)
		}

		if loc.AccountID > 0 {
			_ = p.db.IncrementCacheBucketUsage(ctx, loc.AccountID, -loc.SizeBytes)
		}

		if err := p.db.DeleteObjectLocationByTier(ctx, loc.ObjectID, models.TierCache); err == nil {
			evictedCount++
		}
	}

	return evictedCount, nil
}
