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

// SetCacheClient attaches the Hugging Face Storage Bucket client for Tier 1 Cache
func (p *PoolManager) SetCacheClient(client *hfstorage.S3Client) {
	p.cacheClient = client
}

// CacheClient returns the active Hugging Face Storage Bucket client
func (p *PoolManager) CacheClient() *hfstorage.S3Client {
	return p.cacheClient
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
	if p.cacheClient != nil && p.cacheClient.IsConfigured() {
		cacheLoc, err := p.db.GetObjectLocationByTier(ctx, obj.ID, models.TierCache)
		if err == nil && cacheLoc != nil {
			// Cache HIT: Generate S3 SigV4 Presigned GET URL
			presignedURL, err := p.cacheClient.PresignGetObject(p.cacheClient.Bucket(), cacheLoc.RemotePath, 15*time.Minute)
			if err == nil && presignedURL != "" {
				// Record access stats for LRU tracking asynchronously
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

	// 3. Auto-promote cold object to Tier 1 Cache in the background
	if p.cacheClient != nil && p.cacheClient.IsConfigured() {
		go func(b, k string) {
			promoteCtx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()
			if err := p.PromoteToCache(promoteCtx, b, k); err != nil {
				log.Printf("[HF2S3 Multi-Tier] Background promotion failed for %s/%s: %v", b, k, err)
			} else {
				log.Printf("[HF2S3 Multi-Tier] Successfully promoted %s/%s to HF Storage Bucket Cache", b, k)
			}
		}(bucket, key)
	}

	return "", obj, reader, nil
}

// PromoteToCache fetches an object from cold storage, decrypts it, and uploads the unencrypted copy
// to the Hugging Face Storage Bucket, updating the object's locations.
func (p *PoolManager) PromoteToCache(ctx context.Context, bucket, key string) error {
	if p.cacheClient == nil || !p.cacheClient.IsConfigured() {
		return ErrNoCacheConfigured
	}

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

	// Read decrypted data into memory (or temp file for large files)
	plainBytes, err := io.ReadAll(stream)
	if err != nil {
		return fmt.Errorf("read plain stream: %w", err)
	}

	remoteCachePath := fmt.Sprintf("%s/%s", bucket, key)
	cType := obj.ContentType
	if cType == "" {
		cType = "application/octet-stream"
	}

	// Upload unencrypted plaintext to Hugging Face Storage Bucket
	err = p.cacheClient.PutObject(ctx, p.cacheClient.Bucket(), remoteCachePath, bytes.NewReader(plainBytes), int64(len(plainBytes)), cType)
	if err != nil {
		return fmt.Errorf("upload to cache bucket: %w", err)
	}

	// Record TierCache location in database
	now := time.Now().UTC()
	cacheLoc := &models.ObjectLocation{
		ObjectID:       obj.ID,
		Tier:           models.TierCache,
		RemotePath:     remoteCachePath,
		IsEncrypted:    false,
		SizeBytes:      int64(len(plainBytes)),
		AccessCount:    1,
		LastAccessedAt: now,
		CreatedAt:      now,
	}

	if err := p.db.SaveObjectLocation(ctx, cacheLoc); err != nil {
		return fmt.Errorf("save cache location: %w", err)
	}

	return nil
}

// EvictCache removes an object from the Hugging Face Storage Bucket cache and removes its
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
	if p.cacheClient != nil && p.cacheClient.IsConfigured() {
		_ = p.cacheClient.DeleteObject(ctx, p.cacheClient.Bucket(), cacheLoc.RemotePath)
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
	if p.cacheClient == nil || !p.cacheClient.IsConfigured() {
		return 0, ErrNoCacheConfigured
	}

	locs, err := p.db.ListLRUCacheLocations(ctx, maxEntries)
	if err != nil {
		return 0, err
	}

	evictedCount := 0
	for _, loc := range locs {
		// Delete from HF Storage Bucket
		_ = p.cacheClient.DeleteObject(ctx, p.cacheClient.Bucket(), loc.RemotePath)

		// Delete from SQLite location table
		if err := p.db.DeleteObjectLocationByTier(ctx, loc.ObjectID, models.TierCache); err == nil {
			evictedCount++
		}
	}

	return evictedCount, nil
}
