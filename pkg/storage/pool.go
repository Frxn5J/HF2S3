package storage

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"hf2s3/pkg/crypto"
	"hf2s3/pkg/db"
	"hf2s3/pkg/hfclient"
	"hf2s3/pkg/hfstorage"
	"hf2s3/pkg/models"
)

var (
	ErrPoolOutOfSpace = errors.New("insufficient storage space across active Hugging Face accounts")
	ErrBucketNotFound = errors.New("bucket not found")
	ErrInvalidRange   = errors.New("invalid byte range")
)

type ByteRange struct {
	Start int64
	End   int64
}

type PoolManager struct {
	db             *db.DB
	hfClient       *hfclient.Client
	cacheClientsMu sync.RWMutex
	cacheClients   map[int64]*hfstorage.S3Client
	masterKey      []byte
	chunkSizeBytes int64

	// High concurrency memory pool & in-flight load balancing
	bufferPool     sync.Pool
	inFlightMu     sync.Mutex
	inFlightBytes  map[int64]int64 // accountID -> currently uploading bytes
	inFlightCounts map[int64]int   // accountID -> active chunk upload count
}

func NewPoolManager(database *db.DB, client *hfclient.Client, masterKey []byte, chunkSizeBytes int64) *PoolManager {
	if chunkSizeBytes <= 0 {
		chunkSizeBytes = 32 * 1024 * 1024 // Default 32MB
	}
	p := &PoolManager{
		db:             database,
		hfClient:       client,
		cacheClients:   make(map[int64]*hfstorage.S3Client),
		masterKey:      masterKey,
		chunkSizeBytes: chunkSizeBytes,
		inFlightBytes:  make(map[int64]int64),
		inFlightCounts: make(map[int64]int),
	}
	p.bufferPool.New = func() interface{} {
		buf := make([]byte, p.chunkSizeBytes)
		return &buf
	}
	return p
}

func (p *PoolManager) DB() *db.DB {
	return p.db
}

func (p *PoolManager) HFClient() *hfclient.Client {
	return p.hfClient
}

func (p *PoolManager) getBuffer() []byte {
	bufPtr := p.bufferPool.Get().(*[]byte)
	return (*bufPtr)[:p.chunkSizeBytes]
}

func (p *PoolManager) putBuffer(b []byte) {
	if int64(cap(b)) >= p.chunkSizeBytes {
		slice := b[:p.chunkSizeBytes]
		p.bufferPool.Put(&slice)
	}
}

func (p *PoolManager) SetMasterKey(key []byte) {
	if len(key) > 0 {
		p.masterKey = key
	}
}

func (p *PoolManager) SetChunkSize(chunkSizeBytes int64) {
	if chunkSizeBytes > 0 {
		p.chunkSizeBytes = chunkSizeBytes
	}
}

func (p *PoolManager) MasterKey() []byte {
	return p.masterKey
}

func (p *PoolManager) ChunkSizeBytes() int64 {
	return p.chunkSizeBytes
}

// AcquireAccountForChunk selects the least loaded account taking into account both stored and in-flight bytes.
// It prioritizes accounts that are not throttled or near rate-limit exhaustion.
func (p *PoolManager) AcquireAccountForChunk(ctx context.Context, neededBytes int64) (*models.Account, func(), error) {
	return p.AcquireAccountExcluding(ctx, neededBytes, 0)
}

// AcquireAccountExcluding selects the best account while optionally excluding a specific account ID (used for rate-limit failover).
func (p *PoolManager) AcquireAccountExcluding(ctx context.Context, neededBytes int64, excludeAccountID int64) (*models.Account, func(), error) {
	p.inFlightMu.Lock()
	defer p.inFlightMu.Unlock()

	accounts, err := p.db.ListActiveAccounts(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("list active accounts: %w", err)
	}
	if len(accounts) == 0 {
		return nil, nil, errors.New("no active Hugging Face accounts configured")
	}

	var bestAccount *models.Account
	var minEffectiveRatio float64 = 1.0
	var minInFlightCount int = -1

	var bestThrottledAccount *models.Account
	var minThrottledRatio float64 = 1.0
	var minThrottledInFlight int = -1

	for i := range accounts {
		acc := &accounts[i]
		if excludeAccountID > 0 && acc.ID == excludeAccountID {
			continue
		}

		currentInFlight := p.inFlightBytes[acc.ID]
		inFlightCount := p.inFlightCounts[acc.ID]

		effectiveUsage := acc.UsedBytes + currentInFlight
		if effectiveUsage+neededBytes <= acc.QuotaBytes {
			effectiveRatio := float64(effectiveUsage) / float64(acc.QuotaBytes)
			isThrottled, _ := p.hfClient.IsThrottled(acc.Token)

			if !isThrottled {
				if bestAccount == nil || effectiveRatio < minEffectiveRatio || (effectiveRatio == minEffectiveRatio && inFlightCount < minInFlightCount) {
					bestAccount = acc
					minEffectiveRatio = effectiveRatio
					minInFlightCount = inFlightCount
				}
			} else {
				if bestThrottledAccount == nil || effectiveRatio < minThrottledRatio || (effectiveRatio == minThrottledRatio && inFlightCount < minThrottledInFlight) {
					bestThrottledAccount = acc
					minThrottledRatio = effectiveRatio
					minThrottledInFlight = inFlightCount
				}
			}
		}
	}

	// If all available accounts are currently throttled, fallback to the least loaded throttled account
	if bestAccount == nil {
		bestAccount = bestThrottledAccount
	}

	if bestAccount == nil {
		return nil, nil, ErrPoolOutOfSpace
	}

	accID := bestAccount.ID
	p.inFlightBytes[accID] += neededBytes
	p.inFlightCounts[accID]++

	releaseFunc := func() {
		p.inFlightMu.Lock()
		defer p.inFlightMu.Unlock()
		p.inFlightBytes[accID] -= neededBytes
		if p.inFlightBytes[accID] < 0 {
			p.inFlightBytes[accID] = 0
		}
		p.inFlightCounts[accID]--
		if p.inFlightCounts[accID] < 0 {
			p.inFlightCounts[accID] = 0
		}
	}

	return bestAccount, releaseFunc, nil
}

// SelectAccountForChunk provides backwards compatibility.
func (p *PoolManager) SelectAccountForChunk(ctx context.Context, neededBytes int64) (*models.Account, error) {
	acc, release, err := p.AcquireAccountForChunk(ctx, neededBytes)
	if err != nil {
		return nil, err
	}
	release()
	return acc, nil
}

func (p *PoolManager) PutObject(ctx context.Context, bucket, key, contentType string, reader io.Reader, totalSize int64, metadata map[string]string) (*models.Object, error) {
	bucketExists, err := p.db.BucketExists(ctx, bucket)
	if err != nil {
		return nil, fmt.Errorf("check bucket exists: %w", err)
	}
	if !bucketExists {
		return nil, ErrBucketNotFound
	}

	if contentType == "" {
		contentType = "application/octet-stream"
	}

	buf := p.getBuffer()
	defer p.putBuffer(buf)

	fullHasher := md5.New()
	var chunkIndex int
	var offset int64

	// Bounded concurrent chunk upload pipeline (max 4 parallel chunk uploads per object)
	const maxParallelChunks = 4
	sem := make(chan struct{}, maxParallelChunks)
	uploadCtx, cancelUpload := context.WithCancel(ctx)
	defer cancelUpload()

	var wg sync.WaitGroup
	var completedChunks []models.Chunk
	var chunksMu sync.Mutex
	errCh := make(chan error, 1)

	for {
		n, readErr := io.ReadFull(reader, buf)
		if n > 0 {
			plainChunk := buf[:n]
			fullHasher.Write(plainChunk)

			// SHA256 of plain chunk for integrity
			chunkHash := sha256.Sum256(plainChunk)
			chunkHashHex := hex.EncodeToString(chunkHash[:])

			// Encrypt chunk
			ciphertext, err := crypto.Encrypt(plainChunk, p.masterKey)
			if err != nil {
				return nil, fmt.Errorf("encrypt chunk: %w", err)
			}

			// Select and reserve account with in-flight load balancing
			acc, releaseFn, err := p.AcquireAccountForChunk(uploadCtx, int64(len(ciphertext)))
			if err != nil {
				return nil, err
			}

			chunkUUID := uuid.New().String()
			remotePath := fmt.Sprintf("data/%s.enc", chunkUUID)

			curIdx := chunkIndex
			curOffset := offset
			curPlainSize := int64(n)

			wg.Add(1)
			sem <- struct{}{}

			go func(idx int, off, plainSize int64, cipher []byte, hashHex string, account *models.Account, rPath string, rel func()) {
				defer wg.Done()
				defer func() { <-sem }()
				defer func() {
					if rel != nil {
						rel()
					}
				}()

				uploadErr := p.hfClient.UploadChunk(uploadCtx, account.Token, account.RepoName, rPath, cipher)
				if uploadErr != nil {
					var rle *hfclient.RateLimitError
					if errors.As(uploadErr, &rle) {
						// Mark account throttled and attempt pool account failover
						p.hfClient.SetCooldown(account.Token, rle.RetryAfter)
						altAcc, altRel, altErr := p.AcquireAccountExcluding(uploadCtx, int64(len(cipher)), account.ID)
						if altErr == nil {
							rel()
							rel = altRel
							account = altAcc
							rPath = fmt.Sprintf("data/%s.enc", uuid.New().String())
							uploadErr = p.hfClient.UploadChunk(uploadCtx, account.Token, account.RepoName, rPath, cipher)
						}
					}
				}

				if uploadErr != nil {
					select {
					case errCh <- fmt.Errorf("upload chunk %d to %s: %w", idx, account.Name, uploadErr):
						cancelUpload()
					default:
					}
					return
				}

				_ = p.db.IncrementAccountUsage(ctx, account.ID, int64(len(cipher)))

				chunksMu.Lock()
				completedChunks = append(completedChunks, models.Chunk{
					PartNumber:      1,
					ChunkIndex:      idx,
					OffsetBytes:     off,
					SizeBytes:       plainSize,
					CipherSizeBytes: int64(len(cipher)),
					AccountID:       account.ID,
					RemotePath:      rPath,
					Sha256Hash:      hashHex,
					CreatedAt:       time.Now().UTC(),
				})
				chunksMu.Unlock()
			}(curIdx, curOffset, curPlainSize, ciphertext, chunkHashHex, acc, remotePath, releaseFn)

			offset += int64(n)
			chunkIndex++
		}

		select {
		case err := <-errCh:
			wg.Wait()
			return nil, err
		default:
		}

		if readErr != nil {
			if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
				break
			}
			return nil, fmt.Errorf("read object body: %w", readErr)
		}
	}

	wg.Wait()

	select {
	case err := <-errCh:
		return nil, err
	default:
	}

	// Sort chunks to guarantee exact sequential order
	sort.Slice(completedChunks, func(i, j int) bool {
		return completedChunks[i].ChunkIndex < completedChunks[j].ChunkIndex
	})

	etag := fmt.Sprintf("\"%s\"", hex.EncodeToString(fullHasher.Sum(nil)))

	obj := &models.Object{
		Bucket:         bucket,
		Key:            key,
		Size:           offset,
		ETag:           etag,
		ContentType:    contentType,
		CustomMetadata: metadata,
	}

	if err := p.db.SaveObjectWithChunks(ctx, obj, completedChunks); err != nil {
		return nil, fmt.Errorf("save object metadata: %w", err)
	}

	// If cache client is configured, promote unencrypted copy to Tier 1 Cache
	if p.HasCacheConfigured(ctx) {
		go func(b, k string) {
			promoteCtx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()
			_ = p.PromoteToCache(promoteCtx, b, k)
		}(bucket, key)
	}

	return obj, nil
}

type prefetchItem struct {
	chunkIdx  int
	plainData []byte
	err       error
}

type chunkStreamReader struct {
	ctx        context.Context
	cancel     context.CancelFunc
	pool       *PoolManager
	chunks     []models.Chunk
	accounts   map[int64]*models.Account
	accountsMu sync.Mutex
	currentIdx int
	curReader  *bytes.Reader
	startByte  int64
	endByte    int64
	curOffset  int64

	// Asynchronous prefetch pipeline for high-throughput reads
	prefetchCh chan prefetchItem
}

func (r *chunkStreamReader) getAccount(accountID int64) (*models.Account, error) {
	r.accountsMu.Lock()
	defer r.accountsMu.Unlock()

	if acc, ok := r.accounts[accountID]; ok {
		return acc, nil
	}
	acc, err := r.pool.db.GetAccountByID(r.ctx, accountID)
	if err != nil {
		return nil, err
	}
	r.accounts[accountID] = acc
	return acc, nil
}

func (r *chunkStreamReader) prefetchNext(idx int) {
	if idx >= len(r.chunks) {
		return
	}
	go func() {
		chunk := r.chunks[idx]
		acc, err := r.getAccount(chunk.AccountID)
		if err != nil {
			select {
			case r.prefetchCh <- prefetchItem{chunkIdx: idx, err: err}:
			case <-r.ctx.Done():
			}
			return
		}

		cipherData, err := r.pool.hfClient.DownloadChunk(r.ctx, acc.Token, acc.RepoName, chunk.RemotePath)
		if err != nil {
			select {
			case r.prefetchCh <- prefetchItem{chunkIdx: idx, err: err}:
			case <-r.ctx.Done():
			}
			return
		}

		plainData, err := crypto.Decrypt(cipherData, r.pool.masterKey)
		if err != nil {
			select {
			case r.prefetchCh <- prefetchItem{chunkIdx: idx, err: err}:
			case <-r.ctx.Done():
			}
			return
		}

		select {
		case r.prefetchCh <- prefetchItem{chunkIdx: idx, plainData: plainData}:
		case <-r.ctx.Done():
		}
	}()
}

func (r *chunkStreamReader) Read(p []byte) (int, error) {
	for {
		if r.curReader != nil && r.curReader.Len() > 0 {
			n, err := r.curReader.Read(p)
			r.curOffset += int64(n)
			return n, err
		}

		if r.currentIdx >= len(r.chunks) || (r.endByte >= 0 && r.curOffset > r.endByte) {
			return 0, io.EOF
		}

		// Retrieve chunk data (from prefetch pipeline if available, or direct fetch)
		var plainData []byte
		idx := r.currentIdx
		r.currentIdx++

		// Check if prefetch item is ready
		var item prefetchItem
		select {
		case item = <-r.prefetchCh:
			if item.chunkIdx != idx {
				// Mismatched index fallback
				item = prefetchItem{}
			}
		default:
		}

		if item.plainData != nil || item.err != nil {
			if item.err != nil {
				return 0, fmt.Errorf("read chunk %d: %w", idx, item.err)
			}
			plainData = item.plainData
		} else {
			// Direct fetch
			chunk := r.chunks[idx]
			acc, err := r.getAccount(chunk.AccountID)
			if err != nil {
				return 0, fmt.Errorf("get account: %w", err)
			}

			cipherData, err := r.pool.hfClient.DownloadChunk(r.ctx, acc.Token, acc.RepoName, chunk.RemotePath)
			if err != nil {
				return 0, fmt.Errorf("download chunk %s: %w", chunk.RemotePath, err)
			}

			plainData, err = crypto.Decrypt(cipherData, r.pool.masterKey)
			if err != nil {
				return 0, fmt.Errorf("decrypt chunk %s: %w", chunk.RemotePath, err)
			}
		}

		// Immediately trigger prefetch for the next chunk in pipeline
		r.prefetchNext(r.currentIdx)

		chunk := r.chunks[idx]
		chunkStart := chunk.OffsetBytes
		chunkEnd := chunkStart + chunk.SizeBytes - 1

		sliceStart := int64(0)
		sliceEnd := chunk.SizeBytes

		if r.startByte > chunkStart {
			sliceStart = r.startByte - chunkStart
		}
		if r.endByte >= 0 && r.endByte < chunkEnd {
			sliceEnd = (r.endByte - chunkStart) + 1
		}

		if sliceStart > sliceEnd || sliceStart >= chunk.SizeBytes {
			continue
		}

		r.curReader = bytes.NewReader(plainData[sliceStart:sliceEnd])
	}
}

func (r *chunkStreamReader) Close() error {
	r.cancel()
	r.curReader = nil
	return nil
}

func (p *PoolManager) GetObjectStream(ctx context.Context, obj *models.Object, chunks []models.Chunk, byteRange *ByteRange) (io.ReadCloser, error) {
	start := int64(0)
	end := obj.Size - 1

	if byteRange != nil {
		if byteRange.Start < 0 || byteRange.End >= obj.Size || byteRange.Start > byteRange.End {
			return nil, ErrInvalidRange
		}
		start = byteRange.Start
		end = byteRange.End
	}

	var relevantChunks []models.Chunk
	for _, c := range chunks {
		cStart := c.OffsetBytes
		cEnd := c.OffsetBytes + c.SizeBytes - 1
		if cEnd >= start && cStart <= end {
			relevantChunks = append(relevantChunks, c)
		}
	}

	readerCtx, cancel := context.WithCancel(ctx)
	reader := &chunkStreamReader{
		ctx:        readerCtx,
		cancel:     cancel,
		pool:       p,
		chunks:     relevantChunks,
		accounts:   make(map[int64]*models.Account),
		currentIdx: 0,
		startByte:  start,
		endByte:    end,
		curOffset:  start,
		prefetchCh: make(chan prefetchItem, 1),
	}

	if len(relevantChunks) > 0 {
		reader.prefetchNext(0)
	}

	return reader, nil
}

func (p *PoolManager) GetObject(ctx context.Context, bucket, key string, byteRange *ByteRange) (*models.Object, io.ReadCloser, error) {
	obj, chunks, err := p.db.GetObjectWithChunks(ctx, bucket, key)
	if err != nil {
		return nil, nil, err
	}

	// Fast path: if whole object is requested and Tier 1 Cache is available, stream unencrypted directly from HF Storage Bucket
	if byteRange == nil && p.HasCacheConfigured(ctx) {
		cacheLoc, err := p.db.GetObjectLocationByTier(ctx, obj.ID, models.TierCache)
		if err == nil && cacheLoc != nil {
			client, _, err := p.GetCacheClient(ctx, cacheLoc.AccountID)
			if err == nil && client != nil && client.IsConfigured() {
				cacheReader, _, _, err := client.GetObject(ctx, client.Bucket(), cacheLoc.RemotePath)
				if err == nil && cacheReader != nil {
					go func(locID int64) {
						_ = p.db.RecordLocationAccess(context.Background(), locID)
					}(cacheLoc.ID)
					return obj, cacheReader, nil
				}
			}
		}
	}

	reader, err := p.GetObjectStream(ctx, obj, chunks, byteRange)
	if err != nil {
		return nil, nil, err
	}

	// Trigger auto-promotion to Tier 1 Cache in background if configured
	if p.HasCacheConfigured(ctx) {
		go func(b, k string) {
			promoteCtx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()
			_ = p.PromoteToCache(promoteCtx, b, k)
		}(bucket, key)
	}

	return obj, reader, nil
}

func (p *PoolManager) DeleteObject(ctx context.Context, bucket, key string) error {
	_ = p.EvictCache(ctx, bucket, key)

	chunks, err := p.db.DeleteObject(ctx, bucket, key)
	if err != nil {
		return fmt.Errorf("delete object from db: %w", err)
	}

	if len(chunks) == 0 {
		return nil
	}

	// Group chunks by account for batch deletion on Hugging Face
	chunksByAccount := make(map[int64][]string)
	bytesByAccount := make(map[int64]int64)

	for _, c := range chunks {
		chunksByAccount[c.AccountID] = append(chunksByAccount[c.AccountID], c.RemotePath)
		bytesByAccount[c.AccountID] += c.CipherSizeBytes
	}

	// Concurrently delete chunks across all accounts
	var wg sync.WaitGroup
	for accID, paths := range chunksByAccount {
		wg.Add(1)
		go func(id int64, pths []string, bytesCount int64) {
			defer wg.Done()
			acc, err := p.db.GetAccountByID(ctx, id)
			if err == nil {
				_ = p.hfClient.DeleteChunks(ctx, acc.Token, acc.RepoName, pths)
				_ = p.db.IncrementAccountUsage(ctx, id, -bytesCount)
			}
		}(accID, paths, bytesByAccount[accID])
	}
	wg.Wait()

	return nil
}

// --- Multipart Upload Operations ---

func (p *PoolManager) InitiateMultipartUpload(ctx context.Context, bucket, key, contentType string) (string, error) {
	bucketExists, err := p.db.BucketExists(ctx, bucket)
	if err != nil {
		return "", err
	}
	if !bucketExists {
		return "", ErrBucketNotFound
	}

	uploadID := uuid.New().String()
	mp := &models.MultipartUpload{
		UploadID:    uploadID,
		Bucket:      bucket,
		Key:         key,
		ContentType: contentType,
	}

	if err := p.db.CreateMultipartUpload(ctx, mp); err != nil {
		return "", err
	}
	return uploadID, nil
}

func (p *PoolManager) UploadPart(ctx context.Context, uploadID string, partNumber int, reader io.Reader) (string, error) {
	mp, err := p.db.GetMultipartUpload(ctx, uploadID)
	if err != nil {
		return "", err
	}

	hasher := md5.New()
	buf := p.getBuffer()
	defer p.putBuffer(buf)

	var partChunks []models.Chunk
	var chunkIdx int
	var offset int64

	for {
		n, readErr := io.ReadFull(reader, buf)
		if n > 0 {
			plainChunk := buf[:n]
			hasher.Write(plainChunk)

			ciphertext, err := crypto.Encrypt(plainChunk, p.masterKey)
			if err != nil {
				return "", fmt.Errorf("encrypt chunk: %w", err)
			}

			acc, release, err := p.AcquireAccountForChunk(ctx, int64(len(ciphertext)))
			if err != nil {
				return "", err
			}

			chunkUUID := uuid.New().String()
			remotePath := fmt.Sprintf("data/%s.enc", chunkUUID)

			uploadErr := p.hfClient.UploadChunk(ctx, acc.Token, acc.RepoName, remotePath, ciphertext)
			if uploadErr != nil {
				var rle *hfclient.RateLimitError
				if errors.As(uploadErr, &rle) {
					p.hfClient.SetCooldown(acc.Token, rle.RetryAfter)
					altAcc, altRel, altErr := p.AcquireAccountExcluding(ctx, int64(len(ciphertext)), acc.ID)
					if altErr == nil {
						release()
						acc = altAcc
						release = altRel
						remotePath = fmt.Sprintf("data/%s.enc", uuid.New().String())
						uploadErr = p.hfClient.UploadChunk(ctx, acc.Token, acc.RepoName, remotePath, ciphertext)
					}
				}
			}
			release()
			if uploadErr != nil {
				return "", fmt.Errorf("upload part chunk: %w", uploadErr)
			}

			_ = p.db.IncrementAccountUsage(ctx, acc.ID, int64(len(ciphertext)))

			hash := sha256.Sum256(plainChunk)
			partChunks = append(partChunks, models.Chunk{
				PartNumber:      partNumber,
				ChunkIndex:      chunkIdx,
				OffsetBytes:     offset,
				SizeBytes:       int64(n),
				CipherSizeBytes: int64(len(ciphertext)),
				AccountID:       acc.ID,
				RemotePath:      remotePath,
				Sha256Hash:      hex.EncodeToString(hash[:]),
				CreatedAt:       time.Now().UTC(),
			})

			offset += int64(n)
			chunkIdx++
		}

		if readErr != nil {
			if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
				break
			}
			return "", fmt.Errorf("read part body: %w", readErr)
		}
	}

	etag := fmt.Sprintf("\"%s\"", hex.EncodeToString(hasher.Sum(nil)))

	chunksJSON, _ := json.Marshal(partChunks)
	part := &models.MultipartPart{
		UploadID:   mp.UploadID,
		PartNumber: partNumber,
		ETag:       etag,
		SizeBytes:  offset,
		ChunksJSON: string(chunksJSON),
	}

	if err := p.db.SaveMultipartPart(ctx, part); err != nil {
		return "", err
	}

	return etag, nil
}

func (p *PoolManager) CompleteMultipartUpload(ctx context.Context, uploadID string) (*models.Object, error) {
	mp, err := p.db.GetMultipartUpload(ctx, uploadID)
	if err != nil {
		return nil, err
	}

	parts, err := p.db.ListMultipartParts(ctx, uploadID)
	if err != nil {
		return nil, err
	}
	if len(parts) == 0 {
		return nil, errors.New("cannot complete multipart upload with 0 parts")
	}

	var allChunks []models.Chunk
	var totalSize int64
	var chunkIdx int

	for _, pt := range parts {
		var ptChunks []models.Chunk
		_ = json.Unmarshal([]byte(pt.ChunksJSON), &ptChunks)
		for _, c := range ptChunks {
			c.ChunkIndex = chunkIdx
			c.OffsetBytes = totalSize
			allChunks = append(allChunks, c)
			totalSize += c.SizeBytes
			chunkIdx++
		}
	}

	// Multi-part ETag format: "<md5-hash>-<part-count>"
	finalETag := fmt.Sprintf("\"%s-%d\"", uuid.New().String()[:16], len(parts))

	obj := &models.Object{
		Bucket:      mp.Bucket,
		Key:         mp.Key,
		Size:        totalSize,
		ETag:        finalETag,
		ContentType: mp.ContentType,
	}

	if err := p.db.SaveObjectWithChunks(ctx, obj, allChunks); err != nil {
		return nil, err
	}

	_ = p.db.AbortMultipartUpload(ctx, uploadID)
	return obj, nil
}

func (p *PoolManager) AbortMultipartUpload(ctx context.Context, uploadID string) error {
	parts, err := p.db.ListMultipartParts(ctx, uploadID)
	if err == nil {
		var wg sync.WaitGroup
		for _, pt := range parts {
			var ptChunks []models.Chunk
			_ = json.Unmarshal([]byte(pt.ChunksJSON), &ptChunks)
			for _, c := range ptChunks {
				wg.Add(1)
				go func(chunk models.Chunk) {
					defer wg.Done()
					acc, err := p.db.GetAccountByID(ctx, chunk.AccountID)
					if err == nil {
						_ = p.hfClient.DeleteChunks(ctx, acc.Token, acc.RepoName, []string{chunk.RemotePath})
						_ = p.db.IncrementAccountUsage(ctx, acc.ID, -chunk.CipherSizeBytes)
					}
				}(c)
			}
		}
		wg.Wait()
	}
	return p.db.AbortMultipartUpload(ctx, uploadID)
}
