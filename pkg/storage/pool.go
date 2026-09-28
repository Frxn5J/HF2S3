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
	"log/slog"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"hf2s3/pkg/crypto"
	"hf2s3/pkg/db"
	"hf2s3/pkg/hfclient"
	"hf2s3/pkg/hfstorage"
	"hf2s3/pkg/models"
)

var (
	ErrPoolOutOfSpace   = errors.New("insufficient storage space across active Hugging Face accounts")
	ErrBucketNotFound   = errors.New("bucket not found")
	ErrInvalidRange     = errors.New("invalid byte range")
	ErrSizeMismatch     = errors.New("uploaded size does not match the declared size")
	ErrInvalidPart      = errors.New("one or more of the specified parts could not be found or the ETag does not match")
	ErrInvalidPartOrder = errors.New("the list of parts was not in ascending order")
	ErrNoSuchUpload     = errors.New("the specified multipart upload does not exist")
	ErrIntegrity        = errors.New("stored chunk failed its integrity check")
)

// maxParallelChunks bounds concurrent chunk uploads for one object/part.
const maxParallelChunks = 4

// defaultPrefetchDepth is how many chunks ahead a reader downloads.
const defaultPrefetchDepth = 2

type ByteRange struct {
	Start int64
	End   int64
}

type PoolManager struct {
	db       *db.DB
	hfClient *hfclient.Client

	cacheClientsMu sync.RWMutex
	cacheClients   map[int64]*hfstorage.S3Client

	keyring   atomic.Pointer[crypto.Keyring]
	chunkSize atomic.Int64

	// High concurrency memory pool & in-flight load balancing
	bufferPool     sync.Pool
	inFlightMu     sync.Mutex
	inFlightBytes  map[int64]int64 // accountID -> currently uploading bytes
	inFlightCounts map[int64]int   // accountID -> active chunk upload count

	// Background work (promotion, garbage collection, eviction).
	bgCtx    context.Context
	bgCancel context.CancelFunc
	bgWG     sync.WaitGroup
	bgMu     sync.Mutex // guards bgClosed against WaitGroup.Add racing Wait
	bgClosed bool

	promoteSem         chan struct{}
	promoteMu          sync.Mutex
	promoting          map[string]*promotion
	maxCacheObjectSize atomic.Int64

	gcMu   sync.Mutex
	gcWake chan struct{}
}

// NewPoolManager builds a pool from an already derived 32-byte key. New chunks
// are written in the v2 format keyed from it, and the key also decrypts legacy
// v1 chunks. Production code should prefer NewPoolManagerWithKeyring.
func NewPoolManager(database *db.DB, client *hfclient.Client, masterKey []byte, chunkSizeBytes int64) *PoolManager {
	return NewPoolManagerWithKeyring(database, client, crypto.NewKeyringFromLegacyKey(masterKey), chunkSizeBytes)
}

func NewPoolManagerWithKeyring(database *db.DB, client *hfclient.Client, kr *crypto.Keyring, chunkSizeBytes int64) *PoolManager {
	if chunkSizeBytes <= 0 {
		chunkSizeBytes = 32 * 1024 * 1024 // Default 32MB
	}
	bgCtx, bgCancel := context.WithCancel(context.Background())
	p := &PoolManager{
		db:             database,
		hfClient:       client,
		cacheClients:   make(map[int64]*hfstorage.S3Client),
		inFlightBytes:  make(map[int64]int64),
		inFlightCounts: make(map[int64]int),
		bgCtx:          bgCtx,
		bgCancel:       bgCancel,
		promoteSem:     make(chan struct{}, 2),
		promoting:      make(map[string]*promotion),
		gcWake:         make(chan struct{}, 1),
	}
	p.keyring.Store(kr)
	p.chunkSize.Store(chunkSizeBytes)
	p.maxCacheObjectSize.Store(5 << 30)
	p.bufferPool.New = func() interface{} {
		buf := make([]byte, 0, p.chunkSize.Load())
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

func (p *PoolManager) Keyring() *crypto.Keyring { return p.keyring.Load() }

func (p *PoolManager) SetKeyring(kr *crypto.Keyring) {
	if kr != nil {
		p.keyring.Store(kr)
	}
}

func (p *PoolManager) ChunkSizeBytes() int64 { return p.chunkSize.Load() }

func (p *PoolManager) SetChunkSize(chunkSizeBytes int64) {
	if chunkSizeBytes > 0 {
		p.chunkSize.Store(chunkSizeBytes)
	}
}

// SetMaxCacheObjectSize caps the size of objects promoted to the cache tier.
func (p *PoolManager) SetMaxCacheObjectSize(n int64) {
	if n > 0 {
		p.maxCacheObjectSize.Store(n)
	}
}

func (p *PoolManager) getBuffer() []byte {
	size := p.chunkSize.Load()
	bufPtr := p.bufferPool.Get().(*[]byte)
	if int64(cap(*bufPtr)) < size {
		*bufPtr = make([]byte, size)
	}
	return (*bufPtr)[:size]
}

func (p *PoolManager) putBuffer(b []byte) {
	full := b[:cap(b)]
	p.bufferPool.Put(&full)
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
		hasSpace := acc.QuotaBytes <= 0 || (effectiveUsage+neededBytes <= acc.QuotaBytes)
		if hasSpace {
			var effectiveRatio float64
			if acc.QuotaBytes > 0 {
				effectiveRatio = float64(effectiveUsage) / float64(acc.QuotaBytes)
			} else {
				// Dynamic / unknown quota: balance chunks evenly across accounts by usage
				effectiveRatio = float64(effectiveUsage) / float64(1024*1024*1024*1024)
			}
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

// uploadedChunk is a chunk whose LFS payload is stored but not yet committed.
// The ciphertext is kept until its commit succeeds, so that a rate-limited
// commit can be replayed against another account.
type uploadedChunk struct {
	chunk models.Chunk
	oid   string
	ct    []byte
	acc   *models.Account
}

type pendingGroup struct {
	acc   *models.Account
	items []uploadedChunk
	bytes int64
}

// maxPendingCommitBytes bounds the ciphertext held between LFS upload and
// commit while streaming one object. Chunks are committed in groups per
// account (one commit for many chunks keeps us under the Hugging Face commit
// rate limit) as soon as this much is pending.
const maxPendingCommitBytes = 256 << 20

// commitGroup commits a group of already-uploaded chunks. When the account is
// rate limited it marks it as cooling down, re-uploads the group to another
// healthy account and commits there.
func (p *PoolManager) commitGroup(ctx context.Context, g *pendingGroup) error {
	commit := func(acc *models.Account, items []uploadedChunk) error {
		files := make([]hfclient.CommitLfsFile, len(items))
		for i, u := range items {
			files[i] = hfclient.CommitLfsFile{Path: u.chunk.RemotePath, Oid: u.oid, Size: u.chunk.CipherSizeBytes}
		}
		return p.hfClient.CommitLFSFiles(ctx, acc.Token, acc.RepoName, fmt.Sprintf("Add %d chunks", len(files)), files)
	}

	err := commit(g.acc, g.items)
	var rle *hfclient.RateLimitError
	if err == nil || !errors.As(err, &rle) {
		return err
	}

	p.hfClient.SetCooldown(g.acc.Token, rle.RetryAfter)
	alt, release, altErr := p.AcquireAccountExcluding(ctx, g.bytes, g.acc.ID)
	if altErr != nil {
		return err
	}
	defer release()

	for i := range g.items {
		oid, upErr := p.hfClient.UploadLFS(ctx, alt.Token, alt.RepoName, g.items[i].ct)
		if upErr != nil {
			return fmt.Errorf("failover upload to %s: %w", alt.Name, upErr)
		}
		g.items[i].oid = oid
		g.items[i].acc = alt
		g.items[i].chunk.AccountID = alt.ID
	}
	g.acc = alt
	return commit(alt, g.items)
}

// uploadStream reads reader to EOF, encrypts it in chunks (v2 format bound to
// each chunk's remote path), uploads them to the account pool and commits them
// in per-account groups. It returns fully committed chunks; on any error
// everything it committed is queued for deletion, so nothing is left orphaned.
func (p *PoolManager) uploadStream(ctx context.Context, reader io.Reader, partNumber int) ([]models.Chunk, int64, []byte, error) {
	kr := p.keyring.Load()
	buf := p.getBuffer()
	defer p.putBuffer(buf)

	hasher := md5.New()

	upCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		mu           sync.Mutex
		pending      = map[int64]*pendingGroup{}
		pendingBytes int64
		firstErr     error
		wg           sync.WaitGroup
		committed    []models.Chunk // touched only by this goroutine
	)
	sem := make(chan struct{}, maxParallelChunks)
	setErr := func(err error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = err
			cancel()
		}
		mu.Unlock()
	}
	failed := func() bool {
		mu.Lock()
		defer mu.Unlock()
		return firstErr != nil
	}

	// takeGroup removes and returns the pending group of accID (nil if none).
	takeGroup := func(accID int64) *pendingGroup {
		mu.Lock()
		defer mu.Unlock()
		g := pending[accID]
		if g != nil {
			delete(pending, accID)
			pendingBytes -= g.bytes
		}
		return g
	}
	flush := func(g *pendingGroup) error {
		if g == nil || len(g.items) == 0 {
			return nil
		}
		if err := p.commitGroup(ctx, g); err != nil {
			return fmt.Errorf("commit %d chunks to %s: %w", len(g.items), g.acc.Name, err)
		}
		var groupBytes int64
		for i := range g.items {
			groupBytes += g.items[i].chunk.CipherSizeBytes
			committed = append(committed, g.items[i].chunk)
			g.items[i].ct = nil
		}
		_ = p.db.IncrementAccountUsage(ctx, g.acc.ID, groupBytes)
		return nil
	}
	// largestOverCap returns the account to flush when too much is pending.
	largestOverCap := func() (int64, bool) {
		mu.Lock()
		defer mu.Unlock()
		if pendingBytes < maxPendingCommitBytes {
			return 0, false
		}
		var best int64
		var bestBytes int64 = -1
		for id, g := range pending {
			if g.bytes > bestBytes {
				best, bestBytes = id, g.bytes
			}
		}
		return best, bestBytes >= 0
	}

	var chunkIndex int
	var offset int64

readLoop:
	for {
		n, readErr := io.ReadFull(reader, buf)
		if n > 0 {
			plain := buf[:n]
			hasher.Write(plain)
			plainHash := sha256.Sum256(plain)

			remotePath := fmt.Sprintf("data/%s.enc", uuid.New().String())
			ciphertext, keyID, err := kr.EncryptChunk(plain, remotePath)
			if err != nil {
				setErr(fmt.Errorf("encrypt chunk: %w", err))
				break
			}

			acc, release, err := p.AcquireAccountForChunk(upCtx, int64(len(ciphertext)))
			if err != nil {
				setErr(err)
				break
			}

			select {
			case sem <- struct{}{}:
			case <-upCtx.Done():
				release()
				setErr(upCtx.Err())
				break readLoop
			}

			wg.Add(1)
			go func(idx int, off, plainSize int64, ct []byte, hashHex, keyID, rPath string, account *models.Account, rel func()) {
				defer wg.Done()
				defer func() { <-sem }()
				defer func() { rel() }()

				oid, upErr := p.hfClient.UploadLFS(upCtx, account.Token, account.RepoName, ct)
				if upErr != nil {
					var rle *hfclient.RateLimitError
					if errors.As(upErr, &rle) {
						// Mark account throttled and fail over to another pool account
						p.hfClient.SetCooldown(account.Token, rle.RetryAfter)
						altAcc, altRel, altErr := p.AcquireAccountExcluding(upCtx, int64(len(ct)), account.ID)
						if altErr == nil {
							rel()
							rel = altRel
							account = altAcc
							oid, upErr = p.hfClient.UploadLFS(upCtx, account.Token, account.RepoName, ct)
						}
					}
				}
				if upErr != nil {
					setErr(fmt.Errorf("upload chunk %d to %s: %w", idx, account.Name, upErr))
					return
				}

				u := uploadedChunk{
					oid: oid,
					ct:  ct,
					acc: account,
					chunk: models.Chunk{
						PartNumber:      partNumber,
						ChunkIndex:      idx,
						OffsetBytes:     off,
						SizeBytes:       plainSize,
						CipherSizeBytes: int64(len(ct)),
						AccountID:       account.ID,
						RemotePath:      rPath,
						Sha256Hash:      hashHex,
						EncVersion:      crypto.EncV2,
						KeyID:           keyID,
					},
				}
				mu.Lock()
				g := pending[account.ID]
				if g == nil {
					g = &pendingGroup{acc: account}
					pending[account.ID] = g
				}
				g.items = append(g.items, u)
				g.bytes += int64(len(ct))
				pendingBytes += int64(len(ct))
				mu.Unlock()
			}(chunkIndex, offset, int64(n), ciphertext, hex.EncodeToString(plainHash[:]), keyID, remotePath, acc, release)

			offset += int64(n)
			chunkIndex++
		}

		if failed() {
			break
		}
		if accID, over := largestOverCap(); over {
			if err := flush(takeGroup(accID)); err != nil {
				setErr(err)
				break
			}
		}
		if readErr != nil {
			// io.ErrUnexpectedEOF only means "last, short chunk" here. Callers wrap
			// request bodies so that a truncated upload is a different error.
			if !errors.Is(readErr, io.EOF) && !errors.Is(readErr, io.ErrUnexpectedEOF) {
				setErr(fmt.Errorf("read object body: %w", readErr))
			}
			break
		}
	}

	wg.Wait()
	if err := firstErr; err != nil {
		// Uncommitted LFS blobs are not part of any repo; committed ones are queued.
		p.discardChunks(committed)
		return nil, 0, nil, err
	}

	// Commit whatever is still pending, one commit per account.
	mu.Lock()
	ids := make([]int64, 0, len(pending))
	for id := range pending {
		ids = append(ids, id)
	}
	mu.Unlock()
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		if err := flush(takeGroup(id)); err != nil {
			p.discardChunks(committed)
			return nil, 0, nil, err
		}
	}

	sort.Slice(committed, func(i, j int) bool { return committed[i].ChunkIndex < committed[j].ChunkIndex })
	return committed, offset, hasher.Sum(nil), nil
}

// discardChunks queues committed-but-unreferenced chunks for remote deletion.
// It uses its own context because the caller's is often already cancelled.
func (p *PoolManager) discardChunks(chunks []models.Chunk) {
	if len(chunks) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, c := range chunks {
		if err := p.db.EnqueueDeletion(ctx, models.DeletionChunk, c.AccountID, c.RemotePath, c.CipherSizeBytes); err != nil {
			slog.Error("could not queue orphaned chunk for deletion", "account", c.AccountID, "path", c.RemotePath, "err", err)
		}
	}
	p.wakeGC()
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

	chunks, size, sum, err := p.uploadStream(ctx, reader, 1)
	if err != nil {
		return nil, err
	}
	if totalSize > 0 && size != totalSize {
		p.discardChunks(chunks)
		return nil, fmt.Errorf("%w: got %d bytes, declared %d", ErrSizeMismatch, size, totalSize)
	}

	obj := &models.Object{
		Bucket:         bucket,
		Key:            key,
		Size:           size,
		ETag:           fmt.Sprintf("\"%s\"", hex.EncodeToString(sum)),
		ContentType:    contentType,
		CustomMetadata: metadata,
	}

	if err := p.db.SaveObjectWithChunks(ctx, obj, chunks); err != nil {
		p.discardChunks(chunks)
		if errors.Is(err, db.ErrBucketMissing) {
			return nil, ErrBucketNotFound
		}
		return nil, fmt.Errorf("save object metadata: %w", err)
	}
	// The replaced version's chunks (if any) were queued in the same transaction.
	p.wakeGC()

	p.promoteAsync(bucket, key)
	return obj, nil
}

// --- Reading -----------------------------------------------------------------

type fetchResult struct {
	data []byte
	err  error
}

// chunkStreamReader streams the plaintext of consecutive chunks. Downloads run
// up to `depth` chunks ahead, each chunk is fetched exactly once, and results
// are consumed strictly in order.
type chunkStreamReader struct {
	ctx    context.Context
	cancel context.CancelFunc
	pool   *PoolManager
	kr     *crypto.Keyring

	chunks   []models.Chunk
	accounts map[int64]*models.Account
	accMu    sync.Mutex

	futures []chan fetchResult
	depth   int

	next      int // index of the next chunk to consume
	cur       *bytes.Reader
	startByte int64
	endByte   int64
	err       error
}

func (r *chunkStreamReader) getAccount(accountID int64) (*models.Account, error) {
	r.accMu.Lock()
	defer r.accMu.Unlock()

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

// fetch downloads, decrypts and verifies one chunk.
func (r *chunkStreamReader) fetch(chunk models.Chunk) fetchResult {
	acc, err := r.getAccount(chunk.AccountID)
	if err != nil {
		return fetchResult{err: fmt.Errorf("get account: %w", err)}
	}
	cipherData, err := r.pool.hfClient.DownloadChunk(r.ctx, acc.Token, acc.RepoName, chunk.RemotePath)
	if err != nil {
		return fetchResult{err: fmt.Errorf("download chunk %s: %w", chunk.RemotePath, err)}
	}
	plain, err := r.kr.DecryptChunk(cipherData, chunk.RemotePath, chunk.EncVersion, chunk.KeyID)
	if err != nil {
		return fetchResult{err: fmt.Errorf("decrypt chunk %s: %w", chunk.RemotePath, err)}
	}
	if int64(len(plain)) != chunk.SizeBytes {
		return fetchResult{err: fmt.Errorf("%w: chunk %s has %d bytes, expected %d", ErrIntegrity, chunk.RemotePath, len(plain), chunk.SizeBytes)}
	}
	if chunk.Sha256Hash != "" {
		sum := sha256.Sum256(plain)
		if hex.EncodeToString(sum[:]) != chunk.Sha256Hash {
			return fetchResult{err: fmt.Errorf("%w: chunk %s checksum mismatch", ErrIntegrity, chunk.RemotePath)}
		}
	}
	return fetchResult{data: plain}
}

func (r *chunkStreamReader) start(idx int) {
	if idx >= len(r.chunks) || r.futures[idx] != nil {
		return
	}
	ch := make(chan fetchResult, 1)
	r.futures[idx] = ch
	chunk := r.chunks[idx]
	go func() { ch <- r.fetch(chunk) }()
}

func (r *chunkStreamReader) await(idx int) ([]byte, error) {
	for i := idx; i <= idx+r.depth; i++ {
		r.start(i)
	}
	select {
	case res := <-r.futures[idx]:
		r.futures[idx] = nil
		return res.data, res.err
	case <-r.ctx.Done():
		return nil, r.ctx.Err()
	}
}

func (r *chunkStreamReader) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	for {
		if r.cur != nil && r.cur.Len() > 0 {
			return r.cur.Read(p)
		}
		if r.next >= len(r.chunks) {
			return 0, io.EOF
		}

		idx := r.next
		r.next++
		plain, err := r.await(idx)
		if err != nil {
			r.err = err
			return 0, err
		}

		chunk := r.chunks[idx]
		sliceStart, sliceEnd := int64(0), chunk.SizeBytes
		if r.startByte > chunk.OffsetBytes {
			sliceStart = r.startByte - chunk.OffsetBytes
		}
		if lastByte := chunk.OffsetBytes + chunk.SizeBytes - 1; r.endByte < lastByte {
			sliceEnd = r.endByte - chunk.OffsetBytes + 1
		}
		if sliceStart >= sliceEnd {
			continue
		}
		r.cur = bytes.NewReader(plain[sliceStart:sliceEnd])
	}
}

func (r *chunkStreamReader) Close() error {
	r.cancel()
	r.cur = nil
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
		ctx:       readerCtx,
		cancel:    cancel,
		pool:      p,
		kr:        p.keyring.Load(),
		chunks:    relevantChunks,
		accounts:  make(map[int64]*models.Account),
		futures:   make([]chan fetchResult, len(relevantChunks)),
		depth:     defaultPrefetchDepth,
		startByte: start,
		endByte:   end,
	}
	if len(relevantChunks) > 0 {
		reader.start(0)
	}
	return reader, nil
}

// GetObject returns the object (or the requested range). A cached object is
// read from its cache bucket (whole object or range); otherwise the encrypted
// chunks are fetched from the cold tier and the object is scheduled for
// promotion.
func (p *PoolManager) GetObject(ctx context.Context, bucket, key string, byteRange *ByteRange) (*models.Object, io.ReadCloser, error) {
	obj, chunks, err := p.db.GetObjectWithChunks(ctx, bucket, key)
	if err != nil {
		return nil, nil, err
	}

	if byteRange != nil && (byteRange.Start < 0 || byteRange.End >= obj.Size || byteRange.Start > byteRange.End) {
		return obj, nil, ErrInvalidRange
	}

	if obj.HasCache {
		if rc := p.openFromCache(ctx, obj, byteRange); rc != nil {
			return obj, rc, nil
		}
	}

	reader, err := p.GetObjectStream(ctx, obj, chunks, byteRange)
	if err != nil {
		return obj, nil, err
	}

	p.promoteAsync(bucket, key)
	return obj, reader, nil
}

// openFromCache streams the cached copy, or returns nil to fall back to cold.
func (p *PoolManager) openFromCache(ctx context.Context, obj *models.Object, byteRange *ByteRange) io.ReadCloser {
	loc, err := p.db.GetObjectLocationByTier(ctx, obj.ID, models.TierCache)
	if err != nil || loc == nil {
		return nil
	}
	client, err := p.cacheClientStrict(ctx, loc.AccountID)
	if err != nil || client == nil || !client.IsConfigured() {
		return nil
	}

	var rc io.ReadCloser
	if byteRange == nil {
		rc, _, _, err = client.GetObject(ctx, client.Bucket(), loc.RemotePath)
	} else {
		rc, err = client.GetObjectRange(ctx, client.Bucket(), loc.RemotePath, byteRange.Start, byteRange.End)
	}
	if err != nil || rc == nil {
		return nil
	}

	go func(locID int64) {
		_ = p.db.RecordLocationAccess(context.Background(), locID)
	}(loc.ID)
	return rc
}

func (p *PoolManager) DeleteObject(ctx context.Context, bucket, key string) error {
	// Rows, chunks and cache copies are queued for remote deletion in one
	// transaction; the queue makes deletion retryable if Hugging Face is down.
	if _, err := p.db.DeleteObject(ctx, bucket, key); err != nil {
		return fmt.Errorf("delete object from db: %w", err)
	}
	p.gcNow(ctx)
	return nil
}

// --- Multipart Upload Operations ---

func (p *PoolManager) InitiateMultipartUpload(ctx context.Context, bucket, key, contentType string, metadata map[string]string) (string, error) {
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
		Metadata:    metadata,
	}

	if err := p.db.CreateMultipartUpload(ctx, mp); err != nil {
		return "", err
	}
	return uploadID, nil
}

// MultipartUpload returns the bookkeeping record of an upload.
func (p *PoolManager) MultipartUpload(ctx context.Context, uploadID string) (*models.MultipartUpload, error) {
	mp, err := p.db.GetMultipartUpload(ctx, uploadID)
	if errors.Is(err, db.ErrNotFound) {
		return nil, ErrNoSuchUpload
	}
	return mp, err
}

func decodePartChunks(blob string) []models.Chunk {
	var chunks []models.Chunk
	_ = json.Unmarshal([]byte(blob), &chunks)
	return chunks
}

func (p *PoolManager) UploadPart(ctx context.Context, uploadID string, partNumber int, reader io.Reader) (string, error) {
	mp, err := p.MultipartUpload(ctx, uploadID)
	if err != nil {
		return "", err
	}

	// A re-uploaded part number replaces the previous one; remember its chunks.
	var previous []models.Chunk
	if parts, err := p.db.ListMultipartParts(ctx, uploadID); err == nil {
		for _, pt := range parts {
			if pt.PartNumber == partNumber {
				previous = decodePartChunks(pt.ChunksJSON)
			}
		}
	}

	chunks, size, sum, err := p.uploadStream(ctx, reader, partNumber)
	if err != nil {
		return "", err
	}

	etag := fmt.Sprintf("\"%s\"", hex.EncodeToString(sum))
	chunksJSON, _ := json.Marshal(chunks)
	part := &models.MultipartPart{
		UploadID:   mp.UploadID,
		PartNumber: partNumber,
		ETag:       etag,
		SizeBytes:  size,
		ChunksJSON: string(chunksJSON),
	}
	if err := p.db.SaveMultipartPart(ctx, part); err != nil {
		p.discardChunks(chunks)
		return "", err
	}
	p.discardChunks(previous)

	return etag, nil
}

// CompletedPart is one entry of a CompleteMultipartUpload request.
type CompletedPart struct {
	PartNumber int
	ETag       string
}

func normalizeETag(s string) string {
	return strings.ToLower(strings.Trim(strings.TrimSpace(s), `"`))
}

// CompleteMultipartUpload assembles the listed parts (all uploaded parts when
// requested is empty), validating part order and ETags. Uploaded parts that
// are not listed have their chunks queued for deletion.
func (p *PoolManager) CompleteMultipartUpload(ctx context.Context, uploadID string, requested []CompletedPart) (*models.Object, error) {
	mp, err := p.MultipartUpload(ctx, uploadID)
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
	byNumber := make(map[int]models.MultipartPart, len(parts))
	for _, pt := range parts {
		byNumber[pt.PartNumber] = pt
	}

	var selected []models.MultipartPart
	if len(requested) == 0 {
		selected = parts
	} else {
		last := 0
		for _, rq := range requested {
			if rq.PartNumber <= last {
				return nil, ErrInvalidPartOrder
			}
			last = rq.PartNumber
			pt, ok := byNumber[rq.PartNumber]
			if !ok || (rq.ETag != "" && normalizeETag(rq.ETag) != normalizeETag(pt.ETag)) {
				return nil, ErrInvalidPart
			}
			selected = append(selected, pt)
		}
	}

	used := make(map[int]bool, len(selected))
	var allChunks []models.Chunk
	var totalSize int64
	var chunkIdx int
	etagHash := md5.New()

	for _, pt := range selected {
		used[pt.PartNumber] = true
		if raw, err := hex.DecodeString(normalizeETag(pt.ETag)); err == nil {
			etagHash.Write(raw)
		}
		for _, c := range decodePartChunks(pt.ChunksJSON) {
			c.ChunkIndex = chunkIdx
			c.OffsetBytes = totalSize
			allChunks = append(allChunks, c)
			totalSize += c.SizeBytes
			chunkIdx++
		}
	}

	// Multi-part ETag format: "<md5 of concatenated part md5s>-<part-count>"
	finalETag := fmt.Sprintf("\"%s-%d\"", hex.EncodeToString(etagHash.Sum(nil)), len(selected))

	obj := &models.Object{
		Bucket:         mp.Bucket,
		Key:            mp.Key,
		Size:           totalSize,
		ETag:           finalETag,
		ContentType:    mp.ContentType,
		CustomMetadata: mp.Metadata,
	}

	if err := p.db.SaveObjectWithChunks(ctx, obj, allChunks); err != nil {
		if errors.Is(err, db.ErrBucketMissing) {
			return nil, ErrBucketNotFound
		}
		return nil, err
	}

	for _, pt := range parts {
		if !used[pt.PartNumber] {
			p.discardChunks(decodePartChunks(pt.ChunksJSON))
		}
	}
	// Chunks now belong to the object; only the upload bookkeeping goes away.
	_ = p.db.AbortMultipartUpload(ctx, uploadID)
	p.wakeGC()

	p.promoteAsync(mp.Bucket, mp.Key)
	return obj, nil
}

func (p *PoolManager) AbortMultipartUpload(ctx context.Context, uploadID string) error {
	if err := p.db.AbortMultipartUploadAndQueue(ctx, uploadID); err != nil {
		return err
	}
	p.gcNow(ctx)
	return nil
}
