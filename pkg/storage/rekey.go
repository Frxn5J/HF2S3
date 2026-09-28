package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"

	"github.com/google/uuid"
	"hf2s3/pkg/db"
	"hf2s3/pkg/hfclient"
	"hf2s3/pkg/models"
)

// RekeyOptions controls Rekey.
type RekeyOptions struct {
	DryRun    bool
	BatchSize int                 // chunks per commit group (default 32)
	Workers   int                 // parallel chunk transfers (default 4)
	Progress  func(RekeyProgress) // called after each batch
}

// RekeyProgress reports how far a Rekey/Drain run has got.
type RekeyProgress struct {
	Total  int64
	Done   int64
	Failed int64
}

type RekeyFailure struct {
	ChunkID int64
	Path    string
	Err     string
}

// RekeyReport summarises a run.
type RekeyReport struct {
	Total    int64 // chunks needing work at the start
	Bytes    int64 // ciphertext bytes needing work at the start
	Done     int64
	Failed   int64
	Failures []RekeyFailure
}

// Rekey re-encrypts every chunk that is not yet in the current format and key
// (legacy v1 chunks, or chunks written under a retired key). For each chunk it
// downloads the ciphertext, decrypts it with whichever configured key opens it,
// verifies the stored SHA-256, re-encrypts it in the v2 format under a new
// remote path, and uploads it. The database is switched over only after the
// new copies are committed, and the old copies are then queued for deletion, so
// an interrupted run loses nothing and can simply be started again.
func (p *PoolManager) Rekey(ctx context.Context, opts RekeyOptions) (*RekeyReport, error) {
	kr := p.keyring.Load()
	current := kr.CurrentKeyID()

	total, bytes, err := p.db.CountChunksToRekey(ctx, current)
	if err != nil {
		return nil, err
	}
	report := &RekeyReport{Total: total, Bytes: bytes}
	if opts.DryRun || total == 0 {
		return report, nil
	}

	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = 32
	}

	var after int64
	for ctx.Err() == nil {
		batch, err := p.db.ListChunksToRekey(ctx, current, after, batchSize)
		if err != nil {
			return report, err
		}
		if len(batch) == 0 {
			break
		}
		after = batch[len(batch)-1].ID

		done, failures := p.rekeyBatch(ctx, batch, opts.Workers)
		report.Done += int64(done)
		report.Failed += int64(len(failures))
		report.Failures = append(report.Failures, failures...)
		if opts.Progress != nil {
			opts.Progress(RekeyProgress{Total: total, Done: report.Done, Failed: report.Failed})
		}
	}
	p.wakeGC()
	return report, ctx.Err()
}

type rekeyItem struct {
	src    models.Chunk
	newCT  []byte
	path   string
	oid    string
	acc    *models.Account
	failed error
}

func (p *PoolManager) rekeyBatch(ctx context.Context, batch []models.Chunk, workers int) (int, []RekeyFailure) {
	if workers <= 0 {
		workers = 4
	}
	kr := p.keyring.Load()
	items := make([]*rekeyItem, len(batch))

	var wg sync.WaitGroup
	sem := make(chan struct{}, workers)
	for i := range batch {
		items[i] = &rekeyItem{src: batch[i]}
		wg.Add(1)
		sem <- struct{}{}
		go func(it *rekeyItem) {
			defer wg.Done()
			defer func() { <-sem }()
			it.failed = p.rekeyOne(ctx, kr.CurrentKeyID(), it)
		}(items[i])
	}
	wg.Wait()

	var failures []RekeyFailure
	byAccount := map[int64][]*rekeyItem{}
	for _, it := range items {
		if it.failed != nil {
			failures = append(failures, RekeyFailure{ChunkID: it.src.ID, Path: it.src.RemotePath, Err: it.failed.Error()})
			slog.Warn("rekey: chunk skipped", "chunk", it.src.ID, "path", it.src.RemotePath, "err", it.failed)
			continue
		}
		byAccount[it.acc.ID] = append(byAccount[it.acc.ID], it)
	}

	done := 0
	var applied []db.RekeyedChunk
	accIDs := make([]int64, 0, len(byAccount))
	for id := range byAccount {
		accIDs = append(accIDs, id)
	}
	sort.Slice(accIDs, func(i, j int) bool { return accIDs[i] < accIDs[j] })

	for _, accID := range accIDs {
		group := byAccount[accID]
		acc := group[0].acc
		files := make([]hfclient.CommitLfsFile, len(group))
		for i, it := range group {
			files[i] = hfclient.CommitLfsFile{Path: it.path, Oid: it.oid, Size: int64(len(it.newCT))}
		}
		if err := p.hfClient.CommitLFSFiles(ctx, acc.Token, acc.RepoName, fmt.Sprintf("Re-key %d chunks", len(files)), files); err != nil {
			for _, it := range group {
				failures = append(failures, RekeyFailure{ChunkID: it.src.ID, Path: it.src.RemotePath, Err: "commit: " + err.Error()})
			}
			continue
		}
		for _, it := range group {
			applied = append(applied, db.RekeyedChunk{
				ID:            it.src.ID,
				OldAccountID:  it.src.AccountID,
				OldRemotePath: it.src.RemotePath,
				OldCipherSize: it.src.CipherSizeBytes,
				NewAccountID:  acc.ID,
				NewRemotePath: it.path,
				NewCipherSize: int64(len(it.newCT)),
				NewEncVersion: 2,
				NewKeyID:      p.keyring.Load().CurrentKeyID(),
			})
		}
	}

	if len(applied) > 0 {
		if err := p.db.ApplyRekeyed(ctx, applied); err != nil {
			// The new copies exist but are unreferenced: queue them for deletion.
			for _, r := range applied {
				failures = append(failures, RekeyFailure{ChunkID: r.ID, Path: r.OldRemotePath, Err: "database update: " + err.Error()})
				_ = p.db.EnqueueDeletion(context.WithoutCancel(ctx), models.DeletionChunk, r.NewAccountID, r.NewRemotePath, r.NewCipherSize)
			}
			return 0, failures
		}
		done = len(applied)
	}
	return done, failures
}

// rekeyOne downloads, decrypts, re-encrypts and uploads (without committing)
// a single chunk.
func (p *PoolManager) rekeyOne(ctx context.Context, keyID string, it *rekeyItem) error {
	kr := p.keyring.Load()
	src, err := p.db.GetAccountByID(ctx, it.src.AccountID)
	if err != nil {
		return fmt.Errorf("source account: %w", err)
	}
	cipherData, err := p.hfClient.DownloadChunk(ctx, src.Token, src.RepoName, it.src.RemotePath)
	if err != nil {
		return fmt.Errorf("download: %w", err)
	}
	plain, err := kr.DecryptChunk(cipherData, it.src.RemotePath, it.src.EncVersion, it.src.KeyID)
	if err != nil {
		return fmt.Errorf("decrypt (is the old key listed in HF2S3_LEGACY_MASTER_KEYS?): %w", err)
	}
	if it.src.Sha256Hash != "" {
		sum := sha256.Sum256(plain)
		if hex.EncodeToString(sum[:]) != it.src.Sha256Hash {
			return fmt.Errorf("%w: plaintext hash mismatch after decryption", ErrIntegrity)
		}
	}

	it.path = fmt.Sprintf("data/%s.enc", uuid.New().String())
	it.newCT, _, err = kr.EncryptChunk(plain, it.path)
	if err != nil {
		return err
	}

	// Keep the chunk on the same account when it is still usable, so the
	// distribution across accounts does not change; otherwise pick another.
	dst := src
	if !src.IsActive {
		alt, release, aerr := p.AcquireAccountForChunk(ctx, int64(len(it.newCT)))
		if aerr != nil {
			return aerr
		}
		defer release()
		dst = alt
	}
	it.acc = dst

	oid, err := p.hfClient.UploadLFS(ctx, dst.Token, dst.RepoName, it.newCT)
	if err != nil {
		var rle *hfclient.RateLimitError
		if errors.As(err, &rle) {
			p.hfClient.SetCooldown(dst.Token, rle.RetryAfter)
		}
		return fmt.Errorf("upload: %w", err)
	}
	it.oid = oid
	return nil
}

// DrainAccount moves every chunk stored in accountID to other active accounts
// so the account can be removed. Ciphertext is copied unchanged (the remote
// path, which is the AEAD context, stays the same), then the old copy is queued
// for deletion. The account should be deactivated first so it receives no new
// chunks. Returns how many chunks were moved.
func (p *PoolManager) DrainAccount(ctx context.Context, accountID int64, progress func(RekeyProgress)) (int64, error) {
	src, err := p.db.GetAccountByID(ctx, accountID)
	if err != nil {
		return 0, err
	}
	total, err := p.db.ChunkCountForAccount(ctx, accountID)
	if err != nil || total == 0 {
		return 0, err
	}

	var moved, failed int64
	var after int64
	for ctx.Err() == nil {
		batch, err := p.db.ListChunksByAccount(ctx, accountID, after, 32)
		if err != nil {
			return moved, err
		}
		if len(batch) == 0 {
			break
		}
		after = batch[len(batch)-1].ID

		type moveItem struct {
			c   models.Chunk
			dst *models.Account
			oid string
		}
		var items []*moveItem
		for _, c := range batch {
			cipherData, err := p.hfClient.DownloadChunk(ctx, src.Token, src.RepoName, c.RemotePath)
			if err != nil {
				failed++
				slog.Warn("drain: download failed", "path", c.RemotePath, "err", err)
				continue
			}
			dst, release, err := p.AcquireAccountExcluding(ctx, int64(len(cipherData)), accountID)
			if err != nil {
				return moved, fmt.Errorf("no account can receive chunks: %w", err)
			}
			oid, err := p.hfClient.UploadLFS(ctx, dst.Token, dst.RepoName, cipherData)
			release()
			if err != nil {
				failed++
				slog.Warn("drain: upload failed", "path", c.RemotePath, "err", err)
				continue
			}
			items = append(items, &moveItem{c: c, dst: dst, oid: oid})
		}

		byDst := map[int64][]*moveItem{}
		for _, it := range items {
			byDst[it.dst.ID] = append(byDst[it.dst.ID], it)
		}
		for _, group := range byDst {
			dst := group[0].dst
			files := make([]hfclient.CommitLfsFile, len(group))
			for i, it := range group {
				files[i] = hfclient.CommitLfsFile{Path: it.c.RemotePath, Oid: it.oid, Size: it.c.CipherSizeBytes}
			}
			if err := p.hfClient.CommitLFSFiles(ctx, dst.Token, dst.RepoName, fmt.Sprintf("Drain %d chunks", len(files)), files); err != nil {
				failed += int64(len(group))
				continue
			}
			var applied []db.RekeyedChunk
			for _, it := range group {
				applied = append(applied, db.RekeyedChunk{
					ID: it.c.ID, OldAccountID: it.c.AccountID, OldRemotePath: it.c.RemotePath, OldCipherSize: it.c.CipherSizeBytes,
					NewAccountID: dst.ID, NewRemotePath: it.c.RemotePath, NewCipherSize: it.c.CipherSizeBytes,
					NewEncVersion: it.c.EncVersion, NewKeyID: it.c.KeyID,
				})
			}
			if err := p.db.ApplyRekeyed(ctx, applied); err != nil {
				failed += int64(len(group))
				continue
			}
			moved += int64(len(group))
		}
		if progress != nil {
			progress(RekeyProgress{Total: total, Done: moved, Failed: failed})
		}
	}
	p.wakeGC()
	if failed > 0 {
		return moved, fmt.Errorf("%d chunks could not be moved", failed)
	}
	return moved, ctx.Err()
}
