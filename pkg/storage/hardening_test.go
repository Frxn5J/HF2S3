package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"hf2s3/pkg/crypto"
	"hf2s3/pkg/models"
)

func readObject(t *testing.T, env *testEnv, key string, r *ByteRange) []byte {
	t.Helper()
	_, rc, err := env.pool.GetObject(context.Background(), "bkt", key, r)
	if err != nil {
		t.Fatalf("GetObject(%s): %v", key, err)
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read %s: %v", key, err)
	}
	return data
}

func TestPutObjectBatchesCommitsAndRoundTrips(t *testing.T) {
	env := newTestEnv(t, 100, 2)
	ctx := context.Background()

	data := pattern(1000) // 10 chunks
	obj, err := env.pool.PutObject(ctx, "bkt", "big.bin", "application/octet-stream", bytes.NewReader(data), int64(len(data)), nil)
	if err != nil {
		t.Fatal(err)
	}
	if obj.Size != 1000 {
		t.Fatalf("size = %d", obj.Size)
	}

	// One commit per account, not one per chunk (HF limits commit rate).
	if got := env.hf.totalCommits(); got > 2 {
		t.Fatalf("expected at most one commit per account, got %d commits for 10 chunks", got)
	}
	if got := env.hf.fileCount(); got != 10 {
		t.Fatalf("expected 10 committed chunks, got %d", got)
	}

	_, chunks, _ := env.db.GetObjectWithChunks(ctx, "bkt", "big.bin")
	for _, c := range chunks {
		if c.EncVersion != crypto.EncV2 || c.KeyID != env.pool.Keyring().CurrentKeyID() {
			t.Fatalf("new chunks must use the current v2 key, got v%d/%s", c.EncVersion, c.KeyID)
		}
	}

	if got := readObject(t, env, "big.bin", nil); !bytes.Equal(got, data) {
		t.Fatal("round trip mismatch")
	}
	if n := env.hf.maxDownloadsOfAnyChunk(); n != 1 {
		t.Fatalf("each chunk must be downloaded exactly once, max downloads = %d", n)
	}
}

func TestRangeReadsAcrossChunks(t *testing.T) {
	env := newTestEnv(t, 100, 1)
	data := pattern(1000)
	if _, err := env.pool.PutObject(context.Background(), "bkt", "r.bin", "x", bytes.NewReader(data), 1000, nil); err != nil {
		t.Fatal(err)
	}

	for _, r := range []ByteRange{{0, 0}, {250, 449}, {99, 100}, {900, 999}, {999, 999}, {0, 999}} {
		r := r
		got := readObject(t, env, "r.bin", &r)
		if !bytes.Equal(got, data[r.Start:r.End+1]) {
			t.Fatalf("range %d-%d returned wrong bytes (len %d)", r.Start, r.End, len(got))
		}
	}

	if _, _, err := env.pool.GetObject(context.Background(), "bkt", "r.bin", &ByteRange{Start: 5, End: 1000}); !errors.Is(err, ErrInvalidRange) {
		t.Fatalf("out of range must fail with ErrInvalidRange, got %v", err)
	}
}

// deleteBucketAtEOF removes the bucket right before the body ends, so the
// upload fails only after every chunk has already been committed remotely.
type deleteBucketAtEOF struct {
	r    io.Reader
	once sync.Once
	drop func()
}

func (d *deleteBucketAtEOF) Read(p []byte) (int, error) {
	n, err := d.r.Read(p)
	if errors.Is(err, io.EOF) {
		d.once.Do(d.drop)
	}
	return n, err
}

func TestFailedUploadDoesNotOrphanCommittedChunks(t *testing.T) {
	env := newTestEnv(t, 100, 2)
	ctx := context.Background()

	body := &deleteBucketAtEOF{
		r:    bytes.NewReader(pattern(500)),
		drop: func() { _ = env.db.DeleteBucket(ctx, "bkt") },
	}
	_, err := env.pool.PutObject(ctx, "bkt", "doomed.bin", "x", body, 500, nil)
	if !errors.Is(err, ErrBucketNotFound) {
		t.Fatalf("expected ErrBucketNotFound, got %v", err)
	}
	if env.hf.fileCount() != 5 {
		t.Fatalf("precondition: chunks should have been committed, got %d", env.hf.fileCount())
	}

	if n, _ := env.db.CountPendingDeletions(ctx); n != 5 {
		t.Fatalf("all 5 committed chunks must be queued for deletion, queue = %d", n)
	}
	env.pool.ProcessPendingDeletions(ctx)
	if env.hf.fileCount() != 0 {
		t.Fatalf("orphaned chunks were not deleted: %v", env.hf.fileKeys())
	}
	for _, a := range env.accounts {
		got, _ := env.db.GetAccountByID(ctx, a.ID)
		if got.UsedBytes != 0 {
			t.Fatalf("account %d still charged %d bytes", a.ID, got.UsedBytes)
		}
	}
}

func TestPartialFailureAfterCommitCleansUp(t *testing.T) {
	env := newTestEnv(t, 100, 1)
	ctx := context.Background()

	// A reader failing mid-body: nothing may be saved, nothing may leak.
	failing := io.MultiReader(bytes.NewReader(pattern(350)), errReader{errors.New("client went away")})
	if _, err := env.pool.PutObject(ctx, "bkt", "half.bin", "x", failing, -1, nil); err == nil {
		t.Fatal("upload with a failing body must fail")
	}
	if _, err := env.db.HeadObject(ctx, "bkt", "half.bin"); err == nil {
		t.Fatal("no object may be recorded for a failed upload")
	}
	env.pool.ProcessPendingDeletions(ctx)
	if env.hf.fileCount() != 0 {
		t.Fatalf("committed chunks leaked: %v", env.hf.fileKeys())
	}
}

type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

func TestDeclaredSizeMismatchIsRejectedAndCleaned(t *testing.T) {
	env := newTestEnv(t, 100, 1)
	ctx := context.Background()

	_, err := env.pool.PutObject(ctx, "bkt", "short.bin", "x", bytes.NewReader(pattern(500)), 10, nil)
	if !errors.Is(err, ErrSizeMismatch) {
		t.Fatalf("expected ErrSizeMismatch, got %v", err)
	}
	env.pool.ProcessPendingDeletions(ctx)
	if env.hf.fileCount() != 0 {
		t.Fatalf("chunks of the rejected upload leaked: %v", env.hf.fileKeys())
	}
}

func TestCorruptedAndSwappedChunksAreDetected(t *testing.T) {
	env := newTestEnv(t, 100, 1)
	ctx := context.Background()
	data := pattern(300)
	if _, err := env.pool.PutObject(ctx, "bkt", "c.bin", "x", bytes.NewReader(data), 300, nil); err != nil {
		t.Fatal(err)
	}
	_, chunks, _ := env.db.GetObjectWithChunks(ctx, "bkt", "c.bin")
	k0 := "user/repo1/" + chunks[0].RemotePath
	k1 := "user/repo1/" + chunks[1].RemotePath

	readErr := func() error {
		_, rc, err := env.pool.GetObject(ctx, "bkt", "c.bin", nil)
		if err != nil {
			return err
		}
		defer rc.Close()
		_, err = io.ReadAll(rc)
		return err
	}
	if err := readErr(); err != nil {
		t.Fatalf("pristine object must read fine: %v", err)
	}

	// Flip one bit of the first chunk.
	env.hf.mu.Lock()
	orig := append([]byte(nil), env.hf.files[k0]...)
	env.hf.files[k0][20] ^= 0x01
	env.hf.mu.Unlock()
	if err := readErr(); err == nil {
		t.Fatal("a flipped bit must be detected by AES-GCM")
	}
	env.hf.mu.Lock()
	env.hf.files[k0] = orig
	env.hf.mu.Unlock()
	if err := readErr(); err != nil {
		t.Fatalf("restored object must read again: %v", err)
	}

	// Swap two valid ciphertexts: the AAD (remote path) must catch it.
	env.hf.mu.Lock()
	env.hf.files[k0], env.hf.files[k1] = env.hf.files[k1], env.hf.files[k0]
	env.hf.mu.Unlock()
	if err := readErr(); err == nil {
		t.Fatal("swapped chunks must be detected")
	}
}

func TestMultipartCompletionValidatesParts(t *testing.T) {
	env := newTestEnv(t, 100, 1)
	ctx := context.Background()
	p1, p2, p3 := bytes.Repeat([]byte{1}, 250), bytes.Repeat([]byte{2}, 250), bytes.Repeat([]byte{3}, 250)

	newUpload := func() (string, []string) {
		id, err := env.pool.InitiateMultipartUpload(ctx, "bkt", "mp.bin", "application/x-test", map[string]string{"owner": "me"})
		if err != nil {
			t.Fatal(err)
		}
		var etags []string
		for i, part := range [][]byte{p1, p2, p3} {
			etag, err := env.pool.UploadPart(ctx, id, i+1, bytes.NewReader(part))
			if err != nil {
				t.Fatal(err)
			}
			etags = append(etags, etag)
		}
		return id, etags
	}

	t.Run("wrong etag is rejected", func(t *testing.T) {
		id, etags := newUpload()
		_, err := env.pool.CompleteMultipartUpload(ctx, id, []CompletedPart{{1, etags[0]}, {2, `"deadbeef"`}})
		if !errors.Is(err, ErrInvalidPart) {
			t.Fatalf("expected ErrInvalidPart, got %v", err)
		}
		_ = env.pool.AbortMultipartUpload(ctx, id)
	})

	t.Run("out of order is rejected", func(t *testing.T) {
		id, etags := newUpload()
		_, err := env.pool.CompleteMultipartUpload(ctx, id, []CompletedPart{{2, etags[1]}, {1, etags[0]}})
		if !errors.Is(err, ErrInvalidPartOrder) {
			t.Fatalf("expected ErrInvalidPartOrder, got %v", err)
		}
		_ = env.pool.AbortMultipartUpload(ctx, id)
	})

	t.Run("only listed parts are kept and the rest is queued", func(t *testing.T) {
		env.pool.ProcessPendingDeletions(ctx)
		before := env.hf.fileCount()
		id, etags := newUpload() // 9 chunks committed (3 per part)
		if got := env.hf.fileCount() - before; got != 9 {
			t.Fatalf("expected 9 new chunks, got %d", got)
		}

		obj, err := env.pool.CompleteMultipartUpload(ctx, id, []CompletedPart{{1, etags[0]}, {3, etags[2]}})
		if err != nil {
			t.Fatal(err)
		}
		if obj.Size != 500 || !strings.HasSuffix(obj.ETag, `-2"`) {
			t.Fatalf("unexpected object: size=%d etag=%s", obj.Size, obj.ETag)
		}
		if obj.CustomMetadata["owner"] != "me" {
			t.Fatalf("metadata given at initiate was lost: %v", obj.CustomMetadata)
		}
		want := append(append([]byte(nil), p1...), p3...)
		if got := readObject(t, env, "mp.bin", nil); !bytes.Equal(got, want) {
			t.Fatal("assembled content is wrong")
		}

		env.pool.ProcessPendingDeletions(ctx)
		if got := env.hf.fileCount() - before; got != 6 {
			t.Fatalf("part 2's chunks must be deleted, %d new chunks remain (want 6)", got)
		}
		if n, _ := env.db.CountPendingDeletions(ctx); n != 0 {
			t.Fatalf("queue should be empty, has %d", n)
		}
	})

	t.Run("re-uploading a part number releases the old chunks", func(t *testing.T) {
		id, _ := env.pool.InitiateMultipartUpload(ctx, "bkt", "re.bin", "x", nil)
		if _, err := env.pool.UploadPart(ctx, id, 1, bytes.NewReader(p1)); err != nil {
			t.Fatal(err)
		}
		env.pool.ProcessPendingDeletions(ctx)
		before := env.hf.fileCount()
		if _, err := env.pool.UploadPart(ctx, id, 1, bytes.NewReader(p2)); err != nil {
			t.Fatal(err)
		}
		env.pool.ProcessPendingDeletions(ctx)
		if after := env.hf.fileCount(); after != before {
			t.Fatalf("replaced part left chunks behind: %d -> %d", before, after)
		}
		_ = env.pool.AbortMultipartUpload(ctx, id)
	})
}

func TestAbortMultipartReleasesEverything(t *testing.T) {
	env := newTestEnv(t, 100, 1)
	ctx := context.Background()
	id, _ := env.pool.InitiateMultipartUpload(ctx, "bkt", "ab.bin", "x", nil)
	_, _ = env.pool.UploadPart(ctx, id, 1, bytes.NewReader(pattern(300)))
	if env.hf.fileCount() != 3 {
		t.Fatalf("precondition failed: %d", env.hf.fileCount())
	}
	if err := env.pool.AbortMultipartUpload(ctx, id); err != nil {
		t.Fatal(err)
	}
	if env.hf.fileCount() != 0 {
		t.Fatalf("abort left %d chunks", env.hf.fileCount())
	}
	if _, err := env.pool.MultipartUpload(ctx, id); !errors.Is(err, ErrNoSuchUpload) {
		t.Fatalf("upload should be gone, got %v", err)
	}
}

func TestDeleteIsRetriedWhenHuggingFaceFails(t *testing.T) {
	env := newTestEnv(t, 100, 1)
	ctx := context.Background()
	if _, err := env.pool.PutObject(ctx, "bkt", "d.bin", "x", bytes.NewReader(pattern(300)), 300, nil); err != nil {
		t.Fatal(err)
	}
	usedBefore, _ := env.db.GetAccountByID(ctx, env.accounts[0].ID)

	env.hf.mu.Lock()
	env.hf.failDeletes = true
	env.hf.mu.Unlock()
	if err := env.pool.DeleteObject(ctx, "bkt", "d.bin"); err != nil {
		t.Fatalf("delete must succeed locally even if HF is down: %v", err)
	}
	if env.hf.fileCount() != 3 {
		t.Fatalf("chunks should still exist remotely, got %d", env.hf.fileCount())
	}
	if n, _ := env.db.CountPendingDeletions(ctx); n != 3 {
		t.Fatalf("3 deletions should be queued, got %d", n)
	}
	stillUsed, _ := env.db.GetAccountByID(ctx, env.accounts[0].ID)
	if stillUsed.UsedBytes != usedBefore.UsedBytes {
		t.Fatalf("usage must not be released before the delete really happens: %d -> %d", usedBefore.UsedBytes, stillUsed.UsedBytes)
	}
	// While chunks are pending the account must not be removable.
	if err := env.db.DeleteAccount(ctx, env.accounts[0].ID); err == nil {
		t.Fatal("account with pending deletions must not be deletable")
	}

	env.hf.mu.Lock()
	env.hf.failDeletes = false
	env.hf.mu.Unlock()
	_ = env.db.RetryDeletionsNow(ctx)
	env.pool.ProcessPendingDeletions(ctx)

	if env.hf.fileCount() != 0 {
		t.Fatalf("retry did not delete: %v", env.hf.fileKeys())
	}
	freed, _ := env.db.GetAccountByID(ctx, env.accounts[0].ID)
	if freed.UsedBytes != 0 {
		t.Fatalf("usage not released after successful delete: %d", freed.UsedBytes)
	}
}

func TestOverwriteReleasesPreviousChunks(t *testing.T) {
	env := newTestEnv(t, 100, 1)
	ctx := context.Background()
	if _, err := env.pool.PutObject(ctx, "bkt", "o.bin", "x", bytes.NewReader(pattern(300)), 300, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := env.pool.PutObject(ctx, "bkt", "o.bin", "x", bytes.NewReader(pattern(200)), 200, nil); err != nil {
		t.Fatal(err)
	}
	env.pool.ProcessPendingDeletions(ctx)
	if env.hf.fileCount() != 2 {
		t.Fatalf("only the new version's 2 chunks may remain, got %d", env.hf.fileCount())
	}
	if got := readObject(t, env, "o.bin", nil); !bytes.Equal(got, pattern(200)) {
		t.Fatal("overwritten content wrong")
	}
}

func TestRekeyMigratesLegacyChunks(t *testing.T) {
	env := newTestEnv(t, 100, 1)
	ctx := context.Background()
	acc := env.accounts[0]

	// Data as the previous release stored it: v1 (SHA-256 of a passphrase, no AAD).
	legacyKey := crypto.DeriveKey("old-passphrase")
	plains := [][]byte{bytes.Repeat([]byte("a"), 100), bytes.Repeat([]byte("b"), 60)}
	var chunks []models.Chunk
	var off int64
	for i, pl := range plains {
		blob, err := crypto.Encrypt(pl, legacyKey)
		if err != nil {
			t.Fatal(err)
		}
		path := "data/legacy" + string(rune('0'+i)) + ".enc"
		env.hf.mu.Lock()
		env.hf.files["user/repo1/"+path] = blob
		env.hf.mu.Unlock()
		chunks = append(chunks, models.Chunk{
			PartNumber: 1, ChunkIndex: i, OffsetBytes: off, SizeBytes: int64(len(pl)),
			CipherSizeBytes: int64(len(blob)), AccountID: acc.ID, RemotePath: path, Sha256Hash: plainHash(pl),
		})
		off += int64(len(pl))
	}
	obj := &models.Object{Bucket: "bkt", Key: "old.bin", Size: off, ETag: `"e"`, ContentType: "x"}
	if err := env.db.SaveObjectWithChunks(ctx, obj, chunks); err != nil {
		t.Fatal(err)
	}
	want := append(append([]byte(nil), plains[0]...), plains[1]...)

	// Legacy data is readable before migrating (old key is configured).
	if got := readObject(t, env, "old.bin", nil); !bytes.Equal(got, want) {
		t.Fatal("legacy chunks must stay readable")
	}

	dry, err := env.pool.Rekey(ctx, RekeyOptions{DryRun: true})
	if err != nil || dry.Total != 2 || dry.Done != 0 {
		t.Fatalf("dry run: %+v %v", dry, err)
	}

	rep, err := env.pool.Rekey(ctx, RekeyOptions{BatchSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Done != 2 || rep.Failed != 0 {
		t.Fatalf("rekey report: %+v", rep)
	}

	_, after, _ := env.db.GetObjectWithChunks(ctx, "bkt", "old.bin")
	for _, c := range after {
		if c.EncVersion != crypto.EncV2 || c.KeyID != env.pool.Keyring().CurrentKeyID() {
			t.Fatalf("chunk not migrated: v%d/%s", c.EncVersion, c.KeyID)
		}
		if strings.HasPrefix(c.RemotePath, "data/legacy") {
			t.Fatalf("chunk still points at the old path %s", c.RemotePath)
		}
	}
	if got := readObject(t, env, "old.bin", nil); !bytes.Equal(got, want) {
		t.Fatal("content changed by re-keying")
	}

	// Old ciphertext is only removed after the database points at the new copy.
	env.pool.ProcessPendingDeletions(ctx)
	for _, k := range env.hf.fileKeys() {
		if strings.Contains(k, "data/legacy") {
			t.Fatalf("old ciphertext survived: %s", k)
		}
	}
	if env.hf.fileCount() != 2 {
		t.Fatalf("expected exactly the 2 new chunks, got %v", env.hf.fileKeys())
	}
	if n, _, _ := env.db.CountChunksToRekey(ctx, env.pool.Keyring().CurrentKeyID()); n != 0 {
		t.Fatalf("%d chunks still need re-keying", n)
	}

	// Running again is a no-op.
	again, _ := env.pool.Rekey(ctx, RekeyOptions{})
	if again.Total != 0 {
		t.Fatalf("second run should find nothing, found %d", again.Total)
	}
}

func TestRekeyWithoutOldKeyLeavesDataUntouched(t *testing.T) {
	env := newTestEnv(t, 100, 1)
	ctx := context.Background()
	acc := env.accounts[0]

	pl := bytes.Repeat([]byte("z"), 50)
	blob, _ := crypto.Encrypt(pl, crypto.DeriveKey("some-other-passphrase"))
	env.hf.mu.Lock()
	env.hf.files["user/repo1/data/x.enc"] = blob
	env.hf.mu.Unlock()
	obj := &models.Object{Bucket: "bkt", Key: "k", Size: 50, ETag: `"e"`, ContentType: "x"}
	_ = env.db.SaveObjectWithChunks(ctx, obj, []models.Chunk{{
		PartNumber: 1, SizeBytes: 50, CipherSizeBytes: int64(len(blob)), AccountID: acc.ID,
		RemotePath: "data/x.enc", Sha256Hash: plainHash(pl),
	}})

	rep, err := env.pool.Rekey(ctx, RekeyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Failed != 1 || rep.Done != 0 || len(rep.Failures) != 1 {
		t.Fatalf("expected one reported failure, got %+v", rep)
	}
	_, chunks, _ := env.db.GetObjectWithChunks(ctx, "bkt", "k")
	if chunks[0].RemotePath != "data/x.enc" || chunks[0].EncVersion != crypto.EncV1 {
		t.Fatalf("a chunk that could not be decrypted must not be touched: %+v", chunks[0])
	}
	if n, _ := env.db.CountPendingDeletions(ctx); n != 0 {
		t.Fatalf("nothing may be queued for deletion, got %d", n)
	}
}

func TestDrainAccountMovesChunksAndAllowsRemoval(t *testing.T) {
	env := newTestEnv(t, 100, 2)
	ctx := context.Background()
	data := pattern(1000)
	if _, err := env.pool.PutObject(ctx, "bkt", "dr.bin", "x", bytes.NewReader(data), 1000, nil); err != nil {
		t.Fatal(err)
	}
	a1, a2 := env.accounts[0], env.accounts[1]
	if n, _ := env.db.ChunkCountForAccount(ctx, a1.ID); n == 0 {
		t.Skip("distribution put nothing on account 1")
	}

	a1.IsActive = false
	if err := env.db.UpdateAccount(ctx, a1); err != nil {
		t.Fatal(err)
	}
	moved, err := env.pool.DrainAccount(ctx, a1.ID, nil)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if moved == 0 {
		t.Fatal("nothing moved")
	}
	if n, _ := env.db.ChunkCountForAccount(ctx, a1.ID); n != 0 {
		t.Fatalf("account 1 still owns %d chunks", n)
	}
	if got := readObject(t, env, "dr.bin", nil); !bytes.Equal(got, data) {
		t.Fatal("content changed by draining")
	}

	env.pool.ProcessPendingDeletions(ctx)
	for _, k := range env.hf.fileKeys() {
		if strings.HasPrefix(k, "user/repo1/") {
			t.Fatalf("drained account still stores %s", k)
		}
	}
	if err := env.db.DeleteAccount(ctx, a1.ID); err != nil {
		t.Fatalf("drained account should be removable: %v", err)
	}
	_ = a2
}

// fakeCacheS3 is a minimal S3 endpoint storing objects in memory.
func fakeCacheS3(t *testing.T) (*httptest.Server, func() []string) {
	t.Helper()
	var mu sync.Mutex
	store := map[string][]byte{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodPut:
			data, _ := io.ReadAll(r.Body)
			store[r.URL.Path] = data
		case http.MethodDelete:
			delete(store, r.URL.Path)
			w.WriteHeader(http.StatusNoContent)
		case http.MethodGet:
			data, ok := store[r.URL.Path]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_, _ = w.Write(data)
		}
	}))
	t.Cleanup(ts.Close)
	return ts, func() []string {
		mu.Lock()
		defer mu.Unlock()
		var keys []string
		for k := range store {
			keys = append(keys, k)
		}
		return keys
	}
}

func TestEvictToWatermarksRemovesLeastRecentlyUsed(t *testing.T) {
	env := newTestEnv(t, 1000, 1)
	ctx := context.Background()
	ts, cacheKeys := fakeCacheS3(t)

	cb := &models.CacheBucket{Name: "c", Endpoint: ts.URL, AccessKey: "ak", SecretKey: "sk", BucketName: "cache", QuotaBytes: 300, IsActive: true}
	if err := env.db.CreateCacheBucket(ctx, cb); err != nil {
		t.Fatal(err)
	}

	for _, k := range []string{"one", "two", "three"} {
		if _, err := env.pool.PutObject(ctx, "bkt", k, "x", bytes.NewReader(pattern(100)), 100, nil); err != nil {
			t.Fatal(err)
		}
		env.pool.bgWG.Wait() // background promotion started by PutObject
		if err := env.pool.PromoteToCache(ctx, "bkt", k); err != nil {
			t.Fatalf("promote %s: %v", k, err)
		}
	}
	if len(cacheKeys()) != 3 {
		t.Fatalf("precondition: 3 cached copies, got %v", cacheKeys())
	}

	// A promotion into a full bucket must not overrun the quota.
	if _, err := env.pool.PutObject(ctx, "bkt", "four", "x", bytes.NewReader(pattern(100)), 100, nil); err != nil {
		t.Fatal(err)
	}
	env.pool.bgWG.Wait()
	if err := env.pool.PromoteToCache(ctx, "bkt", "four"); !errors.Is(err, ErrNoCacheSpace) {
		t.Fatalf("expected ErrNoCacheSpace, got %v", err)
	}

	n, err := env.pool.EvictToWatermarks(ctx, 0.9, 0.5) // 300 > 270 -> evict down to 150
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("evicted %d, want 2", n)
	}
	env.pool.ProcessPendingDeletions(ctx)

	keys := cacheKeys()
	if len(keys) != 1 || !strings.HasSuffix(keys[0], "/three") {
		t.Fatalf("the most recently used object must be the survivor, got %v", keys)
	}
	got, _ := env.db.GetCacheBucketByID(ctx, cb.ID)
	if got.UsedBytes != 100 {
		t.Fatalf("used_bytes = %d, want 100", got.UsedBytes)
	}
	// The cold copies are untouched.
	if got := readObject(t, env, "one", nil); !bytes.Equal(got, pattern(100)) {
		t.Fatal("evicted object must still be readable from the cold tier")
	}
}
