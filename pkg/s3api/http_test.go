package s3api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	"hf2s3/pkg/crypto"
	"hf2s3/pkg/db"
	"hf2s3/pkg/hfclient"
	"hf2s3/pkg/hfclient/hftest"
	"hf2s3/pkg/models"
	"hf2s3/pkg/sigv4"
	"hf2s3/pkg/storage"
)

type httpEnv struct {
	srv  *Server
	pool *storage.PoolManager
	db   *db.DB
	hf   *hftest.Fake
}

func newHTTPEnv(t *testing.T, chunkSize int64) *httpEnv {
	t.Helper()
	database, err := db.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })

	hf := hftest.New(t)
	client := hfclient.NewClient(hfclient.WithBaseURL(hf.URL()))
	kr := crypto.NewKeyringFromLegacyKey(crypto.DeriveKey("http-test"))
	pool := storage.NewPoolManagerWithKeyring(database, client, kr, chunkSize)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = pool.Shutdown(ctx)
	})

	if err := database.CreateAccount(context.Background(), &models.Account{
		Name: "acc", Username: "u", Token: "tok", RepoName: "user/repo", IsActive: true,
	}); err != nil {
		t.Fatal(err)
	}

	return &httpEnv{srv: NewServer(pool, NewAuthManager(testAccessKey, testSecretKey)), pool: pool, db: database, hf: hf}
}

// do sends a signed request. payload is hashed into x-amz-content-sha256.
func (e *httpEnv) do(t *testing.T, method, target string, body []byte, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, bytes.NewReader(body))
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	sum := sha256.Sum256(body)
	signAs(req, testAccessKey, testSecretKey, hex.EncodeToString(sum[:]))
	rec := httptest.NewRecorder()
	e.srv.ServeHTTP(rec, req)
	return rec
}

func (e *httpEnv) mustBucket(t *testing.T, name string) {
	t.Helper()
	if rec := e.do(t, http.MethodPut, "/"+name, nil, nil); rec.Code != http.StatusOK {
		t.Fatalf("create bucket %s: %d %s", name, rec.Code, rec.Body.String())
	}
}

func (e *httpEnv) mustPut(t *testing.T, target string, body []byte) {
	t.Helper()
	if rec := e.do(t, http.MethodPut, target, body, nil); rec.Code != http.StatusOK {
		t.Fatalf("PUT %s: %d %s", target, rec.Code, rec.Body.String())
	}
}

func errorCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var e S3ErrorResponse
	if err := xml.Unmarshal(rec.Body.Bytes(), &e); err != nil {
		t.Fatalf("response is not an S3 error (status %d): %q", rec.Code, rec.Body.String())
	}
	return e.Code
}

func TestUnauthenticatedRequestsAreRejectedEverywhere(t *testing.T) {
	env := newHTTPEnv(t, 128)
	env.mustBucket(t, "vids")
	env.mustPut(t, "/vids/movie.mp4", pattern(500))

	for _, target := range []string{"/", "/vids", "/vids/movie.mp4", "/media/vids/movie.mp4"} {
		req := httptest.NewRequest(http.MethodGet, target, nil)
		rec := httptest.NewRecorder()
		env.srv.ServeHTTP(rec, req)
		if rec.Code != http.StatusForbidden {
			t.Errorf("GET %s without credentials = %d, want 403", target, rec.Code)
		}
		if strings.Contains(rec.Body.String(), "movie") && rec.Code == http.StatusOK {
			t.Errorf("content leaked on %s", target)
		}
	}

	// Knowing the access key ID alone (the old check) must not be enough.
	req := httptest.NewRequest(http.MethodGet, "/vids/movie.mp4", nil)
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+testAccessKey+"/20260926/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=00")
	rec := httptest.NewRecorder()
	env.srv.ServeHTTP(rec, req)
	if rec.Code == http.StatusOK || rec.Code == http.StatusFound {
		t.Fatalf("access key ID without a valid signature was accepted: %d", rec.Code)
	}
}

func TestMediaRouteWithPresignedURL(t *testing.T) {
	env := newHTTPEnv(t, 128)
	env.mustBucket(t, "vids")
	data := pattern(700)
	env.mustPut(t, "/vids/clips/a%20b%2Bc.mp4", data)

	u := sigv4.PresignGET("http", "example.com", "/media/vids/clips/a b+c.mp4", testAccessKey, testSecretKey, testRegion, "s3", time.Now(), time.Minute)

	// Full download.
	rec := httptest.NewRecorder()
	env.srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, u, nil))
	if rec.Code != http.StatusOK || !bytes.Equal(rec.Body.Bytes(), data) {
		t.Fatalf("presigned /media GET = %d (len %d)", rec.Code, rec.Body.Len())
	}

	// Seeking, as a <video> element does.
	req := httptest.NewRequest(http.MethodGet, u, nil)
	req.Header.Set("Range", "bytes=300-399")
	rec = httptest.NewRecorder()
	env.srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusPartialContent || !bytes.Equal(rec.Body.Bytes(), data[300:400]) {
		t.Fatalf("ranged /media GET = %d", rec.Code)
	}
	if rec.Header().Get("Content-Range") != "bytes 300-399/700" {
		t.Fatalf("Content-Range = %q", rec.Header().Get("Content-Range"))
	}

	// POST is not a media operation.
	pu := sigv4.PresignGET("http", "example.com", "/media/vids/clips/a b+c.mp4", testAccessKey, testSecretKey, testRegion, "s3", time.Now(), time.Minute)
	rec = httptest.NewRecorder()
	env.srv.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, pu, nil))
	if rec.Code == http.StatusOK {
		t.Fatal("POST on /media must not succeed")
	}
}

func TestRangeAndConditionalRequests(t *testing.T) {
	env := newHTTPEnv(t, 100)
	env.mustBucket(t, "rng")
	data := pattern(1000)
	env.mustPut(t, "/rng/f.bin", data)
	etag := env.do(t, http.MethodHead, "/rng/f.bin", nil, nil).Header().Get("ETag")

	cases := []struct {
		header string
		status int
		want   []byte
		cr     string
	}{
		{"bytes=0-9", 206, data[0:10], "bytes 0-9/1000"},
		{"bytes=990-", 206, data[990:], "bytes 990-999/1000"},
		{"bytes=-10", 206, data[990:], "bytes 990-999/1000"},
		{"bytes=900-5000", 206, data[900:], "bytes 900-999/1000"}, // end is clamped
		{"bytes=-5000", 206, data, "bytes 0-999/1000"},            // suffix larger than the object
		{"bytes=1000-1010", 416, nil, "bytes */1000"},
		{"bytes=5-1", 200, data, ""},     // malformed: ignored
		{"bytes=0-1,5-6", 200, data, ""}, // multi-range: ignored
		{"items=0-1", 200, data, ""},     // unknown unit: ignored
	}
	for _, c := range cases {
		rec := env.do(t, http.MethodGet, "/rng/f.bin", nil, map[string]string{"Range": c.header})
		if rec.Code != c.status {
			t.Errorf("Range %q: status %d, want %d", c.header, rec.Code, c.status)
			continue
		}
		if c.want != nil && !bytes.Equal(rec.Body.Bytes(), c.want) {
			t.Errorf("Range %q: wrong body (%d bytes)", c.header, rec.Body.Len())
		}
		if c.cr != "" && rec.Header().Get("Content-Range") != c.cr {
			t.Errorf("Range %q: Content-Range %q, want %q", c.header, rec.Header().Get("Content-Range"), c.cr)
		}
	}

	if rec := env.do(t, http.MethodGet, "/rng/f.bin", nil, map[string]string{"If-None-Match": etag}); rec.Code != http.StatusNotModified {
		t.Errorf("If-None-Match: %d, want 304", rec.Code)
	}
	if rec := env.do(t, http.MethodGet, "/rng/f.bin", nil, map[string]string{"If-Match": `"nope"`}); rec.Code != http.StatusPreconditionFailed {
		t.Errorf("If-Match mismatch: %d, want 412", rec.Code)
	}
	if rec := env.do(t, http.MethodGet, "/rng/f.bin", nil, map[string]string{"If-Match": etag}); rec.Code != http.StatusOK {
		t.Errorf("If-Match hit: %d, want 200", rec.Code)
	}
	future := time.Now().Add(time.Hour).UTC().Format(http.TimeFormat)
	if rec := env.do(t, http.MethodGet, "/rng/f.bin", nil, map[string]string{"If-Modified-Since": future}); rec.Code != http.StatusNotModified {
		t.Errorf("If-Modified-Since in the future: %d, want 304", rec.Code)
	}

	head := env.do(t, http.MethodHead, "/rng/f.bin", nil, map[string]string{"Range": "bytes=0-9"})
	if head.Code != http.StatusPartialContent || head.Header().Get("Content-Length") != "10" || head.Body.Len() != 0 {
		t.Errorf("HEAD with Range: %d len=%s body=%d", head.Code, head.Header().Get("Content-Length"), head.Body.Len())
	}
}

func listKeys(t *testing.T, rec *httptest.ResponseRecorder) ListBucketResult {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("list failed: %d %s", rec.Code, rec.Body.String())
	}
	var res ListBucketResult
	if err := xml.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatal(err)
	}
	return res
}

func TestListObjectsPaginationV1AndV2(t *testing.T) {
	env := newHTTPEnv(t, 1024)
	env.mustBucket(t, "lst")

	var keys []string
	for i := 0; i < 25; i++ {
		keys = append(keys, fmt.Sprintf("photos/2026/img%02d.jpg", i))
	}
	keys = append(keys, "docs/a.txt", "docs/b.txt", "docs/sub/c.txt", "root.txt", "a b+c%d.txt")
	for _, k := range keys {
		env.mustPut(t, "/lst/"+(&url.URL{Path: k}).EscapedPath(), []byte("x"))
	}
	sort.Strings(keys)

	t.Run("v2 pages cover every key once", func(t *testing.T) {
		var got []string
		token := ""
		for page := 0; page < 20; page++ {
			target := "/lst?list-type=2&max-keys=7"
			if token != "" {
				target += "&continuation-token=" + url.QueryEscape(token)
			}
			res := listKeys(t, env.do(t, http.MethodGet, target, nil, nil))
			for _, c := range res.Contents {
				got = append(got, c.Key)
			}
			if !res.IsTruncated {
				break
			}
			if res.NextContinuationToken == "" {
				t.Fatal("truncated result without a continuation token")
			}
			token = res.NextContinuationToken
		}
		if strings.Join(got, "|") != strings.Join(keys, "|") {
			t.Fatalf("paging lost or duplicated keys:\n got %v\nwant %v", got, keys)
		}
	})

	t.Run("v1 marker paging", func(t *testing.T) {
		var got []string
		marker := ""
		for page := 0; page < 20; page++ {
			res := listKeys(t, env.do(t, http.MethodGet, "/lst?max-keys=10&marker="+url.QueryEscape(marker), nil, nil))
			for _, c := range res.Contents {
				got = append(got, c.Key)
			}
			if !res.IsTruncated {
				break
			}
			marker = res.NextMarker
			if marker == "" {
				t.Fatal("v1 truncated result without NextMarker")
			}
		}
		if strings.Join(got, "|") != strings.Join(keys, "|") {
			t.Fatalf("v1 paging lost or duplicated keys: %v", got)
		}
	})

	t.Run("delimiter groups common prefixes and pages through them", func(t *testing.T) {
		var contents, prefixes []string
		token := ""
		for page := 0; page < 10; page++ {
			target := "/lst?list-type=2&delimiter=/&max-keys=2"
			if token != "" {
				target += "&continuation-token=" + url.QueryEscape(token)
			}
			res := listKeys(t, env.do(t, http.MethodGet, target, nil, nil))
			for _, c := range res.Contents {
				contents = append(contents, c.Key)
			}
			for _, p := range res.CommonPrefixes {
				prefixes = append(prefixes, p.Prefix)
			}
			if !res.IsTruncated {
				break
			}
			token = res.NextContinuationToken
		}
		if strings.Join(prefixes, ",") != "docs/,photos/" {
			t.Fatalf("common prefixes = %v", prefixes)
		}
		if strings.Join(contents, ",") != "a b+c%d.txt,root.txt" {
			t.Fatalf("top-level keys = %v", contents)
		}
	})

	t.Run("prefix listing", func(t *testing.T) {
		res := listKeys(t, env.do(t, http.MethodGet, "/lst?list-type=2&prefix=docs/&delimiter=/", nil, nil))
		if len(res.Contents) != 2 || len(res.CommonPrefixes) != 1 || res.CommonPrefixes[0].Prefix != "docs/sub/" {
			t.Fatalf("unexpected prefix listing: %+v", res)
		}
		if res.KeyCount != 3 {
			t.Fatalf("KeyCount = %d, want 3", res.KeyCount)
		}
	})

	t.Run("encoding-type=url escapes keys", func(t *testing.T) {
		res := listKeys(t, env.do(t, http.MethodGet, "/lst?list-type=2&prefix=a%20b&encoding-type=url", nil, nil))
		if res.EncodingType != "url" || len(res.Contents) != 1 {
			t.Fatalf("unexpected: %+v", res)
		}
		if res.Contents[0].Key != "a%20b%2Bc%25d.txt" {
			t.Fatalf("key not URL-encoded: %q", res.Contents[0].Key)
		}
		if res.Prefix != "a%20b" {
			t.Fatalf("prefix not URL-encoded: %q", res.Prefix)
		}
	})

	t.Run("max-keys=0 and invalid arguments", func(t *testing.T) {
		res := listKeys(t, env.do(t, http.MethodGet, "/lst?list-type=2&max-keys=0", nil, nil))
		if len(res.Contents) != 0 || res.IsTruncated {
			t.Fatalf("max-keys=0 must return nothing: %+v", res)
		}
		if rec := env.do(t, http.MethodGet, "/lst?max-keys=abc", nil, nil); rec.Code != http.StatusBadRequest {
			t.Fatalf("bad max-keys = %d", rec.Code)
		}
		if rec := env.do(t, http.MethodGet, "/lst?list-type=2&continuation-token=%25%25", nil, nil); rec.Code != http.StatusBadRequest {
			t.Fatalf("bad token = %d", rec.Code)
		}
	})
}

func TestUnsupportedFeaturesAreRefusedNotFaked(t *testing.T) {
	env := newHTTPEnv(t, 128)
	env.mustBucket(t, "feat")
	env.mustPut(t, "/feat/src.txt", []byte("source"))

	// CopyObject must not silently create an empty object.
	rec := env.do(t, http.MethodPut, "/feat/copy.txt", nil, map[string]string{"X-Amz-Copy-Source": "/feat/src.txt"})
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("CopyObject = %d, want 501", rec.Code)
	}
	if rec := env.do(t, http.MethodHead, "/feat/copy.txt", nil, nil); rec.Code != http.StatusNotFound {
		t.Fatal("a refused copy must not create the destination")
	}

	// A sub-resource DELETE must never delete the bucket itself.
	if rec := env.do(t, http.MethodDelete, "/feat?lifecycle", nil, nil); rec.Code != http.StatusNotImplemented {
		t.Fatalf("DELETE /bucket?lifecycle = %d, want 501", rec.Code)
	}
	if rec := env.do(t, http.MethodHead, "/feat", nil, nil); rec.Code != http.StatusOK {
		t.Fatal("the bucket must survive a sub-resource DELETE")
	}

	for _, target := range []string{"/feat?acl", "/feat?versioning", "/feat?uploads", "/feat/src.txt?acl", "/feat/src.txt?tagging"} {
		if rec := env.do(t, http.MethodGet, target, nil, nil); rec.Code != http.StatusNotImplemented {
			t.Errorf("GET %s = %d, want 501", target, rec.Code)
		}
	}

	rec = env.do(t, http.MethodGet, "/feat?location", nil, nil)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "LocationConstraint") {
		t.Fatalf("GetBucketLocation = %d %s", rec.Code, rec.Body.String())
	}
}

func TestPutObjectRejectsBadBodiesAndLeavesNothing(t *testing.T) {
	env := newHTTPEnv(t, 100)
	env.mustBucket(t, "bad")

	// x-amz-content-sha256 that does not match the payload.
	req := httptest.NewRequest(http.MethodPut, "/bad/k1", bytes.NewReader(pattern(500)))
	signAs(req, testAccessKey, testSecretKey, sigv4.HashHex("some other payload"))
	rec := httptest.NewRecorder()
	env.srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest || errorCode(t, rec) != "XAmzContentSHA256Mismatch" {
		t.Fatalf("hash mismatch = %d %s", rec.Code, rec.Body.String())
	}

	// Client announced 1000 bytes but sent 500.
	req = httptest.NewRequest(http.MethodPut, "/bad/k2", bytes.NewReader(pattern(500)))
	req.ContentLength = 1000
	signAs(req, testAccessKey, testSecretKey, sigv4.UnsignedPayload)
	rec = httptest.NewRecorder()
	env.srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest || errorCode(t, rec) != "IncompleteBody" {
		t.Fatalf("truncated body = %d %s", rec.Code, rec.Body.String())
	}

	// Content-MD5 mismatch.
	rec = env.do(t, http.MethodPut, "/bad/k3", pattern(200), map[string]string{"Content-MD5": base64.StdEncoding.EncodeToString(make([]byte, 16))})
	if rec.Code != http.StatusBadRequest || errorCode(t, rec) != "BadDigest" {
		t.Fatalf("MD5 mismatch = %d %s", rec.Code, rec.Body.String())
	}

	for _, k := range []string{"k1", "k2", "k3"} {
		if rec := env.do(t, http.MethodHead, "/bad/"+k, nil, nil); rec.Code != http.StatusNotFound {
			t.Errorf("rejected upload %s left an object behind (%d)", k, rec.Code)
		}
	}
	// And no orphaned remote data survives cleanup.
	env.pool.ProcessPendingDeletions(context.Background())
	if n := env.hf.FileCount(); n != 0 {
		t.Fatalf("%d chunks leaked by rejected uploads: %v", n, env.hf.Files())
	}
}

func TestAWSChunkedPutThroughHTTP(t *testing.T) {
	env := newHTTPEnv(t, 100)
	env.mustBucket(t, "chk")
	data := pattern(1234)

	put := func(mode string, build func(res *AuthResult) []byte, extra map[string]string) *httptest.ResponseRecorder {
		probe := httptest.NewRequest(http.MethodPut, "/chk/streamed.bin", nil)
		probe.Header.Set("Content-Encoding", "aws-chunked")
		signAs(probe, testAccessKey, testSecretKey, mode)
		res, aerr := env.srv.auth.Authenticate(probe)
		if aerr != nil {
			t.Fatal(aerr)
		}
		req := httptest.NewRequest(http.MethodPut, "/chk/streamed.bin", bytes.NewReader(build(res)))
		req.Header = probe.Header.Clone()
		for k, v := range extra {
			req.Header.Set(k, v)
		}
		rec := httptest.NewRecorder()
		env.srv.ServeHTTP(rec, req)
		return rec
	}

	rec := put("STREAMING-AWS4-HMAC-SHA256-PAYLOAD", func(res *AuthResult) []byte {
		return signedChunkedBody(data, 500, res.SigningKey, res.AmzDate, res.Scope, res.Signature, "", "")
	}, map[string]string{"X-Amz-Decoded-Content-Length": "1234"})
	if rec.Code != http.StatusOK {
		t.Fatalf("signed streaming PUT = %d %s", rec.Code, rec.Body.String())
	}
	got := env.do(t, http.MethodGet, "/chk/streamed.bin", nil, nil)
	if !bytes.Equal(got.Body.Bytes(), data) {
		t.Fatal("object stored from an aws-chunked upload differs (framing leaked into the data?)")
	}

	// A tampered chunk must not create or replace anything.
	rec = put("STREAMING-AWS4-HMAC-SHA256-PAYLOAD", func(res *AuthResult) []byte {
		b := signedChunkedBody(pattern(800), 500, res.SigningKey, res.AmzDate, res.Scope, res.Signature, "", "")
		b[120] ^= 0xff
		return b
	}, map[string]string{"X-Amz-Decoded-Content-Length": "800"})
	if rec.Code != http.StatusForbidden {
		t.Fatalf("tampered streaming PUT = %d, want 403", rec.Code)
	}
	if got := env.do(t, http.MethodGet, "/chk/streamed.bin", nil, nil); !bytes.Equal(got.Body.Bytes(), data) {
		t.Fatal("a rejected upload replaced the existing object")
	}

	rec = put("STREAMING-UNSIGNED-PAYLOAD-TRAILER", func(*AuthResult) []byte {
		return unsignedTrailerBody(data, 400, "x-amz-checksum-crc32", crc32b64(data))
	}, map[string]string{"X-Amz-Trailer": "x-amz-checksum-crc32", "X-Amz-Decoded-Content-Length": "1234"})
	if rec.Code != http.StatusOK {
		t.Fatalf("unsigned trailer PUT = %d %s", rec.Code, rec.Body.String())
	}
	rec = put("STREAMING-UNSIGNED-PAYLOAD-TRAILER", func(*AuthResult) []byte {
		return unsignedTrailerBody(data, 400, "x-amz-checksum-crc32", crc32b64([]byte("wrong")))
	}, map[string]string{"X-Amz-Trailer": "x-amz-checksum-crc32", "X-Amz-Decoded-Content-Length": "1234"})
	if rec.Code != http.StatusBadRequest || errorCode(t, rec) != "BadDigest" {
		t.Fatalf("bad trailer checksum = %d %s", rec.Code, rec.Body.String())
	}
}

func TestMultipartThroughHTTP(t *testing.T) {
	env := newHTTPEnv(t, 100)
	env.mustBucket(t, "mpu")

	rec := env.do(t, http.MethodPost, "/mpu/big.bin?uploads", nil, map[string]string{"Content-Type": "video/mp4", "X-Amz-Meta-Origin": "cam1"})
	if rec.Code != http.StatusOK {
		t.Fatalf("initiate = %d %s", rec.Code, rec.Body.String())
	}
	var init InitiateMultipartUploadResult
	_ = xml.Unmarshal(rec.Body.Bytes(), &init)
	if init.UploadId == "" {
		t.Fatal("no upload id")
	}

	parts := [][]byte{pattern(250), bytes.Repeat([]byte{9}, 250), pattern(120)}
	etags := make([]string, len(parts))
	for i, p := range parts {
		rec := env.do(t, http.MethodPut, fmt.Sprintf("/mpu/big.bin?partNumber=%d&uploadId=%s", i+1, init.UploadId), p, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("part %d = %d %s", i+1, rec.Code, rec.Body.String())
		}
		etags[i] = rec.Header().Get("ETag")
	}

	// An upload id is bound to its bucket/key.
	if rec := env.do(t, http.MethodPut, fmt.Sprintf("/mpu/other.bin?partNumber=1&uploadId=%s", init.UploadId), []byte("x"), nil); rec.Code != http.StatusNotFound {
		t.Fatalf("upload id replayed on another key = %d, want 404", rec.Code)
	}
	if rec := env.do(t, http.MethodPut, "/mpu/big.bin?partNumber=0&uploadId="+init.UploadId, []byte("x"), nil); rec.Code != http.StatusBadRequest {
		t.Fatalf("part number 0 = %d, want 400", rec.Code)
	}

	complete := func(order []int, badETag bool) *httptest.ResponseRecorder {
		var b strings.Builder
		b.WriteString("<CompleteMultipartUpload>")
		for _, i := range order {
			etag := etags[i]
			if badETag {
				etag = `"00000000000000000000000000000000"`
			}
			fmt.Fprintf(&b, "<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>", i+1, etag)
		}
		b.WriteString("</CompleteMultipartUpload>")
		return env.do(t, http.MethodPost, "/mpu/big.bin?uploadId="+init.UploadId, []byte(b.String()), nil)
	}

	if rec := complete([]int{1, 0, 2}, false); rec.Code != http.StatusBadRequest || errorCode(t, rec) != "InvalidPartOrder" {
		t.Fatalf("out-of-order complete = %d %s", rec.Code, rec.Body.String())
	}
	if rec := complete([]int{0, 1, 2}, true); rec.Code != http.StatusBadRequest || errorCode(t, rec) != "InvalidPart" {
		t.Fatalf("wrong etag complete = %d %s", rec.Code, rec.Body.String())
	}
	rec = complete([]int{0, 2}, false) // skip part 2
	if rec.Code != http.StatusOK {
		t.Fatalf("complete = %d %s", rec.Code, rec.Body.String())
	}
	var done CompleteMultipartUploadResult
	_ = xml.Unmarshal(rec.Body.Bytes(), &done)
	if !strings.HasSuffix(done.ETag, `-2"`) {
		t.Fatalf("multipart ETag = %s", done.ETag)
	}

	want := append(append([]byte(nil), parts[0]...), parts[2]...)
	got := env.do(t, http.MethodGet, "/mpu/big.bin", nil, nil)
	if !bytes.Equal(got.Body.Bytes(), want) {
		t.Fatal("assembled object differs")
	}
	if got.Header().Get("Content-Type") != "video/mp4" || got.Header().Get("X-Amz-Meta-Origin") != "cam1" {
		t.Fatalf("content type / metadata from initiate lost: %v", got.Header())
	}

	// The skipped part's chunks are released.
	env.pool.ProcessPendingDeletions(context.Background())
	// Parts 1 (250 bytes = 3 chunks) and 3 (120 bytes = 2 chunks) remain.
	if n := env.hf.FileCount(); n != 5 {
		t.Fatalf("expected the 5 chunks of parts 1 and 3 to remain, got %d: %v", n, env.hf.Files())
	}
}

func TestCORSIsOffByDefault(t *testing.T) {
	env := newHTTPEnv(t, 128)

	pre := httptest.NewRequest(http.MethodOptions, "/anything", nil)
	pre.Header.Set("Origin", "https://evil.example")
	rec := httptest.NewRecorder()
	env.srv.ServeHTTP(rec, pre)
	if rec.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("no CORS origin is configured, none may be allowed")
	}

	env.srv.SetCORSOrigins([]string{"https://app.example.com"})
	ok := httptest.NewRequest(http.MethodOptions, "/anything", nil)
	ok.Header.Set("Origin", "https://app.example.com")
	rec = httptest.NewRecorder()
	env.srv.ServeHTTP(rec, ok)
	if rec.Header().Get("Access-Control-Allow-Origin") != "https://app.example.com" {
		t.Fatalf("configured origin not allowed: %v", rec.Header())
	}
	rec = httptest.NewRecorder()
	env.srv.ServeHTTP(rec, pre)
	if rec.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("a different origin must still be refused")
	}
}

func TestErrorsDoNotLeakInternals(t *testing.T) {
	env := newHTTPEnv(t, 128)
	env.mustBucket(t, "leak")
	// Break the database underneath the server.
	_ = env.db.Close()

	rec := env.do(t, http.MethodGet, "/leak?list-type=2", nil, nil)
	if rec.Code < 400 {
		t.Fatalf("expected an error, got %d", rec.Code)
	}
	body := rec.Body.String()
	for _, needle := range []string{"sql", "SQL", "database", "sqlite", "closed"} {
		if strings.Contains(body, needle) {
			t.Fatalf("internal detail %q leaked to the client: %s", needle, body)
		}
	}
}

func TestBucketNameValidation(t *testing.T) {
	env := newHTTPEnv(t, 128)
	for _, name := range []string{"ab", "UPPER", "has_underscore", "-lead", "trail-", "a..b", strings.Repeat("x", 64), "media", "api", "static"} {
		if rec := env.do(t, http.MethodPut, "/"+name, nil, nil); rec.Code != http.StatusBadRequest {
			t.Errorf("bucket %q accepted (%d)", name, rec.Code)
		}
	}
	if rec := env.do(t, http.MethodPut, "/valid-name.1", nil, nil); rec.Code != http.StatusOK {
		t.Errorf("valid bucket refused: %d", rec.Code)
	}
	if rec := env.do(t, http.MethodPut, "/valid-name.1/"+strings.Repeat("k", 1025), []byte("x"), nil); rec.Code != http.StatusBadRequest {
		t.Errorf("1025-byte key accepted (%d)", rec.Code)
	}
}

var _ = io.EOF
