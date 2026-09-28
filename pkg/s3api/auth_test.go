package s3api

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"hf2s3/pkg/sigv4"
)

const (
	awsAccess = "AKIAIOSFODNN7EXAMPLE"
	awsSecret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
)

func awsAuthManager() *AuthManager {
	a := NewAuthManager(awsAccess, awsSecret)
	a.now = func() time.Time { return time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC) }
	return a
}

// Official vector: "GET Object" with a Range header
// (AWS docs, Signature Calculations for the Authorization Header).
func TestAuthenticateAWSVectorGetObjectWithRange(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/test.txt", nil)
	req.Host = "examplebucket.s3.amazonaws.com"
	req.Header.Set("Range", "bytes=0-9")
	req.Header.Set("x-amz-content-sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	req.Header.Set("x-amz-date", "20130524T000000Z")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+awsAccess+"/20130524/us-east-1/s3/aws4_request,"+
		"SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,"+
		"Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41")

	res, aerr := awsAuthManager().Authenticate(req)
	if aerr != nil {
		t.Fatalf("official AWS request must verify: %v", aerr)
	}
	if res.Mode != AuthHeader || res.AccessKey != awsAccess {
		t.Fatalf("unexpected result: %+v", res)
	}

	// Any tampering must break it.
	bad := httptest.NewRequest(http.MethodGet, "/test.txt", nil)
	bad.Host = req.Host
	bad.Header = req.Header.Clone()
	bad.Header.Set("Range", "bytes=0-99")
	if _, aerr := awsAuthManager().Authenticate(bad); aerr == nil || aerr.Code != "SignatureDoesNotMatch" {
		t.Fatalf("a modified signed header must be rejected, got %v", aerr)
	}
}

// Official vector: "PUT Object" (path contains '$', extra x-amz-storage-class
// header and a Date header).
func TestAuthenticateAWSVectorPutObject(t *testing.T) {
	req := httptest.NewRequest(http.MethodPut, "/test%24file.text", strings.NewReader("Welcome to Amazon S3."))
	req.Host = "examplebucket.s3.amazonaws.com"
	req.Header.Set("Date", "Fri, 24 May 2013 00:00:00 GMT")
	req.Header.Set("x-amz-date", "20130524T000000Z")
	req.Header.Set("x-amz-storage-class", "REDUCED_REDUNDANCY")
	req.Header.Set("x-amz-content-sha256", "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+awsAccess+"/20130524/us-east-1/s3/aws4_request,"+
		"SignedHeaders=date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class,"+
		"Signature=98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd")

	if _, aerr := awsAuthManager().Authenticate(req); aerr != nil {
		t.Fatalf("official AWS PUT request must verify: %v", aerr)
	}
}

// Official vector: "Transferring Payload in Multiple Chunks" (aws-chunked with
// chunk signatures). Validates the chained chunk-signature math end to end.
func TestAWSChunkedOfficialVector(t *testing.T) {
	build := func(data1 []byte, sig1 string) *http.Request {
		var body bytes.Buffer
		fmt.Fprintf(&body, "10000;chunk-signature=%s\r\n", sig1)
		body.Write(data1)
		body.WriteString("\r\n")
		fmt.Fprintf(&body, "400;chunk-signature=0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497\r\n")
		body.Write(bytes.Repeat([]byte("a"), 1024))
		body.WriteString("\r\n")
		fmt.Fprintf(&body, "0;chunk-signature=b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9\r\n\r\n")

		req := httptest.NewRequest(http.MethodPut, "/examplebucket/chunkObject.txt", bytes.NewReader(body.Bytes()))
		req.Host = "s3.amazonaws.com"
		req.Header.Set("x-amz-date", "20130524T000000Z")
		req.Header.Set("x-amz-storage-class", "REDUCED_REDUNDANCY")
		req.Header.Set("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
		req.Header.Set("Content-Encoding", "aws-chunked")
		req.Header.Set("x-amz-decoded-content-length", "66560")
		req.ContentLength = 66824
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+awsAccess+"/20130524/us-east-1/s3/aws4_request,"+
			"SignedHeaders=content-encoding;content-length;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-storage-class,"+
			"Signature=4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9")
		if body.Len() != 66824 {
			t.Fatalf("framing differs from the AWS example: %d bytes, want 66824", body.Len())
		}
		return req
	}

	good := build(bytes.Repeat([]byte("a"), 65536), "ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648")
	auth := awsAuthManager()
	res, aerr := auth.Authenticate(good)
	if aerr != nil {
		t.Fatalf("seed signature must verify: %v", aerr)
	}
	reader, declared, err := prepareBody(good, res)
	if err != nil || declared != 66560 {
		t.Fatalf("prepareBody: %v (declared %d)", err, declared)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("official chunked body must decode: %v", err)
	}
	if len(data) != 66560 || !bytes.Equal(data, bytes.Repeat([]byte("a"), 66560)) {
		t.Fatalf("decoded %d bytes", len(data))
	}

	// One flipped payload byte breaks the chained signature.
	tampered := bytes.Repeat([]byte("a"), 65536)
	tampered[100] = 'b'
	badReq := build(tampered, "ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648")
	res, aerr = auth.Authenticate(badReq)
	if aerr != nil {
		t.Fatal(aerr)
	}
	reader, _, _ = prepareBody(badReq, res)
	if _, err := io.ReadAll(reader); !errors.Is(err, ErrChunkSignature) {
		t.Fatalf("tampered chunk must fail with ErrChunkSignature, got %v", err)
	}
}

func TestAuthenticateRejections(t *testing.T) {
	auth := NewAuthManager(testAccessKey, testSecretKey)

	mk := func() *http.Request {
		req := httptest.NewRequest(http.MethodGet, "/bkt/some%20key%2B1.txt?x=1", nil)
		signAs(req, testAccessKey, testSecretKey, sigv4.UnsignedPayload)
		return req
	}
	if _, aerr := auth.Authenticate(mk()); aerr != nil {
		t.Fatalf("valid signature (key with space and +) rejected: %v", aerr)
	}

	cases := map[string]func(*http.Request){
		"wrong secret": func(r *http.Request) {
			r.Header.Del("Authorization")
			signAs(r, testAccessKey, "another-secret", sigv4.UnsignedPayload)
		},
		"wrong access key": func(r *http.Request) {
			r.Header.Del("Authorization")
			signAs(r, "someone-else", testSecretKey, sigv4.UnsignedPayload)
		},
		"modified path":   func(r *http.Request) { r.URL.Path = "/bkt/other.txt" },
		"modified query":  func(r *http.Request) { r.URL.RawQuery = "x=2" },
		"modified host":   func(r *http.Request) { r.Host = "evil.example.net" },
		"modified method": func(r *http.Request) { r.Method = http.MethodDelete },
		"no credentials":  func(r *http.Request) { r.Header.Del("Authorization") },
		"sigv2 header":    func(r *http.Request) { r.Header.Set("Authorization", "AWS "+testAccessKey+":c2ln") },
		"empty signature": func(r *http.Request) {
			r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+testAccessKey+"/20260926/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=")
		},
		"stale timestamp": func(r *http.Request) {
			r.Header.Del("Authorization")
			sigv4.SignRequest(r, testAccessKey, testSecretKey, testRegion, "s3", time.Now().Add(-time.Hour), sigv4.UnsignedPayload)
		},
		"future timestamp": func(r *http.Request) {
			r.Header.Del("Authorization")
			sigv4.SignRequest(r, testAccessKey, testSecretKey, testRegion, "s3", time.Now().Add(time.Hour), sigv4.UnsignedPayload)
		},
	}
	for name, mutate := range cases {
		req := mk()
		mutate(req)
		if _, aerr := auth.Authenticate(req); aerr == nil {
			t.Errorf("%s: request must be rejected", name)
		}
	}

	// With no credentials configured the gateway must fail closed.
	closed := NewAuthManager("", "")
	if _, aerr := closed.Authenticate(mk()); aerr == nil {
		t.Fatal("empty configuration must reject everything")
	}
}

func TestPresignedURLs(t *testing.T) {
	auth := NewAuthManager(testAccessKey, testSecretKey)
	now := time.Now()

	url := func(method, path string, ts time.Time, expires time.Duration) *http.Request {
		u := sigv4.PresignGET("http", "example.com", path, testAccessKey, testSecretKey, testRegion, "s3", ts, expires)
		return httptest.NewRequest(method, u, nil)
	}

	if res, aerr := auth.Authenticate(url(http.MethodGet, "/bkt/my file+1.mp4", now, time.Minute)); aerr != nil || res.Mode != AuthPresigned {
		t.Fatalf("valid presigned URL rejected: %v", aerr)
	}
	// /media accepts URLs signed for either spelling of the path.
	if _, aerr := auth.Authenticate(url(http.MethodGet, "/media/bkt/movie.mp4", now, time.Minute)); aerr != nil {
		t.Fatalf("URL signed for the /media path: %v", aerr)
	}
	plain := sigv4.PresignGET("http", "example.com", "/bkt/movie.mp4", testAccessKey, testSecretKey, testRegion, "s3", now, time.Minute)
	aliased := strings.Replace(plain, "example.com/bkt/", "example.com/media/bkt/", 1)
	if _, aerr := auth.Authenticate(httptest.NewRequest(http.MethodGet, aliased, nil)); aerr != nil {
		t.Fatalf("URL signed for /bkt/... must also work through /media: %v", aerr)
	}

	if _, aerr := auth.Authenticate(url(http.MethodGet, "/bkt/a.mp4", now.Add(-2*time.Minute), time.Minute)); aerr == nil {
		t.Fatal("expired presigned URL must be rejected")
	}
	if _, aerr := auth.Authenticate(url(http.MethodGet, "/bkt/a.mp4", now, 8*24*time.Hour)); aerr == nil {
		t.Fatal("expiry beyond 7 days must be rejected")
	}
	if _, aerr := auth.Authenticate(url(http.MethodDelete, "/bkt/a.mp4", now, time.Minute)); aerr == nil {
		t.Fatal("a GET-signed URL must not authorize DELETE")
	}
	swapped := url(http.MethodGet, "/bkt/a.mp4", now, time.Minute)
	swapped.URL.Path = "/bkt/b.mp4"
	if _, aerr := auth.Authenticate(swapped); aerr == nil {
		t.Fatal("a URL for another object must be rejected")
	}
}

// --- aws-chunked encoders written from the specification ----------------------

func signedChunkedBody(data []byte, chunk int, key []byte, amzDate, scope, seed string, trailerName, trailerValue string) []byte {
	var out bytes.Buffer
	prev := seed
	sign := func(payload []byte) string {
		sts := "AWS4-HMAC-SHA256-PAYLOAD\n" + amzDate + "\n" + scope + "\n" + prev + "\n" + sigv4.EmptySHA256 + "\n" + sigv4.HashHex(string(payload))
		prev = sigv4.Signature(key, sts)
		return prev
	}
	for off := 0; off < len(data); off += chunk {
		end := off + chunk
		if end > len(data) {
			end = len(data)
		}
		piece := data[off:end]
		fmt.Fprintf(&out, "%x;chunk-signature=%s\r\n", len(piece), sign(piece))
		out.Write(piece)
		out.WriteString("\r\n")
	}
	fmt.Fprintf(&out, "0;chunk-signature=%s\r\n", sign(nil))
	if trailerName != "" {
		canonical := trailerName + ":" + trailerValue + "\n"
		sts := "AWS4-HMAC-SHA256-TRAILER\n" + amzDate + "\n" + scope + "\n" + prev + "\n" + sigv4.HashHex(canonical)
		fmt.Fprintf(&out, "%s:%s\r\nx-amz-trailer-signature:%s\r\n", trailerName, trailerValue, sigv4.Signature(key, sts))
	}
	out.WriteString("\r\n")
	return out.Bytes()
}

func unsignedTrailerBody(data []byte, chunk int, trailerName, trailerValue string) []byte {
	var out bytes.Buffer
	for off := 0; off < len(data); off += chunk {
		end := off + chunk
		if end > len(data) {
			end = len(data)
		}
		fmt.Fprintf(&out, "%x\r\n", end-off)
		out.Write(data[off:end])
		out.WriteString("\r\n")
	}
	out.WriteString("0\r\n")
	fmt.Fprintf(&out, "%s:%s\r\n\r\n", trailerName, trailerValue)
	return out.Bytes()
}

func crc32b64(data []byte) string {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], crc32.ChecksumIEEE(data))
	return base64.StdEncoding.EncodeToString(b[:])
}

// streamingRequest builds a signed request whose body is the given aws-chunked
// payload, authenticates it and returns the decoded reader.
func streamingRequest(t *testing.T, mode string, build func(res *AuthResult, key []byte) []byte, trailerHeader string) (*http.Request, *AuthResult) {
	t.Helper()
	auth := NewAuthManager(testAccessKey, testSecretKey)

	probe := httptest.NewRequest(http.MethodPut, "/bkt/obj.bin", nil)
	probe.Header.Set("Content-Encoding", "aws-chunked")
	signAs(probe, testAccessKey, testSecretKey, mode)
	res, aerr := auth.Authenticate(probe)
	if aerr != nil {
		t.Fatalf("probe auth: %v", aerr)
	}
	body := build(res, res.SigningKey)

	req := httptest.NewRequest(http.MethodPut, "/bkt/obj.bin", bytes.NewReader(body))
	req.Header = probe.Header.Clone()
	if trailerHeader != "" {
		req.Header.Set("X-Amz-Trailer", trailerHeader)
	}
	return req, res
}

func TestAWSChunkedSignedWithTrailerChecksum(t *testing.T) {
	data := bytes.Repeat([]byte("0123456789"), 500) // 5000 bytes
	sum := crc32b64(data)

	newReq := func(value string) (*http.Request, *AuthResult) {
		return streamingRequest(t, "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER", func(res *AuthResult, key []byte) []byte {
			return signedChunkedBody(data, 1024, key, res.AmzDate, res.Scope, res.Signature, "x-amz-checksum-crc32", value)
		}, "x-amz-checksum-crc32")
	}

	req, res := newReq(sum)
	reader, _, err := prepareBody(req, res)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("signed chunked + trailer must round trip: %v (len %d)", err, len(got))
	}

	req, res = newReq(crc32b64([]byte("something else")))
	reader, _, _ = prepareBody(req, res)
	if _, err := io.ReadAll(reader); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("a wrong trailer checksum must be rejected, got %v", err)
	}
}

func TestAWSChunkedUnsignedTrailer(t *testing.T) {
	data := pattern(3000)
	build := func(sum string) (*http.Request, *AuthResult) {
		return streamingRequest(t, "STREAMING-UNSIGNED-PAYLOAD-TRAILER", func(*AuthResult, []byte) []byte {
			return unsignedTrailerBody(data, 700, "x-amz-checksum-crc32", sum)
		}, "x-amz-checksum-crc32")
	}

	req, res := build(crc32b64(data))
	req.Header.Set("X-Amz-Decoded-Content-Length", "3000")
	reader, declared, err := prepareBody(req, res)
	if err != nil || declared != 3000 {
		t.Fatal(err, declared)
	}
	if got, err := io.ReadAll(reader); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("unsigned trailer round trip: %v", err)
	}

	req, res = build(crc32b64([]byte("x")))
	reader, _, _ = prepareBody(req, res)
	if _, err := io.ReadAll(reader); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("expected checksum mismatch, got %v", err)
	}

	// Decoded length that disagrees with the payload is an incomplete body.
	req, res = build(crc32b64(data))
	req.Header.Set("X-Amz-Decoded-Content-Length", "9999")
	reader, _, _ = prepareBody(req, res)
	if _, err := io.ReadAll(reader); !errors.Is(err, ErrIncompleteBody) {
		t.Fatalf("expected ErrIncompleteBody, got %v", err)
	}
}

func TestAWSChunkedTruncatedBodyIsNeverAccepted(t *testing.T) {
	data := pattern(3000)
	body := unsignedTrailerBody(data, 700, "x-amz-checksum-crc32", crc32b64(data))
	for _, cut := range []int{10, 700, 1500, len(body) - 5, len(body) - 1} {
		req, res := streamingRequest(t, "STREAMING-UNSIGNED-PAYLOAD-TRAILER", func(*AuthResult, []byte) []byte { return body[:cut] }, "x-amz-checksum-crc32")
		reader, _, _ := prepareBody(req, res)
		if _, err := io.ReadAll(reader); err == nil || errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("truncation at %d must fail with a non-EOF error (pool treats ErrUnexpectedEOF as end of data), got %v", cut, err)
		}
	}
}

func TestPlainBodyIntegrity(t *testing.T) {
	data := []byte("hello integrity")
	read := func(req *http.Request) error {
		r, _, err := prepareBody(req, nil)
		if err != nil {
			return err
		}
		_, err = io.ReadAll(r)
		return err
	}

	req := httptest.NewRequest(http.MethodPut, "/b/k", bytes.NewReader(data))
	req.Header.Set("X-Amz-Content-Sha256", sigv4.HashHex(string(data)))
	if err := read(req); err != nil {
		t.Fatalf("matching sha256: %v", err)
	}

	req = httptest.NewRequest(http.MethodPut, "/b/k", bytes.NewReader(data))
	req.Header.Set("X-Amz-Content-Sha256", sigv4.HashHex("not the body"))
	if err := read(req); !errors.Is(err, ErrSHA256Mismatch) {
		t.Fatalf("expected ErrSHA256Mismatch, got %v", err)
	}

	req = httptest.NewRequest(http.MethodPut, "/b/k", bytes.NewReader(data))
	req.Header.Set("Content-MD5", base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 16)))
	if err := read(req); !errors.Is(err, ErrMD5Mismatch) {
		t.Fatalf("expected ErrMD5Mismatch, got %v", err)
	}

	req = httptest.NewRequest(http.MethodPut, "/b/k", bytes.NewReader(data))
	req.Header.Set("X-Amz-Checksum-Crc32", crc32b64([]byte("other")))
	if err := read(req); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("expected ErrChecksumMismatch, got %v", err)
	}

	req = httptest.NewRequest(http.MethodPut, "/b/k", bytes.NewReader(data))
	req.ContentLength = 100 // client promised more than it sent
	if err := read(req); !errors.Is(err, ErrIncompleteBody) {
		t.Fatalf("a short body must be ErrIncompleteBody, got %v", err)
	}

	// The body reader of net/http reports a dropped connection as io.ErrUnexpectedEOF.
	req = httptest.NewRequest(http.MethodPut, "/b/k", io.MultiReader(bytes.NewReader(data), errReader{io.ErrUnexpectedEOF}))
	req.ContentLength = 100
	if err := read(req); !errors.Is(err, ErrIncompleteBody) || errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("connection loss must become ErrIncompleteBody, got %v", err)
	}
}

type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

func pattern(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}

func TestCRC64NVMEVector(t *testing.T) {
	h := newChecksumHash("crc64nvme")
	h.Write([]byte("123456789"))
	if got := fmt.Sprintf("%016X", h.(hash.Hash64).Sum64()); got != "AE8B14860A799888" {
		t.Fatalf("crc64nvme(123456789) = %s, want AE8B14860A799888", got)
	}
}
