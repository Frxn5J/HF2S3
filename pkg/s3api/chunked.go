package s3api

import (
	"bufio"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"hash/crc64"
	"io"
	"net/http"
	"strconv"
	"strings"

	"hf2s3/pkg/sigv4"
)

// Body integrity errors. They are distinct from io.ErrUnexpectedEOF on purpose:
// the storage pool treats ErrUnexpectedEOF as a normal end of data, so a
// truncated upload must never surface as that error.
var (
	ErrIncompleteBody   = errors.New("request body ended before the declared length")
	ErrBadChunkFraming  = errors.New("malformed aws-chunked body")
	ErrChunkSignature   = errors.New("aws-chunked chunk signature does not match")
	ErrChecksumMismatch = errors.New("payload checksum does not match")
	ErrSHA256Mismatch   = errors.New("x-amz-content-sha256 does not match the payload")
	ErrMD5Mismatch      = errors.New("Content-MD5 does not match the payload")
)

// Values of x-amz-content-sha256 that announce an aws-chunked body.
const (
	streamSigned          = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	streamSignedTrailer   = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
	streamUnsignedTrailer = "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
)

func isStreamingPayload(payloadHash string) bool {
	switch payloadHash {
	case streamSigned, streamSignedTrailer, streamUnsignedTrailer:
		return true
	}
	return false
}

var crc64NVMETable = crc64.MakeTable(0x9A6C9329AC4BC9B5)

// newChecksumHash returns the hasher for an x-amz-checksum-<algo> suffix.
func newChecksumHash(algo string) hash.Hash {
	switch algo {
	case "crc32":
		return crc32.NewIEEE()
	case "crc32c":
		return crc32.New(crc32.MakeTable(crc32.Castagnoli))
	case "crc64nvme":
		return crc64.New(crc64NVMETable)
	case "sha1":
		return sha1.New()
	case "sha256":
		return sha256.New()
	}
	return nil
}

const checksumHeaderPrefix = "x-amz-checksum-"

// checksumSet accumulates the x-amz-checksum-* digests to verify at EOF.
type checksumSet struct {
	hashers map[string]hash.Hash // header name (lowercase) -> hasher
	want    map[string]string    // header name -> base64 expected value (may be filled late)
}

func newChecksumSet() *checksumSet {
	return &checksumSet{hashers: map[string]hash.Hash{}, want: map[string]string{}}
}

func (c *checksumSet) declare(headerName string) {
	headerName = strings.ToLower(strings.TrimSpace(headerName))
	if !strings.HasPrefix(headerName, checksumHeaderPrefix) {
		return
	}
	if h := newChecksumHash(strings.TrimPrefix(headerName, checksumHeaderPrefix)); h != nil {
		c.hashers[headerName] = h
	}
}

func (c *checksumSet) write(p []byte) {
	for _, h := range c.hashers {
		h.Write(p)
	}
}

func (c *checksumSet) verify() error {
	for name, want := range c.want {
		h, ok := c.hashers[name]
		if !ok {
			continue
		}
		if subtle.ConstantTimeCompare([]byte(base64.StdEncoding.EncodeToString(h.Sum(nil))), []byte(want)) != 1 {
			return fmt.Errorf("%w (%s)", ErrChecksumMismatch, name)
		}
	}
	return nil
}

// awsChunkedReader decodes an aws-chunked request body, verifying chunk
// signatures (signed modes) and trailing checksums (trailer modes) as it goes.
// Any verification failure aborts the read, so the upload is never committed.
type awsChunkedReader struct {
	br      *bufio.Reader
	signed  bool // chunks carry chunk-signature
	trailer bool // trailing headers follow the final chunk
	verify  bool // actually check signatures (false only when auth is disabled)

	signingKey []byte
	amzDate    string
	scope      string
	prevSig    string

	sums *checksumSet

	inChunk   bool
	remaining int64
	chunkSig  string
	chunkHash hash.Hash
	done      bool
	err       error
}

func newAWSChunkedReader(src io.Reader, payloadHash string, res *AuthResult, trailerNames string) *awsChunkedReader {
	c := &awsChunkedReader{
		br:      bufio.NewReaderSize(src, 64*1024),
		signed:  payloadHash == streamSigned || payloadHash == streamSignedTrailer,
		trailer: payloadHash == streamSignedTrailer || payloadHash == streamUnsignedTrailer,
		sums:    newChecksumSet(),
	}
	if res != nil {
		c.verify = true
		c.signingKey = res.SigningKey
		c.amzDate = res.AmzDate
		c.scope = res.Scope
		c.prevSig = res.Signature
	}
	for _, name := range strings.Split(trailerNames, ",") {
		c.sums.declare(name)
	}
	return c
}

func (c *awsChunkedReader) readLine() (string, error) {
	line, err := c.br.ReadSlice('\n')
	if err != nil {
		if errors.Is(err, io.EOF) {
			return "", ErrIncompleteBody
		}
		return "", fmt.Errorf("%w: %v", ErrBadChunkFraming, err)
	}
	return strings.TrimRight(string(line), "\r\n"), nil
}

func (c *awsChunkedReader) chunkStringToSign(dataHash string) string {
	return "AWS4-HMAC-SHA256-PAYLOAD\n" + c.amzDate + "\n" + c.scope + "\n" + c.prevSig + "\n" + sigv4.EmptySHA256 + "\n" + dataHash
}

func (c *awsChunkedReader) Read(p []byte) (int, error) {
	if c.err != nil {
		return 0, c.err
	}
	if len(p) == 0 {
		return 0, nil
	}
	for {
		if c.done {
			return 0, io.EOF
		}

		if !c.inChunk {
			if err := c.startChunk(); err != nil {
				c.err = err
				return 0, err
			}
			continue
		}

		toRead := int64(len(p))
		if toRead > c.remaining {
			toRead = c.remaining
		}
		n, err := c.br.Read(p[:toRead])
		if n > 0 {
			if c.chunkHash != nil {
				c.chunkHash.Write(p[:n])
			}
			c.sums.write(p[:n])
			c.remaining -= int64(n)
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = ErrIncompleteBody
			}
			c.err = err
			return n, err
		}
		if c.remaining == 0 {
			if err := c.endChunk(); err != nil {
				c.err = err
				return n, err
			}
		}
		if n > 0 {
			return n, nil
		}
	}
}

// startChunk parses "<hex-size>[;chunk-signature=<sig>]".
func (c *awsChunkedReader) startChunk() error {
	line, err := c.readLine()
	if err != nil {
		return err
	}
	sizePart, extPart, _ := strings.Cut(line, ";")
	size, perr := strconv.ParseInt(strings.TrimSpace(sizePart), 16, 64)
	if perr != nil || size < 0 {
		return fmt.Errorf("%w: bad chunk size %q", ErrBadChunkFraming, sizePart)
	}

	c.chunkSig = ""
	if c.signed {
		k, v, ok := strings.Cut(strings.TrimSpace(extPart), "=")
		if !ok || k != "chunk-signature" {
			return fmt.Errorf("%w: missing chunk-signature", ErrBadChunkFraming)
		}
		c.chunkSig = v
		c.chunkHash = sha256.New()
	}

	c.remaining = size
	c.inChunk = true
	if size == 0 {
		return c.finishStream()
	}
	return nil
}

// endChunk verifies the signature of a data chunk and consumes its trailing CRLF.
func (c *awsChunkedReader) endChunk() error {
	if err := c.checkChunkSignature(); err != nil {
		return err
	}
	crlf := make([]byte, 2)
	if _, err := io.ReadFull(c.br, crlf); err != nil {
		return ErrIncompleteBody
	}
	if crlf[0] != '\r' || crlf[1] != '\n' {
		return fmt.Errorf("%w: missing chunk terminator", ErrBadChunkFraming)
	}
	c.inChunk = false
	return nil
}

func (c *awsChunkedReader) checkChunkSignature() error {
	if !c.signed || !c.verify {
		if c.signed {
			c.prevSig = c.chunkSig
		}
		return nil
	}
	want := sigv4.Signature(c.signingKey, c.chunkStringToSign(hex.EncodeToString(c.chunkHash.Sum(nil))))
	if subtle.ConstantTimeCompare([]byte(want), []byte(c.chunkSig)) != 1 {
		return ErrChunkSignature
	}
	c.prevSig = c.chunkSig
	return nil
}

// finishStream handles the zero-length final chunk and any trailing headers.
func (c *awsChunkedReader) finishStream() error {
	if err := c.checkChunkSignature(); err != nil {
		return err
	}

	var canonicalTrailer strings.Builder
	var trailerSig string
	for {
		line, err := c.readLine()
		if err != nil {
			return err
		}
		if line == "" {
			break
		}
		name, value, ok := strings.Cut(line, ":")
		if !ok {
			return fmt.Errorf("%w: bad trailer line", ErrBadChunkFraming)
		}
		name = strings.ToLower(strings.TrimSpace(name))
		value = strings.TrimSpace(value)
		if name == "x-amz-trailer-signature" {
			trailerSig = value
			continue
		}
		canonicalTrailer.WriteString(name + ":" + value + "\n")
		if strings.HasPrefix(name, checksumHeaderPrefix) {
			c.sums.want[name] = value
		}
	}

	if c.trailer && c.signed && c.verify {
		sts := "AWS4-HMAC-SHA256-TRAILER\n" + c.amzDate + "\n" + c.scope + "\n" + c.prevSig + "\n" + sigv4.HashHex(canonicalTrailer.String())
		want := sigv4.Signature(c.signingKey, sts)
		if subtle.ConstantTimeCompare([]byte(want), []byte(trailerSig)) != 1 {
			return ErrChunkSignature
		}
	}

	if err := c.sums.verify(); err != nil {
		return err
	}
	c.done = true
	return nil
}

// bodyVerifier guards a plain (non-chunked) request body: it turns silent
// truncation into ErrIncompleteBody and checks the declared digests at EOF.
type bodyVerifier struct {
	src         io.Reader
	expectedLen int64 // -1 when unknown
	n           int64

	sha    hash.Hash
	wantSH string // lowercase hex
	md5h   hash.Hash
	wantMD string // base64
	sums   *checksumSet
	err    error
}

func (v *bodyVerifier) Read(p []byte) (int, error) {
	if v.err != nil {
		return 0, v.err
	}
	n, err := v.src.Read(p)
	if n > 0 {
		v.n += int64(n)
		if v.sha != nil {
			v.sha.Write(p[:n])
		}
		if v.md5h != nil {
			v.md5h.Write(p[:n])
		}
		v.sums.write(p[:n])
	}
	if err == nil {
		return n, nil
	}
	if errors.Is(err, io.EOF) {
		if verr := v.finish(); verr != nil {
			v.err = verr
			return n, verr
		}
		v.err = io.EOF
		return n, io.EOF
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		err = ErrIncompleteBody
	}
	v.err = err
	return n, err
}

func (v *bodyVerifier) finish() error {
	if v.expectedLen >= 0 && v.n != v.expectedLen {
		return ErrIncompleteBody
	}
	if v.sha != nil && hex.EncodeToString(v.sha.Sum(nil)) != v.wantSH {
		return ErrSHA256Mismatch
	}
	if v.md5h != nil && base64.StdEncoding.EncodeToString(v.md5h.Sum(nil)) != v.wantMD {
		return ErrMD5Mismatch
	}
	return v.sums.verify()
}

func isHexSHA256(s string) bool {
	if len(s) != 64 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

// prepareBody returns a verified reader over the request payload and the
// expected decoded length (-1 if unknown). res is nil when auth is disabled.
func prepareBody(r *http.Request, res *AuthResult) (io.Reader, int64, error) {
	payloadHash := r.Header.Get("X-Amz-Content-Sha256")

	if isStreamingPayload(payloadHash) || strings.Contains(strings.ToLower(r.Header.Get("Content-Encoding")), "aws-chunked") {
		if !isStreamingPayload(payloadHash) {
			// Content-Encoding: aws-chunked without a streaming marker: framing is
			// still present; treat it as the unsigned-trailer flavour.
			payloadHash = streamUnsignedTrailer
		}
		if res != nil && res.Mode != AuthHeader {
			return nil, 0, fmt.Errorf("%w: streaming payload requires header authentication", ErrBadChunkFraming)
		}
		decodedLen := int64(-1)
		if v := r.Header.Get("X-Amz-Decoded-Content-Length"); v != "" {
			if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 {
				decodedLen = n
			}
		}
		dec := newAWSChunkedReader(r.Body, payloadHash, res, r.Header.Get("X-Amz-Trailer"))
		return &bodyVerifier{src: dec, expectedLen: decodedLen, sums: newChecksumSet()}, decodedLen, nil
	}

	v := &bodyVerifier{src: r.Body, expectedLen: r.ContentLength, sums: newChecksumSet()}
	if isHexSHA256(payloadHash) {
		v.sha = sha256.New()
		v.wantSH = strings.ToLower(payloadHash)
	}
	if md := r.Header.Get("Content-MD5"); md != "" {
		v.md5h = md5.New()
		v.wantMD = md
	}
	for name, vals := range r.Header {
		lname := strings.ToLower(name)
		if strings.HasPrefix(lname, checksumHeaderPrefix) && len(vals) > 0 {
			v.sums.declare(lname)
			if _, ok := v.sums.hashers[lname]; ok {
				v.sums.want[lname] = vals[0]
			}
		}
	}
	return v, r.ContentLength, nil
}
