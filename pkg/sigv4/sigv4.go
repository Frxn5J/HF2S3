// Package sigv4 implements the pieces of AWS Signature Version 4 that HF2S3
// needs both as a client (talking to Hugging Face Storage) and as a server
// (authenticating S3 clients). It follows the S3 flavour of the spec: the
// canonical URI is encoded exactly once.
package sigv4

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

const (
	Algorithm       = "AWS4-HMAC-SHA256"
	UnsignedPayload = "UNSIGNED-PAYLOAD"
	TimeFormat      = "20060102T150405Z"
	DateFormat      = "20060102"

	// EmptySHA256 is the hex SHA-256 of the empty string.
	EmptySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

// URIEncode encodes s the way AWS requires: only A-Z a-z 0-9 - _ . ~ are left
// untouched, everything else becomes %XX (uppercase hex). When encodeSlash is
// false, '/' is preserved (used for object paths).
func URIEncode(s string, encodeSlash bool) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9',
			c == '-', c == '_', c == '.', c == '~':
			b.WriteByte(c)
		case c == '/' && !encodeSlash:
			b.WriteByte('/')
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

// CanonicalURI returns the canonical (single-encoded) form of a decoded path.
func CanonicalURI(path string) string {
	if path == "" {
		return "/"
	}
	return URIEncode(path, false)
}

// CanonicalQuery builds the canonical query string, skipping the named keys.
func CanonicalQuery(v url.Values, exclude ...string) string {
	skip := make(map[string]bool, len(exclude))
	for _, e := range exclude {
		skip[e] = true
	}
	type pair struct{ k, v string }
	var pairs []pair
	for k, vals := range v {
		if skip[k] {
			continue
		}
		ek := URIEncode(k, true)
		if len(vals) == 0 {
			pairs = append(pairs, pair{ek, ""})
			continue
		}
		for _, val := range vals {
			pairs = append(pairs, pair{ek, URIEncode(val, true)})
		}
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].k != pairs[j].k {
			return pairs[i].k < pairs[j].k
		}
		return pairs[i].v < pairs[j].v
	})
	parts := make([]string, len(pairs))
	for i, p := range pairs {
		parts[i] = p.k + "=" + p.v
	}
	return strings.Join(parts, "&")
}

// Scope returns the credential scope "date/region/service/aws4_request".
func Scope(date, region, service string) string {
	return date + "/" + region + "/" + service + "/aws4_request"
}

// HMAC computes HMAC-SHA256(key, data).
func HMAC(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

// SigningKey derives the SigV4 signing key.
func SigningKey(secret, date, region, service string) []byte {
	kDate := HMAC([]byte("AWS4"+secret), []byte(date))
	kRegion := HMAC(kDate, []byte(region))
	kService := HMAC(kRegion, []byte(service))
	return HMAC(kService, []byte("aws4_request"))
}

// Signature returns the lowercase hex HMAC of stringToSign.
func Signature(signingKey []byte, stringToSign string) string {
	return hex.EncodeToString(HMAC(signingKey, []byte(stringToSign)))
}

// HashHex returns the lowercase hex SHA-256 of data.
func HashHex(data string) string {
	sum := sha256.Sum256([]byte(data))
	return hex.EncodeToString(sum[:])
}

// CanonicalRequest assembles the canonical request string.
func CanonicalRequest(method, canonicalURI, canonicalQuery, canonicalHeaders, signedHeaders, payloadHash string) string {
	return strings.Join([]string{
		method, canonicalURI, canonicalQuery, canonicalHeaders, signedHeaders, payloadHash,
	}, "\n")
}

// StringToSign assembles the string to sign for a canonical request.
func StringToSign(amzDate, scope, canonicalRequest string) string {
	return Algorithm + "\n" + amzDate + "\n" + scope + "\n" + HashHex(canonicalRequest)
}

// SignRequest signs req with an Authorization header, covering host,
// x-amz-content-sha256 and x-amz-date. req.URL.Path must be the decoded path;
// callers should also set req.URL.RawPath = URIEncode(path) so the bytes on the
// wire match what was signed.
func SignRequest(req *http.Request, accessKey, secretKey, region, service string, t time.Time, payloadHash string) {
	t = t.UTC()
	date := t.Format(DateFormat)
	amzDate := t.Format(TimeFormat)

	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", payloadHash)

	host := req.URL.Host
	if host == "" {
		host = req.Host
	}

	signedHeaders := "host;x-amz-content-sha256;x-amz-date"
	canonicalHeaders := "host:" + host + "\n" +
		"x-amz-content-sha256:" + payloadHash + "\n" +
		"x-amz-date:" + amzDate + "\n"

	canonical := CanonicalRequest(
		req.Method,
		CanonicalURI(req.URL.Path),
		CanonicalQuery(req.URL.Query()),
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	)

	scope := Scope(date, region, service)
	sig := Signature(SigningKey(secretKey, date, region, service), StringToSign(amzDate, scope, canonical))

	req.Header.Set("Authorization", fmt.Sprintf(
		"%s Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		Algorithm, accessKey, scope, signedHeaders, sig,
	))
}

// PresignGET returns a presigned GET URL "scheme://host<path>?X-Amz-...".
// path is the decoded object path (e.g. "/bucket/some key.mp4").
func PresignGET(scheme, host, path, accessKey, secretKey, region, service string, t time.Time, expires time.Duration) string {
	t = t.UTC()
	date := t.Format(DateFormat)
	amzDate := t.Format(TimeFormat)
	scope := Scope(date, region, service)

	q := url.Values{}
	q.Set("X-Amz-Algorithm", Algorithm)
	q.Set("X-Amz-Credential", accessKey+"/"+scope)
	q.Set("X-Amz-Date", amzDate)
	q.Set("X-Amz-Expires", fmt.Sprintf("%d", int64(expires.Seconds())))
	q.Set("X-Amz-SignedHeaders", "host")
	canonicalQuery := CanonicalQuery(q)

	canonical := CanonicalRequest(
		http.MethodGet,
		CanonicalURI(path),
		canonicalQuery,
		"host:"+host+"\n",
		"host",
		UnsignedPayload,
	)
	sig := Signature(SigningKey(secretKey, date, region, service), StringToSign(amzDate, scope, canonical))

	return scheme + "://" + host + CanonicalURI(path) + "?" + canonicalQuery + "&X-Amz-Signature=" + sig
}
