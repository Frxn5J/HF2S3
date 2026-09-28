package s3api

import (
	"crypto/subtle"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"hf2s3/pkg/sigv4"
)

// AuthMode says how a request proved its identity.
type AuthMode int

const (
	AuthHeader    AuthMode = iota + 1 // Authorization: AWS4-HMAC-SHA256 ...
	AuthPresigned                     // ?X-Amz-Signature=...
)

const (
	maxClockSkew   = 15 * time.Minute
	maxPresignSecs = 7 * 24 * 3600
)

// AuthError carries the S3 error code/status for a failed authentication.
type AuthError struct {
	Status  int
	Code    string
	Message string
}

func (e *AuthError) Error() string { return e.Code + ": " + e.Message }

func denied(code, msg string) *AuthError {
	return &AuthError{Status: http.StatusForbidden, Code: code, Message: msg}
}

// AuthResult is the outcome of a successful authentication. The streaming
// fields are needed to verify aws-chunked bodies.
type AuthResult struct {
	Mode        AuthMode
	AccessKey   string
	AmzDate     string
	Scope       string
	SigningKey  []byte
	Signature   string // seed signature (header auth only)
	PayloadHash string // x-amz-content-sha256 (header auth only)
}

type AuthManager struct {
	mu        sync.RWMutex
	accessKey string
	secretKey string
	now       func() time.Time
}

func NewAuthManager(accessKey, secretKey string) *AuthManager {
	return &AuthManager{
		accessKey: accessKey,
		secretKey: secretKey,
		now:       time.Now,
	}
}

func (a *AuthManager) UpdateCredentials(accessKey, secretKey string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if accessKey != "" {
		a.accessKey = accessKey
	}
	if secretKey != "" {
		a.secretKey = secretKey
	}
}

// Validate reports whether the request carries a valid SigV4 signature.
func (a *AuthManager) Validate(r *http.Request) bool {
	_, err := a.Authenticate(r)
	return err == nil
}

// Authenticate verifies the SigV4 signature of r (header or presigned form).
// It fails closed: with no credentials configured every request is rejected.
func (a *AuthManager) Authenticate(r *http.Request) (*AuthResult, *AuthError) {
	a.mu.RLock()
	accessKey, secretKey := a.accessKey, a.secretKey
	a.mu.RUnlock()

	if accessKey == "" || secretKey == "" {
		return nil, denied("AccessDenied", "Access Denied: no credentials configured")
	}

	// A request may be addressed through the /media alias; clients sign either
	// the path they requested or the plain /{bucket}/{key} form.
	paths := []string{r.URL.Path}
	if strings.HasPrefix(r.URL.Path, "/media/") {
		paths = append(paths, strings.TrimPrefix(r.URL.Path, "/media"))
	}

	var lastErr *AuthError
	for _, p := range paths {
		var res *AuthResult
		var err *AuthError
		if r.URL.Query().Get("X-Amz-Algorithm") != "" {
			res, err = a.verifyPresigned(r, p, accessKey, secretKey)
		} else if strings.HasPrefix(r.Header.Get("Authorization"), sigv4.Algorithm+" ") {
			res, err = a.verifyHeader(r, p, accessKey, secretKey)
		} else {
			return nil, denied("AccessDenied", "Access Denied: missing or unsupported credentials")
		}
		if err == nil {
			return res, nil
		}
		lastErr = err
		if err.Code != "SignatureDoesNotMatch" {
			break
		}
	}
	return nil, lastErr
}

// parseCredential splits "AK/date/region/s3/aws4_request".
func parseCredential(cred string) (accessKey, date, region string, ok bool) {
	parts := strings.Split(cred, "/")
	if len(parts) != 5 || parts[3] != "s3" || parts[4] != "aws4_request" {
		return "", "", "", false
	}
	return parts[0], parts[1], parts[2], true
}

func (a *AuthManager) verifyHeader(r *http.Request, path, accessKey, secretKey string) (*AuthResult, *AuthError) {
	fields := parseAuthFields(strings.TrimPrefix(r.Header.Get("Authorization"), sigv4.Algorithm))
	gotKey, date, region, ok := parseCredential(fields["Credential"])
	signedHeadersStr, gotSig := fields["SignedHeaders"], fields["Signature"]
	if !ok || signedHeadersStr == "" || gotSig == "" {
		return nil, &AuthError{Status: http.StatusBadRequest, Code: "AuthorizationHeaderMalformed", Message: "The authorization header is malformed."}
	}
	if subtle.ConstantTimeCompare([]byte(gotKey), []byte(accessKey)) != 1 {
		return nil, denied("InvalidAccessKeyId", "The AWS Access Key Id you provided does not exist in our records.")
	}

	amzDate := r.Header.Get("X-Amz-Date")
	if amzDate == "" {
		if t, err := http.ParseTime(r.Header.Get("Date")); err == nil {
			amzDate = t.UTC().Format(sigv4.TimeFormat)
		}
	}
	reqTime, err := time.Parse(sigv4.TimeFormat, amzDate)
	if err != nil || !strings.HasPrefix(amzDate, date) {
		return nil, &AuthError{Status: http.StatusBadRequest, Code: "AuthorizationHeaderMalformed", Message: "Missing or invalid x-amz-date."}
	}
	if d := a.now().Sub(reqTime); d > maxClockSkew || d < -maxClockSkew {
		return nil, denied("RequestTimeTooSkewed", "The difference between the request time and the current time is too large.")
	}

	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		return nil, &AuthError{Status: http.StatusBadRequest, Code: "InvalidRequest", Message: "Missing required header for this request: x-amz-content-sha256"}
	}

	signedHeaders := strings.Split(signedHeadersStr, ";")
	canonHeaders, ok := canonicalHeaders(r, signedHeaders)
	if !ok {
		return nil, denied("SignatureDoesNotMatch", "The request signature we calculated does not match the signature you provided.")
	}

	canonical := sigv4.CanonicalRequest(
		r.Method,
		sigv4.CanonicalURI(path),
		sigv4.CanonicalQuery(r.URL.Query()),
		canonHeaders,
		signedHeadersStr,
		payloadHash,
	)
	scope := sigv4.Scope(date, region, "s3")
	key := sigv4.SigningKey(secretKey, date, region, "s3")
	want := sigv4.Signature(key, sigv4.StringToSign(amzDate, scope, canonical))
	if subtle.ConstantTimeCompare([]byte(want), []byte(gotSig)) != 1 {
		return nil, denied("SignatureDoesNotMatch", "The request signature we calculated does not match the signature you provided.")
	}

	return &AuthResult{
		Mode:        AuthHeader,
		AccessKey:   gotKey,
		AmzDate:     amzDate,
		Scope:       scope,
		SigningKey:  key,
		Signature:   gotSig,
		PayloadHash: payloadHash,
	}, nil
}

func (a *AuthManager) verifyPresigned(r *http.Request, path, accessKey, secretKey string) (*AuthResult, *AuthError) {
	q := r.URL.Query()
	if q.Get("X-Amz-Algorithm") != sigv4.Algorithm {
		return nil, &AuthError{Status: http.StatusBadRequest, Code: "AuthorizationQueryParametersError", Message: "X-Amz-Algorithm only supports \"AWS4-HMAC-SHA256\"."}
	}
	gotKey, date, region, ok := parseCredential(q.Get("X-Amz-Credential"))
	signedHeadersStr, gotSig, amzDate := q.Get("X-Amz-SignedHeaders"), q.Get("X-Amz-Signature"), q.Get("X-Amz-Date")
	if !ok || signedHeadersStr == "" || gotSig == "" {
		return nil, &AuthError{Status: http.StatusBadRequest, Code: "AuthorizationQueryParametersError", Message: "Error parsing the X-Amz-Credential parameter."}
	}
	if subtle.ConstantTimeCompare([]byte(gotKey), []byte(accessKey)) != 1 {
		return nil, denied("InvalidAccessKeyId", "The AWS Access Key Id you provided does not exist in our records.")
	}

	reqTime, err := time.Parse(sigv4.TimeFormat, amzDate)
	if err != nil || !strings.HasPrefix(amzDate, date) {
		return nil, &AuthError{Status: http.StatusBadRequest, Code: "AuthorizationQueryParametersError", Message: "Invalid X-Amz-Date."}
	}
	expires, err := strconv.ParseInt(q.Get("X-Amz-Expires"), 10, 64)
	if err != nil || expires < 1 || expires > maxPresignSecs {
		return nil, &AuthError{Status: http.StatusBadRequest, Code: "AuthorizationQueryParametersError", Message: "X-Amz-Expires must be between 1 and 604800 seconds."}
	}
	now := a.now()
	if now.Before(reqTime.Add(-maxClockSkew)) {
		return nil, denied("AccessDenied", "Request is not yet valid.")
	}
	if now.After(reqTime.Add(time.Duration(expires) * time.Second)) {
		return nil, denied("AccessDenied", "Request has expired.")
	}

	signedHeaders := strings.Split(signedHeadersStr, ";")
	canonHeaders, ok := canonicalHeaders(r, signedHeaders)
	if !ok {
		return nil, denied("SignatureDoesNotMatch", "The request signature we calculated does not match the signature you provided.")
	}

	payloadHash := q.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		payloadHash = sigv4.UnsignedPayload
	}

	canonical := sigv4.CanonicalRequest(
		r.Method,
		sigv4.CanonicalURI(path),
		sigv4.CanonicalQuery(q, "X-Amz-Signature"),
		canonHeaders,
		signedHeadersStr,
		payloadHash,
	)
	scope := sigv4.Scope(date, region, "s3")
	key := sigv4.SigningKey(secretKey, date, region, "s3")
	want := sigv4.Signature(key, sigv4.StringToSign(amzDate, scope, canonical))
	if subtle.ConstantTimeCompare([]byte(want), []byte(gotSig)) != 1 {
		return nil, denied("SignatureDoesNotMatch", "The request signature we calculated does not match the signature you provided.")
	}

	return &AuthResult{
		Mode:        AuthPresigned,
		AccessKey:   gotKey,
		AmzDate:     amzDate,
		Scope:       scope,
		SigningKey:  key,
		Signature:   gotSig,
		PayloadHash: payloadHash,
	}, nil
}

// parseAuthFields parses "Credential=..., SignedHeaders=..., Signature=...".
func parseAuthFields(s string) map[string]string {
	out := make(map[string]string, 3)
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if i := strings.IndexByte(part, '='); i > 0 {
			out[part[:i]] = part[i+1:]
		}
	}
	return out
}

// canonicalHeaders renders the signed headers of r. It reports false when a
// signed header (other than host) is absent, in which case the signature
// cannot possibly match.
func canonicalHeaders(r *http.Request, signed []string) (string, bool) {
	hasHost := false
	var b strings.Builder
	for _, name := range signed {
		name = strings.ToLower(strings.TrimSpace(name))
		var val string
		switch name {
		case "host":
			hasHost = true
			val = r.Host
		case "content-length":
			if v := r.Header.Get("Content-Length"); v != "" {
				val = v
			} else if r.ContentLength >= 0 {
				val = strconv.FormatInt(r.ContentLength, 10)
			}
		default:
			values := r.Header.Values(name)
			if len(values) == 0 {
				return "", false
			}
			trimmed := make([]string, len(values))
			for i, v := range values {
				trimmed[i] = collapseSpaces(v)
			}
			val = strings.Join(trimmed, ",")
		}
		b.WriteString(name)
		b.WriteByte(':')
		b.WriteString(collapseSpaces(val))
		b.WriteByte('\n')
	}
	return b.String(), hasHost
}

// collapseSpaces trims and folds runs of spaces, as SigV4 header canonicalisation requires.
func collapseSpaces(s string) string {
	return strings.Join(strings.Fields(s), " ")
}
