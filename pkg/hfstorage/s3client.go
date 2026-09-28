package hfstorage

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

var (
	ErrObjectNotFound = errors.New("object not found in storage bucket")
	ErrStorageConfig  = errors.New("storage bucket configuration incomplete")
)

type S3Client struct {
	endpoint   string
	region     string
	accessKey  string
	secretKey  string
	bucket     string
	namespace  string
	httpClient *http.Client
}

type S3ClientConfig struct {
	Endpoint   string
	Region     string
	AccessKey  string
	SecretKey  string
	Bucket     string
	Namespace  string
	HTTPClient *http.Client
}

func NewS3Client(cfg S3ClientConfig) *S3Client {
	endpoint := strings.TrimRight(cfg.Endpoint, "/")
	if endpoint == "" {
		endpoint = "https://s3.hf.co"
	}
	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{
			Timeout: 60 * time.Second,
		}
	}

	return &S3Client{
		endpoint:   endpoint,
		region:     region,
		accessKey:  cfg.AccessKey,
		secretKey:  cfg.SecretKey,
		bucket:     cfg.Bucket,
		namespace:  cfg.Namespace,
		httpClient: client,
	}
}

func (c *S3Client) IsConfigured() bool {
	return c.accessKey != "" && c.secretKey != "" && c.bucket != ""
}

func (c *S3Client) Bucket() string {
	return c.bucket
}

func (c *S3Client) Endpoint() string {
	return c.endpoint
}

// buildURLPath constructs the URL path respecting namespace and bucket
func (c *S3Client) buildURLPath(bucket, key string) string {
	cleanKey := strings.TrimPrefix(key, "/")
	targetBucket := bucket
	if targetBucket == "" {
		targetBucket = c.bucket
	}

	if c.namespace != "" && !strings.Contains(c.endpoint, c.namespace) {
		return fmt.Sprintf("/%s/%s/%s", c.namespace, targetBucket, cleanKey)
	}
	return fmt.Sprintf("/%s/%s", targetBucket, cleanKey)
}

// PresignGetObject generates an AWS SigV4 presigned GET URL for direct client download.
func (c *S3Client) PresignGetObject(bucket, key string, expires time.Duration) (string, error) {
	if !c.IsConfigured() {
		return "", ErrStorageConfig
	}
	if expires <= 0 {
		expires = 15 * time.Minute
	}

	targetBucket := bucket
	if targetBucket == "" {
		targetBucket = c.bucket
	}

	parsedEndpoint, err := url.Parse(c.endpoint)
	if err != nil {
		return "", fmt.Errorf("parse endpoint: %w", err)
	}

	reqPath := c.buildURLPath(targetBucket, key)
	now := time.Now().UTC()
	dateStamp := now.Format("20060102")
	amzDate := now.Format("20060102T150405Z")
	credentialScope := fmt.Sprintf("%s/%s/s3/aws4_request", dateStamp, c.region)
	credential := fmt.Sprintf("%s/%s", c.accessKey, credentialScope)
	expiresSec := int64(expires.Seconds())

	queryParams := url.Values{}
	queryParams.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	queryParams.Set("X-Amz-Credential", credential)
	queryParams.Set("X-Amz-Date", amzDate)
	queryParams.Set("X-Amz-Expires", fmt.Sprintf("%d", expiresSec))
	queryParams.Set("X-Amz-SignedHeaders", "host")

	canonicalQuery := buildCanonicalQueryString(queryParams)

	host := parsedEndpoint.Host
	canonicalHeaders := fmt.Sprintf("host:%s\n", host)
	signedHeaders := "host"
	payloadHash := "UNSIGNED-PAYLOAD"

	canonicalURI := escapePath(reqPath)
	canonicalRequest := fmt.Sprintf("GET\n%s\n%s\n%s\n%s\n%s",
		canonicalURI,
		canonicalQuery,
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	)

	crHash := sha256.Sum256([]byte(canonicalRequest))
	crHashHex := hex.EncodeToString(crHash[:])

	stringToSign := fmt.Sprintf("AWS4-HMAC-SHA256\n%s\n%s\n%s",
		amzDate,
		credentialScope,
		crHashHex,
	)

	signingKey := deriveSigningKey(c.secretKey, dateStamp, c.region, "s3")
	signature := hmacHex(signingKey, []byte(stringToSign))

	queryParams.Set("X-Amz-Signature", signature)

	presignedURL := fmt.Sprintf("%s://%s%s?%s",
		parsedEndpoint.Scheme,
		host,
		reqPath,
		queryParams.Encode(),
	)

	return presignedURL, nil
}

// PutObject uploads an unencrypted object to the Hugging Face Storage Bucket.
func (c *S3Client) PutObject(ctx context.Context, bucket, key string, body io.Reader, size int64, contentType string) error {
	if !c.IsConfigured() {
		return ErrStorageConfig
	}

	targetBucket := bucket
	if targetBucket == "" {
		targetBucket = c.bucket
	}

	parsedEndpoint, err := url.Parse(c.endpoint)
	if err != nil {
		return fmt.Errorf("parse endpoint: %w", err)
	}

	reqPath := c.buildURLPath(targetBucket, key)
	fullURL := fmt.Sprintf("%s://%s%s", parsedEndpoint.Scheme, parsedEndpoint.Host, reqPath)

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, fullURL, body)
	if err != nil {
		return fmt.Errorf("create put request: %w", err)
	}

	if size > 0 {
		req.ContentLength = size
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	req.Header.Set("Content-Type", contentType)

	now := time.Now().UTC()
	c.signRequest(req, now, "UNSIGNED-PAYLOAD")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("storage bucket put: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("storage bucket put failed (status %d): %s", resp.StatusCode, string(respBody))
	}

	return nil
}

// DeleteObject deletes an object from the Hugging Face Storage Bucket (used for cache eviction).
func (c *S3Client) DeleteObject(ctx context.Context, bucket, key string) error {
	if !c.IsConfigured() {
		return ErrStorageConfig
	}

	targetBucket := bucket
	if targetBucket == "" {
		targetBucket = c.bucket
	}

	parsedEndpoint, err := url.Parse(c.endpoint)
	if err != nil {
		return fmt.Errorf("parse endpoint: %w", err)
	}

	reqPath := c.buildURLPath(targetBucket, key)
	fullURL := fmt.Sprintf("%s://%s%s", parsedEndpoint.Scheme, parsedEndpoint.Host, reqPath)

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, fullURL, nil)
	if err != nil {
		return fmt.Errorf("create delete request: %w", err)
	}

	now := time.Now().UTC()
	c.signRequest(req, now, "UNSIGNED-PAYLOAD")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("storage bucket delete: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusOK {
		return nil
	}

	respBody, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("storage bucket delete failed (status %d): %s", resp.StatusCode, string(respBody))
}

// GetObject downloads an object from the Hugging Face Storage Bucket (fallback or verification).
func (c *S3Client) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, int64, string, error) {
	if !c.IsConfigured() {
		return nil, 0, "", ErrStorageConfig
	}

	targetBucket := bucket
	if targetBucket == "" {
		targetBucket = c.bucket
	}

	parsedEndpoint, err := url.Parse(c.endpoint)
	if err != nil {
		return nil, 0, "", fmt.Errorf("parse endpoint: %w", err)
	}

	reqPath := c.buildURLPath(targetBucket, key)
	fullURL := fmt.Sprintf("%s://%s%s", parsedEndpoint.Scheme, parsedEndpoint.Host, reqPath)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
	if err != nil {
		return nil, 0, "", fmt.Errorf("create get request: %w", err)
	}

	now := time.Now().UTC()
	c.signRequest(req, now, "UNSIGNED-PAYLOAD")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, "", fmt.Errorf("storage bucket get: %w", err)
	}

	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, 0, "", ErrObjectNotFound
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, 0, "", fmt.Errorf("storage bucket get failed (status %d): %s", resp.StatusCode, string(body))
	}

	return resp.Body, resp.ContentLength, resp.Header.Get("Content-Type"), nil
}

// signRequest applies AWS SigV4 Authorization header to an HTTP request
func (c *S3Client) signRequest(req *http.Request, t time.Time, payloadHash string) {
	dateStamp := t.Format("20060102")
	amzDate := t.Format("20060102T150405Z")

	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", payloadHash)

	host := req.URL.Host
	if host == "" {
		host = req.Host
	}

	headersToSign := []string{"host", "x-amz-content-sha256", "x-amz-date"}
	sort.Strings(headersToSign)

	var canonicalHeaders strings.Builder
	for _, h := range headersToSign {
		var val string
		switch h {
		case "host":
			val = host
		default:
			val = req.Header.Get(h)
		}
		canonicalHeaders.WriteString(fmt.Sprintf("%s:%s\n", h, strings.TrimSpace(val)))
	}
	signedHeaders := strings.Join(headersToSign, ";")

	canonicalQuery := buildCanonicalQueryString(req.URL.Query())
	canonicalURI := escapePath(req.URL.Path)

	canonicalRequest := fmt.Sprintf("%s\n%s\n%s\n%s\n%s\n%s",
		req.Method,
		canonicalURI,
		canonicalQuery,
		canonicalHeaders.String(),
		signedHeaders,
		payloadHash,
	)

	crHash := sha256.Sum256([]byte(canonicalRequest))
	crHashHex := hex.EncodeToString(crHash[:])

	credentialScope := fmt.Sprintf("%s/%s/s3/aws4_request", dateStamp, c.region)
	stringToSign := fmt.Sprintf("AWS4-HMAC-SHA256\n%s\n%s\n%s",
		amzDate,
		credentialScope,
		crHashHex,
	)

	signingKey := deriveSigningKey(c.secretKey, dateStamp, c.region, "s3")
	signature := hmacHex(signingKey, []byte(stringToSign))

	authHeader := fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		c.accessKey,
		credentialScope,
		signedHeaders,
		signature,
	)

	req.Header.Set("Authorization", authHeader)
}

func buildCanonicalQueryString(v url.Values) string {
	if len(v) == 0 {
		return ""
	}
	keys := make([]string, 0, len(v))
	for k := range v {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var pairs []string
	for _, k := range keys {
		escapedKey := url.QueryEscape(k)
		for _, val := range v[k] {
			escapedVal := url.QueryEscape(val)
			pairs = append(pairs, fmt.Sprintf("%s=%s", escapedKey, escapedVal))
		}
	}
	return strings.Join(pairs, "&")
}

func escapePath(path string) string {
	var segments []string
	for _, seg := range strings.Split(path, "/") {
		segments = append(segments, url.PathEscape(seg))
	}
	return strings.Join(segments, "/")
}

func deriveSigningKey(secret, dateStamp, region, service string) []byte {
	kDate := hmacSha256([]byte("AWS4"+secret), []byte(dateStamp))
	kRegion := hmacSha256(kDate, []byte(region))
	kService := hmacSha256(kRegion, []byte(service))
	kSigning := hmacSha256(kService, []byte("aws4_request"))
	return kSigning
}

func hmacSha256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

func hmacHex(key, data []byte) string {
	return hex.EncodeToString(hmacSha256(key, data))
}
