package hfstorage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"hf2s3/pkg/sigv4"
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

// defaultHTTPClient has no overall Timeout on purpose: cache promotions and
// downloads stream multi-GB bodies, and http.Client.Timeout would abort them
// mid-transfer. Connection and header timeouts still bound stalled peers.
func defaultHTTPClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   15 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			TLSHandshakeTimeout:   15 * time.Second,
			ResponseHeaderTimeout: 60 * time.Second,
			IdleConnTimeout:       90 * time.Second,
			MaxIdleConnsPerHost:   16,
		},
	}
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
		client = defaultHTTPClient()
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

// buildURLPath constructs the (decoded) URL path respecting namespace and bucket.
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

// newRequest builds a request whose wire path is exactly the AWS-encoded form
// of the signed path, so keys containing spaces, '+', '?', '#' or '%' work.
func (c *S3Client) newRequest(ctx context.Context, method, bucket, key string, body io.Reader) (*http.Request, error) {
	parsedEndpoint, err := url.Parse(c.endpoint)
	if err != nil {
		return nil, fmt.Errorf("parse endpoint: %w", err)
	}
	reqPath := c.buildURLPath(bucket, key)

	req, err := http.NewRequestWithContext(ctx, method, c.endpoint, body)
	if err != nil {
		return nil, err
	}
	req.URL = &url.URL{
		Scheme:  parsedEndpoint.Scheme,
		Host:    parsedEndpoint.Host,
		Path:    reqPath,
		RawPath: sigv4.URIEncode(reqPath, false),
	}
	req.Host = parsedEndpoint.Host
	return req, nil
}

// PresignGetObject generates an AWS SigV4 presigned GET URL for direct client download.
func (c *S3Client) PresignGetObject(bucket, key string, expires time.Duration) (string, error) {
	if !c.IsConfigured() {
		return "", ErrStorageConfig
	}
	if expires <= 0 {
		expires = 15 * time.Minute
	}

	parsedEndpoint, err := url.Parse(c.endpoint)
	if err != nil {
		return "", fmt.Errorf("parse endpoint: %w", err)
	}

	return sigv4.PresignGET(
		parsedEndpoint.Scheme,
		parsedEndpoint.Host,
		c.buildURLPath(bucket, key),
		c.accessKey, c.secretKey, c.region, "s3",
		time.Now(), expires,
	), nil
}

// PutObject uploads an unencrypted object to the Hugging Face Storage Bucket.
// body is streamed (never buffered); size must be the exact body length.
func (c *S3Client) PutObject(ctx context.Context, bucket, key string, body io.Reader, size int64, contentType string) error {
	if !c.IsConfigured() {
		return ErrStorageConfig
	}

	req, err := c.newRequest(ctx, http.MethodPut, bucket, key, body)
	if err != nil {
		return fmt.Errorf("create put request: %w", err)
	}

	req.ContentLength = size
	if size == 0 {
		req.Body = http.NoBody
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	req.Header.Set("Content-Type", contentType)

	c.signRequest(req, time.Now().UTC(), sigv4.UnsignedPayload)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("storage bucket put: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("storage bucket put failed (status %d): %s", resp.StatusCode, string(respBody))
	}

	return nil
}

// DeleteObject deletes an object from the Hugging Face Storage Bucket (used for cache eviction).
func (c *S3Client) DeleteObject(ctx context.Context, bucket, key string) error {
	if !c.IsConfigured() {
		return ErrStorageConfig
	}

	req, err := c.newRequest(ctx, http.MethodDelete, bucket, key, nil)
	if err != nil {
		return fmt.Errorf("create delete request: %w", err)
	}

	c.signRequest(req, time.Now().UTC(), sigv4.UnsignedPayload)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("storage bucket delete: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusOK {
		return nil
	}

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return fmt.Errorf("storage bucket delete failed (status %d): %s", resp.StatusCode, string(respBody))
}

// GetObject downloads an object from the Hugging Face Storage Bucket (fallback or verification).
func (c *S3Client) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, int64, string, error) {
	if !c.IsConfigured() {
		return nil, 0, "", ErrStorageConfig
	}

	req, err := c.newRequest(ctx, http.MethodGet, bucket, key, nil)
	if err != nil {
		return nil, 0, "", fmt.Errorf("create get request: %w", err)
	}

	c.signRequest(req, time.Now().UTC(), sigv4.UnsignedPayload)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, "", fmt.Errorf("storage bucket get: %w", err)
	}

	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, 0, "", ErrObjectNotFound
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		return nil, 0, "", fmt.Errorf("storage bucket get failed (status %d): %s", resp.StatusCode, string(body))
	}

	return resp.Body, resp.ContentLength, resp.Header.Get("Content-Type"), nil
}

// signRequest applies an AWS SigV4 Authorization header to an HTTP request.
func (c *S3Client) signRequest(req *http.Request, t time.Time, payloadHash string) {
	sigv4.SignRequest(req, c.accessKey, c.secretKey, c.region, "s3", t, payloadHash)
}

// ErrRangeNotHonored means the server answered a ranged GET with the whole object.
var ErrRangeNotHonored = errors.New("storage bucket ignored the Range header")

// GetObjectRange downloads bytes [start, end] (inclusive; end < 0 means "to the
// end") of an object. The Range header is not part of the signature.
func (c *S3Client) GetObjectRange(ctx context.Context, bucket, key string, start, end int64) (io.ReadCloser, error) {
	if !c.IsConfigured() {
		return nil, ErrStorageConfig
	}

	req, err := c.newRequest(ctx, http.MethodGet, bucket, key, nil)
	if err != nil {
		return nil, fmt.Errorf("create ranged get request: %w", err)
	}
	if end >= 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	} else {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", start))
	}

	c.signRequest(req, time.Now().UTC(), sigv4.UnsignedPayload)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("storage bucket ranged get: %w", err)
	}

	switch resp.StatusCode {
	case http.StatusPartialContent:
		return resp.Body, nil
	case http.StatusNotFound:
		resp.Body.Close()
		return nil, ErrObjectNotFound
	case http.StatusOK:
		resp.Body.Close()
		return nil, ErrRangeNotHonored
	default:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		return nil, fmt.Errorf("storage bucket ranged get failed (status %d): %s", resp.StatusCode, string(body))
	}
}
