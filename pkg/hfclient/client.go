package hfclient

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"time"
)

type ClientOption func(*Client)

type Client struct {
	baseURL     string
	httpClient  *http.Client
	rateLimiter *TokenRateLimiter
}

func defaultTransport() *http.Transport {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          1000,
		MaxIdleConnsPerHost:   200,
		MaxConnsPerHost:       0, // Unlimited concurrent connections
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   15 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: 60 * time.Second,
	}
}

func WithBaseURL(url string) ClientOption {
	return func(c *Client) {
		c.baseURL = strings.TrimRight(url, "/")
	}
}

func WithHTTPClient(httpClient *http.Client) ClientOption {
	return func(c *Client) {
		c.httpClient = httpClient
	}
}

func WithRateLimiter(rateLimiter *TokenRateLimiter) ClientOption {
	return func(c *Client) {
		c.rateLimiter = rateLimiter
	}
}

func NewClient(opts ...ClientOption) *Client {
	c := &Client{
		baseURL: "https://huggingface.co",
		httpClient: &http.Client{
			Transport: defaultTransport(),
			Timeout:   10 * time.Minute, // Large chunk uploads may require time
		},
		rateLimiter: NewTokenRateLimiter(),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func (c *Client) RateLimiter() *TokenRateLimiter {
	return c.rateLimiter
}

func (c *Client) IsThrottled(token string) (bool, time.Duration) {
	if c.rateLimiter == nil {
		return false, 0
	}
	return c.rateLimiter.IsThrottled(token)
}

func (c *Client) SetCooldown(token string, d time.Duration) {
	if c.rateLimiter != nil {
		c.rateLimiter.SetCooldown(token, d)
	}
}

func (c *Client) GetRateLimitStats(token string) RateLimitStats {
	if c.rateLimiter == nil {
		return RateLimitStats{APIRemaining: 1000, ResolversRemaining: 5000}
	}
	return c.rateLimiter.GetStats(token)
}

// doRequestWithRetry executes an HTTP request with exponential backoff and jitter for transient errors (429, 502, 503, 504).
func (c *Client) doRequestWithRetry(ctx context.Context, makeReq func() (*http.Request, error), maxRetries int) (*http.Response, error) {
	var resp *http.Response
	var lastErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(150*(1<<attempt))*time.Millisecond + time.Duration(rand.Intn(100))*time.Millisecond
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		req, err := makeReq()
		if err != nil {
			return nil, err
		}

		if req.Header.Get("User-Agent") == "" {
			req.Header.Set("User-Agent", UserAgent)
		}

		// Extract Bearer token if present to track rate limits per account
		token := ""
		if auth := req.Header.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
			token = strings.TrimPrefix(auth, "Bearer ")
		}

		resp, err = c.httpClient.Do(req)
		if err != nil {
			lastErr = err
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			continue
		}

		// Record telemetry and rate limit headers for this account
		info := c.rateLimiter.RecordResponse(token, resp)

		if resp.StatusCode == http.StatusTooManyRequests {
			_ = resp.Body.Close()
			var resetIn time.Duration = 15 * time.Second
			var resetAt time.Time = time.Now().Add(resetIn)
			bucket := BucketAPI
			remaining := 0
			if info != nil {
				if info.ResetIn > 0 {
					resetIn = info.ResetIn
				}
				if !info.ResetAt.IsZero() {
					resetAt = info.ResetAt
				}
				if info.Bucket != "" {
					bucket = info.Bucket
				}
				remaining = info.Remaining
			}

			// If reset is very short (<= 2 seconds) and retries remain, back off briefly
			if resetIn <= 2*time.Second && attempt < maxRetries {
				select {
				case <-time.After(resetIn):
					continue
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}

			// Return RateLimitError so pool can immediately failover to another healthy account
			return nil, &RateLimitError{
				StatusCode: http.StatusTooManyRequests,
				Bucket:     bucket,
				Remaining:  remaining,
				RetryAfter: resetIn,
				ResetAt:    resetAt,
				Message:    fmt.Sprintf("rate limit tier reached for Hugging Face (%s)", bucket),
			}
		}

		if resp.StatusCode == http.StatusBadGateway ||
			resp.StatusCode == http.StatusServiceUnavailable ||
			resp.StatusCode == http.StatusGatewayTimeout {
			_ = resp.Body.Close()
			lastErr = fmt.Errorf("transient status code: %d", resp.StatusCode)
			continue
		}

		return resp, nil
	}

	return nil, fmt.Errorf("request failed after %d retries: %w", maxRetries, lastErr)
}

type WhoamiResponse struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Email    string `json:"email"`
	Fullname string `json:"fullname"`
	Avatar   string `json:"avatarUrl"`
}

func (c *Client) VerifyToken(ctx context.Context, token string) (*WhoamiResponse, error) {
	resp, err := c.doRequestWithRetry(ctx, func() (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/api/whoami-v2", nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		return req, nil
	}, 3)
	if err != nil {
		return nil, fmt.Errorf("whoami request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("verify token status %d: %s", resp.StatusCode, string(body))
	}

	var whoami WhoamiResponse
	if err := json.NewDecoder(resp.Body).Decode(&whoami); err != nil {
		return nil, fmt.Errorf("decode whoami response: %w", err)
	}
	return &whoami, nil
}

func (c *Client) EnsureDatasetRepo(ctx context.Context, token, repoName string) error {
	return c.EnsureDatasetRepoWithVisibility(ctx, token, repoName, false) // Default to public dataset as requested
}

func (c *Client) EnsureDatasetRepoWithVisibility(ctx context.Context, token, repoName string, isPrivate bool) error {
	payload := map[string]interface{}{
		"name":    repoName,
		"type":    "dataset",
		"private": isPrivate,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	resp, err := c.doRequestWithRetry(ctx, func() (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/repos/create", bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		req.Header.Set("Content-Type", "application/json")
		return req, nil
	}, 3)
	if err != nil {
		return fmt.Errorf("create repo request: %w", err)
	}
	defer resp.Body.Close()

	// 200/201: Created, 409: Already exists
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusConflict {
		return nil
	}

	body, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("ensure dataset repo status %d: %s", resp.StatusCode, string(body))
}


// Git LFS Batch Protocol Models
type LfsBatchRequest struct {
	Operation string         `json:"operation"`
	Transfers []string       `json:"transfers"`
	Objects   []LfsObjectReq `json:"objects"`
	HashAlgo  string         `json:"hash_algo,omitempty"`
}

type LfsObjectReq struct {
	Oid  string `json:"oid"`
	Size int64  `json:"size"`
}

type LfsBatchResponse struct {
	Transfer string          `json:"transfer"`
	Objects  []LfsObjectResp `json:"objects"`
}

type LfsObjectResp struct {
	Oid     string               `json:"oid"`
	Size    int64                `json:"size"`
	Actions map[string]LfsAction `json:"actions"`
	Error   *LfsError            `json:"error,omitempty"`
}

type LfsAction struct {
	Href   string            `json:"href"`
	Header map[string]string `json:"header,omitempty"`
}

type LfsError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type CommitFile struct {
	Path     string `json:"path"`
	Content  string `json:"content"`
	Encoding string `json:"encoding"`
}

type CommitLfsFile struct {
	Path string `json:"path"`
	Oid  string `json:"oid"`
	Size int64  `json:"size"`
}

type CommitDeletedEntry struct {
	Path string `json:"path"`
}

type CommitPayload struct {
	Summary        string               `json:"summary"`
	Files          []CommitFile         `json:"files,omitempty"`
	LfsFiles       []CommitLfsFile      `json:"lfsFiles,omitempty"`
	DeletedEntries []CommitDeletedEntry `json:"deletedEntries,omitempty"`
}

func (c *Client) UploadChunk(ctx context.Context, token, repoID, remotePath string, chunkData []byte) error {
	// SHA256 hex digest for Git LFS object ID
	hash := sha256.Sum256(chunkData)
	oid := hex.EncodeToString(hash[:])
	size := int64(len(chunkData))

	// Git LFS batch upload negotiation
	batchURL := fmt.Sprintf("%s/datasets/%s.git/info/lfs/objects/batch", c.baseURL, repoID)
	batchReqBody := LfsBatchRequest{
		Operation: "upload",
		Transfers: []string{"basic"},
		Objects: []LfsObjectReq{
			{Oid: oid, Size: size},
		},
		HashAlgo: "sha256",
	}

	batchBytes, err := json.Marshal(batchReqBody)
	if err != nil {
		return fmt.Errorf("marshal lfs batch request: %w", err)
	}

	bResp, err := c.doRequestWithRetry(ctx, func() (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, batchURL, bytes.NewReader(batchBytes))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Accept", "application/vnd.git-lfs+json")
		req.Header.Set("Content-Type", "application/vnd.git-lfs+json")
		return req, nil
	}, 3)
	if err != nil {
		return fmt.Errorf("lfs batch request to %s: %w", batchURL, err)
	}
	defer bResp.Body.Close()

	if bResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(bResp.Body)
		return fmt.Errorf("lfs batch status %d: %s", bResp.StatusCode, string(body))
	}

	var batchResp LfsBatchResponse
	if err := json.NewDecoder(bResp.Body).Decode(&batchResp); err != nil {
		return fmt.Errorf("decode lfs batch response: %w", err)
	}

	if len(batchResp.Objects) == 0 {
		return errors.New("empty objects list in lfs batch response")
	}

	lfsObj := batchResp.Objects[0]
	if lfsObj.Error != nil {
		return fmt.Errorf("lfs batch object error: %s (code %d)", lfsObj.Error.Message, lfsObj.Error.Code)
	}

	// Upload binary payload to negotiated LFS storage endpoint
	if uploadAction, exists := lfsObj.Actions["upload"]; exists && uploadAction.Href != "" {
		putResp, err := c.doRequestWithRetry(ctx, func() (*http.Request, error) {
			req, err := http.NewRequestWithContext(ctx, http.MethodPut, uploadAction.Href, bytes.NewReader(chunkData))
			if err != nil {
				return nil, fmt.Errorf("create lfs upload request: %w", err)
			}
			for k, v := range uploadAction.Header {
				req.Header.Set(k, v)
			}
			return req, nil
		}, 3)
		if err != nil {
			return fmt.Errorf("lfs upload PUT request: %w", err)
		}
		defer putResp.Body.Close()

		if putResp.StatusCode < 200 || putResp.StatusCode >= 300 {
			body, _ := io.ReadAll(putResp.Body)
			return fmt.Errorf("lfs upload storage status %d: %s", putResp.StatusCode, string(body))
		}
	}

	// Verify upload status with LFS server if action exists
	if verifyAction, exists := lfsObj.Actions["verify"]; exists && verifyAction.Href != "" {
		verifyPayload := map[string]interface{}{
			"oid":  oid,
			"size": size,
		}
		verifyBytes, _ := json.Marshal(verifyPayload)

		vResp, err := c.doRequestWithRetry(ctx, func() (*http.Request, error) {
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, verifyAction.Href, bytes.NewReader(verifyBytes))
			if err != nil {
				return nil, fmt.Errorf("create lfs verify request: %w", err)
			}
			req.Header.Set("Authorization", "Bearer "+token)
			req.Header.Set("Accept", "application/vnd.git-lfs+json")
			req.Header.Set("Content-Type", "application/vnd.git-lfs+json")
			for k, v := range verifyAction.Header {
				req.Header.Set(k, v)
			}
			return req, nil
		}, 3)
		if err != nil {
			return fmt.Errorf("lfs verify request: %w", err)
		}
		defer vResp.Body.Close()

		if vResp.StatusCode < 200 || vResp.StatusCode >= 300 {
			body, _ := io.ReadAll(vResp.Body)
			return fmt.Errorf("lfs verify status %d: %s", vResp.StatusCode, string(body))
		}
	}

	// Commit LFS pointer metadata to dataset repo
	commitPayload := CommitPayload{
		Summary: fmt.Sprintf("Upload chunk %s", remotePath),
		LfsFiles: []CommitLfsFile{
			{
				Path: remotePath,
				Oid:  oid,
				Size: size,
			},
		},
	}

	commitBytes, err := json.Marshal(commitPayload)
	if err != nil {
		return fmt.Errorf("marshal commit payload: %w", err)
	}

	// Pace repository commits to avoid triggering Hugging Face burst rate limits
	if err := c.rateLimiter.WaitCommit(ctx, token); err != nil {
		return fmt.Errorf("commit pacer: %w", err)
	}

	commitURL := fmt.Sprintf("%s/api/datasets/%s/commit/main", c.baseURL, repoID)
	cResp, err := c.doRequestWithRetry(ctx, func() (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, commitURL, bytes.NewReader(commitBytes))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		return req, nil
	}, 3)
	if err != nil {
		return fmt.Errorf("commit request: %w", err)
	}
	defer cResp.Body.Close()

	if cResp.StatusCode != http.StatusOK && cResp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(cResp.Body)
		return fmt.Errorf("upload chunk commit status %d: %s", cResp.StatusCode, string(body))
	}

	return nil
}

func (c *Client) DownloadChunk(ctx context.Context, token, repoID, remotePath string) ([]byte, error) {
	url := fmt.Sprintf("%s/datasets/%s/resolve/main/%s", c.baseURL, repoID, remotePath)
	resp, err := c.doRequestWithRetry(ctx, func() (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		return req, nil
	}, 3)
	if err != nil {
		return nil, fmt.Errorf("download chunk request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("download chunk status %d: %s", resp.StatusCode, string(body))
	}

	return io.ReadAll(resp.Body)
}

func (c *Client) DeleteChunks(ctx context.Context, token, repoID string, remotePaths []string) error {
	if len(remotePaths) == 0 {
		return nil
	}

	entries := make([]CommitDeletedEntry, len(remotePaths))
	for i, path := range remotePaths {
		entries[i] = CommitDeletedEntry{Path: path}
	}

	payload := CommitPayload{
		Summary:        fmt.Sprintf("Delete %d chunks", len(remotePaths)),
		DeletedEntries: entries,
	}

	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	// Pace repository commits to avoid triggering Hugging Face burst rate limits
	if err := c.rateLimiter.WaitCommit(ctx, token); err != nil {
		return fmt.Errorf("delete commit pacer: %w", err)
	}

	url := fmt.Sprintf("%s/api/datasets/%s/commit/main", c.baseURL, repoID)
	resp, err := c.doRequestWithRetry(ctx, func() (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		return req, nil
	}, 3)
	if err != nil {
		return fmt.Errorf("delete chunks request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("delete chunks status %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

func (c *Client) GetRepoTreeSize(ctx context.Context, token, repoID string) (int64, error) {
	url := fmt.Sprintf("%s/api/datasets/%s/treesize/main/", c.baseURL, repoID)
	resp, err := c.doRequestWithRetry(ctx, func() (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		return req, nil
	}, 3)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return 0, nil
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("treesize status %d", resp.StatusCode)
	}

	var result struct {
		Size int64 `json:"size"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, errors.New("cannot decode treesize response")
	}
	return result.Size, nil
}
