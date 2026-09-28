package hfclient

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	UserAgent = "HF2S3-Gateway/1.0 (+https://github.com/Frxn5J/HF2S3)"

	BucketAPI       = "api"
	BucketResolvers = "resolvers"
	BucketPages     = "pages"

	// Proactive thresholds to switch accounts before receiving 429
	APIQuotaLowThreshold       = 5
	ResolversQuotaLowThreshold = 20

	// Default commit pacing per account: 5 commits/sec with burst of 10
	DefaultCommitPacingRate  = 5.0
	DefaultCommitPacingBurst = 10.0

	// Official Hugging Face rate limits per 5-minute interval (Free user):
	// API: 1,000 req / 5 min | Resolvers: 5,000 req / 5 min | Pages: 200 req / 5 min
	DefaultAPILimit        = 1000
	DefaultResolversLimit  = 5000
	DefaultPagesLimit      = 200
	DefaultRateLimitWindow = 5 * time.Minute
)

var ErrRateLimited = errors.New("hugging face rate limit exceeded")

// RateLimitInfo holds parsed IETF draft-ietf-httpapi-ratelimit-headers or standard headers.
type RateLimitInfo struct {
	Bucket    string
	Remaining int
	ResetIn   time.Duration
	ResetAt   time.Time
	Limit     int
}

// RateLimitError represents a 429 Too Many Requests response with quota details.
type RateLimitError struct {
	StatusCode int
	Bucket     string
	Remaining  int
	RetryAfter time.Duration
	ResetAt    time.Time
	Message    string
}

func (e *RateLimitError) Error() string {
	return fmt.Sprintf("hugging face rate limit exceeded (bucket: %s, retry after: %v, reset at: %s): %s",
		e.Bucket, e.RetryAfter, e.ResetAt.Format(time.RFC3339), e.Message)
}

func (e *RateLimitError) Is(target error) bool {
	return target == ErrRateLimited
}

// ParseRateLimitHeaders parses Hugging Face rate limit headers from HTTP response headers.
// Format:
//
//	RateLimit: "api";r=12;t=140
//	RateLimit-Policy: "fixed window";"api";q=1000;w=300
//	Retry-After: 30
//	X-RateLimit-Remaining: 12
//	X-RateLimit-Reset: 1727376000
func ParseRateLimitHeaders(header http.Header, statusCode int) *RateLimitInfo {
	info := &RateLimitInfo{
		Bucket:    BucketAPI,
		Remaining: -1,
	}

	hasRateLimitData := false

	// Helper to lookup header case-insensitively or via canonical key
	getHeader := func(key string) string {
		if v := header.Get(key); v != "" {
			return v
		}
		for k, vals := range header {
			if strings.EqualFold(k, key) && len(vals) > 0 {
				return vals[0]
			}
		}
		return ""
	}

	// 1. Parse IETF RateLimit header
	raw := getHeader("RateLimit")
	if raw == "" {
		raw = getHeader("Rate-Limit")
	}
	if raw != "" {
		hasRateLimitData = true
		parts := strings.Split(raw, ";")
		if len(parts) > 0 {
			bucketCandidate := strings.Trim(parts[0], " \"'")
			if bucketCandidate != "" {
				info.Bucket = bucketCandidate
			}
		}
		for _, part := range parts[1:] {
			part = strings.TrimSpace(part)
			if strings.HasPrefix(part, "r=") {
				if val, err := strconv.Atoi(part[2:]); err == nil {
					info.Remaining = val
				}
			} else if strings.HasPrefix(part, "t=") {
				if sec, err := strconv.Atoi(part[2:]); err == nil && sec >= 0 {
					info.ResetIn = time.Duration(sec) * time.Second
					info.ResetAt = time.Now().Add(info.ResetIn)
				}
			}
		}
	}

	// 2. Parse RateLimit-Policy header
	rawPolicy := getHeader("RateLimit-Policy")
	if rawPolicy == "" {
		rawPolicy = getHeader("Rate-Limit-Policy")
	}
	if rawPolicy != "" {
		hasRateLimitData = true
		parts := strings.Split(rawPolicy, ";")
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if strings.HasPrefix(part, "q=") {
				if val, err := strconv.Atoi(part[2:]); err == nil {
					info.Limit = val
				}
			}
		}
	}

	// 3. Fallback: Retry-After header (seconds)
	if rawRetry := getHeader("Retry-After"); rawRetry != "" {
		hasRateLimitData = true
		if sec, err := strconv.Atoi(strings.TrimSpace(rawRetry)); err == nil && sec >= 0 {
			info.ResetIn = time.Duration(sec) * time.Second
			info.ResetAt = time.Now().Add(info.ResetIn)
		}
	}

	// 4. Fallback: X-RateLimit-* headers
	if rawRem := getHeader("X-RateLimit-Remaining"); rawRem != "" && info.Remaining < 0 {
		hasRateLimitData = true
		if val, err := strconv.Atoi(strings.TrimSpace(rawRem)); err == nil {
			info.Remaining = val
		}
	}
	if rawReset := getHeader("X-RateLimit-Reset"); rawReset != "" && info.ResetIn == 0 {
		hasRateLimitData = true
		if epoch, err := strconv.ParseInt(strings.TrimSpace(rawReset), 10, 64); err == nil {
			info.ResetAt = time.Unix(epoch, 0)
			diff := time.Until(info.ResetAt)
			if diff > 0 {
				info.ResetIn = diff
			}
		}
	}
	if rawLim := getHeader("X-RateLimit-Limit"); rawLim != "" && info.Limit == 0 {
		hasRateLimitData = true
		if val, err := strconv.Atoi(strings.TrimSpace(rawLim)); err == nil {
			info.Limit = val
		}
	}

	if statusCode == http.StatusTooManyRequests {
		hasRateLimitData = true
		if info.ResetIn == 0 {
			info.ResetIn = 15 * time.Second
			info.ResetAt = time.Now().Add(info.ResetIn)
		}
		if info.Remaining < 0 {
			info.Remaining = 0
		}
	}

	if !hasRateLimitData {
		return nil
	}
	return info
}

// RatePacer implements a token-bucket rate limiter to smooth bursts.
type RatePacer struct {
	mu         sync.Mutex
	rate       float64
	burst      float64
	tokens     float64
	lastUpdate time.Time
}

func NewRatePacer(rate, burst float64) *RatePacer {
	return &RatePacer{
		rate:       rate,
		burst:      burst,
		tokens:     burst,
		lastUpdate: time.Now(),
	}
}

func (p *RatePacer) Wait(ctx context.Context) error {
	p.mu.Lock()
	now := time.Now()
	elapsed := now.Sub(p.lastUpdate).Seconds()
	p.lastUpdate = now

	p.tokens += elapsed * p.rate
	if p.tokens > p.burst {
		p.tokens = p.burst
	}

	if p.tokens >= 1.0 {
		p.tokens -= 1.0
		p.mu.Unlock()
		return nil
	}

	// Must wait for token
	missing := 1.0 - p.tokens
	waitDuration := time.Duration((missing / p.rate) * float64(time.Second))
	p.tokens = 0
	p.mu.Unlock()

	select {
	case <-time.After(waitDuration):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type AccountRateLimitState struct {
	APILimit           int
	APIRemaining       int
	APIResetAt         time.Time
	ResolversLimit     int
	ResolversRemaining int
	ResolversResetAt   time.Time
	PagesLimit         int
	PagesRemaining     int
	CooldownUntil      time.Time
	Last429            time.Time
	RequestsInWindow   int
	Pacer              *RatePacer
}

// TokenRateLimiter tracks per-token rate limit state and pacing.
type TokenRateLimiter struct {
	mu     sync.RWMutex
	states map[string]*AccountRateLimitState
}

func NewTokenRateLimiter() *TokenRateLimiter {
	return &TokenRateLimiter{
		states: make(map[string]*AccountRateLimitState),
	}
}

func (t *TokenRateLimiter) getStateLocked(token string) *AccountRateLimitState {
	s, ok := t.states[token]
	if !ok {
		now := time.Now()
		s = &AccountRateLimitState{
			APILimit:           DefaultAPILimit,
			APIRemaining:       DefaultAPILimit,
			APIResetAt:         now.Add(DefaultRateLimitWindow),
			ResolversLimit:     DefaultResolversLimit,
			ResolversRemaining: DefaultResolversLimit,
			ResolversResetAt:   now.Add(DefaultRateLimitWindow),
			PagesLimit:         DefaultPagesLimit,
			PagesRemaining:     DefaultPagesLimit,
			Pacer:              NewRatePacer(DefaultCommitPacingRate, DefaultCommitPacingBurst),
		}
		t.states[token] = s
	}
	return s
}

// RecordRequest registers an outgoing HTTP call in the current 5-minute window.
func (t *TokenRateLimiter) RecordRequest(token string, req *http.Request) {
	if token == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	s := t.getStateLocked(token)
	now := time.Now()

	// Window rotation check
	if !s.APIResetAt.IsZero() && now.After(s.APIResetAt) {
		limit := s.APILimit
		if limit <= 0 {
			limit = DefaultAPILimit
		}
		s.APIRemaining = limit
		s.APIResetAt = now.Add(DefaultRateLimitWindow)
		s.ResolversRemaining = s.ResolversLimit
		s.ResolversResetAt = now.Add(DefaultRateLimitWindow)
		s.PagesRemaining = s.PagesLimit
		s.RequestsInWindow = 0
	} else if s.APIResetAt.IsZero() {
		s.APIResetAt = now.Add(DefaultRateLimitWindow)
	}

	bucket := BucketAPI
	if req != nil && strings.Contains(req.URL.Path, "/resolve/") {
		bucket = BucketResolvers
	}

	if bucket == BucketResolvers {
		if s.ResolversRemaining > 0 {
			s.ResolversRemaining--
		}
	} else {
		if s.APIRemaining > 0 {
			s.APIRemaining--
		}
	}
	s.RequestsInWindow++
}

// RecordResponse updates state based on HTTP response headers.
func (t *TokenRateLimiter) RecordResponse(token string, resp *http.Response) *RateLimitInfo {
	if token == "" || resp == nil {
		return nil
	}

	info := ParseRateLimitHeaders(resp.Header, resp.StatusCode)
	if info == nil && resp.StatusCode != http.StatusTooManyRequests {
		return nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	s := t.getStateLocked(token)

	if info != nil {
		switch info.Bucket {
		case BucketResolvers:
			if info.Remaining >= 0 {
				s.ResolversRemaining = info.Remaining
			}
			if !info.ResetAt.IsZero() {
				s.ResolversResetAt = info.ResetAt
			}
		default: // BucketAPI and others
			if info.Remaining >= 0 {
				s.APIRemaining = info.Remaining
			}
			if !info.ResetAt.IsZero() {
				s.APIResetAt = info.ResetAt
			}
		}

		if info.Limit > 0 {
			s.APILimit = info.Limit
		}
	}

	if resp.StatusCode == http.StatusTooManyRequests {
		s.Last429 = time.Now()
		var cooldown time.Duration = 15 * time.Second
		if info != nil && info.ResetIn > 0 {
			cooldown = info.ResetIn
		}
		s.CooldownUntil = time.Now().Add(cooldown)
		s.APIRemaining = 0
	}

	return info
}

// SetCooldown manually sets a cooldown window for a token.
func (t *TokenRateLimiter) SetCooldown(token string, d time.Duration) {
	if token == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.getStateLocked(token)
	s.CooldownUntil = time.Now().Add(d)
}

// IsThrottled checks whether a token is in cooldown or proactively near exhaustion.
func (t *TokenRateLimiter) IsThrottled(token string) (bool, time.Duration) {
	if token == "" {
		return false, 0
	}
	t.mu.RLock()
	defer t.mu.RUnlock()

	s, ok := t.states[token]
	if !ok {
		return false, 0
	}

	now := time.Now()

	// 1. Explicit cooldown from 429
	if now.Before(s.CooldownUntil) {
		return true, s.CooldownUntil.Sub(now)
	}

	// 2. Proactive throttle: API remaining quota near zero
	if s.APIRemaining >= 0 && s.APIRemaining <= APIQuotaLowThreshold && now.Before(s.APIResetAt) {
		return true, s.APIResetAt.Sub(now)
	}

	return false, 0
}

// WaitCommit paces repository commits.
func (t *TokenRateLimiter) WaitCommit(ctx context.Context, token string) error {
	if token == "" {
		return nil
	}
	t.mu.Lock()
	s := t.getStateLocked(token)
	pacer := s.Pacer
	t.mu.Unlock()

	return pacer.Wait(ctx)
}

type RateLimitStats struct {
	APIRemaining       int       `json:"api_remaining"`
	APILimit           int       `json:"api_limit"`
	APIResetAt         time.Time `json:"api_reset_at"`
	APIResetInS        int       `json:"api_reset_in_s"`
	ResolversRemaining int       `json:"resolvers_remaining"`
	ResolversLimit     int       `json:"resolvers_limit"`
	ResolversResetAt   time.Time `json:"resolvers_reset_at"`
	PagesRemaining     int       `json:"pages_remaining"`
	PagesLimit         int       `json:"pages_limit"`
	WindowSeconds      int       `json:"window_seconds"`
	RequestsInWindow   int       `json:"requests_in_window"`
	IsThrottled        bool      `json:"is_throttled"`
	CooldownRemainingS int       `json:"cooldown_remaining_s"`
}

func (t *TokenRateLimiter) GetStats(token string) RateLimitStats {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	s, ok := t.states[token]
	if !ok {
		return RateLimitStats{
			APIRemaining:       DefaultAPILimit,
			APILimit:           DefaultAPILimit,
			APIResetAt:         now.Add(DefaultRateLimitWindow),
			APIResetInS:        300,
			ResolversRemaining: DefaultResolversLimit,
			ResolversLimit:     DefaultResolversLimit,
			PagesRemaining:     DefaultPagesLimit,
			PagesLimit:         DefaultPagesLimit,
			WindowSeconds:      300,
			RequestsInWindow:   0,
			IsThrottled:        false,
		}
	}

	// Window expiration check
	if !s.APIResetAt.IsZero() && now.After(s.APIResetAt) {
		limit := s.APILimit
		if limit <= 0 {
			limit = DefaultAPILimit
		}
		s.APIRemaining = limit
		s.APIResetAt = now.Add(DefaultRateLimitWindow)
		s.ResolversRemaining = s.ResolversLimit
		s.ResolversResetAt = now.Add(DefaultRateLimitWindow)
		s.PagesRemaining = s.PagesLimit
		s.RequestsInWindow = 0
	} else if s.APIResetAt.IsZero() {
		s.APIResetAt = now.Add(DefaultRateLimitWindow)
	}

	throttled, dur := false, time.Duration(0)
	if now.Before(s.CooldownUntil) {
		throttled = true
		dur = s.CooldownUntil.Sub(now)
	} else if s.APIRemaining >= 0 && s.APIRemaining <= APIQuotaLowThreshold && now.Before(s.APIResetAt) {
		throttled = true
		dur = s.APIResetAt.Sub(now)
	}

	resetInS := 0
	if !s.APIResetAt.IsZero() && s.APIResetAt.After(now) {
		resetInS = int(s.APIResetAt.Sub(now).Seconds())
	}

	apiLimit := s.APILimit
	if apiLimit <= 0 {
		apiLimit = DefaultAPILimit
	}
	resLimit := s.ResolversLimit
	if resLimit <= 0 {
		resLimit = DefaultResolversLimit
	}
	pagesLimit := s.PagesLimit
	if pagesLimit <= 0 {
		pagesLimit = DefaultPagesLimit
	}

	return RateLimitStats{
		APIRemaining:       s.APIRemaining,
		APILimit:           apiLimit,
		APIResetAt:         s.APIResetAt,
		APIResetInS:        resetInS,
		ResolversRemaining: s.ResolversRemaining,
		ResolversLimit:     resLimit,
		ResolversResetAt:   s.ResolversResetAt,
		PagesRemaining:     s.PagesRemaining,
		PagesLimit:         pagesLimit,
		WindowSeconds:      300,
		RequestsInWindow:   s.RequestsInWindow,
		IsThrottled:        throttled,
		CooldownRemainingS: int(dur.Seconds()),
	}
}
