package hfclient

import (
	"context"
	"net/http"
	"testing"
	"time"
)

func TestParseRateLimitHeaders(t *testing.T) {
	// Case 1: Standard IETF RateLimit & Policy headers
	h := http.Header{}
	h.Set("RateLimit", `"api";r=12;t=140`)
	h.Set("RateLimit-Policy", `"fixed window";"api";q=1000;w=300`)

	info := ParseRateLimitHeaders(h, http.StatusOK)
	if info == nil {
		t.Fatal("expected non-nil RateLimitInfo")
	}
	if info.Bucket != "api" {
		t.Errorf("expected bucket api, got %s", info.Bucket)
	}
	if info.Remaining != 12 {
		t.Errorf("expected remaining 12, got %d", info.Remaining)
	}
	if info.ResetIn != 140*time.Second {
		t.Errorf("expected resetIn 140s, got %v", info.ResetIn)
	}
	if info.Limit != 1000 {
		t.Errorf("expected limit 1000, got %d", info.Limit)
	}

	// Case 2: Resolvers bucket
	h2 := http.Header{}
	h2.Set("RateLimit", `"resolvers";r=4500;t=280`)
	info2 := ParseRateLimitHeaders(h2, http.StatusOK)
	if info2 == nil || info2.Bucket != "resolvers" || info2.Remaining != 4500 {
		t.Errorf("expected resolvers bucket with 4500 remaining, got %+v", info2)
	}

	// Case 3: 429 Too Many Requests with Retry-After
	h3 := http.Header{}
	h3.Set("Retry-After", "45")
	info3 := ParseRateLimitHeaders(h3, http.StatusTooManyRequests)
	if info3 == nil || info3.ResetIn != 45*time.Second {
		t.Errorf("expected 45s Retry-After, got %+v", info3)
	}
}

func TestTokenRateLimiter_Throttling(t *testing.T) {
	limiter := NewTokenRateLimiter()
	token := "hf_test_token"

	// Initial state: not throttled
	throttled, _ := limiter.IsThrottled(token)
	if throttled {
		t.Error("new token should not be throttled")
	}

	// Record 429 response
	resp := &http.Response{
		StatusCode: http.StatusTooManyRequests,
		Header: http.Header{
			"Retry-After": []string{"10"},
		},
	}
	limiter.RecordResponse(token, resp)

	throttled, dur := limiter.IsThrottled(token)
	if !throttled {
		t.Error("token should be throttled after 429")
	}
	if dur <= 0 || dur > 11*time.Second {
		t.Errorf("unexpected cooldown duration: %v", dur)
	}

	// Record proactive near-exhaustion
	token2 := "hf_token_low"
	resp2 := &http.Response{
		StatusCode: http.StatusOK,
		Header: http.Header{
			"RateLimit": []string{`"api";r=2;t=120`},
		},
	}
	limiter.RecordResponse(token2, resp2)
	throttled2, _ := limiter.IsThrottled(token2)
	if !throttled2 {
		t.Error("token with r <= 5 should be proactively throttled")
	}
}

func TestRatePacer(t *testing.T) {
	pacer := NewRatePacer(100.0, 1.0) // 100 req/sec, burst of 1
	ctx := context.Background()

	// First wait should be immediate
	start := time.Now()
	if err := pacer.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 20*time.Millisecond {
		t.Error("first token should be immediate")
	}

	// Second wait should take ~10ms
	start = time.Now()
	if err := pacer.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < 5*time.Millisecond {
		t.Errorf("expected paced delay, but took %v", elapsed)
	}
}

func TestTokenRateLimiter_5MinIntervalAndStats(t *testing.T) {
	limiter := NewTokenRateLimiter()
	token := "hf_token_stats_test"

	stats0 := limiter.GetStats(token)
	if stats0.APIRemaining != 1000 || stats0.APILimit != 1000 {
		t.Errorf("expected 1000/1000 API, got %d/%d", stats0.APIRemaining, stats0.APILimit)
	}
	if stats0.ResolversRemaining != 5000 || stats0.ResolversLimit != 5000 {
		t.Errorf("expected 5000/5000 Resolvers, got %d/%d", stats0.ResolversRemaining, stats0.ResolversLimit)
	}
	if stats0.PagesRemaining != 200 || stats0.PagesLimit != 200 {
		t.Errorf("expected 200/200 Pages, got %d/%d", stats0.PagesRemaining, stats0.PagesLimit)
	}
	if stats0.WindowSeconds != 300 {
		t.Errorf("expected window of 300s (5 min), got %d", stats0.WindowSeconds)
	}

	// Dispatch an API request
	reqAPI, _ := http.NewRequest("GET", "https://huggingface.co/api/datasets/foo", nil)
	limiter.RecordRequest(token, reqAPI)

	stats1 := limiter.GetStats(token)
	if stats1.APIRemaining != 999 {
		t.Errorf("expected 999 remaining, got %d", stats1.APIRemaining)
	}
	if stats1.RequestsInWindow != 1 {
		t.Errorf("expected 1 request in window, got %d", stats1.RequestsInWindow)
	}

	// Dispatch a Resolvers request
	reqResolver, _ := http.NewRequest("GET", "https://huggingface.co/datasets/foo/resolve/main/chunk-0", nil)
	limiter.RecordRequest(token, reqResolver)

	stats2 := limiter.GetStats(token)
	if stats2.ResolversRemaining != 4999 {
		t.Errorf("expected 4999 resolvers remaining, got %d", stats2.ResolversRemaining)
	}
	if stats2.RequestsInWindow != 2 {
		t.Errorf("expected 2 requests in window, got %d", stats2.RequestsInWindow)
	}
}
