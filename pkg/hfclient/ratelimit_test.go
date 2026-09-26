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
