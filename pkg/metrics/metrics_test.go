package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestExpositionFormat(t *testing.T) {
	r := New()
	r.Describe("hf2s3_http_requests_total", "HTTP requests.")
	r.Add("hf2s3_http_requests_total", `route="s3",class="2xx"`, 2)
	r.Add("hf2s3_http_requests_total", `route="s3",class="2xx"`, 3)
	r.Add("hf2s3_http_requests_total", `route="api",class="4xx"`, 1)
	r.Gauge("hf2s3_pending_deletions", "Queued remote deletions.", func() float64 { return 4 })

	var b strings.Builder
	r.WriteText(&b)
	out := b.String()
	for _, want := range []string{
		"# HELP hf2s3_http_requests_total HTTP requests.",
		"# TYPE hf2s3_http_requests_total counter",
		`hf2s3_http_requests_total{route="s3",class="2xx"} 5`,
		`hf2s3_http_requests_total{route="api",class="4xx"} 1`,
		"# TYPE hf2s3_pending_deletions gauge",
		"hf2s3_pending_deletions 4",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	if strings.Count(out, "# TYPE hf2s3_http_requests_total") != 1 {
		t.Error("TYPE line must appear once per metric")
	}
}

func TestHandlerIsOptInAndTokenProtected(t *testing.T) {
	r := New()
	r.Add("x_total", "", 1)

	rec := httptest.NewRecorder()
	r.Handler("").ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("metrics without a configured token = %d, want 404", rec.Code)
	}

	h := r.Handler("s3cret-token")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("no token = %d", rec.Code)
	}
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.Header.Set("Authorization", "Bearer wrong")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("wrong token = %d", rec.Code)
	}
	req.Header.Set("Authorization", "Bearer s3cret-token")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "x_total 1") {
		t.Fatalf("valid token = %d %s", rec.Code, rec.Body.String())
	}
}
