// Package metrics is a tiny dependency-free Prometheus text-format exporter.
package metrics

import (
	"crypto/subtle"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

type Registry struct {
	mu       sync.RWMutex
	counters map[string]*atomic.Int64 // "name{labels}" -> value
	helps    map[string]string        // metric name -> help
	gauges   map[string]gauge
}

type gauge struct {
	help string
	fn   func() float64
}

func New() *Registry {
	return &Registry{
		counters: map[string]*atomic.Int64{},
		helps:    map[string]string{},
		gauges:   map[string]gauge{},
	}
}

// Describe sets the HELP text of a counter.
func (r *Registry) Describe(name, help string) {
	r.mu.Lock()
	r.helps[name] = help
	r.mu.Unlock()
}

// Add increments the counter name{labels} by delta. labels is the raw label
// list, e.g. `route="s3",class="2xx"`, and must come from a small fixed set.
func (r *Registry) Add(name, labels string, delta int64) {
	key := name
	if labels != "" {
		key += "{" + labels + "}"
	}
	r.mu.RLock()
	c := r.counters[key]
	r.mu.RUnlock()
	if c == nil {
		r.mu.Lock()
		if c = r.counters[key]; c == nil {
			c = &atomic.Int64{}
			r.counters[key] = c
		}
		r.mu.Unlock()
	}
	c.Add(delta)
}

// Gauge registers a value computed at scrape time.
func (r *Registry) Gauge(name, help string, fn func() float64) {
	r.mu.Lock()
	r.gauges[name] = gauge{help: help, fn: fn}
	r.mu.Unlock()
}

func baseName(key string) string {
	if i := strings.IndexByte(key, '{'); i >= 0 {
		return key[:i]
	}
	return key
}

// WriteText renders the registry in the Prometheus exposition format.
func (r *Registry) WriteText(w *strings.Builder) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	keys := make([]string, 0, len(r.counters))
	for k := range r.counters {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	lastName := ""
	for _, k := range keys {
		name := baseName(k)
		if name != lastName {
			if h := r.helps[name]; h != "" {
				fmt.Fprintf(w, "# HELP %s %s\n", name, h)
			}
			fmt.Fprintf(w, "# TYPE %s counter\n", name)
			lastName = name
		}
		fmt.Fprintf(w, "%s %d\n", k, r.counters[k].Load())
	}

	names := make([]string, 0, len(r.gauges))
	for n := range r.gauges {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		g := r.gauges[n]
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n%s %g\n", n, g.help, n, n, g.fn())
	}
}

// Handler serves the metrics. It answers 404 when token is empty (metrics are
// opt-in) and requires "Authorization: Bearer <token>" otherwise.
func (r *Registry) Handler(token string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if token == "" {
			http.NotFound(w, req)
			return
		}
		got := strings.TrimPrefix(req.Header.Get("Authorization"), "Bearer ")
		if subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="hf2s3-metrics"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var b strings.Builder
		r.WriteText(&b)
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte(b.String()))
	})
}
