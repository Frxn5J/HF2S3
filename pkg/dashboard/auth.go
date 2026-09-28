package dashboard

import (
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	passwordHashPrefix = "pbkdf2-sha256$"
	sessionCookieName  = "hf2s3_session"
	sessionLifetime    = 12 * time.Hour
	minPasswordLength  = 12
)

// passwordIterations follows the OWASP recommendation for PBKDF2-HMAC-SHA256.
// Tests lower it.
var passwordIterations = 600_000

// ErrInvalidCredentials is returned for any failed login.
var ErrInvalidCredentials = errors.New("invalid credentials")

// ErrRateLimited is returned when too many logins failed recently.
type ErrRateLimited struct{ RetryAfter time.Duration }

func (e *ErrRateLimited) Error() string {
	return fmt.Sprintf("too many failed login attempts, retry in %s", e.RetryAfter.Round(time.Second))
}

// HashPassword returns a salted PBKDF2 hash: "pbkdf2-sha256$<iterations>$<salt>$<hash>".
func HashPassword(password string) (string, error) {
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	key, err := pbkdf2.Key(sha256.New, password, salt, passwordIterations, 32)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s%d$%s$%s", passwordHashPrefix, passwordIterations,
		base64.RawStdEncoding.EncodeToString(salt), base64.RawStdEncoding.EncodeToString(key)), nil
}

// IsPasswordHash reports whether s is a value produced by HashPassword.
func IsPasswordHash(s string) bool { return strings.HasPrefix(s, passwordHashPrefix) }

// VerifyPassword checks password against a stored hash in constant time.
func VerifyPassword(stored, password string) bool {
	parts := strings.Split(strings.TrimPrefix(stored, passwordHashPrefix), "$")
	if !IsPasswordHash(stored) || len(parts) != 3 {
		return false
	}
	iter, err := strconv.Atoi(parts[0])
	if err != nil || iter < 1 || iter > 10_000_000 {
		return false
	}
	salt, err1 := base64.RawStdEncoding.DecodeString(parts[1])
	want, err2 := base64.RawStdEncoding.DecodeString(parts[2])
	if err1 != nil || err2 != nil {
		return false
	}
	got, err := pbkdf2.Key(sha256.New, password, salt, iter, len(want))
	if err != nil {
		return false
	}
	return subtle.ConstantTimeCompare(got, want) == 1
}

type SessionInfo struct {
	Username  string
	ExpiresAt time.Time
}

// loginLimiter throttles failed logins per client IP and globally.
type loginLimiter struct {
	mu     sync.Mutex
	perIP  map[string][]time.Time
	global []time.Time

	window    time.Duration
	maxPerIP  int
	maxGlobal int
}

func newLoginLimiter() *loginLimiter {
	return &loginLimiter{
		perIP:     map[string][]time.Time{},
		window:    5 * time.Minute,
		maxPerIP:  5,
		maxGlobal: 40,
	}
}

func prune(ts []time.Time, cutoff time.Time) []time.Time {
	i := 0
	for i < len(ts) && ts[i].Before(cutoff) {
		i++
	}
	return ts[i:]
}

// retryAfter returns how long the caller must wait (0 = allowed).
func (l *loginLimiter) retryAfter(ip string, now time.Time) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	cutoff := now.Add(-l.window)
	l.global = prune(l.global, cutoff)
	ts := prune(l.perIP[ip], cutoff)
	if len(ts) == 0 {
		delete(l.perIP, ip)
	} else {
		l.perIP[ip] = ts
	}
	switch {
	case len(ts) >= l.maxPerIP:
		return ts[0].Add(l.window).Sub(now)
	case len(l.global) >= l.maxGlobal:
		return l.global[0].Add(l.window).Sub(now)
	}
	return 0
}

func (l *loginLimiter) fail(ip string, now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.perIP) > 20000 { // bound memory under a spoofed-address flood
		l.perIP = map[string][]time.Time{}
	}
	l.perIP[ip] = append(l.perIP[ip], now)
	l.global = append(l.global, now)
}

func (l *loginLimiter) success(ip string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.perIP, ip)
}

type AdminAuthManager struct {
	mu           sync.RWMutex
	username     string
	passwordHash string
	sessions     sync.Map // token (string) -> SessionInfo
	limiter      *loginLimiter
	trustProxy   bool
	now          func() time.Time
}

// NewAdminAuthManager creates the manager. password may be a hash produced by
// HashPassword (as persisted) or a plaintext password, which is hashed here.
// Empty values fall back to admin/admin123, which callers must refuse in
// production (see IsDefaultCredentials).
func NewAdminAuthManager(username, password string) *AdminAuthManager {
	if strings.TrimSpace(username) == "" {
		username = "admin"
	}
	if password == "" {
		password = "admin123"
	}
	a := &AdminAuthManager{
		username: strings.TrimSpace(username),
		limiter:  newLoginLimiter(),
		now:      time.Now,
	}
	if IsPasswordHash(password) {
		a.passwordHash = password
	} else if h, err := HashPassword(password); err == nil {
		a.passwordHash = h
	}
	return a
}

// SetTrustProxy makes the manager honour X-Forwarded-For / X-Forwarded-Proto.
// Enable it only behind a reverse proxy that overwrites those headers.
func (a *AdminAuthManager) SetTrustProxy(v bool) { a.trustProxy = v }

// PasswordHash returns the stored form of the password, for persistence.
func (a *AdminAuthManager) PasswordHash() string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.passwordHash
}

// VerifyPassword reports whether password is the current admin password.
func (a *AdminAuthManager) VerifyPassword(password string) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return VerifyPassword(a.passwordHash, password)
}

// IsDefaultCredentials reports whether the well-known default login still works.
func (a *AdminAuthManager) IsDefaultCredentials() bool {
	return a.GetUsername() == "admin" && a.VerifyPassword("admin123")
}

func ValidatePasswordStrength(p string) error {
	if len(p) < minPasswordLength {
		return fmt.Errorf("la contraseña debe tener al menos %d caracteres", minPasswordLength)
	}
	return nil
}

// SetCredentials replaces the username and/or password (plaintext or hash).
// Every existing session is revoked when the password changes.
func (a *AdminAuthManager) SetCredentials(user, pass string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if u := strings.TrimSpace(user); u != "" {
		a.username = u
	}
	if pass == "" {
		return nil
	}
	if IsPasswordHash(pass) {
		a.passwordHash = pass
	} else {
		h, err := HashPassword(pass)
		if err != nil {
			return err
		}
		a.passwordHash = h
	}
	a.sessions.Range(func(k, _ any) bool { a.sessions.Delete(k); return true })
	return nil
}

func (a *AdminAuthManager) GetUsername() string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.username
}

// ClientIP returns the address used for rate limiting.
func (a *AdminAuthManager) ClientIP(r *http.Request) string {
	if a.trustProxy {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			// The proxy appends the real client address last; earlier entries are client-supplied.
			parts := strings.Split(xff, ",")
			return strings.TrimSpace(parts[len(parts)-1])
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// IsSecureRequest reports whether the client connection used HTTPS.
func (a *AdminAuthManager) IsSecureRequest(r *http.Request) bool {
	if r.TLS != nil {
		return true
	}
	return a.trustProxy && strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https")
}

// Login verifies the credentials and returns a new session token. Failures are
// rate limited per client IP.
func (a *AdminAuthManager) Login(r *http.Request, user, pass string) (string, error) {
	ip := a.ClientIP(r)
	now := a.now()
	if wait := a.limiter.retryAfter(ip, now); wait > 0 {
		return "", &ErrRateLimited{RetryAfter: wait}
	}

	a.mu.RLock()
	targetU, hash := a.username, a.passwordHash
	a.mu.RUnlock()

	// Always run the password hash so a wrong user name costs the same time.
	userMatch := subtle.ConstantTimeCompare([]byte(strings.TrimSpace(user)), []byte(targetU)) == 1
	passMatch := VerifyPassword(hash, pass)
	if !userMatch || !passMatch {
		a.limiter.fail(ip, now)
		return "", ErrInvalidCredentials
	}
	a.limiter.success(ip)

	tokenBytes := make([]byte, 32)
	if _, err := rand.Read(tokenBytes); err != nil {
		return "", err
	}
	token := hex.EncodeToString(tokenBytes)

	a.sweepExpired(now)
	a.sessions.Store(token, SessionInfo{Username: targetU, ExpiresAt: now.Add(sessionLifetime)})
	return token, nil
}

func (a *AdminAuthManager) sweepExpired(now time.Time) {
	a.sessions.Range(func(k, v any) bool {
		if now.After(v.(SessionInfo).ExpiresAt) {
			a.sessions.Delete(k)
		}
		return true
	})
}

func (a *AdminAuthManager) ValidateToken(token string) bool {
	if token == "" {
		return false
	}
	val, ok := a.sessions.Load(token)
	if !ok {
		return false
	}
	if a.now().After(val.(SessionInfo).ExpiresAt) {
		a.sessions.Delete(token)
		return false
	}
	return true
}

// ValidateRequest accepts only the HttpOnly session cookie. Header and query
// parameter tokens were removed: they can leak through logs, referrers and
// JavaScript-readable storage.
func (a *AdminAuthManager) ValidateRequest(r *http.Request) bool {
	cookie, err := r.Cookie(sessionCookieName)
	return err == nil && a.ValidateToken(cookie.Value)
}

func (a *AdminAuthManager) Logout(token string) {
	if token != "" {
		a.sessions.Delete(token)
	}
}

// sameOrigin rejects cross-site state-changing requests (CSRF). It is applied
// on top of the SameSite=Strict cookie for browsers that mishandle it.
func (a *AdminAuthManager) sameOrigin(r *http.Request) bool {
	switch r.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return true
	}
	if origin := r.Header.Get("Origin"); origin != "" {
		u, err := url.Parse(origin)
		if err != nil {
			return false
		}
		host := r.Host
		if a.trustProxy {
			if fh := r.Header.Get("X-Forwarded-Host"); fh != "" {
				host = fh
			}
		}
		return strings.EqualFold(u.Host, host)
	}
	if site := r.Header.Get("Sec-Fetch-Site"); site != "" {
		return site == "same-origin" || site == "none"
	}
	return true // non-browser client (curl, scripts) carrying the cookie explicitly
}

func (a *AdminAuthManager) RequireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !a.ValidateRequest(r) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"Unauthorized. Admin login required."}`))
			return
		}
		if !a.sameOrigin(r) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"error":"Cross-site request refused."}`))
			return
		}
		next(w, r)
	}
}
