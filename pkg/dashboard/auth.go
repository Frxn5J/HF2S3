package dashboard

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"net/http"
	"strings"
	"sync"
	"time"
)

type SessionInfo struct {
	Username  string
	ExpiresAt time.Time
}

type AdminAuthManager struct {
	username string
	password string
	sessions sync.Map // token (string) -> SessionInfo
}

func NewAdminAuthManager(username, password string) *AdminAuthManager {
	if username == "" {
		username = "admin"
	}
	if password == "" {
		password = "admin"
	}
	return &AdminAuthManager{
		username: username,
		password: password,
	}
}

func (a *AdminAuthManager) Login(user, pass string) (string, error) {
	u := strings.TrimSpace(user)
	targetU := strings.TrimSpace(a.username)
	userMatch := subtle.ConstantTimeCompare([]byte(u), []byte(targetU)) == 1
	passMatch := subtle.ConstantTimeCompare([]byte(pass), []byte(a.password)) == 1

	if !userMatch || !passMatch {
		return "", http.ErrNoCookie // Invalid credentials
	}

	tokenBytes := make([]byte, 24)
	if _, err := rand.Read(tokenBytes); err != nil {
		return "", err
	}
	token := hex.EncodeToString(tokenBytes)

	a.sessions.Store(token, SessionInfo{
		Username:  user,
		ExpiresAt: time.Now().Add(24 * time.Hour),
	})

	return token, nil
}

func (a *AdminAuthManager) ValidateToken(token string) bool {
	if token == "" {
		return false
	}
	val, ok := a.sessions.Load(token)
	if !ok {
		return false
	}
	info := val.(SessionInfo)
	if time.Now().After(info.ExpiresAt) {
		a.sessions.Delete(token)
		return false
	}
	return true
}

func (a *AdminAuthManager) ValidateRequest(r *http.Request) bool {
	// Bearer authorization header
	authHeader := r.Header.Get("Authorization")
	if strings.HasPrefix(authHeader, "Bearer ") {
		token := strings.TrimPrefix(authHeader, "Bearer ")
		if a.ValidateToken(token) {
			return true
		}
	}

	// Custom header token
	if customToken := r.Header.Get("X-Admin-Token"); customToken != "" {
		if a.ValidateToken(customToken) {
			return true
		}
	}

	// Session cookie token
	if cookie, err := r.Cookie("hf2s3_session"); err == nil && cookie.Value != "" {
		if a.ValidateToken(cookie.Value) {
			return true
		}
	}

	// Query parameter token for direct browser downloads
	if qToken := r.URL.Query().Get("token"); qToken != "" {
		if a.ValidateToken(qToken) {
			return true
		}
	}

	return false
}

func (a *AdminAuthManager) Logout(token string) {
	if token != "" {
		a.sessions.Delete(token)
	}
}

func (a *AdminAuthManager) RequireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !a.ValidateRequest(r) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"Unauthorized. Admin login required."}`))
			return
		}
		next(w, r)
	}
}
