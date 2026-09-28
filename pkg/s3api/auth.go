package s3api

import (
	"net/http"
	"strings"
	"sync"
)

type AuthManager struct {
	mu        sync.RWMutex
	accessKey string
	secretKey string
}

func NewAuthManager(accessKey, secretKey string) *AuthManager {
	return &AuthManager{
		accessKey: accessKey,
		secretKey: secretKey,
	}
}

func (a *AuthManager) UpdateCredentials(accessKey, secretKey string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if accessKey != "" {
		a.accessKey = accessKey
	}
	if secretKey != "" {
		a.secretKey = secretKey
	}
}

func (a *AuthManager) Validate(r *http.Request) bool {
	a.mu.RLock()
	accessKey := a.accessKey
	a.mu.RUnlock()

	// If no credentials configured, allow access
	if accessKey == "" {
		return true
	}

	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		// Check query auth (X-Amz-Credential)
		qCred := r.URL.Query().Get("X-Amz-Credential")
		if qCred != "" {
			parts := strings.Split(qCred, "/")
			if len(parts) > 0 && parts[0] == accessKey {
				return true
			}
		}
		return false
	}

	// AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request...
	if strings.HasPrefix(authHeader, "AWS4-HMAC-SHA256") {
		parts := strings.Split(authHeader, "Credential=")
		if len(parts) > 1 {
			credPart := strings.Split(parts[1], ",")[0]
			credPieces := strings.Split(credPart, "/")
			if len(credPieces) > 0 && credPieces[0] == accessKey {
				return true
			}
		}
		return false
	}

	// AWS AKIAIOSFODNN7EXAMPLE:signature (SigV2)
	if strings.HasPrefix(authHeader, "AWS ") {
		tokenPart := strings.TrimPrefix(authHeader, "AWS ")
		keyAndSig := strings.Split(tokenPart, ":")
		if len(keyAndSig) > 0 && keyAndSig[0] == accessKey {
			return true
		}
		return false
	}

	return false
}
