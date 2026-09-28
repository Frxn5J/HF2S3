package dashboard

import (
	"context"
	"crypto/rand"
	"embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"hf2s3/pkg/db"
	"hf2s3/pkg/hfclient"
	"hf2s3/pkg/hfstorage"
	"hf2s3/pkg/models"
	"hf2s3/pkg/sigv4"
	"hf2s3/pkg/storage"
)

//go:embed static/*
var staticFS embed.FS

const (
	maxJSONBody      = 1 << 20  // 1 MiB
	maxUploadBytes   = 20 << 30 // 20 GiB per upload through the web console
	maxKeyBytes      = 1024
	defaultPresign   = time.Hour
	maxPresign       = 7 * 24 * time.Hour
	objectListLimit  = 200
	objectListMax    = 1000
	minS3SecretBytes = 32
)

var repoNameRE = regexp.MustCompile(`^([A-Za-z0-9][A-Za-z0-9._-]{0,95}/)?[A-Za-z0-9][A-Za-z0-9._-]{0,95}$`)

type drainState struct {
	Running bool   `json:"running"`
	Total   int64  `json:"total"`
	Done    int64  `json:"done"`
	Failed  int64  `json:"failed"`
	Error   string `json:"error,omitempty"`
}

type DashboardHandler struct {
	pool       *storage.PoolManager
	adminAuth  *AdminAuthManager
	serverPort int
	fileServer http.Handler

	mu                 sync.RWMutex // guards settings and the fields below
	settings           *models.SystemSettings
	credentialsUpdater func(accessKey, secretKey string)
	publicURL          string
	envManaged         map[string]bool

	drainMu sync.Mutex
	drains  map[int64]*drainState
}

func NewDashboardHandler(pool *storage.PoolManager, settings *models.SystemSettings, serverPort int, adminAuth *AdminAuthManager) *DashboardHandler {
	sub, err := fs.Sub(staticFS, "static")
	var fsHandler http.Handler
	if err == nil {
		fsHandler = http.FileServer(http.FS(sub))
	} else {
		fsHandler = http.NotFoundHandler()
	}

	if adminAuth == nil {
		adminAuth = NewAdminAuthManager("admin", "admin123")
	}

	return &DashboardHandler{
		pool:       pool,
		settings:   settings,
		adminAuth:  adminAuth,
		serverPort: serverPort,
		fileServer: fsHandler,
		envManaged: map[string]bool{},
		drains:     map[int64]*drainState{},
	}
}

func (h *DashboardHandler) SetCredentialsUpdater(fn func(accessKey, secretKey string)) {
	h.credentialsUpdater = fn
}

// SetPublicURL sets the externally visible base URL ("https://s3.example.com")
// used in snippets and presigned links.
func (h *DashboardHandler) SetPublicURL(u string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.publicURL = strings.TrimRight(strings.TrimSpace(u), "/")
}

// SetEnvManaged lists the settings that come from environment variables and
// therefore cannot be edited from the console. Keys: access_key_id,
// secret_access_key, s3_region, chunk_size_mb, admin_username, admin_password,
// hf_storage.
func (h *DashboardHandler) SetEnvManaged(keys ...string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, k := range keys {
		h.envManaged[k] = true
	}
}

// Handler returns the complete web console handler (routes plus security headers).
func (h *DashboardHandler) Handler() http.Handler {
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	return SecurityHeaders(mux)
}

// SecurityHeaders adds hardening headers to every console response.
func SecurityHeaders(next http.Handler) http.Handler {
	const csp = "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; " +
		"font-src https://fonts.gstatic.com; img-src 'self' data:; connect-src 'self'; object-src 'none'; " +
		"base-uri 'none'; form-action 'self'; frame-ancestors 'none'"
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hd := w.Header()
		hd.Set("Content-Security-Policy", csp)
		hd.Set("X-Content-Type-Options", "nosniff")
		hd.Set("X-Frame-Options", "DENY")
		hd.Set("Referrer-Policy", "no-referrer")
		next.ServeHTTP(w, r)
	})
}

func (h *DashboardHandler) RegisterRoutes(mux *http.ServeMux) {
	auth := h.adminAuth.RequireAuth

	// Public endpoints (auth & probes)
	mux.HandleFunc("POST /api/auth/login", h.handleLogin)
	mux.HandleFunc("GET /api/auth/check", h.handleAuthCheck)
	mux.HandleFunc("POST /api/auth/logout", h.handleLogout)
	mux.HandleFunc("GET /api/health", h.handleHealth)
	mux.HandleFunc("GET /api/ready", h.handleReady)

	// Protected admin endpoints
	mux.HandleFunc("GET /api/stats", auth(h.handleGetStats))
	mux.HandleFunc("GET /api/accounts", auth(h.handleListAccounts))
	mux.HandleFunc("POST /api/accounts", auth(h.handleCreateAccount))
	mux.HandleFunc("POST /api/accounts/{id}/toggle", auth(h.handleToggleAccount))
	mux.HandleFunc("POST /api/accounts/{id}/sync", auth(h.handleSyncAccount))
	mux.HandleFunc("POST /api/accounts/{id}/quota", auth(h.handleUpdateAccountQuota))
	mux.HandleFunc("PUT /api/accounts/{id}", auth(h.handleUpdateAccount))
	mux.HandleFunc("POST /api/accounts/{id}/update", auth(h.handleUpdateAccount))
	mux.HandleFunc("POST /api/accounts/{id}/drain", auth(h.handleStartDrain))
	mux.HandleFunc("GET /api/accounts/{id}/drain", auth(h.handleDrainStatus))
	mux.HandleFunc("DELETE /api/accounts/{id}", auth(h.handleDeleteAccount))

	// Multi-bucket S3 cache
	mux.HandleFunc("GET /api/cache-buckets", auth(h.handleListCacheBuckets))
	mux.HandleFunc("POST /api/cache-buckets", auth(h.handleCreateCacheBucket))
	mux.HandleFunc("POST /api/cache-buckets/{id}/toggle", auth(h.handleToggleCacheBucket))
	mux.HandleFunc("DELETE /api/cache-buckets/{id}", auth(h.handleDeleteCacheBucket))

	mux.HandleFunc("GET /api/buckets", auth(h.handleListBuckets))
	mux.HandleFunc("POST /api/buckets", auth(h.handleCreateBucket))
	mux.HandleFunc("DELETE /api/buckets/{name}", auth(h.handleDeleteBucket))

	mux.HandleFunc("GET /api/objects", auth(h.handleListObjects))
	mux.HandleFunc("GET /api/objects/detail", auth(h.handleGetObjectDetail))
	mux.HandleFunc("POST /api/objects/upload", auth(h.handleUploadObject))
	mux.HandleFunc("GET /api/objects/download", auth(h.handleDownloadObject))
	mux.HandleFunc("DELETE /api/objects", auth(h.handleDeleteObject))
	mux.HandleFunc("POST /api/objects/presign", auth(h.handlePresign))

	// Multi-tier cache and maintenance
	mux.HandleFunc("POST /api/objects/evict-cache", auth(h.handleEvictCache))
	mux.HandleFunc("POST /api/objects/promote-cache", auth(h.handlePromoteCache))
	mux.HandleFunc("GET /api/media/info", auth(h.handleGetMediaInfo))
	mux.HandleFunc("POST /api/gc/retry", auth(h.handleGCRetry))

	mux.HandleFunc("GET /api/settings", auth(h.handleGetSettings))
	mux.HandleFunc("POST /api/settings", auth(h.handleUpdateSettings))
	mux.HandleFunc("POST /api/settings/rotate-s3-secret", auth(h.handleRotateS3Secret))

	// Database backup (restore is a CLI operation: `hf2s3 restore`)
	mux.HandleFunc("GET /api/admin/backup", auth(h.handleDownloadBackup))

	// Static UI assets and SPA fallback
	mux.Handle("/", h.fileServer)
}

func (h *DashboardHandler) writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func (h *DashboardHandler) writeError(w http.ResponseWriter, status int, message string) {
	h.writeJSON(w, status, map[string]string{"error": message})
}

// fail logs the real cause and tells the client only publicMsg: upstream
// errors can carry URLs, tokens or account names.
func (h *DashboardHandler) fail(w http.ResponseWriter, status int, publicMsg string, err error) {
	slog.Error("console request failed", "message", publicMsg, "err", err)
	h.writeError(w, status, publicMsg)
}

func decodeJSON(w http.ResponseWriter, r *http.Request, v any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxJSONBody)
	return json.NewDecoder(r.Body).Decode(v)
}

func pathID(r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	return id, err == nil && id > 0
}

// --- Auth Endpoints ---

func (h *DashboardHandler) handleLogin(w http.ResponseWriter, r *http.Request) {
	if !h.adminAuth.sameOrigin(r) {
		h.writeError(w, http.StatusForbidden, "Cross-site request refused.")
		return
	}
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := decodeJSON(w, r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	token, err := h.adminAuth.Login(r, req.Username, req.Password)
	var limited *ErrRateLimited
	switch {
	case errors.As(err, &limited):
		w.Header().Set("Retry-After", strconv.Itoa(int(limited.RetryAfter.Seconds())+1))
		h.writeError(w, http.StatusTooManyRequests, "Demasiados intentos fallidos. Inténtalo de nuevo más tarde.")
		return
	case err != nil:
		h.writeError(w, http.StatusUnauthorized, "Credenciales de administrador inválidas")
		return
	}

	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    token,
		Path:     "/",
		Expires:  time.Now().Add(sessionLifetime),
		HttpOnly: true,
		Secure:   h.adminAuth.IsSecureRequest(r),
		SameSite: http.SameSiteStrictMode,
	})
	h.writeJSON(w, http.StatusOK, map[string]interface{}{"username": h.adminAuth.GetUsername()})
}

func (h *DashboardHandler) handleAuthCheck(w http.ResponseWriter, r *http.Request) {
	if !h.adminAuth.ValidateRequest(r) {
		h.writeJSON(w, http.StatusOK, map[string]interface{}{"authenticated": false})
		return
	}
	h.writeJSON(w, http.StatusOK, map[string]interface{}{
		"authenticated": true,
		"username":      h.adminAuth.GetUsername(),
	})
}

func (h *DashboardHandler) handleLogout(w http.ResponseWriter, r *http.Request) {
	if !h.adminAuth.sameOrigin(r) {
		h.writeError(w, http.StatusForbidden, "Cross-site request refused.")
		return
	}
	if cookie, err := r.Cookie(sessionCookieName); err == nil {
		h.adminAuth.Logout(cookie.Value)
	}
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    "",
		Path:     "/",
		Expires:  time.Unix(0, 0),
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   h.adminAuth.IsSecureRequest(r),
		SameSite: http.SameSiteStrictMode,
	})
	h.writeJSON(w, http.StatusOK, map[string]bool{"success": true})
}

// handleHealth is the liveness probe: the process is up.
func (h *DashboardHandler) handleHealth(w http.ResponseWriter, r *http.Request) {
	h.writeJSON(w, http.StatusOK, map[string]string{
		"status":  "ok",
		"service": "hf2s3",
		"time":    time.Now().UTC().Format(time.RFC3339),
	})
}

// handleReady is the readiness probe: the database answers.
func (h *DashboardHandler) handleReady(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	if err := h.pool.DB().Ping(ctx); err != nil {
		h.writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable"})
		return
	}
	h.writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

// --- Database Backup ---

func (h *DashboardHandler) handleDownloadBackup(w http.ResponseWriter, r *http.Request) {
	filename := fmt.Sprintf("hf2s3_backup_%s.db", time.Now().Format("2006-01-02_150405"))
	w.Header().Set("Content-Type", "application/vnd.sqlite3")
	w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": filename}))
	w.Header().Set("Cache-Control", "no-store")

	if err := h.pool.DB().ExportBackup(r.Context(), w); err != nil {
		slog.Error("backup export failed", "err", err)
	}
}

// --- Stats ---

func (h *DashboardHandler) handleGetStats(w http.ResponseWriter, r *http.Request) {
	stats, err := h.pool.DB().GetStats(r.Context())
	if err != nil {
		h.fail(w, http.StatusInternalServerError, "No se pudieron leer las estadísticas", err)
		return
	}
	if n, _, err := h.pool.DB().CountChunksToRekey(r.Context(), h.pool.Keyring().CurrentKeyID()); err == nil {
		stats.ChunksNeedingRekey = n
	}
	h.writeJSON(w, http.StatusOK, stats)
}

func (h *DashboardHandler) handleGCRetry(w http.ResponseWriter, r *http.Request) {
	if err := h.pool.DB().RetryDeletionsNow(r.Context()); err != nil {
		h.fail(w, http.StatusInternalServerError, "No se pudo reprogramar la cola", err)
		return
	}
	h.pool.Go(func(ctx context.Context) { h.pool.ProcessPendingDeletions(ctx) })
	h.writeJSON(w, http.StatusAccepted, map[string]bool{"success": true})
}

// --- Accounts ---

type AccountResponse struct {
	models.Account
	RateLimit hfclient.RateLimitStats `json:"rate_limit"`
}

func maskToken(tok string) string {
	if len(tok) > 8 {
		return tok[:4] + "..." + tok[len(tok)-4:]
	}
	return "********"
}

func (h *DashboardHandler) handleListAccounts(w http.ResponseWriter, r *http.Request) {
	accounts, err := h.pool.DB().ListAccounts(r.Context())
	if err != nil {
		h.fail(w, http.StatusInternalServerError, "No se pudieron listar las cuentas", err)
		return
	}

	result := make([]AccountResponse, len(accounts))
	for i, acc := range accounts {
		stats := h.pool.HFClient().GetRateLimitStats(acc.Token)
		acc.Token = maskToken(acc.Token)
		result[i] = AccountResponse{Account: acc, RateLimit: stats}
	}
	h.writeJSON(w, http.StatusOK, result)
}

type CreateAccountRequest struct {
	Name     string `json:"name"`
	Token    string `json:"token"`
	RepoName string `json:"repo_name"`
	QuotaGB  int64  `json:"quota_gb"`
}

func (h *DashboardHandler) handleCreateAccount(w http.ResponseWriter, r *http.Request) {
	var req CreateAccountRequest
	if err := decodeJSON(w, r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	token := strings.TrimSpace(req.Token)
	if token == "" {
		h.writeError(w, http.StatusBadRequest, "Hugging Face token is required")
		return
	}

	repoName := strings.TrimSpace(req.RepoName)
	if repoName == "" {
		repoName = "hf2s3-vault"
	}
	if !repoNameRE.MatchString(repoName) {
		h.writeError(w, http.StatusBadRequest, "Nombre de repositorio inválido")
		return
	}

	// Verify token identity via Hugging Face whoami
	whoami, err := h.pool.HFClient().VerifyToken(r.Context(), token)
	if err != nil {
		h.fail(w, http.StatusUnauthorized, "Token de Hugging Face inválido", err)
		return
	}

	fullRepoID := repoName
	if !strings.Contains(repoName, "/") {
		fullRepoID = fmt.Sprintf("%s/%s", whoami.Name, repoName)
	}

	// The dataset holding the encrypted chunks is created public (downloads need
	// no token and do not count against private storage).
	if err := h.pool.HFClient().EnsureDatasetRepo(r.Context(), token, repoName); err != nil {
		h.fail(w, http.StatusInternalServerError, "No se pudo preparar el repositorio de datasets", err)
		return
	}

	// Quota allocation: if req.QuotaGB <= 0, quota is 0 (dynamic/unknown for public datasets)
	var quotaBytes int64
	if req.QuotaGB > 0 {
		quotaBytes = req.QuotaGB * 1024 * 1024 * 1024
	}

	name := strings.TrimSpace(req.Name)
	if name == "" {
		name = fmt.Sprintf("%s (%s)", whoami.Name, repoName)
	}

	usedBytes, _ := h.pool.HFClient().GetRepoTreeSize(r.Context(), token, fullRepoID)

	acc := &models.Account{
		Name:       name,
		Username:   whoami.Name,
		Token:      token,
		RepoName:   fullRepoID,
		QuotaBytes: quotaBytes,
		UsedBytes:  usedBytes,
		IsActive:   true,
	}

	if err := h.pool.DB().CreateAccount(r.Context(), acc); err != nil {
		h.fail(w, http.StatusInternalServerError, "No se pudo guardar la cuenta", err)
		return
	}

	acc.Token = maskToken(acc.Token)
	h.writeJSON(w, http.StatusCreated, acc)
}

func (h *DashboardHandler) handleToggleAccount(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(r)
	if !ok {
		h.writeError(w, http.StatusBadRequest, "Invalid account id")
		return
	}

	acc, err := h.pool.DB().GetAccountByID(r.Context(), id)
	if err != nil {
		h.writeError(w, http.StatusNotFound, "Account not found")
		return
	}

	acc.IsActive = !acc.IsActive
	if err := h.pool.DB().UpdateAccount(r.Context(), acc); err != nil {
		h.fail(w, http.StatusInternalServerError, "No se pudo actualizar la cuenta", err)
		return
	}

	acc.Token = maskToken(acc.Token)
	h.writeJSON(w, http.StatusOK, acc)
}

func (h *DashboardHandler) handleSyncAccount(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(r)
	if !ok {
		h.writeError(w, http.StatusBadRequest, "Invalid account id")
		return
	}

	acc, err := h.pool.DB().GetAccountByID(r.Context(), id)
	if err != nil {
		h.writeError(w, http.StatusNotFound, "Account not found")
		return
	}

	size, err := h.pool.HFClient().GetRepoTreeSize(r.Context(), acc.Token, acc.RepoName)
	if err == nil {
		_ = h.pool.DB().UpdateAccountUsage(r.Context(), acc.ID, size)
		acc.UsedBytes = size
	}

	acc.Token = maskToken(acc.Token)
	h.writeJSON(w, http.StatusOK, acc)
}

func (h *DashboardHandler) handleDeleteAccount(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(r)
	if !ok {
		h.writeError(w, http.StatusBadRequest, "Invalid account id")
		return
	}

	if err := h.pool.DB().DeleteAccount(r.Context(), id); err != nil {
		if errors.Is(err, db.ErrAccountInUse) {
			h.writeError(w, http.StatusConflict, "La cuenta todavía almacena datos (o borrados pendientes). Vacíala primero con «Drain» y espera a que la cola de borrado termine.")
			return
		}
		h.fail(w, http.StatusInternalServerError, "No se pudo eliminar la cuenta", err)
		return
	}
	h.writeJSON(w, http.StatusOK, map[string]bool{"success": true})
}

func (h *DashboardHandler) handleUpdateAccountQuota(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(r)
	if !ok {
		h.writeError(w, http.StatusBadRequest, "Invalid account id")
		return
	}

	var req struct {
		QuotaGB int64 `json:"quota_gb"`
	}
	if err := decodeJSON(w, r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, "Invalid payload")
		return
	}

	var quotaBytes int64
	if req.QuotaGB > 0 {
		quotaBytes = req.QuotaGB * 1024 * 1024 * 1024
	}

	if err := h.pool.DB().UpdateAccountQuota(r.Context(), id, quotaBytes); err != nil {
		h.fail(w, http.StatusInternalServerError, "No se pudo actualizar la cuota", err)
		return
	}

	acc, err := h.pool.DB().GetAccountByID(r.Context(), id)
	if err != nil {
		h.writeError(w, http.StatusNotFound, "Account not found")
		return
	}

	acc.Token = maskToken(acc.Token)
	h.writeJSON(w, http.StatusOK, acc)
}

type UpdateAccountRequest struct {
	Name     string `json:"name"`
	Token    string `json:"token"`
	RepoName string `json:"repo_name"`
	QuotaGB  *int64 `json:"quota_gb"`
	IsActive *bool  `json:"is_active"`
}

func (h *DashboardHandler) handleUpdateAccount(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(r)
	if !ok {
		h.writeError(w, http.StatusBadRequest, "Invalid account id")
		return
	}

	acc, err := h.pool.DB().GetAccountByID(r.Context(), id)
	if err != nil {
		h.writeError(w, http.StatusNotFound, "Account not found")
		return
	}

	var req UpdateAccountRequest
	if err := decodeJSON(w, r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	token := strings.TrimSpace(req.Token)
	if token != "" && token != acc.Token && token != maskToken(acc.Token) {
		whoami, err := h.pool.HFClient().VerifyToken(r.Context(), token)
		if err != nil {
			h.fail(w, http.StatusUnauthorized, "Token de Hugging Face inválido", err)
			return
		}
		acc.Token = token
		acc.Username = whoami.Name
	}

	if name := strings.TrimSpace(req.Name); name != "" {
		acc.Name = name
	}

	repoName := strings.TrimSpace(req.RepoName)
	if repoName != "" && repoName != acc.RepoName {
		cleanRepo := repoName
		if strings.Contains(cleanRepo, "/") {
			parts := strings.Split(cleanRepo, "/")
			cleanRepo = parts[len(parts)-1]
		}
		if !repoNameRE.MatchString(cleanRepo) {
			h.writeError(w, http.StatusBadRequest, "Nombre de repositorio inválido")
			return
		}
		if n, _ := h.pool.DB().ChunkCountForAccount(r.Context(), acc.ID); n > 0 {
			h.writeError(w, http.StatusConflict, "La cuenta ya almacena datos: cambiar de repositorio los dejaría inaccesibles. Vacía la cuenta primero.")
			return
		}
		fullRepoID := fmt.Sprintf("%s/%s", acc.Username, cleanRepo)
		if err := h.pool.HFClient().EnsureDatasetRepoWithVisibility(r.Context(), acc.Token, cleanRepo, false); err != nil {
			h.fail(w, http.StatusInternalServerError, "No se pudo preparar el repositorio de datasets", err)
			return
		}
		acc.RepoName = fullRepoID
	}

	if req.QuotaGB != nil {
		if *req.QuotaGB <= 0 {
			acc.QuotaBytes = 0
		} else {
			acc.QuotaBytes = *req.QuotaGB * 1024 * 1024 * 1024
		}
	}

	if req.IsActive != nil {
		acc.IsActive = *req.IsActive
	}

	if err := h.pool.DB().UpdateAccount(r.Context(), acc); err != nil {
		h.fail(w, http.StatusInternalServerError, "No se pudo actualizar la cuenta", err)
		return
	}

	acc.Token = maskToken(acc.Token)
	h.writeJSON(w, http.StatusOK, acc)
}

// handleStartDrain deactivates an account and moves all its chunks to the other
// accounts in the background, so it can then be removed.
func (h *DashboardHandler) handleStartDrain(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(r)
	if !ok {
		h.writeError(w, http.StatusBadRequest, "Invalid account id")
		return
	}
	acc, err := h.pool.DB().GetAccountByID(r.Context(), id)
	if err != nil {
		h.writeError(w, http.StatusNotFound, "Account not found")
		return
	}

	h.drainMu.Lock()
	if st := h.drains[id]; st != nil && st.Running {
		h.drainMu.Unlock()
		h.writeError(w, http.StatusConflict, "Ya hay un vaciado en curso para esta cuenta")
		return
	}
	total, _ := h.pool.DB().ChunkCountForAccount(r.Context(), id)
	st := &drainState{Running: true, Total: total}
	h.drains[id] = st
	h.drainMu.Unlock()

	acc.IsActive = false
	if err := h.pool.DB().UpdateAccount(r.Context(), acc); err != nil {
		h.drainMu.Lock()
		st.Running = false
		h.drainMu.Unlock()
		h.fail(w, http.StatusInternalServerError, "No se pudo desactivar la cuenta", err)
		return
	}

	h.pool.Go(func(ctx context.Context) {
		_, err := h.pool.DrainAccount(ctx, id, func(p storage.RekeyProgress) {
			h.drainMu.Lock()
			st.Done, st.Failed = p.Done, p.Failed
			h.drainMu.Unlock()
		})
		h.drainMu.Lock()
		st.Running = false
		if err != nil {
			st.Error = "El vaciado terminó con errores; consulta los registros del servidor."
			slog.Error("account drain finished with errors", "account", id, "err", err)
		}
		h.drainMu.Unlock()
	})

	h.writeJSON(w, http.StatusAccepted, st)
}

func (h *DashboardHandler) handleDrainStatus(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(r)
	if !ok {
		h.writeError(w, http.StatusBadRequest, "Invalid account id")
		return
	}
	h.drainMu.Lock()
	st, ok := h.drains[id]
	var snapshot drainState
	if ok {
		snapshot = *st
	}
	h.drainMu.Unlock()
	if !ok {
		remaining, _ := h.pool.DB().ChunkCountForAccount(r.Context(), id)
		snapshot = drainState{Total: remaining}
	}
	h.writeJSON(w, http.StatusOK, snapshot)
}

// --- Cache Buckets (Tier 1 S3 Cache) Handlers ---

func (h *DashboardHandler) handleListCacheBuckets(w http.ResponseWriter, r *http.Request) {
	buckets, err := h.pool.DB().ListCacheBuckets(r.Context())
	if err != nil {
		h.fail(w, http.StatusInternalServerError, "No se pudieron listar los buckets de caché", err)
		return
	}

	type safeCacheBucket struct {
		models.CacheBucket
		SecretKeyMasked string `json:"secret_key_masked"`
	}
	result := make([]safeCacheBucket, len(buckets))
	for i, b := range buckets {
		masked := "********"
		if len(b.SecretKey) > 4 {
			masked = "••••" + b.SecretKey[len(b.SecretKey)-4:]
		}
		b.SecretKey = ""
		result[i] = safeCacheBucket{CacheBucket: b, SecretKeyMasked: masked}
	}

	h.writeJSON(w, http.StatusOK, result)
}

// validEndpoint accepts only absolute http(s) URLs.
func validEndpoint(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && (u.Scheme == "https" || u.Scheme == "http") && u.Host != ""
}

func (h *DashboardHandler) handleCreateCacheBucket(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name       string `json:"name"`
		Endpoint   string `json:"endpoint"`
		Region     string `json:"region"`
		AccessKey  string `json:"access_key"`
		SecretKey  string `json:"secret_key"`
		BucketName string `json:"bucket_name"`
		QuotaBytes int64  `json:"quota_bytes"`
	}
	if err := decodeJSON(w, r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, "Invalid payload")
		return
	}

	if req.AccessKey == "" || req.SecretKey == "" || req.BucketName == "" {
		h.writeError(w, http.StatusBadRequest, "Access Key, Secret Key y Bucket Name son requeridos")
		return
	}

	if req.Name == "" {
		req.Name = req.BucketName
	}
	if req.Endpoint == "" {
		req.Endpoint = "https://s3.hf.co"
	}
	if !validEndpoint(req.Endpoint) {
		h.writeError(w, http.StatusBadRequest, "El endpoint debe ser una URL http(s) válida")
		return
	}
	if req.Region == "" {
		req.Region = "us-east-1"
	}
	if req.QuotaBytes <= 0 {
		req.QuotaBytes = 100 * 1024 * 1024 * 1024
	}

	cb := &models.CacheBucket{
		Name:       req.Name,
		Endpoint:   req.Endpoint,
		Region:     req.Region,
		AccessKey:  req.AccessKey,
		SecretKey:  req.SecretKey,
		BucketName: req.BucketName,
		QuotaBytes: req.QuotaBytes,
		UsedBytes:  0,
		IsActive:   true,
	}

	if err := h.pool.DB().CreateCacheBucket(r.Context(), cb); err != nil {
		h.fail(w, http.StatusInternalServerError, "Error guardando bucket de caché", err)
		return
	}

	cb.SecretKey = ""
	h.writeJSON(w, http.StatusCreated, cb)
}

func (h *DashboardHandler) handleToggleCacheBucket(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(r)
	if !ok {
		h.writeError(w, http.StatusBadRequest, "ID de bucket inválido")
		return
	}

	if err := h.pool.DB().ToggleCacheBucket(r.Context(), id); err != nil {
		h.fail(w, http.StatusInternalServerError, "No se pudo actualizar el bucket de caché", err)
		return
	}

	h.pool.InvalidateCacheClient(id)
	h.writeJSON(w, http.StatusOK, map[string]bool{"success": true})
}

func (h *DashboardHandler) handleDeleteCacheBucket(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(r)
	if !ok {
		h.writeError(w, http.StatusBadRequest, "ID de bucket inválido")
		return
	}

	if err := h.pool.DB().DeleteCacheBucket(r.Context(), id); err != nil {
		h.fail(w, http.StatusInternalServerError, "No se pudo eliminar el bucket de caché", err)
		return
	}

	h.pool.InvalidateCacheClient(id)
	h.writeJSON(w, http.StatusOK, map[string]bool{"success": true})
}

// --- Buckets ---

func (h *DashboardHandler) handleListBuckets(w http.ResponseWriter, r *http.Request) {
	buckets, err := h.pool.DB().ListBuckets(r.Context())
	if err != nil {
		h.fail(w, http.StatusInternalServerError, "No se pudieron listar los buckets", err)
		return
	}
	usage, err := h.pool.DB().BucketUsages(r.Context())
	if err != nil {
		h.fail(w, http.StatusInternalServerError, "No se pudo calcular el uso de los buckets", err)
		return
	}

	type BucketItem struct {
		models.Bucket
		ObjectCount int   `json:"object_count"`
		TotalBytes  int64 `json:"total_bytes"`
	}

	result := make([]BucketItem, 0, len(buckets))
	for _, b := range buckets {
		u := usage[b.Name]
		result = append(result, BucketItem{Bucket: b, ObjectCount: int(u.Objects), TotalBytes: u.Bytes})
	}

	h.writeJSON(w, http.StatusOK, result)
}

func (h *DashboardHandler) handleCreateBucket(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name string `json:"name"`
	}
	if err := decodeJSON(w, r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, "Invalid payload")
		return
	}

	bucketName := strings.TrimSpace(strings.ToLower(req.Name))
	if bucketName == "" {
		h.writeError(w, http.StatusBadRequest, "Bucket name is required")
		return
	}
	if err := models.ValidateBucketName(bucketName); err != nil {
		h.writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := h.pool.DB().CreateBucket(r.Context(), bucketName); err != nil {
		h.fail(w, http.StatusConflict, "No se pudo crear el bucket (¿ya existe?)", err)
		return
	}

	h.writeJSON(w, http.StatusCreated, map[string]string{"name": bucketName})
}

func (h *DashboardHandler) handleDeleteBucket(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := h.pool.DB().DeleteBucket(r.Context(), name); err != nil {
		if strings.Contains(err.Error(), "not empty") {
			h.writeError(w, http.StatusBadRequest, "El bucket no está vacío")
			return
		}
		h.fail(w, http.StatusBadRequest, "No se pudo eliminar el bucket", err)
		return
	}
	h.writeJSON(w, http.StatusOK, map[string]bool{"success": true})
}

// --- Objects ---

func (h *DashboardHandler) handleListObjects(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	bucket := q.Get("bucket")
	if bucket == "" {
		h.writeError(w, http.StatusBadRequest, "Bucket parameter is required")
		return
	}

	limit := objectListLimit
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	if limit > objectListMax {
		limit = objectListMax
	}

	objects, err := h.pool.DB().ListObjectsPage(r.Context(), bucket, q.Get("prefix"), q.Get("after"), false, limit)
	if err != nil {
		h.fail(w, http.StatusInternalServerError, "No se pudieron listar los objetos", err)
		return
	}
	if objects == nil {
		objects = []models.Object{}
	}
	if len(objects) == limit {
		w.Header().Set("X-Next-After", objects[len(objects)-1].Key)
	}

	h.writeJSON(w, http.StatusOK, objects)
}

func (h *DashboardHandler) handleGetObjectDetail(w http.ResponseWriter, r *http.Request) {
	bucket := r.URL.Query().Get("bucket")
	key := r.URL.Query().Get("key")

	obj, chunks, err := h.pool.DB().GetObjectWithChunks(r.Context(), bucket, key)
	if err != nil {
		h.writeError(w, http.StatusNotFound, "Object not found")
		return
	}

	type ChunkWithAccount struct {
		models.Chunk
		AccountName string `json:"account_name"`
	}

	names := map[int64]string{}
	chunkDetails := make([]ChunkWithAccount, 0, len(chunks))
	for _, c := range chunks {
		name, seen := names[c.AccountID]
		if !seen {
			name = "Unknown"
			if acc, err := h.pool.DB().GetAccountByID(r.Context(), c.AccountID); err == nil && acc != nil {
				name = acc.Name
			}
			names[c.AccountID] = name
		}
		chunkDetails = append(chunkDetails, ChunkWithAccount{Chunk: c, AccountName: name})
	}

	h.writeJSON(w, http.StatusOK, map[string]interface{}{
		"object": obj,
		"chunks": chunkDetails,
	})
}

// handleUploadObject streams the uploaded file straight into the pool. The
// form fields "bucket" (and optionally "key") must precede the "file" part.
func (h *DashboardHandler) handleUploadObject(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxUploadBytes)
	mr, err := r.MultipartReader()
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "Invalid multipart request")
		return
	}

	var bucket, key string
	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			h.writeError(w, http.StatusBadRequest, "No file uploaded")
			return
		}
		if err != nil {
			h.writeError(w, http.StatusBadRequest, "Invalid multipart request")
			return
		}

		switch part.FormName() {
		case "bucket":
			b, _ := io.ReadAll(io.LimitReader(part, 256))
			bucket = strings.TrimSpace(string(b))
		case "key":
			b, _ := io.ReadAll(io.LimitReader(part, maxKeyBytes+1))
			key = strings.TrimSpace(string(b))
		case "file":
			if bucket == "" {
				h.writeError(w, http.StatusBadRequest, "Bucket is required (send it before the file)")
				return
			}
			if key == "" {
				key = part.FileName()
			}
			if key == "" || len(key) > maxKeyBytes || strings.ContainsRune(key, 0) {
				h.writeError(w, http.StatusBadRequest, "Invalid object key")
				return
			}
			contentType := part.Header.Get("Content-Type")
			if contentType == "" {
				contentType = "application/octet-stream"
			}

			obj, err := h.pool.PutObject(r.Context(), bucket, key, contentType, part, -1, nil)
			switch {
			case errors.Is(err, storage.ErrBucketNotFound):
				h.writeError(w, http.StatusNotFound, "El bucket no existe")
			case errors.Is(err, storage.ErrPoolOutOfSpace):
				h.writeError(w, http.StatusInsufficientStorage, "No hay espacio suficiente en el pool de cuentas")
			case err != nil:
				h.fail(w, http.StatusInternalServerError, "La subida falló", err)
			default:
				h.writeJSON(w, http.StatusCreated, obj)
			}
			return
		default:
			_, _ = io.Copy(io.Discard, io.LimitReader(part, 1<<20))
		}
	}
}

// handleDownloadObject always serves the bytes as an opaque attachment: the
// stored Content-Type is client-controlled, and rendering it (HTML, SVG) on the
// console's origin would run attacker script with the admin's session.
func (h *DashboardHandler) handleDownloadObject(w http.ResponseWriter, r *http.Request) {
	bucket := r.URL.Query().Get("bucket")
	key := r.URL.Query().Get("key")

	obj, reader, err := h.pool.GetObject(r.Context(), bucket, key, nil)
	if err != nil {
		h.writeError(w, http.StatusNotFound, "Object not found")
		return
	}
	defer reader.Close()

	name := key
	if i := strings.LastIndexByte(name, '/'); i >= 0 {
		name = name[i+1:]
	}
	if disp := mime.FormatMediaType("attachment", map[string]string{"filename": name}); disp != "" {
		w.Header().Set("Content-Disposition", disp)
	} else {
		w.Header().Set("Content-Disposition", "attachment")
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Security-Policy", "sandbox")
	w.Header().Set("Content-Length", strconv.FormatInt(obj.Size, 10))
	w.WriteHeader(http.StatusOK)

	_, _ = io.Copy(w, reader)
}

func (h *DashboardHandler) handleDeleteObject(w http.ResponseWriter, r *http.Request) {
	bucket := r.URL.Query().Get("bucket")
	key := r.URL.Query().Get("key")

	if err := h.pool.DeleteObject(r.Context(), bucket, key); err != nil {
		h.fail(w, http.StatusInternalServerError, "No se pudo eliminar el objeto", err)
		return
	}

	h.writeJSON(w, http.StatusOK, map[string]bool{"success": true})
}

// --- Multi-Tier Cache Operations ---

type bucketKeyRequest struct {
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
}

func (h *DashboardHandler) handleEvictCache(w http.ResponseWriter, r *http.Request) {
	var req bucketKeyRequest
	if err := decodeJSON(w, r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, "Invalid payload")
		return
	}
	if req.Bucket == "" || req.Key == "" {
		h.writeError(w, http.StatusBadRequest, "Bucket and key are required")
		return
	}

	if err := h.pool.EvictCache(r.Context(), req.Bucket, req.Key); err != nil {
		h.fail(w, http.StatusInternalServerError, "No se pudo desalojar la caché", err)
		return
	}
	h.writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "Object evicted from Tier 1 Cache. Golden original copy in public dataset is preserved.",
	})
}

func (h *DashboardHandler) handlePromoteCache(w http.ResponseWriter, r *http.Request) {
	var req bucketKeyRequest
	if err := decodeJSON(w, r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, "Invalid payload")
		return
	}
	if req.Bucket == "" || req.Key == "" {
		h.writeError(w, http.StatusBadRequest, "Bucket and key are required")
		return
	}

	err := h.pool.PromoteToCache(r.Context(), req.Bucket, req.Key)
	switch {
	case errors.Is(err, storage.ErrNoCacheConfigured):
		h.writeError(w, http.StatusConflict, "No hay ningún bucket de caché configurado")
	case errors.Is(err, storage.ErrNoCacheSpace):
		h.writeError(w, http.StatusConflict, "Ningún bucket de caché tiene espacio libre suficiente")
	case errors.Is(err, storage.ErrObjectTooLarge):
		h.writeError(w, http.StatusConflict, "El objeto es demasiado grande para la caché")
	case errors.Is(err, db.ErrNotFound):
		h.writeError(w, http.StatusNotFound, "Object not found")
	case err != nil:
		h.fail(w, http.StatusInternalServerError, "No se pudo promover el objeto a la caché", err)
	default:
		h.writeJSON(w, http.StatusOK, map[string]interface{}{
			"success": true,
			"message": "Object successfully promoted to Tier 1 Cache.",
		})
	}
}

// --- Links -------------------------------------------------------------------

// externalBase returns the scheme and host clients use to reach the gateway.
func (h *DashboardHandler) externalBase(r *http.Request) (scheme, host string) {
	h.mu.RLock()
	pub := h.publicURL
	h.mu.RUnlock()
	if pub != "" {
		if u, err := url.Parse(pub); err == nil && u.Host != "" {
			return u.Scheme, u.Host
		}
	}
	scheme = "http"
	if h.adminAuth.IsSecureRequest(r) {
		scheme = "https"
	}
	host = r.Host
	if h.adminAuth.trustProxy {
		if fh := r.Header.Get("X-Forwarded-Host"); fh != "" {
			host = fh
		}
	}
	if host == "" {
		host = fmt.Sprintf("localhost:%d", h.serverPort)
	}
	return scheme, host
}

func (h *DashboardHandler) s3Credentials() (access, secret, region string) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.settings.AccessKeyID, h.settings.SecretAccessKey, h.settings.S3Region
}

// handlePresign returns a time-limited /media URL for an object. Anyone holding
// the URL can read that one object until it expires; nothing else is exposed.
func (h *DashboardHandler) handlePresign(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Bucket  string `json:"bucket"`
		Key     string `json:"key"`
		Seconds int64  `json:"expires_seconds"`
	}
	if err := decodeJSON(w, r, &req); err != nil || req.Bucket == "" || req.Key == "" {
		h.writeError(w, http.StatusBadRequest, "Bucket and key are required")
		return
	}
	if _, err := h.pool.DB().HeadObject(r.Context(), req.Bucket, req.Key); err != nil {
		h.writeError(w, http.StatusNotFound, "Object not found")
		return
	}

	expires := defaultPresign
	if req.Seconds > 0 {
		expires = time.Duration(req.Seconds) * time.Second
	}
	if expires > maxPresign {
		expires = maxPresign
	}

	access, secret, region := h.s3Credentials()
	scheme, host := h.externalBase(r)
	link := sigv4.PresignGET(scheme, host, "/media/"+req.Bucket+"/"+req.Key, access, secret, region, "s3", time.Now(), expires)

	h.writeJSON(w, http.StatusOK, map[string]interface{}{
		"url":        link,
		"expires_at": time.Now().Add(expires).UTC().Format(time.RFC3339),
	})
}

func (h *DashboardHandler) handleGetMediaInfo(w http.ResponseWriter, r *http.Request) {
	bucket := r.URL.Query().Get("bucket")
	key := r.URL.Query().Get("key")
	if bucket == "" || key == "" {
		h.writeError(w, http.StatusBadRequest, "Bucket and key are required")
		return
	}

	obj, err := h.pool.DB().HeadObject(r.Context(), bucket, key)
	if err != nil {
		h.writeError(w, http.StatusNotFound, "Object not found")
		return
	}

	var directURL string
	if obj.HasCache {
		directURL = h.pool.PresignedCacheURL(r.Context(), obj, 15*time.Minute)
	}

	h.writeJSON(w, http.StatusOK, map[string]interface{}{
		"object":              obj,
		"direct_download_url": directURL,
		"is_cached":           obj.HasCache,
		"is_cold":             obj.HasCold,
		"media_stream_url":    "/media/" + bucket + "/" + key,
	})
}

// --- Settings ---

func (h *DashboardHandler) handleGetSettings(w http.ResponseWriter, r *http.Request) {
	scheme, host := h.externalBase(r)
	endpoint := scheme + "://" + host

	h.mu.RLock()
	s := *h.settings
	env := make(map[string]bool, len(h.envManaged))
	for k, v := range h.envManaged {
		env[k] = v
	}
	h.mu.RUnlock()

	// Secrets are never returned: the snippets carry placeholders.
	const secretPlaceholder = "<SECRET_ACCESS_KEY>"
	rcloneConfig := fmt.Sprintf(`[hf2s3]
type = s3
provider = Other
env_auth = false
access_key_id = %s
secret_access_key = %s
endpoint = %s
region = %s`, s.AccessKeyID, secretPlaceholder, endpoint, s.S3Region)

	awsCLIConfig := fmt.Sprintf(`export AWS_ACCESS_KEY_ID="%s"
export AWS_SECRET_ACCESS_KEY="%s"
export AWS_ENDPOINT_URL="%s"

# List buckets
aws --endpoint-url=%s s3 ls

# Sync directory
aws --endpoint-url=%s s3 sync ./my-data s3://my-bucket/`, s.AccessKeyID, secretPlaceholder, endpoint, endpoint, endpoint)

	pythonSnippet := fmt.Sprintf(`import boto3

s3 = boto3.client(
    's3',
    endpoint_url='%s',
    aws_access_key_id='%s',
    aws_secret_access_key='%s',
    region_name='%s'
)

# Upload
s3.upload_file('local_file.pdf', 'my-bucket', 'remote_file.pdf')

# Time-limited link for a <video> tag
url = s3.generate_presigned_url('get_object', Params={'Bucket': 'my-bucket', 'Key': 'remote_file.pdf'}, ExpiresIn=3600)`,
		endpoint, s.AccessKeyID, secretPlaceholder, s.S3Region)

	kr := h.pool.Keyring()
	needRekey, _, _ := h.pool.DB().CountChunksToRekey(r.Context(), kr.CurrentKeyID())

	h.writeJSON(w, http.StatusOK, map[string]interface{}{
		"endpoint":              endpoint,
		"access_key_id":         s.AccessKeyID,
		"secret_access_key_set": s.SecretAccessKey != "",
		"s3_region":             s.S3Region,
		"chunk_size_mb":         s.ChunkSizeMB,
		"admin_username":        h.adminAuth.GetUsername(),
		"encryption":            "AES-256-GCM (per-chunk key derivation, chunk path bound as AAD)",
		"encryption_key_id":     kr.CurrentKeyID(),
		"legacy_keys_present":   kr.HasLegacyKeys(),
		"chunks_needing_rekey":  needRekey,
		"hf_storage_endpoint":   s.HFStorageEndpoint,
		"hf_storage_region":     s.HFStorageRegion,
		"hf_storage_access_key": s.HFStorageAccessKey,
		"hf_storage_secret_set": s.HFStorageSecretKey != "",
		"hf_storage_bucket":     s.HFStorageBucket,
		"hf_storage_configured": h.pool.HasCacheConfigured(r.Context()),
		"env_managed":           env,
		"snippets": map[string]string{
			"rclone": rcloneConfig,
			"awscli": awsCLIConfig,
			"python": pythonSnippet,
		},
	})
}

// refuseEnvManaged reports (and answers) an attempt to edit an env-managed setting.
func (h *DashboardHandler) refuseEnvManaged(w http.ResponseWriter, key string) bool {
	h.mu.RLock()
	managed := h.envManaged[key]
	h.mu.RUnlock()
	if managed {
		h.writeError(w, http.StatusConflict, "Este ajuste lo define una variable de entorno y no se puede cambiar desde la consola.")
	}
	return managed
}

func (h *DashboardHandler) handleUpdateSettings(w http.ResponseWriter, r *http.Request) {
	var req struct {
		AdminUsername      string `json:"admin_username"`
		AdminPassword      string `json:"admin_password"`
		CurrentPassword    string `json:"current_password"`
		AccessKeyID        string `json:"access_key_id"`
		S3Region           string `json:"s3_region"`
		ChunkSizeMB        int    `json:"chunk_size_mb"`
		HFStorageEndpoint  string `json:"hf_storage_endpoint"`
		HFStorageRegion    string `json:"hf_storage_region"`
		HFStorageAccessKey string `json:"hf_storage_access_key"`
		HFStorageSecretKey string `json:"hf_storage_secret_key"`
		HFStorageBucket    string `json:"hf_storage_bucket"`
	}
	if err := decodeJSON(w, r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, "Invalid payload")
		return
	}
	ctx := r.Context()
	dbase := h.pool.DB()

	// Admin credentials. Changing the password needs the current one.
	if req.AdminPassword != "" {
		if h.refuseEnvManaged(w, "admin_password") {
			return
		}
		if !h.adminAuth.VerifyPassword(req.CurrentPassword) {
			h.writeError(w, http.StatusForbidden, "La contraseña actual no es correcta")
			return
		}
		if err := ValidatePasswordStrength(req.AdminPassword); err != nil {
			h.writeError(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if req.AdminUsername != "" && h.refuseEnvManaged(w, "admin_username") {
		return
	}
	if req.AdminUsername != "" || req.AdminPassword != "" {
		if err := h.adminAuth.SetCredentials(req.AdminUsername, req.AdminPassword); err != nil {
			h.fail(w, http.StatusInternalServerError, "No se pudieron actualizar las credenciales", err)
			return
		}
		if req.AdminUsername != "" {
			_ = dbase.SetSetting(ctx, "admin_username", strings.TrimSpace(req.AdminUsername))
		}
		if req.AdminPassword != "" {
			_ = dbase.SetSetting(ctx, "admin_password", h.adminAuth.PasswordHash())
		}
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	// S3 access key id (the secret is only ever changed through rotation).
	if req.AccessKeyID != "" {
		if h.envManaged["access_key_id"] {
			h.writeError(w, http.StatusConflict, "Este ajuste lo define una variable de entorno y no se puede cambiar desde la consola.")
			return
		}
		h.settings.AccessKeyID = req.AccessKeyID
		_ = dbase.SetSetting(ctx, "access_key_id", req.AccessKeyID)
		if h.credentialsUpdater != nil {
			h.credentialsUpdater(req.AccessKeyID, "")
		}
	}
	if req.S3Region != "" && !h.envManaged["s3_region"] {
		h.settings.S3Region = req.S3Region
		_ = dbase.SetSetting(ctx, "s3_region", req.S3Region)
	}

	// Chunk size only affects new uploads.
	if req.ChunkSizeMB > 0 && !h.envManaged["chunk_size_mb"] {
		if req.ChunkSizeMB > 512 {
			h.writeError(w, http.StatusBadRequest, "El tamaño de fragmento máximo es 512 MB")
			return
		}
		h.settings.ChunkSizeMB = req.ChunkSizeMB
		_ = dbase.SetSetting(ctx, "chunk_size_mb", strconv.Itoa(req.ChunkSizeMB))
		h.pool.SetChunkSize(int64(req.ChunkSizeMB) * 1024 * 1024)
	}

	// Legacy single cache bucket (Tier 1).
	if req.HFStorageAccessKey != "" || req.HFStorageBucket != "" || req.HFStorageSecretKey != "" || req.HFStorageEndpoint != "" || req.HFStorageRegion != "" {
		if h.envManaged["hf_storage"] {
			h.writeError(w, http.StatusConflict, "Este ajuste lo define una variable de entorno y no se puede cambiar desde la consola.")
			return
		}
		endpoint := firstNonEmpty(req.HFStorageEndpoint, h.settings.HFStorageEndpoint, "https://s3.hf.co")
		if !validEndpoint(endpoint) {
			h.writeError(w, http.StatusBadRequest, "El endpoint debe ser una URL http(s) válida")
			return
		}
		region := firstNonEmpty(req.HFStorageRegion, h.settings.HFStorageRegion, "us-east-1")
		accKey := firstNonEmpty(req.HFStorageAccessKey, h.settings.HFStorageAccessKey)
		secKey := firstNonEmpty(req.HFStorageSecretKey, h.settings.HFStorageSecretKey)
		bName := firstNonEmpty(req.HFStorageBucket, h.settings.HFStorageBucket)

		h.pool.SetCacheClient(hfstorage.NewS3Client(hfstorage.S3ClientConfig{
			Endpoint: endpoint, Region: region, AccessKey: accKey, SecretKey: secKey, Bucket: bName,
		}))
		h.settings.HFStorageEndpoint = endpoint
		h.settings.HFStorageRegion = region
		h.settings.HFStorageAccessKey = accKey
		h.settings.HFStorageSecretKey = secKey
		h.settings.HFStorageBucket = bName

		_ = dbase.SetSetting(ctx, "hf_storage_endpoint", endpoint)
		_ = dbase.SetSetting(ctx, "hf_storage_region", region)
		_ = dbase.SetSetting(ctx, "hf_storage_access_key", accKey)
		_ = dbase.SetSetting(ctx, "hf_storage_secret_key", secKey)
		_ = dbase.SetSetting(ctx, "hf_storage_bucket", bName)
	}

	h.writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "Configuración actualizada y persistida correctamente",
	})
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// handleRotateS3Secret generates a new random S3 secret and returns it once.
func (h *DashboardHandler) handleRotateS3Secret(w http.ResponseWriter, r *http.Request) {
	if h.refuseEnvManaged(w, "secret_access_key") {
		return
	}
	raw := make([]byte, minS3SecretBytes)
	if _, err := rand.Read(raw); err != nil {
		h.fail(w, http.StatusInternalServerError, "No se pudo generar el secreto", err)
		return
	}
	secret := base64.RawURLEncoding.EncodeToString(raw)

	if err := h.pool.DB().SetSetting(r.Context(), "secret_access_key", secret); err != nil {
		h.fail(w, http.StatusInternalServerError, "No se pudo guardar el secreto", err)
		return
	}
	h.mu.Lock()
	h.settings.SecretAccessKey = secret
	access := h.settings.AccessKeyID
	updater := h.credentialsUpdater
	h.mu.Unlock()
	if updater != nil {
		updater("", secret)
	}

	h.writeJSON(w, http.StatusOK, map[string]string{
		"access_key_id":     access,
		"secret_access_key": secret,
		"note":              "Guarda este secreto ahora: no volverá a mostrarse.",
	})
}
