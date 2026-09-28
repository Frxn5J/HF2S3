package dashboard

import (
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"strconv"
	"strings"
	"time"

	"hf2s3/pkg/crypto"
	"hf2s3/pkg/hfclient"
	"hf2s3/pkg/hfstorage"
	"hf2s3/pkg/models"
	"hf2s3/pkg/storage"
)

//go:embed static/*
var staticFS embed.FS

type DashboardHandler struct {
	pool               *storage.PoolManager
	settings           *models.SystemSettings
	adminAuth          *AdminAuthManager
	serverPort         int
	fileServer         http.Handler
	credentialsUpdater func(accessKey, secretKey string)
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
	}
}

func (h *DashboardHandler) SetCredentialsUpdater(fn func(accessKey, secretKey string)) {
	h.credentialsUpdater = fn
}

func (h *DashboardHandler) RegisterRoutes(mux *http.ServeMux) {
	// Public endpoints (Auth & Health)
	mux.HandleFunc("POST /api/auth/login", h.handleLogin)
	mux.HandleFunc("GET /api/auth/check", h.handleAuthCheck)
	mux.HandleFunc("POST /api/auth/logout", h.handleLogout)
	mux.HandleFunc("GET /api/health", h.handleHealth)

	// Protected Admin Management endpoints
	mux.HandleFunc("GET /api/stats", h.adminAuth.RequireAuth(h.handleGetStats))
	mux.HandleFunc("GET /api/accounts", h.adminAuth.RequireAuth(h.handleListAccounts))
	mux.HandleFunc("POST /api/accounts", h.adminAuth.RequireAuth(h.handleCreateAccount))
	mux.HandleFunc("POST /api/accounts/{id}/toggle", h.adminAuth.RequireAuth(h.handleToggleAccount))
	mux.HandleFunc("POST /api/accounts/{id}/sync", h.adminAuth.RequireAuth(h.handleSyncAccount))
	mux.HandleFunc("POST /api/accounts/{id}/quota", h.adminAuth.RequireAuth(h.handleUpdateAccountQuota))
	mux.HandleFunc("DELETE /api/accounts/{id}", h.adminAuth.RequireAuth(h.handleDeleteAccount))

	// Multi-Bucket S3 Cache endpoints
	mux.HandleFunc("GET /api/cache-buckets", h.adminAuth.RequireAuth(h.handleListCacheBuckets))
	mux.HandleFunc("POST /api/cache-buckets", h.adminAuth.RequireAuth(h.handleCreateCacheBucket))
	mux.HandleFunc("POST /api/cache-buckets/{id}/toggle", h.adminAuth.RequireAuth(h.handleToggleCacheBucket))
	mux.HandleFunc("DELETE /api/cache-buckets/{id}", h.adminAuth.RequireAuth(h.handleDeleteCacheBucket))

	mux.HandleFunc("GET /api/buckets", h.adminAuth.RequireAuth(h.handleListBuckets))
	mux.HandleFunc("POST /api/buckets", h.adminAuth.RequireAuth(h.handleCreateBucket))
	mux.HandleFunc("DELETE /api/buckets/{name}", h.adminAuth.RequireAuth(h.handleDeleteBucket))

	mux.HandleFunc("GET /api/objects", h.adminAuth.RequireAuth(h.handleListObjects))
	mux.HandleFunc("GET /api/objects/detail", h.adminAuth.RequireAuth(h.handleGetObjectDetail))
	mux.HandleFunc("POST /api/objects/upload", h.adminAuth.RequireAuth(h.handleUploadObject))
	mux.HandleFunc("GET /api/objects/download", h.adminAuth.RequireAuth(h.handleDownloadObject))
	mux.HandleFunc("DELETE /api/objects", h.adminAuth.RequireAuth(h.handleDeleteObject))

	// Multi-Tier Cache routes
	mux.HandleFunc("POST /api/objects/evict-cache", h.adminAuth.RequireAuth(h.handleEvictCache))
	mux.HandleFunc("POST /api/objects/promote-cache", h.adminAuth.RequireAuth(h.handlePromoteCache))
	mux.HandleFunc("GET /api/media/info", h.handleGetMediaInfo)

	mux.HandleFunc("GET /api/settings", h.adminAuth.RequireAuth(h.handleGetSettings))
	mux.HandleFunc("POST /api/settings", h.adminAuth.RequireAuth(h.handleUpdateSettings))

	// Database Backup & Restore endpoints
	mux.HandleFunc("GET /api/admin/backup", h.adminAuth.RequireAuth(h.handleDownloadBackup))
	mux.HandleFunc("POST /api/admin/restore", h.adminAuth.RequireAuth(h.handleRestoreBackup))

	// Static UI assets and SPA fallback
	mux.Handle("/", h.fileServer)
}

func (h *DashboardHandler) writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func (h *DashboardHandler) writeError(w http.ResponseWriter, status int, message string) {
	h.writeJSON(w, status, map[string]string{"error": message})
}

// --- Auth Endpoints ---

func (h *DashboardHandler) handleLogin(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	token, err := h.adminAuth.Login(req.Username, req.Password)
	if err != nil {
		h.writeError(w, http.StatusUnauthorized, "Credenciales de administrador inválidas")
		return
	}

	http.SetCookie(w, &http.Cookie{
		Name:     "hf2s3_session",
		Value:    token,
		Path:     "/",
		Expires:  time.Now().Add(24 * time.Hour),
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
	})

	h.writeJSON(w, http.StatusOK, map[string]interface{}{
		"token":    token,
		"username": req.Username,
	})
}

func (h *DashboardHandler) handleAuthCheck(w http.ResponseWriter, r *http.Request) {
	if !h.adminAuth.ValidateRequest(r) {
		h.writeJSON(w, http.StatusOK, map[string]interface{}{
			"authenticated": false,
		})
		return
	}
	h.writeJSON(w, http.StatusOK, map[string]interface{}{
		"authenticated": true,
		"username":      h.adminAuth.username,
	})
}

func (h *DashboardHandler) handleLogout(w http.ResponseWriter, r *http.Request) {
	var token string
	if cookie, err := r.Cookie("hf2s3_session"); err == nil {
		token = cookie.Value
	}
	if token == "" {
		token = r.Header.Get("X-Admin-Token")
	}
	h.adminAuth.Logout(token)

	http.SetCookie(w, &http.Cookie{
		Name:     "hf2s3_session",
		Value:    "",
		Path:     "/",
		Expires:  time.Unix(0, 0),
		HttpOnly: true,
	})

	h.writeJSON(w, http.StatusOK, map[string]bool{"success": true})
}

func (h *DashboardHandler) handleHealth(w http.ResponseWriter, r *http.Request) {
	h.writeJSON(w, http.StatusOK, map[string]string{
		"status":  "ok",
		"service": "hf2s3",
		"time":    time.Now().UTC().Format(time.RFC3339),
	})
}

// --- Database Backup & Restore ---

func (h *DashboardHandler) handleDownloadBackup(w http.ResponseWriter, r *http.Request) {
	filename := fmt.Sprintf("hf2s3_backup_%s.db", time.Now().Format("2006-01-02_150405"))
	w.Header().Set("Content-Type", "application/vnd.sqlite3")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", filename))

	if err := h.pool.DB().ExportBackup(r.Context(), w); err != nil {
		h.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Backup export failed: %v", err))
		return
	}
}

func (h *DashboardHandler) handleRestoreBackup(w http.ResponseWriter, r *http.Request) {
	// Parse 250MB max file
	if err := r.ParseMultipartForm(32 * 1024 * 1024); err != nil {
		h.writeError(w, http.StatusBadRequest, "Invalid form data")
		return
	}

	file, _, err := r.FormFile("database")
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "No database file provided")
		return
	}
	defer file.Close()

	if err := h.pool.DB().ImportRestore(r.Context(), file); err != nil {
		h.writeError(w, http.StatusBadRequest, fmt.Sprintf("Restore failed: %v", err))
		return
	}

	h.writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "Base de datos restaurada correctamente",
	})
}

// --- Stats ---

func (h *DashboardHandler) handleGetStats(w http.ResponseWriter, r *http.Request) {
	stats, err := h.pool.DB().GetStats(r.Context())
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.writeJSON(w, http.StatusOK, stats)
}

// --- Accounts ---

type AccountResponse struct {
	models.Account
	RateLimit hfclient.RateLimitStats `json:"rate_limit"`
}

func (h *DashboardHandler) handleListAccounts(w http.ResponseWriter, r *http.Request) {
	accounts, err := h.pool.DB().ListAccounts(r.Context())
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	result := make([]AccountResponse, len(accounts))
	for i, acc := range accounts {
		stats := h.pool.HFClient().GetRateLimitStats(acc.Token)
		if len(acc.Token) > 8 {
			acc.Token = acc.Token[:4] + "..." + acc.Token[len(acc.Token)-4:]
		}
		result[i] = AccountResponse{
			Account:   acc,
			RateLimit: stats,
		}
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
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	token := strings.TrimSpace(req.Token)
	if token == "" {
		h.writeError(w, http.StatusBadRequest, "Hugging Face token is required")
		return
	}

	// Verify token identity via Hugging Face whoami
	whoami, err := h.pool.HFClient().VerifyToken(r.Context(), token)
	if err != nil {
		h.writeError(w, http.StatusUnauthorized, fmt.Sprintf("Invalid Hugging Face token: %v", err))
		return
	}

	// Resolve target repository identifier
	repoName := strings.TrimSpace(req.RepoName)
	if repoName == "" {
		repoName = "hf2s3-vault"
	}
	var fullRepoID string
	if strings.Contains(repoName, "/") {
		fullRepoID = repoName
	} else {
		fullRepoID = fmt.Sprintf("%s/%s", whoami.Name, repoName)
	}

	// Ensure private dataset repository exists
	if err := h.pool.HFClient().EnsureDatasetRepo(r.Context(), token, repoName); err != nil {
		h.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to prepare dataset repository: %v", err))
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

	// Check tree size
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
		h.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Save account to DB: %v", err))
		return
	}

	h.writeJSON(w, http.StatusCreated, acc)
}

func (h *DashboardHandler) handleToggleAccount(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
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
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.writeJSON(w, http.StatusOK, acc)
}

func (h *DashboardHandler) handleSyncAccount(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
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

	h.writeJSON(w, http.StatusOK, acc)
}

func (h *DashboardHandler) handleDeleteAccount(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "Invalid account id")
		return
	}

	if err := h.pool.DB().DeleteAccount(r.Context(), id); err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.writeJSON(w, http.StatusOK, map[string]bool{"success": true})
}

func (h *DashboardHandler) handleUpdateAccountQuota(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "Invalid account id")
		return
	}

	var req struct {
		QuotaGB int64 `json:"quota_gb"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "Invalid payload")
		return
	}

	var quotaBytes int64
	if req.QuotaGB > 0 {
		quotaBytes = req.QuotaGB * 1024 * 1024 * 1024
	}

	if err := h.pool.DB().UpdateAccountQuota(r.Context(), id, quotaBytes); err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	acc, err := h.pool.DB().GetAccountByID(r.Context(), id)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.writeJSON(w, http.StatusOK, acc)
}

// --- Cache Buckets (Tier 1 S3 Cache) Handlers ---

func (h *DashboardHandler) handleListCacheBuckets(w http.ResponseWriter, r *http.Request) {
	buckets, err := h.pool.DB().ListCacheBuckets(r.Context())
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
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
		result[i] = safeCacheBucket{
			CacheBucket:     b,
			SecretKeyMasked: masked,
		}
	}

	h.writeJSON(w, http.StatusOK, result)
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
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
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
		h.writeError(w, http.StatusInternalServerError, "Error guardando bucket de caché: "+err.Error())
		return
	}

	h.writeJSON(w, http.StatusCreated, cb)
}

func (h *DashboardHandler) handleToggleCacheBucket(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "ID de bucket inválido")
		return
	}

	if err := h.pool.DB().ToggleCacheBucket(r.Context(), id); err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.pool.InvalidateCacheClient(id)
	h.writeJSON(w, http.StatusOK, map[string]bool{"success": true})
}

func (h *DashboardHandler) handleDeleteCacheBucket(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "ID de bucket inválido")
		return
	}

	if err := h.pool.DB().DeleteCacheBucket(r.Context(), id); err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.pool.InvalidateCacheClient(id)
	h.writeJSON(w, http.StatusOK, map[string]bool{"success": true})
}

// --- Buckets ---

func (h *DashboardHandler) handleListBuckets(w http.ResponseWriter, r *http.Request) {
	buckets, err := h.pool.DB().ListBuckets(r.Context())
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	type BucketItem struct {
		models.Bucket
		ObjectCount int   `json:"object_count"`
		TotalBytes  int64 `json:"total_bytes"`
	}

	var result []BucketItem
	for _, b := range buckets {
		objs, _ := h.pool.DB().ListObjects(r.Context(), b.Name, "", "", 10000)
		var totalSize int64
		for _, o := range objs {
			totalSize += o.Size
		}
		result = append(result, BucketItem{
			Bucket:      b,
			ObjectCount: len(objs),
			TotalBytes:  totalSize,
		})
	}

	h.writeJSON(w, http.StatusOK, result)
}

func (h *DashboardHandler) handleCreateBucket(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "Invalid payload")
		return
	}

	bucketName := strings.TrimSpace(strings.ToLower(req.Name))
	if bucketName == "" {
		h.writeError(w, http.StatusBadRequest, "Bucket name is required")
		return
	}

	if err := h.pool.DB().CreateBucket(r.Context(), bucketName); err != nil {
		h.writeError(w, http.StatusConflict, fmt.Sprintf("Failed to create bucket: %v", err))
		return
	}

	h.writeJSON(w, http.StatusCreated, map[string]string{"name": bucketName})
}

func (h *DashboardHandler) handleDeleteBucket(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := h.pool.DB().DeleteBucket(r.Context(), name); err != nil {
		h.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	h.writeJSON(w, http.StatusOK, map[string]bool{"success": true})
}

// --- Objects ---

func (h *DashboardHandler) handleListObjects(w http.ResponseWriter, r *http.Request) {
	bucket := r.URL.Query().Get("bucket")
	prefix := r.URL.Query().Get("prefix")
	if bucket == "" {
		h.writeError(w, http.StatusBadRequest, "Bucket parameter is required")
		return
	}

	objects, err := h.pool.DB().ListObjects(r.Context(), bucket, prefix, "", 1000)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
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

	var chunkDetails []ChunkWithAccount
	for _, c := range chunks {
		acc, _ := h.pool.DB().GetAccountByID(r.Context(), c.AccountID)
		accName := "Unknown"
		if acc != nil {
			accName = acc.Name
		}
		chunkDetails = append(chunkDetails, ChunkWithAccount{
			Chunk:       c,
			AccountName: accName,
		})
	}

	h.writeJSON(w, http.StatusOK, map[string]interface{}{
		"object": obj,
		"chunks": chunkDetails,
	})
}

func (h *DashboardHandler) handleUploadObject(w http.ResponseWriter, r *http.Request) {
	// Parse 1GB max file upload in multipart form
	if err := r.ParseMultipartForm(1024 * 1024 * 32); err != nil {
		h.writeError(w, http.StatusBadRequest, "Parse multipart form error")
		return
	}

	bucket := r.FormValue("bucket")
	key := r.FormValue("key")
	if bucket == "" {
		h.writeError(w, http.StatusBadRequest, "Bucket is required")
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "No file uploaded")
		return
	}
	defer file.Close()

	if key == "" {
		key = header.Filename
	}

	contentType := header.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	obj, err := h.pool.PutObject(r.Context(), bucket, key, contentType, file, header.Size, nil)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.writeJSON(w, http.StatusCreated, obj)
}

func (h *DashboardHandler) handleDownloadObject(w http.ResponseWriter, r *http.Request) {
	bucket := r.URL.Query().Get("bucket")
	key := r.URL.Query().Get("key")

	obj, reader, err := h.pool.GetObject(r.Context(), bucket, key, nil)
	if err != nil {
		h.writeError(w, http.StatusNotFound, "Object not found")
		return
	}
	defer reader.Close()

	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", key))
	w.Header().Set("Content-Type", obj.ContentType)
	w.Header().Set("Content-Length", strconv.FormatInt(obj.Size, 10))
	w.WriteHeader(http.StatusOK)

	_, _ = io.Copy(w, reader)
}

func (h *DashboardHandler) handleDeleteObject(w http.ResponseWriter, r *http.Request) {
	bucket := r.URL.Query().Get("bucket")
	key := r.URL.Query().Get("key")

	if err := h.pool.DeleteObject(r.Context(), bucket, key); err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.writeJSON(w, http.StatusOK, map[string]bool{"success": true})
}

// --- Multi-Tier Cache Operations ---

func (h *DashboardHandler) handleEvictCache(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Bucket string `json:"bucket"`
		Key    string `json:"key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "Invalid payload")
		return
	}
	if req.Bucket == "" || req.Key == "" {
		h.writeError(w, http.StatusBadRequest, "Bucket and key are required")
		return
	}

	if err := h.pool.EvictCache(r.Context(), req.Bucket, req.Key); err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "Object evicted from Tier 1 Cache. Golden original copy in public dataset is preserved.",
	})
}

func (h *DashboardHandler) handlePromoteCache(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Bucket string `json:"bucket"`
		Key    string `json:"key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "Invalid payload")
		return
	}
	if req.Bucket == "" || req.Key == "" {
		h.writeError(w, http.StatusBadRequest, "Bucket and key are required")
		return
	}

	if err := h.pool.PromoteToCache(r.Context(), req.Bucket, req.Key); err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "Object successfully promoted to Tier 1 Cache.",
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
		loc, err := h.pool.DB().GetObjectLocationByTier(r.Context(), obj.ID, models.TierCache)
		if err == nil && loc != nil {
			client, _, err := h.pool.GetCacheClient(r.Context(), loc.AccountID)
			if err == nil && client != nil && client.IsConfigured() {
				directURL, _ = client.PresignGetObject(client.Bucket(), loc.RemotePath, 15*time.Minute)
			}
		}
	}

	h.writeJSON(w, http.StatusOK, map[string]interface{}{
		"object":              obj,
		"direct_download_url": directURL,
		"is_cached":           obj.HasCache,
		"is_cold":             obj.HasCold,
		"media_stream_url":    fmt.Sprintf("/media/%s/%s", bucket, key),
	})
}

// --- Settings ---

func (h *DashboardHandler) handleGetSettings(w http.ResponseWriter, r *http.Request) {
	host := r.Host
	if host == "" {
		host = fmt.Sprintf("localhost:%d", h.serverPort)
	}
	endpoint := fmt.Sprintf("http://%s", host)

	// Config snippets
	rcloneConfig := fmt.Sprintf(`[hf2s3]
type = s3
provider = Other
env_auth = false
access_key_id = %s
secret_access_key = %s
endpoint = %s
region = %s`, h.settings.AccessKeyID, h.settings.SecretAccessKey, endpoint, h.settings.S3Region)

	awsCLIConfig := fmt.Sprintf(`export AWS_ACCESS_KEY_ID="%s"
export AWS_SECRET_ACCESS_KEY="%s"
export AWS_ENDPOINT_URL="%s"

# List buckets
aws --endpoint-url=%s s3 ls

# Sync directory
aws --endpoint-url=%s s3 sync ./my-data s3://my-bucket/`, h.settings.AccessKeyID, h.settings.SecretAccessKey, endpoint, endpoint, endpoint)

	pythonSnippet := fmt.Sprintf(`import boto3

s3 = boto3.client(
    's3',
    endpoint_url='%s',
    aws_access_key_id='%s',
    aws_secret_access_key='%s',
    region_name='%s'
)

# Upload
s3.upload_file('local_file.pdf', 'my-bucket', 'remote_file.pdf')`, endpoint, h.settings.AccessKeyID, h.settings.SecretAccessKey, h.settings.S3Region)

	response := map[string]interface{}{
		"endpoint":              endpoint,
		"access_key_id":         h.settings.AccessKeyID,
		"secret_access_key":     h.settings.SecretAccessKey,
		"s3_region":             h.settings.S3Region,
		"chunk_size_mb":         h.settings.ChunkSizeMB,
		"master_key":            h.settings.MasterKey,
		"admin_username":        h.adminAuth.GetUsername(),
		"encryption":            "AES-256-GCM (Zero-Knowledge Authenticated)",
		"hf_storage_endpoint":   h.settings.HFStorageEndpoint,
		"hf_storage_region":     h.settings.HFStorageRegion,
		"hf_storage_access_key": h.settings.HFStorageAccessKey,
		"hf_storage_bucket":     h.settings.HFStorageBucket,
		"hf_storage_configured": h.pool.HasCacheConfigured(r.Context()),
		"snippets": map[string]string{
			"rclone": rcloneConfig,
			"awscli": awsCLIConfig,
			"python": pythonSnippet,
		},
	}

	h.writeJSON(w, http.StatusOK, response)
}

func (h *DashboardHandler) handleUpdateSettings(w http.ResponseWriter, r *http.Request) {
	var req struct {
		AdminUsername      string `json:"admin_username"`
		AdminPassword      string `json:"admin_password"`
		AccessKeyID        string `json:"access_key_id"`
		SecretAccessKey    string `json:"secret_access_key"`
		S3Region           string `json:"s3_region"`
		MasterKey          string `json:"master_key"`
		ChunkSizeMB        int    `json:"chunk_size_mb"`
		HFStorageEndpoint  string `json:"hf_storage_endpoint"`
		HFStorageRegion    string `json:"hf_storage_region"`
		HFStorageAccessKey string `json:"hf_storage_access_key"`
		HFStorageSecretKey string `json:"hf_storage_secret_key"`
		HFStorageBucket    string `json:"hf_storage_bucket"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "Invalid payload")
		return
	}

	// Update Admin Credentials
	if req.AdminUsername != "" || req.AdminPassword != "" {
		h.adminAuth.SetCredentials(req.AdminUsername, req.AdminPassword)
		if req.AdminUsername != "" {
			_ = h.pool.DB().SetSetting(r.Context(), "admin_username", req.AdminUsername)
		}
		if req.AdminPassword != "" {
			_ = h.pool.DB().SetSetting(r.Context(), "admin_password", req.AdminPassword)
		}
	}

	// Update S3 Credentials & Region
	if req.AccessKeyID != "" {
		h.settings.AccessKeyID = req.AccessKeyID
		_ = h.pool.DB().SetSetting(r.Context(), "access_key_id", req.AccessKeyID)
		if h.credentialsUpdater != nil {
			h.credentialsUpdater(req.AccessKeyID, "")
		}
	}
	if req.SecretAccessKey != "" {
		h.settings.SecretAccessKey = req.SecretAccessKey
		_ = h.pool.DB().SetSetting(r.Context(), "secret_access_key", req.SecretAccessKey)
		if h.credentialsUpdater != nil {
			h.credentialsUpdater("", req.SecretAccessKey)
		}
	}
	if req.S3Region != "" {
		h.settings.S3Region = req.S3Region
		_ = h.pool.DB().SetSetting(r.Context(), "s3_region", req.S3Region)
	}

	// Update Encryption Master Key (Zero-Knowledge)
	if req.MasterKey != "" {
		h.settings.MasterKey = req.MasterKey
		_ = h.pool.DB().SetSetting(r.Context(), "master_key", req.MasterKey)
		derivedKey := crypto.DeriveKey(req.MasterKey)
		h.pool.SetMasterKey(derivedKey)
	}

	// Update Cold Tier Dataset Chunk Size
	if req.ChunkSizeMB > 0 {
		h.settings.ChunkSizeMB = req.ChunkSizeMB
		_ = h.pool.DB().SetSetting(r.Context(), "chunk_size_mb", strconv.Itoa(req.ChunkSizeMB))
		h.pool.SetChunkSize(int64(req.ChunkSizeMB) * 1024 * 1024)
	}

	// Update HF Storage Bucket Cache (Tier 1)
	if req.HFStorageAccessKey != "" || req.HFStorageBucket != "" || req.HFStorageSecretKey != "" || req.HFStorageEndpoint != "" || req.HFStorageRegion != "" {
		endpoint := req.HFStorageEndpoint
		if endpoint == "" {
			endpoint = h.settings.HFStorageEndpoint
		}
		if endpoint == "" {
			endpoint = "https://s3.hf.co"
		}
		region := req.HFStorageRegion
		if region == "" {
			region = h.settings.HFStorageRegion
		}
		if region == "" {
			region = "us-east-1"
		}
		accKey := req.HFStorageAccessKey
		if accKey == "" {
			accKey = h.settings.HFStorageAccessKey
		}
		secKey := req.HFStorageSecretKey
		if secKey == "" {
			secKey = h.settings.HFStorageSecretKey
		}
		bName := req.HFStorageBucket
		if bName == "" {
			bName = h.settings.HFStorageBucket
		}

		newCache := hfstorage.NewS3Client(hfstorage.S3ClientConfig{
			Endpoint:  endpoint,
			Region:    region,
			AccessKey: accKey,
			SecretKey: secKey,
			Bucket:    bName,
		})
		h.pool.SetCacheClient(newCache)
		h.settings.HFStorageEndpoint = endpoint
		h.settings.HFStorageRegion = region
		h.settings.HFStorageAccessKey = accKey
		h.settings.HFStorageSecretKey = secKey
		h.settings.HFStorageBucket = bName

		_ = h.pool.DB().SetSetting(r.Context(), "hf_storage_endpoint", endpoint)
		_ = h.pool.DB().SetSetting(r.Context(), "hf_storage_region", region)
		_ = h.pool.DB().SetSetting(r.Context(), "hf_storage_access_key", accKey)
		_ = h.pool.DB().SetSetting(r.Context(), "hf_storage_secret_key", secKey)
		_ = h.pool.DB().SetSetting(r.Context(), "hf_storage_bucket", bName)
	}

	h.writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "Configuración actualizada y persistida correctamente",
	})
}
