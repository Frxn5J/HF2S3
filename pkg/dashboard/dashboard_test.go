package dashboard

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"hf2s3/pkg/crypto"
	"hf2s3/pkg/db"
	"hf2s3/pkg/hfclient"
	"hf2s3/pkg/models"
	"hf2s3/pkg/storage"
)

func setupTestDashboard(t *testing.T) (*DashboardHandler, *http.ServeMux, string, func()) {
	t.Helper()
	database, err := db.Open(":memory:")
	if err != nil {
		t.Fatalf("Open test db failed: %v", err)
	}

	client := hfclient.NewClient()
	masterKey := crypto.DeriveKey("dashboard-test-key")
	pool := storage.NewPoolManager(database, client, masterKey, 1024*1024)

	settings := &models.SystemSettings{
		AccessKeyID:     "dash-key",
		SecretAccessKey: "dash-secret",
		MasterKey:       "dash-passphrase",
		ChunkSizeMB:     32,
		S3Region:        "us-east-1",
	}

	adminAuth := NewAdminAuthManager("admin", "admin123")
	token, _ := adminAuth.Login("admin", "admin123")

	handler := NewDashboardHandler(pool, settings, 8080, adminAuth)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	cleanup := func() {
		_ = database.Close()
	}

	return handler, mux, token, cleanup
}

func TestDashboardAuthenticationFlow(t *testing.T) {
	_, mux, validToken, cleanup := setupTestDashboard(t)
	defer cleanup()

	// 1. Unauthenticated request to protected endpoint should be rejected (401)
	req := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("Expected 401 Unauthorized without auth, got %d", rec.Code)
	}

	// 2. Login with incorrect password should fail (401)
	badLogin, _ := json.Marshal(map[string]string{"username": "admin", "password": "wrongpassword"})
	req = httptest.NewRequest(http.MethodPost, "/api/auth/login", bytes.NewReader(badLogin))
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("Expected 401 for bad password, got %d", rec.Code)
	}

	// 3. Login with correct password should succeed (200)
	goodLogin, _ := json.Marshal(map[string]string{"username": "admin", "password": "admin123"})
	req = httptest.NewRequest(http.MethodPost, "/api/auth/login", bytes.NewReader(goodLogin))
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("Expected 200 OK for valid login, got %d", rec.Code)
	}
	var loginResp map[string]string
	_ = json.NewDecoder(rec.Body).Decode(&loginResp)
	if loginResp["token"] == "" {
		t.Fatalf("Expected token in login response, got empty")
	}

	// 4. Authenticated request with token header should succeed (200)
	req = httptest.NewRequest(http.MethodGet, "/api/stats", nil)
	req.Header.Set("X-Admin-Token", validToken)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("Expected 200 OK with valid token, got %d", rec.Code)
	}

	// 5. Auth check endpoint
	req = httptest.NewRequest(http.MethodGet, "/api/auth/check", nil)
	req.Header.Set("X-Admin-Token", validToken)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("Expected 200 OK for auth check, got %d", rec.Code)
	}
	var checkResp map[string]interface{}
	_ = json.NewDecoder(rec.Body).Decode(&checkResp)
	if checkResp["authenticated"] != true {
		t.Fatalf("Expected authenticated true, got: %v", checkResp)
	}
}

func TestDashboardDatabaseBackupDownload(t *testing.T) {
	_, mux, token, cleanup := setupTestDashboard(t)
	defer cleanup()

	// Download backup with admin token
	req := httptest.NewRequest(http.MethodGet, "/api/admin/backup", nil)
	req.Header.Set("X-Admin-Token", token)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected 200 OK from /api/admin/backup, got %d (body: %s)", rec.Code, rec.Body.String())
	}

	if rec.Header().Get("Content-Type") != "application/vnd.sqlite3" {
		t.Fatalf("Expected Content-Type application/vnd.sqlite3, got %s", rec.Header().Get("Content-Type"))
	}

	if rec.Body.Len() == 0 {
		t.Fatalf("Expected non-empty backup database download")
	}
}

func TestDashboardStatsAndBucketsAPI(t *testing.T) {
	_, mux, token, cleanup := setupTestDashboard(t)
	defer cleanup()

	// 1. Get Stats (initially 0)
	req := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
	req.Header.Set("X-Admin-Token", token)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected 200 OK from /api/stats, got %d", rec.Code)
	}

	var stats models.PoolStats
	_ = json.NewDecoder(rec.Body).Decode(&stats)
	if stats.TotalBuckets != 0 {
		t.Fatalf("Expected 0 buckets, got %d", stats.TotalBuckets)
	}

	// 2. Create Bucket via Dashboard
	payload, _ := json.Marshal(map[string]string{"name": "production-data"})
	req = httptest.NewRequest(http.MethodPost, "/api/buckets", bytes.NewReader(payload))
	req.Header.Set("X-Admin-Token", token)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("Expected 201 Created from POST /api/buckets, got %d", rec.Code)
	}

	// 3. List Buckets via Dashboard
	req = httptest.NewRequest(http.MethodGet, "/api/buckets", nil)
	req.Header.Set("X-Admin-Token", token)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected 200 OK from GET /api/buckets, got %d", rec.Code)
	}

	var buckets []map[string]interface{}
	_ = json.NewDecoder(rec.Body).Decode(&buckets)
	if len(buckets) != 1 {
		t.Fatalf("Expected 1 bucket, got %d", len(buckets))
	}

	// 4. Get Settings
	req = httptest.NewRequest(http.MethodGet, "/api/settings", nil)
	req.Header.Set("X-Admin-Token", token)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected 200 OK from GET /api/settings, got %d", rec.Code)
	}
	var setResp map[string]interface{}
	_ = json.NewDecoder(rec.Body).Decode(&setResp)
	if setResp["access_key_id"] != "dash-key" {
		t.Fatalf("Expected access_key_id dash-key, got %v", setResp["access_key_id"])
	}
}

func TestStaticAssetsServing(t *testing.T) {
	_, mux, _, cleanup := setupTestDashboard(t)
	defer cleanup()

	// Check index.html serving (public)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected 200 OK from root index.html, got %d", rec.Code)
	}
	if !bytes.Contains(rec.Body.Bytes(), []byte("HF2S3")) {
		t.Fatalf("Expected index.html to contain HF2S3 branding")
	}

	// Check style.css serving (public)
	req = httptest.NewRequest(http.MethodGet, "/style.css", nil)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected 200 OK from /style.css, got %d", rec.Code)
	}
}
