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

func TestUpdateAllSettingsViaAPI(t *testing.T) {
	handler, mux, token, cleanup := setupTestDashboard(t)
	defer cleanup()

	var updatedKey, updatedSecret string
	handler.SetCredentialsUpdater(func(k, s string) {
		if k != "" {
			updatedKey = k
		}
		if s != "" {
			updatedSecret = s
		}
	})

	// 1. Post new settings configuring EVERYTHING
	updatePayload := map[string]interface{}{
		"admin_username":         "superadmin",
		"admin_password":         "superpass456",
		"access_key_id":          "new-s3-key",
		"secret_access_key":      "new-s3-secret",
		"s3_region":              "eu-west-1",
		"master_key":             "my-new-aes-secret-key-32chars!!",
		"chunk_size_mb":          64,
		"hf_storage_endpoint":    "https://s3.hf.co",
		"hf_storage_region":      "us-east-1",
		"hf_storage_access_key":  "HFAKnewStorageKey",
		"hf_storage_secret_key":  "newStorageSecret123",
		"hf_storage_bucket":      "my-custom-cache-bucket",
	}
	body, _ := json.Marshal(updatePayload)

	req := httptest.NewRequest(http.MethodPost, "/api/settings", bytes.NewReader(body))
	req.Header.Set("X-Admin-Token", token)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected 200 OK from POST /api/settings, got %d (body: %s)", rec.Code, rec.Body.String())
	}

	// Verify live credentials updater was invoked
	if updatedKey != "new-s3-key" || updatedSecret != "new-s3-secret" {
		t.Fatalf("Expected live credentials updater to receive new-s3-key and new-s3-secret, got k=%s s=%s", updatedKey, updatedSecret)
	}

	// 2. Fetch GET /api/settings and verify all fields updated
	req = httptest.NewRequest(http.MethodGet, "/api/settings", nil)
	req.Header.Set("X-Admin-Token", token)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected 200 OK from GET /api/settings, got %d", rec.Code)
	}
	var getResp map[string]interface{}
	_ = json.NewDecoder(rec.Body).Decode(&getResp)

	if getResp["admin_username"] != "superadmin" {
		t.Errorf("Expected admin_username superadmin, got %v", getResp["admin_username"])
	}
	if getResp["access_key_id"] != "new-s3-key" {
		t.Errorf("Expected access_key_id new-s3-key, got %v", getResp["access_key_id"])
	}
	if getResp["s3_region"] != "eu-west-1" {
		t.Errorf("Expected s3_region eu-west-1, got %v", getResp["s3_region"])
	}
	if getResp["master_key"] != "my-new-aes-secret-key-32chars!!" {
		t.Errorf("Expected master_key updated, got %v", getResp["master_key"])
	}
	if getResp["chunk_size_mb"] != float64(64) {
		t.Errorf("Expected chunk_size_mb 64, got %v", getResp["chunk_size_mb"])
	}
	if getResp["hf_storage_bucket"] != "my-custom-cache-bucket" {
		t.Errorf("Expected hf_storage_bucket my-custom-cache-bucket, got %v", getResp["hf_storage_bucket"])
	}

	// 3. Verify login works with the NEW admin credentials
	loginBody, _ := json.Marshal(map[string]string{
		"username": "superadmin",
		"password": "superpass456",
	})
	req = httptest.NewRequest(http.MethodPost, "/api/auth/login", bytes.NewReader(loginBody))
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected 200 OK logging in with new admin credentials, got %d", rec.Code)
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

func TestUpdateAccountDetails(t *testing.T) {
	handler, mux, validToken, cleanup := setupTestDashboard(t)
	defer cleanup()

	ctx := t.Context()
	acc := &models.Account{
		Name:       "Original Name",
		Username:   "testuser",
		Token:      "hf_sampletoken_12345678",
		RepoName:   "testuser/original-repo",
		QuotaBytes: 50 * 1024 * 1024 * 1024,
		IsActive:   true,
	}
	if err := handler.pool.DB().CreateAccount(ctx, acc); err != nil {
		t.Fatalf("Failed to create test account: %v", err)
	}

	// Update account details via PUT /api/accounts/{id}
	newQuota := int64(0) // Dynamic quota
	newActive := false
	updatePayload, _ := json.Marshal(map[string]interface{}{
		"name":      "Renamed Account",
		"repo_name": "testuser/original-repo",
		"quota_gb":  newQuota,
		"is_active": newActive,
	})

	req := httptest.NewRequest(http.MethodPut, "/api/accounts/1", bytes.NewReader(updatePayload))
	req.Header.Set("X-Admin-Token", validToken)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected 200 OK updating account, got %d: %s", rec.Code, rec.Body.String())
	}

	// Verify in DB
	updated, err := handler.pool.DB().GetAccountByID(ctx, 1)
	if err != nil {
		t.Fatalf("Failed to fetch updated account: %v", err)
	}
	if updated.Name != "Renamed Account" {
		t.Errorf("Expected Name Renamed Account, got %s", updated.Name)
	}
	if updated.QuotaBytes != 0 {
		t.Errorf("Expected QuotaBytes 0 (dynamic), got %d", updated.QuotaBytes)
	}
	if updated.IsActive != false {
		t.Errorf("Expected IsActive false, got %v", updated.IsActive)
	}

	// Verify via GET /api/accounts that rate_limit is included
	reqList := httptest.NewRequest(http.MethodGet, "/api/accounts", nil)
	reqList.Header.Set("X-Admin-Token", validToken)
	recList := httptest.NewRecorder()
	mux.ServeHTTP(recList, reqList)

	if recList.Code != http.StatusOK {
		t.Fatalf("Expected 200 OK listing accounts, got %d", recList.Code)
	}

	var listResp []AccountResponse
	if err := json.NewDecoder(recList.Body).Decode(&listResp); err != nil {
		t.Fatalf("Failed to decode accounts list: %v", err)
	}
	if len(listResp) != 1 {
		t.Fatalf("Expected 1 account, got %d", len(listResp))
	}
	if listResp[0].RateLimit.APILimit != 1000 || listResp[0].RateLimit.APIRemaining != 1000 {
		t.Errorf("Expected 1000/1000 API in rate_limit, got %d/%d",
			listResp[0].RateLimit.APIRemaining, listResp[0].RateLimit.APILimit)
	}
}
