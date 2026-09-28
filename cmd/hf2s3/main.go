package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"hf2s3/pkg/crypto"
	"hf2s3/pkg/dashboard"
	"hf2s3/pkg/db"
	"hf2s3/pkg/hfclient"
	"hf2s3/pkg/hfstorage"
	"hf2s3/pkg/models"
	"hf2s3/pkg/s3api"
	"hf2s3/pkg/storage"
)

func loadEnvFile(paths ...string) {
	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		lines := strings.Split(string(data), "\n")
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			parts := strings.SplitN(line, "=", 2)
			if len(parts) != 2 {
				continue
			}
			key := strings.TrimSpace(parts[0])
			val := strings.TrimSpace(parts[1])
			val = strings.Trim(val, `"'`)
			if os.Getenv(key) == "" {
				_ = os.Setenv(key, val)
			}
		}
		log.Printf("[HF2S3] Loaded configuration from %s", p)
		break
	}
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func main() {
	var exeDir string
	if len(os.Args) > 0 {
		exeDir = filepath.Dir(os.Args[0])
	}
	loadEnvFile(".env", filepath.Join(exeDir, ".env"), "../.env")
	portFlag := flag.Int("port", 8080, "Port to listen on for both S3 API and Web Dashboard")
	dbFlag := flag.String("db", "hf2s3_metadata.db", "Path to SQLite database file")
	chunkSizeFlag := flag.Int("chunk-size", 32, "Chunk size in megabytes for multi-account distribution")
	accessKeyFlag := flag.String("access-key", getEnv("HF2S3_ACCESS_KEY", "hf2s3-access-key"), "S3 Access Key ID")
	secretKeyFlag := flag.String("secret-key", getEnv("HF2S3_SECRET_KEY", "hf2s3-secret-key"), "S3 Secret Access Key")
	masterKeyFlag := flag.String("master-key", getEnv("HF2S3_MASTER_KEY", "hf2s3-aes-master-passphrase-2026"), "Master passphrase for AES-256-GCM chunk encryption")
	regionFlag := flag.String("region", getEnv("HF2S3_REGION", "us-east-1"), "S3 Region name")
	adminUserFlag := flag.String("admin-user", getEnv("ADMIN_USERNAME", getEnv("HF2S3_ADMIN_USER", "admin")), "Dashboard Admin Username")
	adminPassFlag := flag.String("admin-pass", getEnv("ADMIN_PASSWORD", getEnv("HF2S3_ADMIN_PASS", "admin123")), "Dashboard Admin Password")

	// Multi-Tier Cache (Hugging Face Storage Buckets s3.hf.co) flags
	hfStorageEndpointFlag := flag.String("hf-storage-endpoint", getEnv("HF_STORAGE_ENDPOINT", "https://s3.hf.co"), "Hugging Face Storage Bucket S3 Endpoint")
	hfStorageRegionFlag := flag.String("hf-storage-region", getEnv("HF_STORAGE_REGION", "us-east-1"), "Hugging Face Storage Bucket Region")
	hfStorageAccessKeyFlag := flag.String("hf-storage-access-key", getEnv("HF_STORAGE_ACCESS_KEY", ""), "Hugging Face Storage Bucket S3 Access Key (HFAK...)")
	hfStorageSecretKeyFlag := flag.String("hf-storage-secret-key", getEnv("HF_STORAGE_SECRET_KEY", ""), "Hugging Face Storage Bucket S3 Secret Key")
	hfStorageBucketFlag := flag.String("hf-storage-bucket", getEnv("HF_STORAGE_BUCKET", ""), "Hugging Face Storage Bucket name for Tier 1 Cache")
	flag.Parse()

	// Environment variable overrides
	if pStr := os.Getenv("PORT"); pStr != "" {
		if p, err := strconv.Atoi(pStr); err == nil {
			*portFlag = p
		}
	}
	if dbEnv := os.Getenv("HF2S3_DB"); dbEnv != "" {
		*dbFlag = dbEnv
	}

	log.Printf("[HF2S3] Starting Hugging Face Multi-Tier S3/R2 Cloud Gateway...")

	// Initialize SQLite metadata database
	database, err := db.Open(*dbFlag)
	if err != nil {
		log.Fatalf("Fatal: Cannot initialize SQLite database at %s: %v", *dbFlag, err)
	}
	defer database.Close()
	log.Printf("[HF2S3] SQLite metadata database ready: %s", *dbFlag)

	// System credential persistence from SQLite
	ctx := context.Background()
	persistedAccessKey, err := database.GetSetting(ctx, "access_key_id")
	if err == nil && persistedAccessKey != "" {
		*accessKeyFlag = persistedAccessKey
	} else {
		_ = database.SetSetting(ctx, "access_key_id", *accessKeyFlag)
	}

	persistedSecretKey, err := database.GetSetting(ctx, "secret_access_key")
	if err == nil && persistedSecretKey != "" {
		*secretKeyFlag = persistedSecretKey
	} else {
		_ = database.SetSetting(ctx, "secret_access_key", *secretKeyFlag)
	}

	persistedRegion, err := database.GetSetting(ctx, "s3_region")
	if err == nil && persistedRegion != "" {
		*regionFlag = persistedRegion
	} else {
		_ = database.SetSetting(ctx, "s3_region", *regionFlag)
	}

	// Persisted Admin Web Console credentials
	if val, err := database.GetSetting(ctx, "admin_username"); err == nil && val != "" {
		*adminUserFlag = val
	} else {
		_ = database.SetSetting(ctx, "admin_username", *adminUserFlag)
	}
	if val, err := database.GetSetting(ctx, "admin_password"); err == nil && val != "" {
		*adminPassFlag = val
	} else {
		_ = database.SetSetting(ctx, "admin_password", *adminPassFlag)
	}

	// Persisted AES Master Key
	if val, err := database.GetSetting(ctx, "master_key"); err == nil && val != "" {
		*masterKeyFlag = val
	} else {
		_ = database.SetSetting(ctx, "master_key", *masterKeyFlag)
	}

	// Persisted Chunk Size (MB)
	if val, err := database.GetSetting(ctx, "chunk_size_mb"); err == nil && val != "" {
		if cs, err := strconv.Atoi(val); err == nil && cs > 0 {
			*chunkSizeFlag = cs
		}
	} else {
		_ = database.SetSetting(ctx, "chunk_size_mb", strconv.Itoa(*chunkSizeFlag))
	}

	// Persisted multi-tier storage bucket cache settings
	if val, err := database.GetSetting(ctx, "hf_storage_endpoint"); err == nil && val != "" {
		*hfStorageEndpointFlag = val
	}
	if val, err := database.GetSetting(ctx, "hf_storage_region"); err == nil && val != "" {
		*hfStorageRegionFlag = val
	}
	if val, err := database.GetSetting(ctx, "hf_storage_access_key"); err == nil && val != "" {
		*hfStorageAccessKeyFlag = val
	}
	if val, err := database.GetSetting(ctx, "hf_storage_secret_key"); err == nil && val != "" {
		*hfStorageSecretKeyFlag = val
	}
	if val, err := database.GetSetting(ctx, "hf_storage_bucket"); err == nil && val != "" {
		*hfStorageBucketFlag = val
	}

	systemSettings := &models.SystemSettings{
		AccessKeyID:        *accessKeyFlag,
		SecretAccessKey:    *secretKeyFlag,
		MasterKey:          *masterKeyFlag,
		ChunkSizeMB:        *chunkSizeFlag,
		S3Region:           *regionFlag,
		HFStorageEndpoint:  *hfStorageEndpointFlag,
		HFStorageRegion:    *hfStorageRegionFlag,
		HFStorageAccessKey: *hfStorageAccessKeyFlag,
		HFStorageSecretKey: *hfStorageSecretKeyFlag,
		HFStorageBucket:    *hfStorageBucketFlag,
	}

	// HF client and AES key derivation
	hfClient := hfclient.NewClient()
	derivedKey := crypto.DeriveKey(*masterKeyFlag)
	chunkBytes := int64(*chunkSizeFlag) * 1024 * 1024

	// Storage pool manager
	pool := storage.NewPoolManager(database, hfClient, derivedKey, chunkBytes)

	// Configure Tier 1 Cache (Hugging Face Storage Bucket S3 client)
	cacheClient := hfstorage.NewS3Client(hfstorage.S3ClientConfig{
		Endpoint:  *hfStorageEndpointFlag,
		Region:    *hfStorageRegionFlag,
		AccessKey: *hfStorageAccessKeyFlag,
		SecretKey: *hfStorageSecretKeyFlag,
		Bucket:    *hfStorageBucketFlag,
	})
	if cacheClient.IsConfigured() {
		pool.SetCacheClient(cacheClient)
		log.Printf("[HF2S3] Tier 1 Cache active: HF Storage Bucket '%s' at %s", *hfStorageBucketFlag, *hfStorageEndpointFlag)
	} else {
		log.Printf("[HF2S3] Tier 1 Cache is in standby (configure via HF_STORAGE_* or Web Console)")
	}

	// S3 REST API service
	authManager := s3api.NewAuthManager(*accessKeyFlag, *secretKeyFlag)
	s3Server := s3api.NewServer(pool, authManager)

	// Web dashboard with admin session management
	adminAuth := dashboard.NewAdminAuthManager(*adminUserFlag, *adminPassFlag)
	dashboardHandler := dashboard.NewDashboardHandler(pool, systemSettings, *portFlag, adminAuth)
	dashboardHandler.SetCredentialsUpdater(authManager.UpdateCredentials)
	dashMux := http.NewServeMux()
	dashboardHandler.RegisterRoutes(dashMux)

	// Dispatcher routing web dashboard vs S3 API requests
	unifiedHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path

		// Dashboard API calls
		if strings.HasPrefix(path, "/api/") {
			dashMux.ServeHTTP(w, r)
			return
		}

		// Static assets for Web Console (CSS, JS, Fonts, Icons)
		if path == "/style.css" || path == "/app.js" || path == "/favicon.ico" || strings.HasPrefix(path, "/static/") {
			dashMux.ServeHTTP(w, r)
			return
		}

		// Root path "/":
		// If requested by a web browser (Accept includes text/html) and without AWS authorization headers,
		// serve the Web Console dashboard. Otherwise serve S3 ListBuckets.
		if path == "/" || path == "" {
			accept := r.Header.Get("Accept")
			auth := r.Header.Get("Authorization")
			if strings.Contains(accept, "text/html") && !strings.HasPrefix(auth, "AWS") {
				dashMux.ServeHTTP(w, r)
				return
			}
			s3Server.ServeHTTP(w, r)
			return
		}

		// All other paths (/bucket, /bucket/key) -> S3 REST Server
		s3Server.ServeHTTP(w, r)
	})

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", *portFlag),
		Handler:           unifiedHandler,
		ReadHeaderTimeout: 30 * time.Second, // Protects against slowloris attacks without cutting off streaming body reads
		IdleTimeout:       120 * time.Second, // Keeps idle keep-alive connections ready for backend reuse
		MaxHeaderBytes:    1 << 20,           // 1 MB max request headers
	}

	// Print startup information
	stats, _ := database.GetStats(ctx)
	fmt.Println("--- HF2S3: Hugging Face Multi-Tier S3/R2 Cloud Gateway ---")
	fmt.Printf(" [Web Dashboard]  http://localhost:%d\n", *portFlag)
	fmt.Printf(" [S3 Endpoint]    http://localhost:%d\n", *portFlag)
	fmt.Printf(" [Media Stream]   http://localhost:%d/media/{bucket}/{key}\n", *portFlag)
	fmt.Printf(" [S3 Region]      %s\n", *regionFlag)
	fmt.Printf(" [Access Key]     %s\n", *accessKeyFlag)
	fmt.Printf(" [Secret Key]     %s\n", *secretKeyFlag)
	if cacheClient.IsConfigured() {
		fmt.Printf(" [Tier 1 Cache]   HF Storage Bucket: %s (Direct-to-Client 302 Download)\n", *hfStorageBucketFlag)
	} else {
		fmt.Printf(" [Tier 1 Cache]   Standby (Configure HF Storage Bucket via Dashboard)\n")
	}
	fmt.Printf(" [Tier 2 Cold]    Public Datasets Hub (AES-256-GCM Encrypted Master Copy)\n")
	fmt.Printf(" [Admin Login]    User: %s (Configured via ADMIN_USERNAME / ADMIN_PASSWORD)\n", *adminUserFlag)
	if stats != nil {
		fmt.Printf(" [Storage Stats]  %d object(s) [%d in cache, %d in cold]\n", stats.TotalObjects, stats.CachedObjects, stats.ColdObjects)
	}
	fmt.Println("=================================================================")

	// Graceful Shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server ListenAndServe error: %v", err)
		}
	}()

	<-stop
	log.Println("[HF2S3] Shutting down server gracefully...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("[HF2S3] Server shutdown error: %v", err)
	}
	log.Println("[HF2S3] Server stopped successfully.")
}
