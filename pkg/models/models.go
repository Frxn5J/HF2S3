package models

import (
	"time"
)

type TierType string

const (
	TierCache TierType = "cache" // Hugging Face Storage Bucket (s3.hf.co) - Unencrypted, direct client presigned download
	TierCold  TierType = "cold"  // Hugging Face Public Dataset - Encrypted AES-256-GCM, golden copy
)

type Account struct {
	ID          int64     `json:"id"`
	Name        string    `json:"name"`
	Username    string    `json:"username"`
	Token       string    `json:"token,omitempty"`
	RepoName    string    `json:"repo_name"`
	QuotaBytes  int64     `json:"quota_bytes"`
	UsedBytes   int64     `json:"used_bytes"`
	IsActive    bool      `json:"is_active"`
	IsPublic    bool      `json:"is_public"`
	S3AccessKey string    `json:"s3_access_key,omitempty"`
	S3SecretKey string    `json:"s3_secret_key,omitempty"`
	S3Endpoint  string    `json:"s3_endpoint,omitempty"`
	S3Bucket    string    `json:"s3_bucket,omitempty"`
	LastChecked time.Time `json:"last_checked"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type CacheBucket struct {
	ID         int64     `json:"id"`
	AccountID  int64     `json:"account_id"`
	Name       string    `json:"name"`
	Endpoint   string    `json:"endpoint"`
	Region     string    `json:"region"`
	AccessKey  string    `json:"access_key"`
	SecretKey  string    `json:"secret_key,omitempty"`
	BucketName string    `json:"bucket_name"`
	QuotaBytes int64     `json:"quota_bytes"`
	UsedBytes  int64     `json:"used_bytes"`
	IsActive   bool      `json:"is_active"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type Bucket struct {
	ID        int64     `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

type ObjectLocation struct {
	ID             int64     `json:"id"`
	ObjectID       int64     `json:"object_id"`
	Tier           TierType  `json:"tier"`
	AccountID      int64     `json:"account_id"`
	RemotePath     string    `json:"remote_path"`
	IsEncrypted    bool      `json:"is_encrypted"`
	SizeBytes      int64     `json:"size_bytes"`
	AccessCount    int64     `json:"access_count"`
	LastAccessedAt time.Time `json:"last_accessed_at"`
	CreatedAt      time.Time `json:"created_at"`
}

type Object struct {
	ID             int64             `json:"id"`
	Bucket         string            `json:"bucket"`
	Key            string            `json:"key"`
	Size           int64             `json:"size"`
	ETag           string            `json:"etag"`
	ContentType    string            `json:"content_type"`
	CustomMetadata map[string]string `json:"custom_metadata,omitempty"`
	Locations      []ObjectLocation  `json:"locations,omitempty"`
	HasCache       bool              `json:"has_cache"`
	HasCold        bool              `json:"has_cold"`
	CreatedAt      time.Time         `json:"created_at"`
	UpdatedAt      time.Time         `json:"updated_at"`
}

type Chunk struct {
	ID              int64     `json:"id"`
	ObjectID        int64     `json:"object_id"`
	PartNumber      int       `json:"part_number"`
	ChunkIndex      int       `json:"chunk_index"`
	OffsetBytes     int64     `json:"offset_bytes"`
	SizeBytes       int64     `json:"size_bytes"`
	CipherSizeBytes int64     `json:"cipher_size_bytes"`
	AccountID       int64     `json:"account_id"`
	RemotePath      string    `json:"remote_path"`
	Sha256Hash      string    `json:"sha256_hash"`
	EncVersion      int       `json:"enc_version"`
	KeyID           string    `json:"key_id"`
	CreatedAt       time.Time `json:"created_at"`
}

type MultipartUpload struct {
	ID          int64             `json:"id"`
	UploadID    string            `json:"upload_id"`
	Bucket      string            `json:"bucket"`
	Key         string            `json:"key"`
	ContentType string            `json:"content_type"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	InitiatedAt time.Time         `json:"initiated_at"`
}

type MultipartPart struct {
	ID         int64     `json:"id"`
	UploadID   string    `json:"upload_id"`
	PartNumber int       `json:"part_number"`
	ETag       string    `json:"etag"`
	SizeBytes  int64     `json:"size_bytes"`
	ChunksJSON string    `json:"chunks_json"`
	UploadedAt time.Time `json:"uploaded_at"`
}

type PoolStats struct {
	TotalCapacityBytes      int64 `json:"total_capacity_bytes"`
	TotalUsedBytes          int64 `json:"total_used_bytes"`
	TotalFreeBytes          int64 `json:"total_free_bytes"`
	TotalAccounts           int   `json:"total_accounts"`
	ActiveAccounts          int   `json:"active_accounts"`
	TotalBuckets            int   `json:"total_buckets"`
	TotalObjects            int   `json:"total_objects"`
	CachedObjects           int   `json:"cached_objects"`
	ColdObjects             int   `json:"cold_objects"`
	TotalCacheBuckets       int   `json:"total_cache_buckets"`
	ActiveCacheBuckets      int   `json:"active_cache_buckets"`
	TotalCacheCapacityBytes int64 `json:"total_cache_capacity_bytes"`
	TotalCacheUsedBytes     int64 `json:"total_cache_used_bytes"`
	PendingDeletions        int   `json:"pending_deletions"`
	ChunksNeedingRekey      int64 `json:"chunks_needing_rekey"`
}

type SystemSettings struct {
	AccessKeyID        string `json:"access_key_id"`
	SecretAccessKey    string `json:"secret_access_key"`
	ChunkSizeMB        int    `json:"chunk_size_mb"`
	S3Region           string `json:"s3_region"`
	HFStorageEndpoint  string `json:"hf_storage_endpoint"`
	HFStorageRegion    string `json:"hf_storage_region"`
	HFStorageAccessKey string `json:"hf_storage_access_key"`
	HFStorageSecretKey string `json:"hf_storage_secret_key"`
	HFStorageBucket    string `json:"hf_storage_bucket"`
}

// Kinds of remote objects queued for deletion.
const (
	DeletionChunk = "chunk" // encrypted chunk in a Hugging Face dataset (account_id = accounts.id)
	DeletionCache = "cache" // unencrypted copy in a cache bucket (account_id = cache_buckets.id, 0 = legacy default)
)

// PendingDeletion is a remote object that must be removed. Rows survive
// restarts and are retried with backoff, so failed deletes never leak storage.
type PendingDeletion struct {
	ID            int64     `json:"id"`
	Kind          string    `json:"kind"`
	AccountID     int64     `json:"account_id"`
	RemotePath    string    `json:"remote_path"`
	SizeBytes     int64     `json:"size_bytes"`
	Attempts      int       `json:"attempts"`
	NextAttemptAt time.Time `json:"next_attempt_at"`
	LastError     string    `json:"last_error"`
	CreatedAt     time.Time `json:"created_at"`
}
