package models

import (
	"time"
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
	LastChecked time.Time `json:"last_checked"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type Bucket struct {
	ID        int64     `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

type Object struct {
	ID             int64             `json:"id"`
	Bucket         string            `json:"bucket"`
	Key            string            `json:"key"`
	Size           int64             `json:"size"`
	ETag           string            `json:"etag"`
	ContentType    string            `json:"content_type"`
	CustomMetadata map[string]string `json:"custom_metadata,omitempty"`
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
	CreatedAt       time.Time `json:"created_at"`
}

type MultipartUpload struct {
	ID          int64     `json:"id"`
	UploadID    string    `json:"upload_id"`
	Bucket      string    `json:"bucket"`
	Key         string    `json:"key"`
	ContentType string    `json:"content_type"`
	InitiatedAt time.Time `json:"initiated_at"`
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
	TotalCapacityBytes int64 `json:"total_capacity_bytes"`
	TotalUsedBytes     int64 `json:"total_used_bytes"`
	TotalFreeBytes     int64 `json:"total_free_bytes"`
	TotalAccounts      int   `json:"total_accounts"`
	ActiveAccounts     int   `json:"active_accounts"`
	TotalBuckets       int   `json:"total_buckets"`
	TotalObjects       int   `json:"total_objects"`
}

type SystemSettings struct {
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	MasterKey       string `json:"master_key"`
	ChunkSizeMB     int    `json:"chunk_size_mb"`
	S3Region        string `json:"s3_region"`
}
