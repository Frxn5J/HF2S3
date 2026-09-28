package hfstorage

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestPresignGetObject(t *testing.T) {
	client := NewS3Client(S3ClientConfig{
		Endpoint:  "https://s3.hf.co",
		Region:    "us-east-1",
		AccessKey: "HFAKTESTACCESSKEY",
		SecretKey: "testsecretkey123",
		Bucket:    "my-cache-bucket",
		Namespace: "myuser",
	})

	presigned, err := client.PresignGetObject("my-cache-bucket", "videos/movie.mp4", 15*time.Minute)
	if err != nil {
		t.Fatalf("PresignGetObject returned error: %v", err)
	}

	u, err := url.Parse(presigned)
	if err != nil {
		t.Fatalf("Failed to parse presigned URL: %v", err)
	}

	if u.Scheme != "https" || u.Host != "s3.hf.co" {
		t.Errorf("Unexpected host/scheme: %s://%s", u.Scheme, u.Host)
	}

	if !strings.Contains(u.Path, "videos/movie.mp4") {
		t.Errorf("Path missing object key: %s", u.Path)
	}

	q := u.Query()
	if q.Get("X-Amz-Algorithm") != "AWS4-HMAC-SHA256" {
		t.Errorf("Unexpected algorithm: %s", q.Get("X-Amz-Algorithm"))
	}
	if !strings.HasPrefix(q.Get("X-Amz-Credential"), "HFAKTESTACCESSKEY/") {
		t.Errorf("Unexpected credential: %s", q.Get("X-Amz-Credential"))
	}
	if q.Get("X-Amz-Expires") != "900" {
		t.Errorf("Expected expires 900, got: %s", q.Get("X-Amz-Expires"))
	}
	if q.Get("X-Amz-Signature") == "" {
		t.Errorf("Missing signature in presigned URL")
	}
}

func TestS3ClientHTTPMethods(t *testing.T) {
	var lastMethod string
	var lastPath string
	var lastAuth string
	var bodyContent string

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lastMethod = r.Method
		lastPath = r.URL.Path
		lastAuth = r.Header.Get("Authorization")

		switch r.Method {
		case http.MethodPut:
			data, _ := io.ReadAll(r.Body)
			bodyContent = string(data)
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("cached payload"))
		case http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer ts.Close()

	client := NewS3Client(S3ClientConfig{
		Endpoint:   ts.URL,
		Region:     "us-east-1",
		AccessKey:  "HFAKTESTACCESSKEY",
		SecretKey:  "testsecretkey123",
		Bucket:     "cache-bucket",
		HTTPClient: ts.Client(),
	})

	ctx := context.Background()

	// 1. PutObject
	payload := "hello unencrypted world"
	err := client.PutObject(ctx, "cache-bucket", "test.txt", strings.NewReader(payload), int64(len(payload)), "text/plain")
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}
	if lastMethod != http.MethodPut {
		t.Errorf("Expected PUT, got %s", lastMethod)
	}
	if !strings.HasPrefix(lastAuth, "AWS4-HMAC-SHA256") {
		t.Errorf("Expected AWS4 auth header, got %s", lastAuth)
	}
	if bodyContent != payload {
		t.Errorf("Expected body %q, got %q", payload, bodyContent)
	}

	// 2. GetObject
	reader, size, cType, err := client.GetObject(ctx, "cache-bucket", "test.txt")
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer reader.Close()
	data, _ := io.ReadAll(reader)
	if string(data) != "cached payload" {
		t.Errorf("Expected 'cached payload', got %q", string(data))
	}
	_ = size
	_ = cType

	// 3. DeleteObject (Eviction)
	err = client.DeleteObject(ctx, "cache-bucket", "test.txt")
	if err != nil {
		t.Fatalf("DeleteObject failed: %v", err)
	}
	if lastMethod != http.MethodDelete {
		t.Errorf("Expected DELETE, got %s", lastMethod)
	}
	_ = lastPath
}
