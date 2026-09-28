package s3api

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"hf2s3/pkg/db"
	"hf2s3/pkg/storage"
)

type Server struct {
	pool *storage.PoolManager
	auth *AuthManager
}

func NewServer(pool *storage.PoolManager, auth *AuthManager) *Server {
	return &Server{
		pool: pool,
		auth: auth,
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// CORS Headers for browser / S3 web tools
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, PUT, POST, DELETE, HEAD, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "*")
	w.Header().Set("Access-Control-Expose-Headers", "ETag, Content-Length, Content-Range, Accept-Ranges, x-amz-request-id, Location, X-HF2S3-Cache-Status, X-HF2S3-Tier, X-HF2S3-Tiers")
	w.Header().Set("x-amz-request-id", uuid.New().String())

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	cleanPath := strings.Trim(r.URL.Path, "/")

	// Dedicated REST Media Streaming route: /media/{bucket}/{key}
	if strings.HasPrefix(cleanPath, "media/") {
		sub := strings.TrimPrefix(cleanPath, "media/")
		parts := strings.SplitN(sub, "/", 2)
		if len(parts) == 2 {
			s.handleMediaStream(w, r, parts[0], parts[1])
			return
		}
	}

	// Validate Auth if configured
	if s.auth != nil && !s.auth.Validate(r) {
		s.writeError(w, http.StatusForbidden, "AccessDenied", "Access Denied: Invalid credentials", r.URL.Path)
		return
	}

	if cleanPath == "" {
		if r.Method == http.MethodGet {
			s.handleListBuckets(w, r)
			return
		}
		s.writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "Method not allowed on root", "/")
		return
	}

	parts := strings.SplitN(cleanPath, "/", 2)
	bucket := parts[0]
	var key string
	if len(parts) > 1 {
		key = parts[1]
	}

	if key == "" {
		// Bucket level operation
		switch r.Method {
		case http.MethodPut:
			s.handleCreateBucket(w, r, bucket)
		case http.MethodGet:
			s.handleListObjects(w, r, bucket)
		case http.MethodHead:
			s.handleHeadBucket(w, r, bucket)
		case http.MethodDelete:
			s.handleDeleteBucket(w, r, bucket)
		case http.MethodPost:
			if _, ok := r.URL.Query()["delete"]; ok {
				s.handleDeleteObjects(w, r, bucket)
			} else {
				s.writeError(w, http.StatusBadRequest, "InvalidRequest", "Unsupported bucket POST action", bucket)
			}
		default:
			s.writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "Method not allowed on bucket", bucket)
		}
		return
	}

	// Object level operation
	switch r.Method {
	case http.MethodPut:
		if r.URL.Query().Get("partNumber") != "" && r.URL.Query().Get("uploadId") != "" {
			s.handleUploadPart(w, r, bucket, key)
		} else {
			s.handlePutObject(w, r, bucket, key)
		}
	case http.MethodGet:
		s.handleGetObject(w, r, bucket, key)
	case http.MethodHead:
		s.handleHeadObject(w, r, bucket, key)
	case http.MethodDelete:
		if r.URL.Query().Get("uploadId") != "" {
			s.handleAbortMultipart(w, r, bucket, key)
		} else {
			s.handleDeleteObject(w, r, bucket, key)
		}
	case http.MethodPost:
		if _, ok := r.URL.Query()["uploads"]; ok {
			s.handleInitiateMultipart(w, r, bucket, key)
		} else if r.URL.Query().Get("uploadId") != "" {
			s.handleCompleteMultipart(w, r, bucket, key)
		} else {
			s.writeError(w, http.StatusBadRequest, "InvalidRequest", "Unsupported object POST action", key)
		}
	default:
		s.writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "Method not allowed on object", key)
	}
}

func (s *Server) handleListBuckets(w http.ResponseWriter, r *http.Request) {
	buckets, err := s.pool.DB().ListBuckets(r.Context())
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "InternalError", err.Error(), "/")
		return
	}

	var entries []BucketEntry
	for _, b := range buckets {
		entries = append(entries, BucketEntry{
			Name:         b.Name,
			CreationDate: FormatTime(b.CreatedAt),
		})
	}

	result := ListAllMyBucketsResult{
		Owner: Owner{
			ID:          "hf2s3-owner",
			DisplayName: "HF2S3",
		},
		Buckets: Buckets{Bucket: entries},
	}

	s.writeXML(w, http.StatusOK, result)
}

func (s *Server) handleCreateBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	if err := s.pool.DB().CreateBucket(r.Context(), bucket); err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			s.writeError(w, http.StatusConflict, "BucketAlreadyOwnedByYou", "Your previous request to create the named bucket succeeded and you already own it.", bucket)
			return
		}
		s.writeError(w, http.StatusInternalServerError, "InternalError", err.Error(), bucket)
		return
	}

	w.Header().Set("Location", "/"+bucket)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleHeadBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	exists, err := s.pool.DB().BucketExists(r.Context(), bucket)
	if err != nil || !exists {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDeleteBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	err := s.pool.DB().DeleteBucket(r.Context(), bucket)
	if errors.Is(err, db.ErrNotFound) {
		s.writeError(w, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist.", bucket)
		return
	}
	if err != nil {
		if strings.Contains(err.Error(), "not empty") {
			s.writeError(w, http.StatusConflict, "BucketNotEmpty", "The bucket you tried to delete is not empty.", bucket)
			return
		}
		s.writeError(w, http.StatusInternalServerError, "InternalError", err.Error(), bucket)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleListObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	exists, err := s.pool.DB().BucketExists(r.Context(), bucket)
	if err != nil || !exists {
		s.writeError(w, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist.", bucket)
		return
	}

	query := r.URL.Query()
	prefix := query.Get("prefix")
	delimiter := query.Get("delimiter")
	startAfter := query.Get("start-after")
	if startAfter == "" {
		startAfter = query.Get("continuation-token")
	}

	maxKeys := 1000
	if mkStr := query.Get("max-keys"); mkStr != "" {
		if mk, err := strconv.Atoi(mkStr); err == nil && mk > 0 {
			maxKeys = mk
		}
	}

	objects, err := s.pool.DB().ListObjects(r.Context(), bucket, prefix, startAfter, maxKeys+1)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "InternalError", err.Error(), bucket)
		return
	}

	isTruncated := false
	if len(objects) > maxKeys {
		isTruncated = true
		objects = objects[:maxKeys]
	}

	var contents []ObjectEntry
	var commonPrefixes []CommonPrefix
	prefixMap := make(map[string]bool)

	for _, o := range objects {
		if delimiter != "" {
			subKey := strings.TrimPrefix(o.Key, prefix)
			if idx := strings.Index(subKey, delimiter); idx >= 0 {
				cp := prefix + subKey[:idx+len(delimiter)]
				if !prefixMap[cp] {
					prefixMap[cp] = true
					commonPrefixes = append(commonPrefixes, CommonPrefix{Prefix: cp})
				}
				continue
			}
		}

		contents = append(contents, ObjectEntry{
			Key:          o.Key,
			LastModified: FormatTime(o.UpdatedAt),
			ETag:         o.ETag,
			Size:         o.Size,
			StorageClass: "STANDARD",
		})
	}

	result := ListBucketResult{
		Name:           bucket,
		Prefix:         prefix,
		KeyCount:       len(contents) + len(commonPrefixes),
		MaxKeys:        maxKeys,
		IsTruncated:    isTruncated,
		Contents:       contents,
		CommonPrefixes: commonPrefixes,
	}

	s.writeXML(w, http.StatusOK, result)
}

func (s *Server) handlePutObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	contentType := r.Header.Get("Content-Type")
	contentLength := r.ContentLength

	// Extract S3 custom metadata headers (x-amz-meta-*)
	customMeta := make(map[string]string)
	for h, values := range r.Header {
		if strings.HasPrefix(strings.ToLower(h), "x-amz-meta-") {
			metaKey := strings.TrimPrefix(strings.ToLower(h), "x-amz-meta-")
			if len(values) > 0 {
				customMeta[metaKey] = values[0]
			}
		}
	}

	obj, err := s.pool.PutObject(r.Context(), bucket, key, contentType, r.Body, contentLength, customMeta)
	if errors.Is(err, storage.ErrBucketNotFound) {
		s.writeError(w, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist.", bucket)
		return
	}
	if errors.Is(err, storage.ErrPoolOutOfSpace) {
		s.writeError(w, http.StatusInsufficientStorage, "InsufficientStorage", "Storage capacity limit reached across accounts.", key)
		return
	}
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "InternalError", err.Error(), key)
		return
	}

	w.Header().Set("ETag", obj.ETag)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleMediaStream(w http.ResponseWriter, r *http.Request, bucket, key string) {
	presignedURL, obj, coldReader, err := s.pool.GetObjectOrPresigned(r.Context(), bucket, key)
	if errors.Is(err, db.ErrNotFound) {
		http.Error(w, "Media not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// 1. In Cache: 302 Found redirecting to HF Storage Bucket Presigned URL (zero VPS bandwidth)
	if presignedURL != "" {
		w.Header().Set("Location", presignedURL)
		w.Header().Set("X-HF2S3-Cache-Status", "HIT")
		w.Header().Set("X-HF2S3-Tier", "cache")
		if obj != nil {
			w.Header().Set("ETag", obj.ETag)
			if obj.ContentType != "" {
				w.Header().Set("Content-Type", obj.ContentType)
			}
		}
		w.WriteHeader(http.StatusFound)
		return
	}

	// 2. Cold Miss: Stream decrypted bytes and trigger auto-promotion
	if coldReader != nil {
		defer coldReader.Close()
		w.Header().Set("X-HF2S3-Cache-Status", "MISS")
		w.Header().Set("X-HF2S3-Tier", "cold")
		w.Header().Set("Accept-Ranges", "bytes")
		if obj != nil {
			w.Header().Set("ETag", obj.ETag)
			w.Header().Set("Content-Length", strconv.FormatInt(obj.Size, 10))
			if obj.ContentType != "" {
				w.Header().Set("Content-Type", obj.ContentType)
			}
			w.Header().Set("Last-Modified", obj.UpdatedAt.UTC().Format(http.TimeFormat))
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(w, coldReader)
		return
	}

	http.Error(w, "Media stream unavailable", http.StatusNotFound)
}

func (s *Server) handleGetObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	var byteRange *storage.ByteRange
	rangeHeader := r.Header.Get("Range")

	if rangeHeader != "" && strings.HasPrefix(rangeHeader, "bytes=") {
		parts := strings.Split(strings.TrimPrefix(rangeHeader, "bytes="), "-")
		if len(parts) == 2 {
			start, err1 := strconv.ParseInt(parts[0], 10, 64)
			if err1 == nil {
				end := int64(-1)
				if parts[1] != "" {
					if endVal, err2 := strconv.ParseInt(parts[1], 10, 64); err2 == nil {
						end = endVal
					}
				}
				if end == -1 {
					// We need object size to compute end
					obj, err := s.pool.DB().HeadObject(r.Context(), bucket, key)
					if err == nil {
						end = obj.Size - 1
					}
				}
				if end >= start {
					byteRange = &storage.ByteRange{Start: start, End: end}
				}
			}
		}
	}

	// Direct download check: If in cache and client did not explicitly opt out with X-HF2S3-Direct: false
	if r.Header.Get("X-HF2S3-Direct") != "false" && byteRange == nil {
		presignedURL, obj, coldReader, err := s.pool.GetObjectOrPresigned(r.Context(), bucket, key)
		if err == nil {
			if presignedURL != "" {
				w.Header().Set("Location", presignedURL)
				w.Header().Set("X-HF2S3-Cache-Status", "HIT")
				w.Header().Set("X-HF2S3-Tier", "cache")
				w.Header().Set("ETag", obj.ETag)
				w.WriteHeader(http.StatusFound)
				return
			}
			if coldReader != nil {
				defer coldReader.Close()
				w.Header().Set("ETag", obj.ETag)
				w.Header().Set("Last-Modified", obj.UpdatedAt.UTC().Format(http.TimeFormat))
				w.Header().Set("Accept-Ranges", "bytes")
				w.Header().Set("X-HF2S3-Cache-Status", "MISS")
				w.Header().Set("X-HF2S3-Tier", "cold")
				if obj.ContentType != "" {
					w.Header().Set("Content-Type", obj.ContentType)
				}
				w.Header().Set("Content-Length", strconv.FormatInt(obj.Size, 10))
				w.WriteHeader(http.StatusOK)
				_, _ = io.Copy(w, coldReader)
				return
			}
		}
	}

	obj, reader, err := s.pool.GetObject(r.Context(), bucket, key, byteRange)
	if errors.Is(err, db.ErrNotFound) {
		s.writeError(w, http.StatusNotFound, "NoSuchKey", "The specified key does not exist.", key)
		return
	}
	if errors.Is(err, storage.ErrInvalidRange) {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", obj.Size))
		s.writeError(w, http.StatusRequestedRangeNotSatisfiable, "InvalidRange", "The requested range cannot be satisfied.", key)
		return
	}
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "InternalError", err.Error(), key)
		return
	}
	defer reader.Close()

	w.Header().Set("ETag", obj.ETag)
	w.Header().Set("Last-Modified", obj.UpdatedAt.UTC().Format(http.TimeFormat))
	w.Header().Set("Accept-Ranges", "bytes")
	if obj.ContentType != "" {
		w.Header().Set("Content-Type", obj.ContentType)
	}

	if byteRange != nil {
		rangeLength := byteRange.End - byteRange.Start + 1
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", byteRange.Start, byteRange.End, obj.Size))
		w.Header().Set("Content-Length", strconv.FormatInt(rangeLength, 10))
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.Header().Set("Content-Length", strconv.FormatInt(obj.Size, 10))
		w.WriteHeader(http.StatusOK)
	}

	_, _ = io.Copy(w, reader)
}

func (s *Server) handleHeadObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	obj, err := s.pool.DB().HeadObject(r.Context(), bucket, key)
	if errors.Is(err, db.ErrNotFound) {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	tiers := "cold"
	if obj.HasCache {
		tiers = "cache,cold"
		w.Header().Set("X-HF2S3-Cache-Status", "HIT")
	} else {
		w.Header().Set("X-HF2S3-Cache-Status", "MISS")
	}
	w.Header().Set("X-HF2S3-Tiers", tiers)

	w.Header().Set("ETag", obj.ETag)
	w.Header().Set("Content-Length", strconv.FormatInt(obj.Size, 10))
	w.Header().Set("Content-Type", obj.ContentType)
	w.Header().Set("Last-Modified", obj.UpdatedAt.UTC().Format(http.TimeFormat))
	w.Header().Set("Accept-Ranges", "bytes")
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDeleteObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	err := s.pool.DeleteObject(r.Context(), bucket, key)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "InternalError", err.Error(), key)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleDeleteObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	var req DeleteObjectsRequest
	if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "MalformedXML", "The XML provided was not well-formed", bucket)
		return
	}

	var deleted []DeletedObject
	var deleteErrors []DeleteError

	for _, o := range req.Objects {
		err := s.pool.DeleteObject(r.Context(), bucket, o.Key)
		if err != nil {
			deleteErrors = append(deleteErrors, DeleteError{
				Key:     o.Key,
				Code:    "InternalError",
				Message: err.Error(),
			})
		} else {
			deleted = append(deleted, DeletedObject{Key: o.Key})
		}
	}

	result := DeleteResult{
		Deleted: deleted,
		Error:   deleteErrors,
	}

	s.writeXML(w, http.StatusOK, result)
}

func (s *Server) handleInitiateMultipart(w http.ResponseWriter, r *http.Request, bucket, key string) {
	contentType := r.Header.Get("Content-Type")
	uploadID, err := s.pool.InitiateMultipartUpload(r.Context(), bucket, key, contentType)
	if errors.Is(err, storage.ErrBucketNotFound) {
		s.writeError(w, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist.", bucket)
		return
	}
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "InternalError", err.Error(), key)
		return
	}

	result := InitiateMultipartUploadResult{
		Bucket:   bucket,
		Key:      key,
		UploadId: uploadID,
	}
	s.writeXML(w, http.StatusOK, result)
}

func (s *Server) handleUploadPart(w http.ResponseWriter, r *http.Request, bucket, key string) {
	partNumStr := r.URL.Query().Get("partNumber")
	uploadID := r.URL.Query().Get("uploadId")

	partNum, err := strconv.Atoi(partNumStr)
	if err != nil || partNum <= 0 {
		s.writeError(w, http.StatusBadRequest, "InvalidArgument", "Invalid partNumber parameter", key)
		return
	}

	etag, err := s.pool.UploadPart(r.Context(), uploadID, partNum, r.Body)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "InternalError", err.Error(), key)
		return
	}

	w.Header().Set("ETag", etag)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleCompleteMultipart(w http.ResponseWriter, r *http.Request, bucket, key string) {
	uploadID := r.URL.Query().Get("uploadId")

	obj, err := s.pool.CompleteMultipartUpload(r.Context(), uploadID)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "InternalError", err.Error(), key)
		return
	}

	result := CompleteMultipartUploadResult{
		Location: "/" + bucket + "/" + key,
		Bucket:   bucket,
		Key:      key,
		ETag:     obj.ETag,
	}
	s.writeXML(w, http.StatusOK, result)
}

func (s *Server) handleAbortMultipart(w http.ResponseWriter, r *http.Request, bucket, key string) {
	uploadID := r.URL.Query().Get("uploadId")
	_ = s.pool.AbortMultipartUpload(r.Context(), uploadID)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) writeXML(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(statusCode)
	_, _ = w.Write([]byte(xml.Header))
	_ = xml.NewEncoder(w).Encode(data)
}

func (s *Server) writeError(w http.ResponseWriter, status int, code, message, resource string) {
	errResp := S3ErrorResponse{
		Code:      code,
		Message:   message,
		Resource:  resource,
		RequestId: uuid.New().String(),
	}
	s.writeXML(w, status, errResp)
}
