package s3api

import (
	"context"
	"encoding/xml"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
	"hf2s3/pkg/db"
	"hf2s3/pkg/models"
	"hf2s3/pkg/storage"
)

// RedirectMode controls when GET answers with a 302 to the cache bucket.
type RedirectMode string

const (
	// RedirectAuto redirects presigned-URL requests (browsers, <video>, /media)
	// and proxies requests signed by header (SDKs, rclone, AWS CLI), whose HTTP
	// clients generally do not follow a redirect to another host.
	RedirectAuto   RedirectMode = "auto"
	RedirectAlways RedirectMode = "always"
	RedirectNever  RedirectMode = "never"
)

const (
	maxKeyBytes        = 1024
	maxXMLBodyBytes    = 4 << 20
	maxDeleteObjects   = 1000
	maxListKeys        = 1000
	maxMultipartPartNo = 10000
)

type Server struct {
	pool        *storage.PoolManager
	auth        *AuthManager
	region      string
	redirect    RedirectMode
	corsOrigins []string
}

func NewServer(pool *storage.PoolManager, auth *AuthManager) *Server {
	return &Server{
		pool:     pool,
		auth:     auth,
		region:   "us-east-1",
		redirect: RedirectAuto,
	}
}

// SetRegion sets the region reported by GetBucketLocation.
func (s *Server) SetRegion(region string) {
	if region != "" {
		s.region = region
	}
}

// SetRedirectMode selects when cached objects are served by redirect.
func (s *Server) SetRedirectMode(m RedirectMode) {
	switch m {
	case RedirectAuto, RedirectAlways, RedirectNever:
		s.redirect = m
	}
}

// SetCORSOrigins configures the allowed browser origins ("*" allows any).
// With none configured no CORS headers are sent.
func (s *Server) SetCORSOrigins(origins []string) {
	s.corsOrigins = origins
}

type authCtxKey struct{}

func authFromRequest(r *http.Request) *AuthResult {
	res, _ := r.Context().Value(authCtxKey{}).(*AuthResult)
	return res
}

// applyCORS reports whether the request's origin is allowed and, if so, sets
// the response headers.
func (s *Server) applyCORS(w http.ResponseWriter, r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" || len(s.corsOrigins) == 0 {
		return false
	}
	allow := ""
	for _, o := range s.corsOrigins {
		if o == "*" {
			allow = "*"
			break
		}
		if strings.EqualFold(o, origin) {
			allow = origin
		}
	}
	if allow == "" {
		return false
	}
	w.Header().Set("Access-Control-Allow-Origin", allow)
	if allow != "*" {
		w.Header().Add("Vary", "Origin")
	}
	w.Header().Set("Access-Control-Expose-Headers", "ETag, Content-Length, Content-Range, Accept-Ranges, x-amz-request-id, Location, X-HF2S3-Cache-Status, X-HF2S3-Tier, X-HF2S3-Tiers")
	return true
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("x-amz-request-id", uuid.New().String())
	corsOK := s.applyCORS(w, r)

	if r.Method == http.MethodOptions {
		if corsOK {
			w.Header().Set("Access-Control-Allow-Methods", "GET, PUT, POST, DELETE, HEAD")
			if reqHeaders := r.Header.Get("Access-Control-Request-Headers"); reqHeaders != "" {
				w.Header().Set("Access-Control-Allow-Headers", reqHeaders)
			} else {
				w.Header().Set("Access-Control-Allow-Headers", "*")
			}
			w.Header().Set("Access-Control-Max-Age", "3600")
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// Every route (including /media) requires a valid SigV4 signature, either in
	// the Authorization header or as a presigned URL.
	if s.auth != nil {
		res, aerr := s.auth.Authenticate(r)
		if aerr != nil {
			s.writeError(w, aerr.Status, aerr.Code, aerr.Message, r.URL.Path)
			return
		}
		r = r.WithContext(context.WithValue(r.Context(), authCtxKey{}, res))
	}

	cleanPath := strings.Trim(r.URL.Path, "/")

	// Dedicated REST media route: /media/{bucket}/{key}
	if strings.HasPrefix(cleanPath, "media/") {
		parts := strings.SplitN(strings.TrimPrefix(cleanPath, "media/"), "/", 2)
		if len(parts) == 2 && parts[1] != "" {
			if r.Method != http.MethodGet && r.Method != http.MethodHead {
				s.writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "Only GET and HEAD are allowed on /media", r.URL.Path)
				return
			}
			s.serveObject(w, r, parts[0], parts[1])
			return
		}
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
		s.routeBucket(w, r, bucket)
		return
	}
	s.routeObject(w, r, bucket, key)
}

// unsupportedBucketSubresources are S3 features this gateway does not implement.
// Answering them with a listing (as a plain GET /bucket would) confuses clients.
var unsupportedBucketSubresources = []string{
	"acl", "cors", "lifecycle", "policy", "tagging", "versioning", "versions",
	"uploads", "website", "logging", "notification", "replication", "encryption",
	"object-lock", "accelerate", "requestPayment", "analytics", "inventory", "metrics",
}

var unsupportedObjectSubresources = []string{"acl", "tagging", "torrent", "retention", "legal-hold", "restore", "select", "attributes"}

func hasAnyQuery(r *http.Request, names []string) bool {
	q := r.URL.Query()
	for _, n := range names {
		if _, ok := q[n]; ok {
			return true
		}
	}
	return false
}

func (s *Server) routeBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	q := r.URL.Query()

	if _, ok := q["location"]; ok && r.Method == http.MethodGet {
		s.handleGetBucketLocation(w, r, bucket)
		return
	}
	// A sub-resource request (?lifecycle, ?acl ...) must never fall through to
	// the plain bucket operation: DELETE /bucket?lifecycle would delete the bucket.
	if r.Method != http.MethodPost && hasAnyQuery(r, unsupportedBucketSubresources) {
		s.notImplemented(w, "This bucket sub-resource is not supported by HF2S3", bucket)
		return
	}

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
		if _, ok := q["delete"]; ok {
			s.handleDeleteObjects(w, r, bucket)
		} else {
			s.writeError(w, http.StatusBadRequest, "InvalidRequest", "Unsupported bucket POST action", bucket)
		}
	default:
		s.writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "Method not allowed on bucket", bucket)
	}
}

func (s *Server) routeObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	q := r.URL.Query()

	if hasAnyQuery(r, unsupportedObjectSubresources) {
		s.notImplemented(w, "This object sub-resource is not supported by HF2S3", key)
		return
	}
	if err := validateKey(key); err != nil {
		s.writeError(w, http.StatusBadRequest, "KeyTooLongError", err.Error(), key)
		return
	}

	switch r.Method {
	case http.MethodPut:
		if r.Header.Get("X-Amz-Copy-Source") != "" {
			s.notImplemented(w, "CopyObject and UploadPartCopy are not supported by HF2S3; download and upload the object instead", key)
			return
		}
		if q.Get("partNumber") != "" && q.Get("uploadId") != "" {
			s.handleUploadPart(w, r, bucket, key)
		} else {
			s.handlePutObject(w, r, bucket, key)
		}
	case http.MethodGet, http.MethodHead:
		if _, ok := q["uploadId"]; ok {
			s.notImplemented(w, "ListParts is not supported by HF2S3", key)
			return
		}
		s.serveObject(w, r, bucket, key)
	case http.MethodDelete:
		if q.Get("uploadId") != "" {
			s.handleAbortMultipart(w, r, bucket, key)
		} else {
			s.handleDeleteObject(w, r, bucket, key)
		}
	case http.MethodPost:
		if _, ok := q["uploads"]; ok {
			s.handleInitiateMultipart(w, r, bucket, key)
		} else if q.Get("uploadId") != "" {
			s.handleCompleteMultipart(w, r, bucket, key)
		} else {
			s.writeError(w, http.StatusBadRequest, "InvalidRequest", "Unsupported object POST action", key)
		}
	default:
		s.writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "Method not allowed on object", key)
	}
}

func validateKey(key string) error {
	if len(key) > maxKeyBytes {
		return errors.New("Your key is too long")
	}
	if !utf8.ValidString(key) || strings.ContainsRune(key, 0) {
		return errors.New("Your key contains invalid characters")
	}
	return nil
}

func (s *Server) handleListBuckets(w http.ResponseWriter, r *http.Request) {
	buckets, err := s.pool.DB().ListBuckets(r.Context())
	if err != nil {
		s.internalError(w, err, "/")
		return
	}

	var entries []BucketEntry
	for _, b := range buckets {
		entries = append(entries, BucketEntry{
			Name:         b.Name,
			CreationDate: FormatTime(b.CreatedAt),
		})
	}

	s.writeXML(w, http.StatusOK, ListAllMyBucketsResult{
		Owner:   Owner{ID: "hf2s3-owner", DisplayName: "HF2S3"},
		Buckets: Buckets{Bucket: entries},
	})
}

func (s *Server) handleGetBucketLocation(w http.ResponseWriter, r *http.Request, bucket string) {
	exists, err := s.pool.DB().BucketExists(r.Context(), bucket)
	if err != nil || !exists {
		s.writeError(w, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist.", bucket)
		return
	}
	region := s.region
	if region == "us-east-1" {
		region = "" // AWS reports the classic region as an empty constraint
	}
	s.writeXML(w, http.StatusOK, LocationConstraint{Region: region})
}

func (s *Server) handleCreateBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	if err := models.ValidateBucketName(bucket); err != nil {
		s.writeError(w, http.StatusBadRequest, "InvalidBucketName", err.Error(), bucket)
		return
	}
	if err := s.pool.DB().CreateBucket(r.Context(), bucket); err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			s.writeError(w, http.StatusConflict, "BucketAlreadyOwnedByYou", "Your previous request to create the named bucket succeeded and you already own it.", bucket)
			return
		}
		s.internalError(w, err, bucket)
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
		s.internalError(w, err, bucket)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) writeXML(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(statusCode)
	_, _ = w.Write([]byte(xml.Header))
	_ = xml.NewEncoder(w).Encode(data)
}

func (s *Server) writeError(w http.ResponseWriter, status int, code, message, resource string) {
	s.writeXML(w, status, S3ErrorResponse{
		Code:      code,
		Message:   message,
		Resource:  resource,
		RequestId: w.Header().Get("x-amz-request-id"),
	})
}

func (s *Server) notImplemented(w http.ResponseWriter, message, resource string) {
	s.writeError(w, http.StatusNotImplemented, "NotImplemented", message, resource)
}

// internalError logs the real cause and returns a generic message: upstream
// errors can contain URLs, tokens or account names that must not reach clients.
func (s *Server) internalError(w http.ResponseWriter, err error, resource string) {
	slog.Error("s3 request failed", "request_id", w.Header().Get("x-amz-request-id"), "resource", resource, "err", err)
	s.writeError(w, http.StatusInternalServerError, "InternalError", "We encountered an internal error. Please try again.", resource)
}
