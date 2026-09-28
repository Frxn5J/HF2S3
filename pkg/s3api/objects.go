package s3api

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"hf2s3/pkg/db"
	"hf2s3/pkg/models"
	"hf2s3/pkg/storage"
)

// --- Range and conditional requests -----------------------------------------

type rangeResult int

const (
	rangeNone rangeResult = iota // no (usable) Range header: serve the whole object
	rangeOK
	rangeUnsatisfiable
)

// parseRange interprets a single "bytes=" range against an object of size
// bytes. Multiple ranges and malformed headers are ignored (the whole object
// is served), as RFC 9110 permits.
func parseRange(header string, size int64) (*storage.ByteRange, rangeResult) {
	if header == "" || !strings.HasPrefix(header, "bytes=") {
		return nil, rangeNone
	}
	spec := strings.TrimSpace(strings.TrimPrefix(header, "bytes="))
	if strings.Contains(spec, ",") {
		return nil, rangeNone
	}
	startStr, endStr, ok := strings.Cut(spec, "-")
	if !ok {
		return nil, rangeNone
	}
	startStr, endStr = strings.TrimSpace(startStr), strings.TrimSpace(endStr)

	if startStr == "" { // suffix range: the last N bytes
		n, err := strconv.ParseInt(endStr, 10, 64)
		if err != nil || n < 0 {
			return nil, rangeNone
		}
		if n == 0 || size == 0 {
			return nil, rangeUnsatisfiable
		}
		if n > size {
			n = size
		}
		return &storage.ByteRange{Start: size - n, End: size - 1}, rangeOK
	}

	start, err := strconv.ParseInt(startStr, 10, 64)
	if err != nil || start < 0 {
		return nil, rangeNone
	}
	end := size - 1
	if endStr != "" {
		e, err := strconv.ParseInt(endStr, 10, 64)
		if err != nil || e < start {
			return nil, rangeNone
		}
		if e < end {
			end = e
		}
	}
	if start >= size {
		return nil, rangeUnsatisfiable
	}
	return &storage.ByteRange{Start: start, End: end}, rangeOK
}

func etagMatches(header, etag string) bool {
	if header == "" {
		return false
	}
	if strings.TrimSpace(header) == "*" {
		return true
	}
	want := strings.Trim(etag, `"`)
	for _, part := range strings.Split(header, ",") {
		if strings.Trim(strings.TrimPrefix(strings.TrimSpace(part), "W/"), `"`) == want {
			return true
		}
	}
	return false
}

// checkPreconditions evaluates If-Match / If-None-Match / If-(Un)Modified-Since
// and returns 0 to proceed, or the status to answer with (304 or 412).
func checkPreconditions(r *http.Request, obj *models.Object) int {
	modified := obj.UpdatedAt.UTC().Truncate(time.Second)

	if im := r.Header.Get("If-Match"); im != "" && !etagMatches(im, obj.ETag) {
		return http.StatusPreconditionFailed
	} else if im == "" {
		if t, err := http.ParseTime(r.Header.Get("If-Unmodified-Since")); err == nil && modified.After(t) {
			return http.StatusPreconditionFailed
		}
	}

	if inm := r.Header.Get("If-None-Match"); inm != "" {
		if etagMatches(inm, obj.ETag) {
			return http.StatusNotModified
		}
		return 0
	}
	if t, err := http.ParseTime(r.Header.Get("If-Modified-Since")); err == nil && !modified.After(t) {
		return http.StatusNotModified
	}
	return 0
}

// --- Serving objects ---------------------------------------------------------

func (s *Server) wantsRedirect(r *http.Request) bool {
	if r.Header.Get("X-HF2S3-Direct") == "false" {
		return false
	}
	switch s.redirect {
	case RedirectAlways:
		return true
	case RedirectNever:
		return false
	}
	// Auto: only clients that authenticated with a presigned URL, or when
	// authentication is disabled altogether, are sent to the cache bucket.
	res := authFromRequest(r)
	return res == nil || res.Mode == AuthPresigned
}

func setObjectHeaders(h http.Header, obj *models.Object) {
	h.Set("ETag", obj.ETag)
	h.Set("Last-Modified", obj.UpdatedAt.UTC().Format(http.TimeFormat))
	h.Set("Accept-Ranges", "bytes")
	ct := obj.ContentType
	if ct == "" {
		ct = "application/octet-stream"
	}
	h.Set("Content-Type", ct)
	for k, v := range obj.CustomMetadata {
		h.Set("X-Amz-Meta-"+k, v)
	}
	if obj.HasCache {
		h.Set("X-HF2S3-Cache-Status", "HIT")
		h.Set("X-HF2S3-Tiers", "cache,cold")
	} else {
		h.Set("X-HF2S3-Cache-Status", "MISS")
		h.Set("X-HF2S3-Tiers", "cold")
	}
}

// serveObject implements GET and HEAD for both /{bucket}/{key} and /media/...
func (s *Server) serveObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	ctx := r.Context()
	head := r.Method == http.MethodHead

	obj, err := s.pool.DB().HeadObject(ctx, bucket, key)
	if errors.Is(err, db.ErrNotFound) {
		if head {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		s.writeError(w, http.StatusNotFound, "NoSuchKey", "The specified key does not exist.", key)
		return
	}
	if err != nil {
		if head {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		s.internalError(w, err, key)
		return
	}

	setObjectHeaders(w.Header(), obj)

	if status := checkPreconditions(r, obj); status != 0 {
		if status == http.StatusNotModified || head {
			w.WriteHeader(status)
			return
		}
		s.writeError(w, status, "PreconditionFailed", "At least one of the pre-conditions you specified did not hold", key)
		return
	}

	byteRange, rr := parseRange(r.Header.Get("Range"), obj.Size)
	if rr == rangeUnsatisfiable {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", obj.Size))
		if head {
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		s.writeError(w, http.StatusRequestedRangeNotSatisfiable, "InvalidRange", "The requested range is not satisfiable", key)
		return
	}

	if head {
		s.writeObjectStatus(w, obj, byteRange)
		return
	}

	// Direct download from the cache bucket (zero VPS bandwidth). The client
	// repeats its own Range header against the presigned URL.
	if obj.HasCache && s.wantsRedirect(r) {
		if url := s.pool.PresignedCacheURL(ctx, obj, 15*time.Minute); url != "" {
			w.Header().Set("Location", url)
			w.Header().Set("X-HF2S3-Tier", "cache")
			w.Header().Set("Cache-Control", "no-store")
			w.WriteHeader(http.StatusFound)
			return
		}
	}

	_, reader, err := s.pool.GetObject(ctx, bucket, key, byteRange)
	if err != nil {
		if s.writeStorageError(w, err, bucket, key) {
			return
		}
		return
	}
	defer reader.Close()

	if obj.HasCache {
		w.Header().Set("X-HF2S3-Tier", "cache")
	} else {
		w.Header().Set("X-HF2S3-Tier", "cold")
	}
	s.writeObjectStatus(w, obj, byteRange)
	_, _ = io.Copy(w, reader)
}

func (s *Server) writeObjectStatus(w http.ResponseWriter, obj *models.Object, byteRange *storage.ByteRange) {
	if byteRange != nil {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", byteRange.Start, byteRange.End, obj.Size))
		w.Header().Set("Content-Length", strconv.FormatInt(byteRange.End-byteRange.Start+1, 10))
		w.WriteHeader(http.StatusPartialContent)
		return
	}
	w.Header().Set("Content-Length", strconv.FormatInt(obj.Size, 10))
	w.WriteHeader(http.StatusOK)
}

// --- Writing objects ---------------------------------------------------------

// writeStorageError maps storage/body errors to S3 errors. It reports whether
// err was non-nil (and a response has been written).
func (s *Server) writeStorageError(w http.ResponseWriter, err error, bucket, key string) bool {
	if err == nil {
		return false
	}
	switch {
	case errors.Is(err, context.Canceled):
		return true // the client went away; nothing to answer
	case errors.Is(err, ErrIncompleteBody), errors.Is(err, storage.ErrSizeMismatch):
		s.writeError(w, http.StatusBadRequest, "IncompleteBody", "You did not provide the number of bytes specified by the Content-Length HTTP header.", key)
	case errors.Is(err, ErrChunkSignature):
		s.writeError(w, http.StatusForbidden, "SignatureDoesNotMatch", "The request signature we calculated does not match the signature you provided.", key)
	case errors.Is(err, ErrSHA256Mismatch):
		s.writeError(w, http.StatusBadRequest, "XAmzContentSHA256Mismatch", "The provided 'x-amz-content-sha256' header does not match what was computed.", key)
	case errors.Is(err, ErrMD5Mismatch), errors.Is(err, ErrChecksumMismatch):
		s.writeError(w, http.StatusBadRequest, "BadDigest", "The checksum you specified did not match what we received.", key)
	case errors.Is(err, ErrBadChunkFraming):
		s.writeError(w, http.StatusBadRequest, "InvalidRequest", "The request body is not valid aws-chunked content.", key)
	case errors.Is(err, storage.ErrBucketNotFound):
		s.writeError(w, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist.", bucket)
	case errors.Is(err, storage.ErrPoolOutOfSpace):
		s.writeError(w, http.StatusInsufficientStorage, "InsufficientStorage", "Storage capacity limit reached across accounts.", key)
	case errors.Is(err, db.ErrNotFound):
		s.writeError(w, http.StatusNotFound, "NoSuchKey", "The specified key does not exist.", key)
	case errors.Is(err, storage.ErrInvalidRange):
		s.writeError(w, http.StatusRequestedRangeNotSatisfiable, "InvalidRange", "The requested range is not satisfiable", key)
	case errors.Is(err, storage.ErrInvalidPart):
		s.writeError(w, http.StatusBadRequest, "InvalidPart", "One or more of the specified parts could not be found. The part may not have been uploaded, or the specified entity tag may not match the part's entity tag.", key)
	case errors.Is(err, storage.ErrInvalidPartOrder):
		s.writeError(w, http.StatusBadRequest, "InvalidPartOrder", "The list of parts was not in ascending order.", key)
	case errors.Is(err, storage.ErrNoSuchUpload):
		s.writeError(w, http.StatusNotFound, "NoSuchUpload", "The specified multipart upload does not exist.", key)
	default:
		s.internalError(w, err, key)
	}
	return true
}

func customMetadata(r *http.Request) map[string]string {
	meta := make(map[string]string)
	for h, values := range r.Header {
		lower := strings.ToLower(h)
		if strings.HasPrefix(lower, "x-amz-meta-") && len(values) > 0 {
			meta[strings.TrimPrefix(lower, "x-amz-meta-")] = values[0]
		}
	}
	return meta
}

func (s *Server) handlePutObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	body, declared, err := prepareBody(r, authFromRequest(r))
	if err != nil {
		s.writeStorageError(w, err, bucket, key)
		return
	}

	obj, err := s.pool.PutObject(r.Context(), bucket, key, r.Header.Get("Content-Type"), body, declared, customMetadata(r))
	if s.writeStorageError(w, err, bucket, key) {
		return
	}

	w.Header().Set("ETag", obj.ETag)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDeleteObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	if err := s.pool.DeleteObject(r.Context(), bucket, key); err != nil {
		s.internalError(w, err, key)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleDeleteObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	var req DeleteObjectsRequest
	if err := xml.NewDecoder(io.LimitReader(r.Body, maxXMLBodyBytes)).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "MalformedXML", "The XML provided was not well-formed", bucket)
		return
	}
	if len(req.Objects) > maxDeleteObjects {
		s.writeError(w, http.StatusBadRequest, "MalformedXML", "The request must not contain more than 1000 keys", bucket)
		return
	}

	var deleted []DeletedObject
	var deleteErrors []DeleteError

	for _, o := range req.Objects {
		if err := s.pool.DeleteObject(r.Context(), bucket, o.Key); err != nil {
			deleteErrors = append(deleteErrors, DeleteError{
				Key:     o.Key,
				Code:    "InternalError",
				Message: "We encountered an internal error. Please try again.",
			})
		} else if !req.Quiet {
			deleted = append(deleted, DeletedObject{Key: o.Key})
		}
	}

	s.writeXML(w, http.StatusOK, DeleteResult{Deleted: deleted, Error: deleteErrors})
}
