package s3api

import (
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"strconv"

	"hf2s3/pkg/storage"
)

func (s *Server) handleInitiateMultipart(w http.ResponseWriter, r *http.Request, bucket, key string) {
	uploadID, err := s.pool.InitiateMultipartUpload(r.Context(), bucket, key, r.Header.Get("Content-Type"), customMetadata(r))
	if s.writeStorageError(w, err, bucket, key) {
		return
	}

	s.writeXML(w, http.StatusOK, InitiateMultipartUploadResult{
		Bucket:   bucket,
		Key:      key,
		UploadId: uploadID,
	})
}

// checkUploadTarget makes sure uploadID belongs to bucket/key, so an upload id
// cannot be replayed against a different object.
func (s *Server) checkUploadTarget(w http.ResponseWriter, r *http.Request, uploadID, bucket, key string) bool {
	mp, err := s.pool.MultipartUpload(r.Context(), uploadID)
	if err != nil {
		s.writeStorageError(w, err, bucket, key)
		return false
	}
	if mp.Bucket != bucket || mp.Key != key {
		s.writeError(w, http.StatusNotFound, "NoSuchUpload", "The specified multipart upload does not exist.", key)
		return false
	}
	return true
}

func (s *Server) handleUploadPart(w http.ResponseWriter, r *http.Request, bucket, key string) {
	partNum, err := strconv.Atoi(r.URL.Query().Get("partNumber"))
	if err != nil || partNum < 1 || partNum > maxMultipartPartNo {
		s.writeError(w, http.StatusBadRequest, "InvalidArgument", "Part number must be an integer between 1 and 10000, inclusive", key)
		return
	}
	uploadID := r.URL.Query().Get("uploadId")
	if !s.checkUploadTarget(w, r, uploadID, bucket, key) {
		return
	}

	body, _, err := prepareBody(r, authFromRequest(r))
	if err != nil {
		s.writeStorageError(w, err, bucket, key)
		return
	}

	etag, err := s.pool.UploadPart(r.Context(), uploadID, partNum, body)
	if s.writeStorageError(w, err, bucket, key) {
		return
	}

	w.Header().Set("ETag", etag)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleCompleteMultipart(w http.ResponseWriter, r *http.Request, bucket, key string) {
	uploadID := r.URL.Query().Get("uploadId")
	if !s.checkUploadTarget(w, r, uploadID, bucket, key) {
		return
	}

	var req CompleteMultipartUploadRequest
	if err := xml.NewDecoder(io.LimitReader(r.Body, maxXMLBodyBytes)).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		s.writeError(w, http.StatusBadRequest, "MalformedXML", "The XML you provided was not well-formed or did not validate against our published schema.", key)
		return
	}
	parts := make([]storage.CompletedPart, len(req.Parts))
	for i, p := range req.Parts {
		parts[i] = storage.CompletedPart{PartNumber: p.PartNumber, ETag: p.ETag}
	}

	obj, err := s.pool.CompleteMultipartUpload(r.Context(), uploadID, parts)
	if s.writeStorageError(w, err, bucket, key) {
		return
	}

	s.writeXML(w, http.StatusOK, CompleteMultipartUploadResult{
		Location: "/" + bucket + "/" + key,
		Bucket:   bucket,
		Key:      key,
		ETag:     obj.ETag,
	})
}

func (s *Server) handleAbortMultipart(w http.ResponseWriter, r *http.Request, bucket, key string) {
	uploadID := r.URL.Query().Get("uploadId")
	if !s.checkUploadTarget(w, r, uploadID, bucket, key) {
		return
	}
	if err := s.pool.AbortMultipartUpload(r.Context(), uploadID); err != nil {
		s.internalError(w, err, key)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
