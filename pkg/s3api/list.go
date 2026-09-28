package s3api

import (
	"encoding/base64"
	"net/http"
	"strconv"
	"strings"

	"hf2s3/pkg/db"
	"hf2s3/pkg/sigv4"
)

// resumeCursor turns a client-supplied marker into the (key, inclusive) pair to
// scan from. When the marker is itself a common prefix returned by a previous
// page (it ends with the delimiter), everything under that prefix has already
// been reported, so the scan jumps past it.
func resumeCursor(after, prefix, delimiter string) (string, bool) {
	if delimiter == "" || after == "" || !strings.HasPrefix(after, prefix) {
		return after, false
	}
	rest := after[len(prefix):]
	if idx := strings.Index(rest, delimiter); idx >= 0 && idx == len(rest)-len(delimiter) {
		if upper, ok := db.PrefixUpperBound(after); ok {
			return upper, true
		}
	}
	return after, false
}

func (s *Server) handleListObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	ctx := r.Context()
	exists, err := s.pool.DB().BucketExists(ctx, bucket)
	if err != nil || !exists {
		s.writeError(w, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist.", bucket)
		return
	}

	q := r.URL.Query()
	v2 := q.Get("list-type") == "2"
	prefix := q.Get("prefix")
	delimiter := q.Get("delimiter")
	encodeURL := q.Get("encoding-type") == "url"
	if et := q.Get("encoding-type"); et != "" && et != "url" {
		s.writeError(w, http.StatusBadRequest, "InvalidArgument", "Invalid Encoding Method specified in Request", bucket)
		return
	}

	maxKeys := maxListKeys
	if mkStr := q.Get("max-keys"); mkStr != "" {
		mk, err := strconv.Atoi(mkStr)
		if err != nil || mk < 0 {
			s.writeError(w, http.StatusBadRequest, "InvalidArgument", "Provided max-keys not an integer or within integer range", bucket)
			return
		}
		if mk < maxKeys {
			maxKeys = mk
		}
	}

	var after, tokenIn string
	if v2 {
		tokenIn = q.Get("continuation-token")
		if tokenIn != "" {
			dec, err := base64.RawURLEncoding.DecodeString(tokenIn)
			if err != nil {
				s.writeError(w, http.StatusBadRequest, "InvalidArgument", "The continuation token provided is incorrect", bucket)
				return
			}
			after = string(dec)
		} else {
			after = q.Get("start-after")
		}
	} else {
		after = q.Get("marker")
	}

	enc := func(v string) string {
		if encodeURL {
			return sigv4.URIEncode(v, false)
		}
		return v
	}

	result := ListBucketResult{
		Name:      bucket,
		Prefix:    enc(prefix),
		Delimiter: enc(delimiter),
		MaxKeys:   maxKeys,
	}
	if encodeURL {
		result.EncodingType = "url"
	}

	cursor, inclusive := resumeCursor(after, prefix, delimiter)
	var (
		count      int
		truncated  bool
		lastReturn string
		lastPrefix string
	)

	pageSize := maxKeys + 1
	if pageSize < 100 {
		pageSize = 100
	}

scan:
	for maxKeys > 0 {
		objs, err := s.pool.DB().ListObjectsPage(ctx, bucket, prefix, cursor, inclusive, pageSize)
		if err != nil {
			s.internalError(w, err, bucket)
			return
		}
		if len(objs) == 0 {
			break
		}

		for _, o := range objs {
			if delimiter != "" {
				sub := strings.TrimPrefix(o.Key, prefix)
				if idx := strings.Index(sub, delimiter); idx >= 0 {
					cp := prefix + sub[:idx+len(delimiter)]
					if cp == lastPrefix {
						continue
					}
					if count == maxKeys {
						truncated = true
						break scan
					}
					result.CommonPrefixes = append(result.CommonPrefixes, CommonPrefix{Prefix: enc(cp)})
					lastPrefix, lastReturn = cp, cp
					count++

					// Everything under cp is covered: jump past it.
					upper, ok := db.PrefixUpperBound(cp)
					if !ok {
						break scan
					}
					cursor, inclusive = upper, true
					continue scan
				}
			}

			if count == maxKeys {
				truncated = true
				break scan
			}
			result.Contents = append(result.Contents, ObjectEntry{
				Key:          enc(o.Key),
				LastModified: FormatTime(o.UpdatedAt),
				ETag:         o.ETag,
				Size:         o.Size,
				StorageClass: "STANDARD",
			})
			lastReturn = o.Key
			count++
			cursor, inclusive = o.Key, false
		}

		if len(objs) < pageSize {
			break
		}
	}

	result.IsTruncated = truncated
	if v2 {
		result.KeyCount = count
		result.ContinuationToken = tokenIn
		result.StartAfter = enc(q.Get("start-after"))
		if truncated {
			result.NextContinuationToken = base64.RawURLEncoding.EncodeToString([]byte(lastReturn))
		}
	} else {
		marker := enc(after)
		result.Marker = &marker
		if truncated {
			result.NextMarker = enc(lastReturn)
		}
	}

	s.writeXML(w, http.StatusOK, result)
}
