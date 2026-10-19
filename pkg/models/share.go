package models

import "time"

// ShareItem is one entry of a shared link: a single object, or a whole
// "folder" (every key that starts with Key, which then ends in "/").
type ShareItem struct {
	Key      string `json:"key"`
	IsPrefix bool   `json:"is_prefix"`
}

// Share is a link that lets anyone holding it download (and stream) a fixed
// set of objects of one bucket, optionally behind a password and until a
// deadline.
type Share struct {
	ID           int64       `json:"id"`
	Token        string      `json:"token,omitempty"` // only filled for the admin
	Bucket       string      `json:"bucket"`
	Title        string      `json:"title"`
	HasPassword  bool        `json:"has_password"`
	PasswordHash string      `json:"-"`
	ExpiresAt    *time.Time  `json:"expires_at"` // nil = never expires
	CreatedAt    time.Time   `json:"created_at"`
	Downloads    int64       `json:"downloads"`
	Items        []ShareItem `json:"items"`
}

// Expired reports whether the link is past its deadline at now.
func (s *Share) Expired(now time.Time) bool {
	return s.ExpiresAt != nil && !now.Before(*s.ExpiresAt)
}

// Covers reports whether key is reachable through this share.
func (s *Share) Covers(key string) bool {
	for _, it := range s.Items {
		if it.IsPrefix {
			if len(key) >= len(it.Key) && key[:len(it.Key)] == it.Key {
				return true
			}
		} else if key == it.Key {
			return true
		}
	}
	return false
}

// CoversFolder reports whether browsing the folder prefix path is allowed:
// it must lie inside one of the shared folders.
func (s *Share) CoversFolder(path string) bool {
	for _, it := range s.Items {
		if it.IsPrefix && len(path) >= len(it.Key) && path[:len(it.Key)] == it.Key {
			return true
		}
	}
	return false
}
