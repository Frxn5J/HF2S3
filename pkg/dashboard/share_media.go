package dashboard

import (
	"mime"
	"path"
	"strings"
)

// Shared links may render a file inline (video/audio player, image preview)
// only for these content types. Anything else, SVG and HTML above all, is
// always served as an opaque download: the stored Content-Type is chosen by
// whoever uploaded the file and must never run as a page on this origin.
var inlineTypes = map[string]string{ // content type -> kind
	"video/mp4":       "video",
	"video/webm":      "video",
	"video/ogg":       "video",
	"video/quicktime": "video",
	"audio/mpeg":      "audio",
	"audio/mp4":       "audio",
	"audio/aac":       "audio",
	"audio/ogg":       "audio",
	"audio/opus":      "audio",
	"audio/wav":       "audio",
	"audio/x-wav":     "audio",
	"audio/flac":      "audio",
	"audio/webm":      "audio",
	"image/png":       "image",
	"image/jpeg":      "image",
	"image/gif":       "image",
	"image/webp":      "image",
	"image/avif":      "image",
}

var extTypes = map[string]string{
	".mp4": "video/mp4", ".m4v": "video/mp4", ".webm": "video/webm", ".ogv": "video/ogg", ".mov": "video/quicktime",
	".mp3": "audio/mpeg", ".m4a": "audio/mp4", ".aac": "audio/aac", ".oga": "audio/ogg", ".ogg": "audio/ogg",
	".opus": "audio/opus", ".wav": "audio/wav", ".flac": "audio/flac",
	".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".gif": "image/gif", ".webp": "image/webp", ".avif": "image/avif",
}

// inlineType returns the safe content type and kind ("video", "audio",
// "image") a file may be shown with, or "" when it can only be downloaded.
// The stored type wins; the extension covers generic or missing types.
func inlineType(contentType, key string) (ctype, kind string) {
	if mt, _, err := mime.ParseMediaType(contentType); err == nil {
		if k, ok := inlineTypes[strings.ToLower(mt)]; ok {
			return strings.ToLower(mt), k
		}
	}
	if t, ok := extTypes[strings.ToLower(path.Ext(key))]; ok {
		return t, inlineTypes[t]
	}
	return "", ""
}

// baseName is the last path segment of a key.
func baseName(key string) string {
	key = strings.TrimSuffix(key, "/")
	if i := strings.LastIndexByte(key, '/'); i >= 0 {
		return key[i+1:]
	}
	return key
}
