package models

import (
	"errors"
	"regexp"
	"strings"
)

var bucketNameRE = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)

// reservedBucketNames collide with URL prefixes served by the gateway itself:
// /media/{bucket}/{key} (media route) and /api, /static (web console).
var reservedBucketNames = map[string]bool{"media": true, "api": true, "static": true}

// ValidateBucketName enforces S3 naming rules plus the gateway's reserved names.
func ValidateBucketName(name string) error {
	switch {
	case !bucketNameRE.MatchString(name) || strings.Contains(name, ".."):
		return errors.New("bucket names must be 3-63 characters of lowercase letters, digits, hyphens and dots, starting and ending with a letter or digit")
	case reservedBucketNames[name]:
		return errors.New("this bucket name is reserved by the gateway (media, api, static)")
	}
	return nil
}
