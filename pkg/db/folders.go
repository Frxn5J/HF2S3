package db

import (
	"context"
	"strings"

	"hf2s3/pkg/models"
)

// FolderPage is one page of a delimiter ("/") listing of a bucket.
type FolderPage struct {
	Folders []string        // full prefixes ending in "/"
	Objects []models.Object // keys directly inside the listed prefix
	Next    string          // opaque cursor for the next page, "" when done
}

// ListFolder lists the immediate children of prefix, like an S3 listing with
// delimiter "/": sub-folders are collapsed into one entry each (without
// scanning what is inside them) and keys that are exactly prefix (folder
// marker objects) are hidden. limit counts folders and objects together, in
// key order; cursor comes from a previous page's Next.
func (d *DB) ListFolder(ctx context.Context, bucket, prefix, cursor string, limit int) (*FolderPage, error) {
	if limit < 1 {
		limit = 1
	}
	after, inclusive := "", false
	switch {
	case strings.HasPrefix(cursor, "i:"):
		after, inclusive = cursor[2:], true
	case strings.HasPrefix(cursor, "e:"):
		after = cursor[2:]
	}

	pageSize := limit + 1
	if pageSize < 100 {
		pageSize = 100
	}

	out := &FolderPage{}
	count := 0
	lastFolder := ""
scan:
	for {
		objs, err := d.ListObjectsPage(ctx, bucket, prefix, after, inclusive, pageSize)
		if err != nil {
			return nil, err
		}
		if len(objs) == 0 {
			return out, nil
		}
		for _, o := range objs {
			rest := o.Key[len(prefix):]
			if rest == "" {
				continue
			}
			if idx := strings.IndexByte(rest, '/'); idx >= 0 {
				folder := prefix + rest[:idx+1]
				if folder == lastFolder {
					continue
				}
				if count == limit {
					out.Next = "i:" + folder
					return out, nil
				}
				out.Folders = append(out.Folders, folder)
				lastFolder = folder
				count++
				upper, ok := PrefixUpperBound(folder)
				if !ok {
					return out, nil
				}
				after, inclusive = upper, true
				continue scan
			}
			if count == limit {
				out.Next = "i:" + o.Key
				return out, nil
			}
			out.Objects = append(out.Objects, o)
			count++
			after, inclusive = o.Key, false
		}
		if len(objs) < pageSize {
			return out, nil
		}
	}
}
