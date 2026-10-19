package db

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"hf2s3/pkg/models"
)

func seedKeys(t *testing.T, d *DB, bucket string, keys ...string) {
	t.Helper()
	ctx := context.Background()
	if err := d.CreateBucket(ctx, bucket); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	for _, k := range keys {
		obj := &models.Object{Bucket: bucket, Key: k, Size: 1, ETag: `"e"`, ContentType: "text/plain", CreatedAt: now, UpdatedAt: now}
		if err := d.SaveObjectWithChunks(ctx, obj, nil); err != nil {
			t.Fatalf("save %s: %v", k, err)
		}
	}
}

func TestShareLifecycle(t *testing.T) {
	d, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	ctx := context.Background()

	exp := time.Now().Add(time.Hour).UTC()
	s := &models.Share{Bucket: "b", Title: "viaje", PasswordHash: "h", ExpiresAt: &exp,
		Items: []models.ShareItem{{Key: "a.txt"}, {Key: "dir/", IsPrefix: true}, {Key: "a.txt"}}}
	if err := d.CreateShare(ctx, s, "tok-123"); err != nil {
		t.Fatal(err)
	}
	if s.ID == 0 || !s.HasPassword || s.Token != "tok-123" {
		t.Fatalf("unexpected share after create: %+v", s)
	}

	got, err := d.GetShareByToken(ctx, "tok-123")
	if err != nil {
		t.Fatal(err)
	}
	if got.Title != "viaje" || !got.HasPassword || got.ExpiresAt == nil || len(got.Items) != 2 {
		t.Fatalf("unexpected share: %+v", got)
	}
	if got.Expired(time.Now()) || !got.Expired(time.Now().Add(2*time.Hour)) {
		t.Fatal("Expired is wrong")
	}
	if !got.Covers("a.txt") || !got.Covers("dir/x/y.mp4") || got.Covers("b.txt") || got.Covers("dir") || got.Covers("directory/x") {
		t.Fatal("Covers is wrong")
	}
	if !got.CoversFolder("dir/x/") || got.CoversFolder("") || got.CoversFolder("other/") {
		t.Fatal("CoversFolder is wrong")
	}

	if _, err := d.GetShareByToken(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown token = %v, want ErrNotFound", err)
	}

	// The raw token is never stored in clear next to its lookup hash.
	var hash string
	_ = d.db.QueryRow(`SELECT token_hash FROM shares`).Scan(&hash)
	if hash == "tok-123" || len(hash) != 64 {
		t.Fatalf("token_hash = %q", hash)
	}

	if err := d.AddShareDownloads(ctx, s.ID, 2); err != nil {
		t.Fatal(err)
	}
	list, err := d.ListShares(ctx)
	if err != nil || len(list) != 1 || list[0].Downloads != 2 || len(list[0].Items) != 2 {
		t.Fatalf("list = %+v, %v", list, err)
	}

	if err := d.DeleteShare(ctx, s.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := d.GetShareByToken(ctx, "tok-123"); !errors.Is(err, ErrNotFound) {
		t.Fatal("a revoked share must disappear")
	}
	var n int
	_ = d.db.QueryRow(`SELECT COUNT(*) FROM share_items`).Scan(&n)
	if n != 0 {
		t.Fatalf("share items were not deleted with their share (%d left)", n)
	}
	if err := d.DeleteShare(ctx, s.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second delete = %v, want ErrNotFound", err)
	}
}

func TestListFolderCollapsesSubfolders(t *testing.T) {
	d, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	ctx := context.Background()
	seedKeys(t, d, "b", "root.txt", "a/", "a/1.txt", "a/sub/2.txt", "a/sub/3.txt", "b/1.txt", "c/x/y/z.txt", "zz.txt")

	page, err := d.ListFolder(ctx, "b", "", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(page.Folders) != "[a/ b/ c/]" || len(page.Objects) != 2 || page.Next != "" {
		t.Fatalf("root = folders %v objects %d next %q", page.Folders, len(page.Objects), page.Next)
	}

	page, _ = d.ListFolder(ctx, "b", "a/", "", 100)
	if fmt.Sprint(page.Folders) != "[a/sub/]" || len(page.Objects) != 1 || page.Objects[0].Key != "a/1.txt" {
		t.Fatalf("a/ = folders %v objects %+v (the folder marker a/ must be hidden)", page.Folders, page.Objects)
	}
}

func TestListFolderPaginates(t *testing.T) {
	d, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	ctx := context.Background()
	var keys []string
	for i := 0; i < 7; i++ {
		keys = append(keys, fmt.Sprintf("f%d/a.txt", i), fmt.Sprintf("f%d/b.txt", i), fmt.Sprintf("file%d.txt", i))
	}
	seedKeys(t, d, "b", keys...)

	var folders, files []string
	cursor := ""
	for pages := 0; ; pages++ {
		if pages > 20 {
			t.Fatal("pagination does not terminate")
		}
		page, err := d.ListFolder(ctx, "b", "", cursor, 4)
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Folders)+len(page.Objects) > 4 {
			t.Fatalf("page over the limit: %d", len(page.Folders)+len(page.Objects))
		}
		folders = append(folders, page.Folders...)
		for _, o := range page.Objects {
			files = append(files, o.Key)
		}
		if page.Next == "" {
			break
		}
		cursor = page.Next
	}
	if len(folders) != 7 || len(files) != 7 {
		t.Fatalf("paged listing returned %d folders and %d files, want 7 and 7 (%v %v)", len(folders), len(files), folders, files)
	}
	seen := map[string]bool{}
	for _, f := range append(folders, files...) {
		if seen[f] {
			t.Fatalf("%s listed twice", f)
		}
		seen[f] = true
	}
}
