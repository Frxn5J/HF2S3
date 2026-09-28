package sigv4

import (
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestURIEncode(t *testing.T) {
	cases := []struct {
		in          string
		encodeSlash bool
		want        string
	}{
		{"abc-_.~XYZ019", true, "abc-_.~XYZ019"},
		{"a b", true, "a%20b"},
		{"a+b", true, "a%2Bb"},
		{"a/b", true, "a%2Fb"},
		{"a/b", false, "a/b"},
		{"100%", true, "100%25"},
		{"ñ", true, "%C3%B1"},
		{"$file.text", false, "%24file.text"},
	}
	for _, c := range cases {
		if got := URIEncode(c.in, c.encodeSlash); got != c.want {
			t.Errorf("URIEncode(%q,%v) = %q, want %q", c.in, c.encodeSlash, got, c.want)
		}
	}
}

// Vector from the AWS documentation: "Authenticating Requests: Using Query
// Parameters (AWS Signature Version 4)".
func TestPresignGETAWSVector(t *testing.T) {
	ts := time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC)
	got := PresignGET("https", "examplebucket.s3.amazonaws.com", "/test.txt",
		"AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		"us-east-1", "s3", ts, 86400*time.Second)

	u, err := url.Parse(got)
	if err != nil {
		t.Fatal(err)
	}
	const wantSig = "aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"
	if u.Query().Get("X-Amz-Signature") != wantSig {
		t.Fatalf("signature mismatch: got %s want %s", u.Query().Get("X-Amz-Signature"), wantSig)
	}
	if !strings.Contains(got, "X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request") {
		t.Fatalf("credential not encoded as expected: %s", got)
	}
}

func TestCanonicalQuerySortsAndEncodes(t *testing.T) {
	v := url.Values{}
	v.Set("b", "2 3")
	v.Set("a", "+")
	v["uploads"] = []string{""}
	got := CanonicalQuery(v, "skipme")
	want := "a=%2B&b=2%203&uploads="
	if got != want {
		t.Fatalf("CanonicalQuery = %q, want %q", got, want)
	}
}

func TestPresignEscapesSpecialKeys(t *testing.T) {
	got := PresignGET("https", "s3.hf.co", "/bucket/my file+1.mp4", "AK", "SK", "us-east-1", "s3", time.Now(), time.Minute)
	if !strings.Contains(got, "/bucket/my%20file%2B1.mp4?") {
		t.Fatalf("path not AWS-encoded: %s", got)
	}
	if _, err := url.Parse(got); err != nil {
		t.Fatal(err)
	}
}
