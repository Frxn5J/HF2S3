package s3api

import (
	"net/http"
	"time"

	"hf2s3/pkg/sigv4"
)

const (
	testAccessKey = "test-access-key"
	testSecretKey = "test-secret-key"
	testRegion    = "us-east-1"
)

// signAs signs req like an S3 SDK would.
func signAs(req *http.Request, access, secret, payloadHash string) {
	sigv4.SignRequest(req, access, secret, testRegion, "s3", time.Now(), payloadHash)
}

// signTest signs req with the test credentials and an unsigned payload.
func signTest(req *http.Request) {
	signAs(req, testAccessKey, testSecretKey, sigv4.UnsignedPayload)
}
