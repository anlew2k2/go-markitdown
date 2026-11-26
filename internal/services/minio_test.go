package services

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func TestParseInternalObjectURL(t *testing.T) {
	t.Setenv("MINIO_ENDPOINT", "minio:9000")
	t.Setenv("MINIO_PUBLIC_URL", "https://files.internal:9443")
	t.Setenv("MINIO_BUCKET", "split-files")
	t.Setenv("MINIO_PRESIGN_TTL", "")

	tests := []struct {
		name    string
		raw     string
		wantOK  bool
		wantKey string
	}{
		{
			name:    "internal endpoint presigned url",
			raw:     "http://minio:9000/split-files/splits/c/m/r/part-001.pdf?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Expires=900",
			wantOK:  true,
			wantKey: "splits/c/m/r/part-001.pdf",
		},
		{
			name:    "public url host match",
			raw:     "https://files.internal:9443/split-files/doc.docx?token=1",
			wantOK:  true,
			wantKey: "doc.docx",
		},
		{
			name:   "wrong bucket",
			raw:    "http://minio:9000/other-bucket/doc.pdf",
			wantOK: false,
		},
		{
			name:   "external host",
			raw:    "https://example.com/split-files/doc.pdf",
			wantOK: false,
		},
		{
			name:   "empty key",
			raw:    "http://minio:9000/split-files/",
			wantOK: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bucket, key, ok := ParseInternalObjectURL(tc.raw)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v (bucket=%q key=%q)", ok, tc.wantOK, bucket, key)
			}
			if ok {
				if bucket != "split-files" {
					t.Fatalf("bucket = %q, want split-files", bucket)
				}
				if key != tc.wantKey {
					t.Fatalf("key = %q, want %q", key, tc.wantKey)
				}
			}
		})
	}
}

func TestLoadMinioConfigPresignTTL(t *testing.T) {
	t.Setenv("MINIO_PRESIGN_TTL", "")
	cfg, err := loadMinioConfig()
	if err != nil {
		t.Fatalf("default config error: %v", err)
	}
	if cfg.presignTTL != 15*time.Minute {
		t.Fatalf("default presignTTL = %s, want 15m", cfg.presignTTL)
	}

	t.Setenv("MINIO_PRESIGN_TTL", "5s")
	cfg, err = loadMinioConfig()
	if err != nil {
		t.Fatalf("low ttl error: %v", err)
	}
	if cfg.presignTTL != minPresignTTL {
		t.Fatalf("clamped low presignTTL = %s, want %s", cfg.presignTTL, minPresignTTL)
	}

	t.Setenv("MINIO_PRESIGN_TTL", "48h")
	cfg, err = loadMinioConfig()
	if err != nil {
		t.Fatalf("high ttl error: %v", err)
	}
	if cfg.presignTTL != maxPresignTTL {
		t.Fatalf("clamped high presignTTL = %s, want %s", cfg.presignTTL, maxPresignTTL)
	}

	t.Setenv("MINIO_PRESIGN_TTL", "not-a-duration")
	if _, err := loadMinioConfig(); err == nil {
		t.Fatal("expected error for invalid MINIO_PRESIGN_TTL")
	}

	t.Setenv("MINIO_PRESIGN_TTL", "0s")
	if _, err := loadMinioConfig(); err == nil {
		t.Fatal("expected error for zero MINIO_PRESIGN_TTL")
	}

	t.Setenv("MINIO_PRESIGN_TTL", "-5m")
	if _, err := loadMinioConfig(); err == nil {
		t.Fatal("expected error for negative MINIO_PRESIGN_TTL")
	}

	t.Setenv("MINIO_PRESIGN_TTL", "")
	t.Setenv("MINIO_BUCKET", "   ")
	if _, err := loadMinioConfig(); err == nil {
		t.Fatal("expected error for whitespace MINIO_BUCKET")
	}
	t.Setenv("MINIO_BUCKET", "split-files")
	t.Setenv("MINIO_ACCESS_KEY", "")
	t.Setenv("MINIO_SECRET_KEY", "")
	if _, _, _, err := getMinioClients(); err == nil {
		t.Fatal("expected error when MINIO_ACCESS_KEY and MINIO_SECRET_KEY are unset")
	}
}

func TestMinioIntegrationPrivateBucketAndPresign(t *testing.T) {
	endpoint := strings.TrimSpace(os.Getenv("TEST_MINIO_ENDPOINT"))
	if endpoint == "" {
		t.Skip("TEST_MINIO_ENDPOINT not set; skipping real MinIO integration test")
	}
	accessKey := envOr("TEST_MINIO_ACCESS_KEY", "testminioadmin")
	secretKey := envOr("TEST_MINIO_SECRET_KEY", "testminiosecret123")
	bucket := envOr("TEST_MINIO_BUCKET", "adr0004-test-bucket")

	t.Setenv("MINIO_ENDPOINT", endpoint)
	t.Setenv("MINIO_PUBLIC_URL", endpoint)
	t.Setenv("MINIO_ACCESS_KEY", accessKey)
	t.Setenv("MINIO_SECRET_KEY", secretKey)
	t.Setenv("MINIO_BUCKET", bucket)
	t.Setenv("MINIO_USE_SSL", "false")
	t.Setenv("MINIO_PRESIGN_TTL", "5m")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Seed a legacy bucket with anonymous public-read policy first to verify EnsureBucket strips it.
	rawClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false,
	})
	if err != nil {
		t.Fatalf("new raw minio client: %v", err)
	}
	exists, err := rawClient.BucketExists(ctx, bucket)
	if err != nil {
		t.Fatalf("bucket exists: %v", err)
	}
	if !exists {
		if err := rawClient.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
			t.Fatalf("make legacy bucket: %v", err)
		}
	}
	legacyPolicy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":["*"]},"Action":["s3:GetObject"],"Resource":["arn:aws:s3:::` + bucket + `/*"]}]}`
	if err := rawClient.SetBucketPolicy(ctx, bucket, legacyPolicy); err != nil {
		t.Fatalf("seed legacy policy: %v", err)
	}

	gotBucket, err := EnsureBucket(ctx)
	if err != nil {
		t.Fatalf("EnsureBucket failed: %v", err)
	}
	if gotBucket != bucket {
		t.Fatalf("EnsureBucket = %q, want %q", gotBucket, bucket)
	}

	payload := []byte("hello adr 0004 private bucket")
	objectKey := "splits/test/sample.txt"
	presignedURL, err := UploadObject(ctx, objectKey, bytes.NewReader(payload), int64(len(payload)), "text/plain")
	if err != nil {
		t.Fatalf("UploadObject failed: %v", err)
	}
	if !strings.Contains(presignedURL, "X-Amz-Signature=") {
		t.Fatalf("expected presigned URL with signature, got %q", presignedURL)
	}

	// Anonymous unsigned HTTP GET must be denied (403).
	unsignedURL := "http://" + endpoint + "/" + bucket + "/" + objectKey
	anonResp, err := http.Get(unsignedURL)
	if err != nil {
		t.Fatalf("anonymous GET failed: %v", err)
	}
	anonResp.Body.Close()
	if anonResp.StatusCode != http.StatusForbidden {
		t.Fatalf("anonymous GET status = %d, want 403 Forbidden", anonResp.StatusCode)
	}

	// Presigned URL must succeed (200) with exact payload.
	presignedResp, err := http.Get(presignedURL)
	if err != nil {
		t.Fatalf("presigned GET failed: %v", err)
	}
	defer presignedResp.Body.Close()
	if presignedResp.StatusCode != http.StatusOK {
		t.Fatalf("presigned GET status = %d, want 200 OK", presignedResp.StatusCode)
	}
	gotBytes, err := io.ReadAll(presignedResp.Body)
	if err != nil {
		t.Fatalf("read presigned body: %v", err)
	}
	if !bytes.Equal(gotBytes, payload) {
		t.Fatalf("presigned body = %q, want %q", gotBytes, payload)
	}

	// Authenticated StatObject and GetObject must succeed.
	stat, err := StatObject(ctx, objectKey)
	if err != nil {
		t.Fatalf("StatObject failed: %v", err)
	}
	if stat.Size != int64(len(payload)) {
		t.Fatalf("stat.Size = %d, want %d", stat.Size, len(payload))
	}
	rc, err := GetObject(ctx, objectKey)
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer rc.Close()
	authBytes, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read GetObject body: %v", err)
	}
	if !bytes.Equal(authBytes, payload) {
		t.Fatalf("GetObject body = %q, want %q", authBytes, payload)
	}

	// FetchInternalPresignedObject must succeed for valid presigned URLs, reject forged signatures, and reject unsigned URLs.
	fetched, err := FetchInternalPresignedObject(ctx, http.MethodGet, presignedURL)
	if err != nil {
		t.Fatalf("FetchInternalPresignedObject valid failed: %v", err)
	}
	fetchedBytes, err := io.ReadAll(fetched.Body)
	fetched.Body.Close()
	if err != nil {
		t.Fatalf("read FetchInternalPresignedObject body: %v", err)
	}
	if fetched.StatusCode != http.StatusOK || !bytes.Equal(fetchedBytes, payload) {
		t.Fatalf("FetchInternalPresignedObject status=%d body=%q, want 200 %q", fetched.StatusCode, fetchedBytes, payload)
	}

	forgedResp, err := FetchInternalPresignedObject(ctx, http.MethodGet, unsignedURL+"?X-Amz-Signature=deadbeef")
	if err != nil {
		t.Fatalf("FetchInternalPresignedObject forged request error: %v", err)
	}
	forgedResp.Body.Close()
	if forgedResp.StatusCode != http.StatusForbidden {
		t.Fatalf("forged presigned status = %d, want 403 Forbidden", forgedResp.StatusCode)
	}

	if IsPresignedInternalObjectURL(unsignedURL) {
		t.Fatalf("expected unsigned internal URL to return false from IsPresignedInternalObjectURL")
	}
}
