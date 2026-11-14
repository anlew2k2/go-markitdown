package services

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var (
	minioClient     *minio.Client
	minioClientErr  error
	minioClientOnce sync.Once
)

type minioConfig struct {
	endpoint  string
	publicURL string
	accessKey string
	secretKey string
	bucket    string
	useSSL    bool
}

func loadMinioConfig() minioConfig {
	cfg := minioConfig{
		endpoint:  envOr("MINIO_ENDPOINT", "minio:9000"),
		accessKey: envOr("MINIO_ACCESS_KEY", "minioadmin"),
		secretKey: envOr("MINIO_SECRET_KEY", "minioadmin"),
		bucket:    envOr("MINIO_BUCKET", "split-files"),
		useSSL:    envOr("MINIO_USE_SSL", "false") == "true",
	}
	cfg.publicURL = envOr("MINIO_PUBLIC_URL", cfg.endpoint)
	return cfg
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getMinioClient() (*minio.Client, minioConfig, error) {
	cfg := loadMinioConfig()
	minioClientOnce.Do(func() {
		minioClient, minioClientErr = minio.New(cfg.endpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(cfg.accessKey, cfg.secretKey, ""),
			Secure: cfg.useSSL,
		})
	})
	return minioClient, cfg, minioClientErr
}

// EnsureBucket creates the configured bucket if it does not exist yet and
// makes it publicly readable, so uploaded objects can be fetched with a
// plain HTTP GET. It returns the bucket name in use.
func EnsureBucket(ctx context.Context) (string, error) {
	client, cfg, err := getMinioClient()
	if err != nil {
		return "", fmt.Errorf("minio client: %w", err)
	}

	exists, err := client.BucketExists(ctx, cfg.bucket)
	if err != nil {
		return "", fmt.Errorf("check bucket: %w", err)
	}
	if !exists {
		if err := client.MakeBucket(ctx, cfg.bucket, minio.MakeBucketOptions{}); err != nil {
			return "", fmt.Errorf("create bucket: %w", err)
		}
		if err := client.SetBucketPolicy(ctx, cfg.bucket, publicReadPolicy(cfg.bucket)); err != nil {
			return "", fmt.Errorf("set bucket policy: %w", err)
		}
	}

	return cfg.bucket, nil
}

// UploadObject streams reader (size bytes) to MinIO under objectName and
// returns its public URL.
func UploadObject(ctx context.Context, objectName string, reader io.Reader, size int64, contentType string) (string, error) {
	client, cfg, err := getMinioClient()
	if err != nil {
		return "", fmt.Errorf("minio client: %w", err)
	}

	if _, err := client.PutObject(ctx, cfg.bucket, objectName, reader, size, minio.PutObjectOptions{
		ContentType: contentType,
	}); err != nil {
		return "", fmt.Errorf("upload %s: %w", objectName, err)
	}

	scheme := "http"
	if cfg.useSSL {
		scheme = "https"
	}
	return fmt.Sprintf("%s://%s/%s/%s", scheme, cfg.publicURL, cfg.bucket, objectName), nil
}

func publicReadPolicy(bucket string) string {
	return fmt.Sprintf(`{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Effect": "Allow",
			"Principal": {"AWS": ["*"]},
			"Action": ["s3:GetObject"],
			"Resource": ["arn:aws:s3:::%s/*"]
		}
	]
}`, bucket)
}
