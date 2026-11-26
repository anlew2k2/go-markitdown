package services

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const (
	defaultPresignTTL = 15 * time.Minute
	minPresignTTL     = 1 * time.Minute
	maxPresignTTL     = 24 * time.Hour
)

var (
	minioMu             sync.Mutex
	cachedMinioCfg      minioConfig
	cachedMinioClient   *minio.Client
	cachedPresignClient *minio.Client
	cachedMinioInit     bool
)

type minioConfig struct {
	endpoint   string
	publicURL  string
	accessKey  string
	secretKey  string
	bucket     string
	useSSL     bool
	presignTTL time.Duration
}

func loadMinioConfig() (minioConfig, error) {
	bucket := strings.TrimSpace(envOr("MINIO_BUCKET", "split-files"))
	if bucket == "" {
		return minioConfig{}, fmt.Errorf("MINIO_BUCKET must not be empty")
	}
	cfg := minioConfig{
		endpoint:   strings.TrimSpace(envOr("MINIO_ENDPOINT", "minio:9000")),
		accessKey:  strings.TrimSpace(os.Getenv("MINIO_ACCESS_KEY")),
		secretKey:  strings.TrimSpace(os.Getenv("MINIO_SECRET_KEY")),
		bucket:     bucket,
		useSSL:     strings.EqualFold(strings.TrimSpace(envOr("MINIO_USE_SSL", "false")), "true"),
		presignTTL: defaultPresignTTL,
	}
	cfg.publicURL = strings.TrimSpace(envOr("MINIO_PUBLIC_URL", cfg.endpoint))
	if rawTTL := strings.TrimSpace(os.Getenv("MINIO_PRESIGN_TTL")); rawTTL != "" {
		d, err := time.ParseDuration(rawTTL)
		if err != nil || d <= 0 {
			return minioConfig{}, fmt.Errorf("invalid MINIO_PRESIGN_TTL %q: must be a positive duration", rawTTL)
		}
		if d < minPresignTTL {
			d = minPresignTTL
		} else if d > maxPresignTTL {
			d = maxPresignTTL
		}
		cfg.presignTTL = d
	}
	return cfg, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func parseEndpointHost(raw string, defaultSSL bool) (host string, secure bool) {
	trimmed := strings.TrimSpace(raw)
	lower := strings.ToLower(trimmed)
	if strings.HasPrefix(lower, "http://") || strings.HasPrefix(lower, "https://") {
		if u, err := url.Parse(trimmed); err == nil && u.Host != "" {
			return u.Host, strings.EqualFold(u.Scheme, "https")
		}
	}
	return strings.TrimRight(trimmed, "/"), defaultSSL
}

func getMinioClients() (*minio.Client, *minio.Client, minioConfig, error) {
	cfg, err := loadMinioConfig()
	if err != nil {
		return nil, nil, minioConfig{}, err
	}
	if cfg.accessKey == "" || cfg.secretKey == "" {
		return nil, nil, minioConfig{}, fmt.Errorf("MINIO_ACCESS_KEY and MINIO_SECRET_KEY are required")
	}

	minioMu.Lock()
	defer minioMu.Unlock()

	if cachedMinioInit && cachedMinioCfg == cfg && cachedMinioClient != nil && cachedPresignClient != nil {
		return cachedMinioClient, cachedPresignClient, cfg, nil
	}

	endpointHost, endpointSSL := parseEndpointHost(cfg.endpoint, cfg.useSSL)
	client, err := minio.New(endpointHost, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.accessKey, cfg.secretKey, ""),
		Secure: endpointSSL,
	})
	if err != nil {
		return nil, nil, minioConfig{}, err
	}

	presignClient := client
	publicHost, publicSSL := parseEndpointHost(cfg.publicURL, cfg.useSSL)
	if !strings.EqualFold(publicHost, endpointHost) || publicSSL != endpointSSL {
		// Pin Region so presigning against an external/alternate host never
		// requires an unauthenticated or unreachable GetBucketLocation roundtrip.
		presignClient, err = minio.New(publicHost, &minio.Options{
			Creds:  credentials.NewStaticV4(cfg.accessKey, cfg.secretKey, ""),
			Secure: publicSSL,
			Region: "us-east-1",
		})
		if err != nil {
			return nil, nil, minioConfig{}, err
		}
	}

	cachedMinioCfg = cfg
	cachedMinioClient = client
	cachedPresignClient = presignClient
	cachedMinioInit = true
	return client, presignClient, cfg, nil
}

// EnsureBucket creates the configured bucket if it does not exist yet (with
// private access) and strips any legacy anonymous bucket policy on existing
// buckets. It returns the bucket name in use.
func EnsureBucket(ctx context.Context) (string, error) {
	client, _, cfg, err := getMinioClients()
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
		return cfg.bucket, nil
	}

	// Clean up any legacy anonymous public-read policy left by prior runs.
	if err := client.SetBucketPolicy(ctx, cfg.bucket, ""); err != nil {
		return "", fmt.Errorf("clear bucket policy: %w", err)
	}
	return cfg.bucket, nil
}

// UploadObject streams reader (size bytes) to MinIO under objectName and
// returns a short-lived presigned GET URL.
func UploadObject(ctx context.Context, objectName string, reader io.Reader, size int64, contentType string) (string, error) {
	client, presignClient, cfg, err := getMinioClients()
	if err != nil {
		return "", fmt.Errorf("minio client: %w", err)
	}

	if _, err := client.PutObject(ctx, cfg.bucket, objectName, reader, size, minio.PutObjectOptions{
		ContentType: contentType,
	}); err != nil {
		return "", fmt.Errorf("upload %s: %w", objectName, err)
	}

	presigned, err := presignClient.PresignedGetObject(ctx, cfg.bucket, objectName, cfg.presignTTL, nil)
	if err != nil {
		return "", fmt.Errorf("presign %s: %w", objectName, err)
	}
	return presigned.String(), nil
}

// ParseInternalObjectURL reports whether rawURL targets the configured MinIO
// endpoint or public URL and configured bucket, returning the bucket and object key.
func ParseInternalObjectURL(rawURL string) (bucket, objectName string, ok bool) {
	cfg, err := loadMinioConfig()
	if err != nil {
		return "", "", false
	}
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || u.Host == "" || u.User != nil {
		return "", "", false
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", "", false
	}

	endpointHost, _ := parseEndpointHost(cfg.endpoint, cfg.useSSL)
	publicHost, _ := parseEndpointHost(cfg.publicURL, cfg.useSSL)
	if !strings.EqualFold(u.Host, endpointHost) && !strings.EqualFold(u.Host, publicHost) {
		return "", "", false
	}

	prefix := "/" + cfg.bucket + "/"
	if !strings.HasPrefix(u.Path, prefix) {
		return "", "", false
	}
	key := strings.TrimPrefix(u.Path, prefix)
	if key == "" {
		return "", "", false
	}
	return cfg.bucket, key, true
}

// IsPresignedInternalObjectURL reports whether rawURL is a presigned internal
// MinIO URL carrying an AWS SigV4 signature (X-Amz-Signature).
func IsPresignedInternalObjectURL(rawURL string) bool {
	if _, _, ok := ParseInternalObjectURL(rawURL); !ok {
		return false
	}
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return false
	}
	return strings.TrimSpace(u.Query().Get("X-Amz-Signature")) != ""
}

// FetchInternalPresignedObject executes method (GET or HEAD) against a
// presigned internal MinIO URL by dialing the configured MINIO_ENDPOINT while
// preserving the signed Host header and query parameters so MinIO itself
// verifies the signature and expiration.
func FetchInternalPresignedObject(ctx context.Context, method, rawURL string) (*http.Response, error) {
	if method != http.MethodGet && method != http.MethodHead {
		return nil, fmt.Errorf("unsupported method %q", method)
	}
	if !IsPresignedInternalObjectURL(rawURL) {
		return nil, fmt.Errorf("url is not a presigned internal MinIO object url")
	}
	cfg, err := loadMinioConfig()
	if err != nil {
		return nil, err
	}
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return nil, err
	}

	endpointHost, endpointSSL := parseEndpointHost(cfg.endpoint, cfg.useSSL)
	targetURL := *u
	if endpointSSL {
		targetURL.Scheme = "https"
	} else {
		targetURL.Scheme = "http"
	}
	targetURL.Host = endpointHost

	req, err := http.NewRequestWithContext(ctx, method, targetURL.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("build presigned request: %w", err)
	}
	req.Host = u.Host

	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	client := &http.Client{
		Timeout: 2 * time.Minute,
		Transport: &http.Transport{
			Proxy: nil,
			DialContext: func(dctx context.Context, network, _ string) (net.Conn, error) {
				addr := endpointHost
				if _, _, splitErr := net.SplitHostPort(addr); splitErr != nil {
					if endpointSSL {
						addr = net.JoinHostPort(addr, "443")
					} else {
						addr = net.JoinHostPort(addr, "80")
					}
				}
				return dialer.DialContext(dctx, network, addr)
			},
		},
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return client.Do(req)
}

// StatObject fetches metadata for objectName in the configured bucket using
// authenticated MinIO credentials.
func StatObject(ctx context.Context, objectName string) (minio.ObjectInfo, error) {
	client, _, cfg, err := getMinioClients()
	if err != nil {
		return minio.ObjectInfo{}, fmt.Errorf("minio client: %w", err)
	}
	return client.StatObject(ctx, cfg.bucket, objectName, minio.StatObjectOptions{})
}

// GetObject opens an authenticated reader for objectName in the configured
// bucket and verifies object readability before returning it.
func GetObject(ctx context.Context, objectName string) (io.ReadCloser, error) {
	client, _, cfg, err := getMinioClients()
	if err != nil {
		return nil, fmt.Errorf("minio client: %w", err)
	}
	obj, err := client.GetObject(ctx, cfg.bucket, objectName, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	if _, err := obj.Stat(); err != nil {
		_ = obj.Close()
		return nil, err
	}
	return obj, nil
}
