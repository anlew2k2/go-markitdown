// Command seedfixtures uploads local fixture files to a MinIO bucket under
// explicit object names, for preparing the k6 load-test dataset. It is a
// support tool: it only PUTs the files you pass it (never deletes, never
// touches Redis) and makes the bucket public-read if it has to create it
// (mirroring internal/services/minio.go's policy, without importing
// application code into this standalone tool).
//
// Usage:
//
//	go run ./loadtest/tools/seedfixtures \
//	    -endpoint localhost:19000 -access-key minioadmin -secret-key minioadmin -bucket dataset \
//	    -file tmp/report.pdf=report.pdf \
//	    -file tmp/pdf_small_1mib.pdf=pdf_small_1mib.pdf
package main

import (
	"context"
	"flag"
	"fmt"
	"mime"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type fileFlag struct{ pairs []string }

func (f *fileFlag) String() string { return strings.Join(f.pairs, ",") }
func (f *fileFlag) Set(v string) error {
	f.pairs = append(f.pairs, v)
	return nil
}

func main() {
	endpoint := flag.String("endpoint", "localhost:19000", "MinIO endpoint (host:port, no scheme)")
	accessKey := flag.String("access-key", "minioadmin", "MinIO access key")
	secretKey := flag.String("secret-key", "minioadmin", "MinIO secret key (never logged)")
	bucket := flag.String("bucket", "dataset", "target bucket")
	useSSL := flag.Bool("use-ssl", false, "use HTTPS")
	var files fileFlag
	flag.Var(&files, "file", "local=remote pair; repeatable")
	flag.Parse()

	if len(files.pairs) == 0 {
		fmt.Fprintln(os.Stderr, "seedfixtures: at least one -file local=remote is required")
		os.Exit(2)
	}

	client, err := minio.New(*endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(*accessKey, *secretKey, ""),
		Secure: *useSSL,
	})
	if err != nil {
		fatal("create minio client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if err := ensureBucket(ctx, client, *bucket); err != nil {
		fatal("ensure bucket %s: %v", *bucket, err)
	}

	failures := 0
	for _, pair := range files.pairs {
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			fmt.Fprintf(os.Stderr, "seedfixtures: SKIP malformed -file %q (want local=remote)\n", pair)
			failures++
			continue
		}
		local, remote := parts[0], parts[1]

		info, err := os.Stat(local)
		if err != nil {
			fmt.Fprintf(os.Stderr, "seedfixtures: FAIL %s: %v\n", local, err)
			failures++
			continue
		}

		contentType := mime.TypeByExtension(filepath.Ext(local))
		if contentType == "" {
			contentType = "application/octet-stream"
		}

		uploadInfo, err := client.FPutObject(ctx, *bucket, remote, local, minio.PutObjectOptions{ContentType: contentType})
		if err != nil {
			fmt.Fprintf(os.Stderr, "seedfixtures: FAIL upload %s -> %s/%s: %v\n", local, *bucket, remote, err)
			failures++
			continue
		}
		fmt.Printf("OK   %s (%d bytes) -> %s/%s (etag=%s)\n", local, info.Size(), *bucket, remote, uploadInfo.ETag)
	}

	if failures > 0 {
		fmt.Fprintf(os.Stderr, "seedfixtures: %d upload(s) failed\n", failures)
		os.Exit(1)
	}
}

func ensureBucket(ctx context.Context, client *minio.Client, bucket string) error {
	exists, err := client.BucketExists(ctx, bucket)
	if err != nil {
		return fmt.Errorf("check bucket: %w", err)
	}
	if exists {
		return nil
	}
	if err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
		return fmt.Errorf("create bucket: %w", err)
	}
	policy := fmt.Sprintf(`{
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
	if err := client.SetBucketPolicy(ctx, bucket, policy); err != nil {
		return fmt.Errorf("set bucket policy: %w", err)
	}
	return nil
}

func fatal(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "seedfixtures: "+format+"\n", args...)
	os.Exit(1)
}
