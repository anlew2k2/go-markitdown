package usecase

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/google/uuid"

	"github.com/go-markitdown/internal/services"
	"github.com/go-markitdown/internal/utils"
)

const maxSplitPartSize = 16 * 1024 * 1024 // 16MB

// ErrOLENotSplittable is returned when a legacy OLE document (.doc, .xls)
// exceeds maxSplitPartSize: these formats have no mature pure-Go support
// for structural splitting, so they're rejected rather than attempted.
var ErrOLENotSplittable = services.ErrOLENotSplittable

// SplitFileInput identifies the message a file belongs to and where to find
// it.
type SplitFileInput struct {
	RequestID      uuid.UUID
	ConversationID uuid.UUID
	MessageID      uuid.UUID
	FilePath       string
}

// SplitFile fetches the file referenced by input.FilePath. If it already
// fits within 16MB, no work is done and the original URL is returned as the
// sole result. Otherwise the file is downloaded, split into parts no larger
// than 16MB and each part is uploaded to MinIO; the URL of every part is
// returned in order.
func SplitFile(ctx context.Context, input SplitFileInput) ([]string, error) {
	ext, err := utils.ExtensionOf(input.FilePath)
	if err != nil {
		return nil, fmt.Errorf("invalid file_path: %w", err)
	}
	if !utils.IsAllowedExtension(ext) {
		return nil, ErrUnsupportedFormat
	}

	if size, ok := remoteFileSize(ctx, input.FilePath); ok && size <= maxSplitPartSize {
		return []string{input.FilePath}, nil
	}

	localPath, err := downloadToTemp(ctx, input.FilePath, ext)
	if err != nil {
		return nil, err
	}
	defer os.Remove(localPath)

	info, err := os.Stat(localPath)
	if err != nil {
		return nil, fmt.Errorf("stat downloaded file: %w", err)
	}
	if info.Size() <= maxSplitPartSize {
		return []string{input.FilePath}, nil
	}

	if _, err := services.EnsureBucket(ctx); err != nil {
		return nil, fmt.Errorf("ensure bucket: %w", err)
	}

	objectPrefix := fmt.Sprintf("splits/%s/%s/%s", input.ConversationID, input.MessageID, input.RequestID)
	return services.SplitAndUpload(ctx, localPath, ext, maxSplitPartSize, objectPrefix)
}

// remoteFileSize does a cheap HEAD request to learn the file size without
// downloading it. ok is false whenever the size could not be determined,
// in which case the caller must fall back to downloading the file.
func remoteFileSize(ctx context.Context, fileURL string) (size int64, ok bool) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, fileURL, nil)
	if err != nil {
		return 0, false
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, false
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK || resp.ContentLength < 0 {
		return 0, false
	}
	return resp.ContentLength, true
}

func downloadToTemp(ctx context.Context, fileURL, ext string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fileURL, nil)
	if err != nil {
		return "", fmt.Errorf("build request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to download file: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to download file: status %d", resp.StatusCode)
	}

	tmpFile, err := os.CreateTemp("", "split-*"+ext)
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}
	defer tmpFile.Close()

	if _, err := io.Copy(tmpFile, resp.Body); err != nil {
		os.Remove(tmpFile.Name())
		return "", fmt.Errorf("failed to save file: %w", err)
	}

	return tmpFile.Name(), nil
}
