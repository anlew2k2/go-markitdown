package usecase

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/google/uuid"

	"github.com/go-markitdown/internal/services"
	"github.com/go-markitdown/internal/utils"
)

const (
	maxSplitPartSize     = 4 * 1024 * 1024   // 4MB
	maxSplitDownloadSize = 100 * 1024 * 1024 // 100MB
)

var ErrSplitFileTooLarge = errors.New("file exceeds the 100MB download limit")

// headRequestTimeout bounds remoteFileSize's HEAD request on its own,
// independent of the job's deadline: an origin that stalls or never answers
// HEAD would otherwise hold a Splitter slot until the job's full timeout
// before falling back to downloadToTemp.
const headRequestTimeout = 5 * time.Second

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

	// OnPhase, if non-nil, is called once per sub-step ("download" for the
	// HEAD/GET fetch, "split_upload" for the structural split plus its
	// per-part uploads) with that step's elapsed time and error. It is
	// optional and never required for correctness; the HTTP handler leaves
	// it nil. "split_upload" is reported as one combined phase rather than
	// separate split/upload figures because services.SplitAndUpload
	// interleaves encoding and uploading each part (see
	// internal/services/splitter.go) — separating them would require
	// restructuring every format splitter into a produce/upload pipeline,
	// out of scope here (see docs/throughput-source-review.md item 7).
	OnPhase PhaseObserver
}

// SplitFile fetches the file referenced by input.FilePath. If it already
// fits within 4MB, no work is done and the original URL is returned as the
// sole result. Otherwise the file is downloaded, split into parts no larger
// than 4MB and each part is uploaded to MinIO; the URL of every part is
// returned in order.
func SplitFile(ctx context.Context, input SplitFileInput) ([]string, error) {
	ext, err := utils.ExtensionOf(input.FilePath)
	if err != nil {
		return nil, fmt.Errorf("invalid file_path: %w", err)
	}
	if !utils.IsAllowedExtension(ext) {
		return nil, ErrUnsupportedFormat
	}

	downloadStart := time.Now()
	if size, ok := remoteFileSize(ctx, input.FilePath); ok && size <= maxSplitPartSize {
		observe(input.OnPhase, "download", downloadStart, nil)
		return []string{input.FilePath}, nil
	}

	localPath, err := downloadToTemp(ctx, input.FilePath, ext)
	observe(input.OnPhase, "download", downloadStart, err)
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
	splitStart := time.Now()
	urls, err := services.SplitAndUpload(ctx, localPath, ext, maxSplitPartSize, objectPrefix)
	observe(input.OnPhase, "split_upload", splitStart, err)
	return urls, err
}

// remoteFileSize learns the file size without downloading it. Presigned internal
// MinIO URLs verify against MinIO directly; external URLs use a bounded HEAD.
func remoteFileSize(ctx context.Context, fileURL string) (size int64, ok bool) {
	headCtx, cancel := context.WithTimeout(ctx, headRequestTimeout)
	defer cancel()

	var (
		resp *http.Response
		err  error
	)
	if services.IsPresignedInternalObjectURL(fileURL) {
		resp, err = services.FetchInternalPresignedObject(headCtx, http.MethodHead, fileURL)
	} else {
		resp, err = doSafeRequest(headCtx, nil, http.MethodHead, fileURL)
	}
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
	return downloadSourceToTemp(ctx, fileURL, "split-*", ext, maxSplitDownloadSize, ErrSplitFileTooLarge)
}

func downloadSourceToTemp(ctx context.Context, fileURL, prefix, ext string, maxSize int64, tooLargeErr error) (string, error) {
	var (
		resp *http.Response
		err  error
	)
	if services.IsPresignedInternalObjectURL(fileURL) {
		resp, err = services.FetchInternalPresignedObject(ctx, http.MethodGet, fileURL)
		if err != nil {
			return "", fmt.Errorf("failed to download internal object: %w", err)
		}
	} else {
		resp, err = doSafeRequest(ctx, nil, http.MethodGet, fileURL)
		if err != nil {
			return "", fmt.Errorf("failed to download file: %w", err)
		}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to download file: status %d", resp.StatusCode)
	}
	body := resp.Body

	tmpFile, err := os.CreateTemp("", prefix+ext)
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}
	defer tmpFile.Close()

	written, err := io.Copy(tmpFile, io.LimitReader(body, maxSize+1))
	if err != nil {
		os.Remove(tmpFile.Name())
		return "", fmt.Errorf("failed to save file: %w", err)
	}
	if written > maxSize {
		os.Remove(tmpFile.Name())
		return "", tooLargeErr
	}

	return tmpFile.Name(), nil
}
