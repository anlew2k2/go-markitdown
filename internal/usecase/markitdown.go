package usecase

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/go-markitdown/internal/services"
	"github.com/go-markitdown/internal/utils"
)

const maxFileSize = 4 * 1024 * 1024 // 4MB

var (
	ErrUnsupportedFormat = errors.New("unsupported file format")
	ErrFileTooLarge      = errors.New("file exceeds the 4MB size limit")
)

// ConvertURLToMarkdown downloads the file at fileURL, validates its format
// and size, converts it to markdown and returns the result.
func ConvertURLToMarkdown(fileURL string) (string, error) {
	ext, err := utils.ExtensionOf(fileURL)
	if err != nil {
		return "", fmt.Errorf("invalid url: %w", err)
	}
	if !utils.IsAllowedExtension(ext) {
		return "", ErrUnsupportedFormat
	}

	filePath, err := downloadFile(fileURL, ext)
	if err != nil {
		return "", err
	}
	defer os.Remove(filePath)

	return services.ConvertToMarkdown(filePath)
}

// ConvertURLToMarkdownFile downloads fileURL (bounded by httpTimeout) and
// converts it to Markdown via converter, writing the result to outputPath
// instead of returning it as a string, so a worker never holds a whole
// document in memory. convertTimeout bounds the conversion step
// specifically (the markitdown CLI subprocess, or one request to a
// persistent worker — see services.Converter). onPhase, if non-nil, is
// called once for "download" and once for "convert" with that step's
// elapsed time and error.
func ConvertURLToMarkdownFile(ctx context.Context, converter services.Converter, fileURL, outputPath string, httpTimeout, convertTimeout time.Duration, onPhase PhaseObserver) error {
	ext, err := utils.ExtensionOf(fileURL)
	if err != nil {
		return fmt.Errorf("invalid url: %w", err)
	}
	if !utils.IsAllowedExtension(ext) {
		return ErrUnsupportedFormat
	}

	dlCtx, cancel := context.WithTimeout(ctx, httpTimeout)
	downloadStart := time.Now()
	filePath, err := downloadFileCtx(dlCtx, fileURL, ext)
	cancel()
	observe(onPhase, "download", downloadStart, err)
	if err != nil {
		return err
	}
	defer os.Remove(filePath)

	convCtx, cancel2 := context.WithTimeout(ctx, convertTimeout)
	defer cancel2()
	convertStart := time.Now()
	err = converter.Convert(convCtx, filePath, outputPath)
	observe(onPhase, "convert", convertStart, err)
	return err
}

func downloadFileCtx(ctx context.Context, fileURL, ext string) (string, error) {
	return downloadSourceToTemp(ctx, fileURL, "extract-*", ext, maxFileSize, ErrFileTooLarge)
}

func downloadFile(fileURL, ext string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultDownloadTimeout)
	defer cancel()

	return downloadSourceToTemp(ctx, fileURL, "markitdown-*", ext, maxFileSize, ErrFileTooLarge)
}
