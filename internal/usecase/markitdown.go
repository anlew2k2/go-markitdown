package usecase

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
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
// converts it to Markdown, writing the result to outputPath instead of
// returning it as a string, so a worker never holds a whole document in
// memory. convertTimeout bounds the markitdown subprocess specifically.
func ConvertURLToMarkdownFile(ctx context.Context, fileURL, outputPath string, httpTimeout, convertTimeout time.Duration, stderrLimit int) error {
	ext, err := utils.ExtensionOf(fileURL)
	if err != nil {
		return fmt.Errorf("invalid url: %w", err)
	}
	if !utils.IsAllowedExtension(ext) {
		return ErrUnsupportedFormat
	}

	dlCtx, cancel := context.WithTimeout(ctx, httpTimeout)
	filePath, err := downloadFileCtx(dlCtx, fileURL, ext)
	cancel()
	if err != nil {
		return err
	}
	defer os.Remove(filePath)

	convCtx, cancel2 := context.WithTimeout(ctx, convertTimeout)
	defer cancel2()
	return services.ConvertToMarkdownFile(convCtx, filePath, outputPath, stderrLimit)
}

func downloadFileCtx(ctx context.Context, fileURL, ext string) (string, error) {
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

	tmpFile, err := os.CreateTemp("", "extract-*"+ext)
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}
	defer tmpFile.Close()

	written, err := io.Copy(tmpFile, io.LimitReader(resp.Body, maxFileSize+1))
	if err != nil {
		os.Remove(tmpFile.Name())
		return "", fmt.Errorf("failed to save file: %w", err)
	}
	if written > maxFileSize {
		os.Remove(tmpFile.Name())
		return "", ErrFileTooLarge
	}

	return tmpFile.Name(), nil
}

func downloadFile(fileURL, ext string) (string, error) {
	resp, err := http.Get(fileURL)
	if err != nil {
		return "", fmt.Errorf("failed to download file: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to download file: status %d", resp.StatusCode)
	}

	tmpFile, err := os.CreateTemp("", "markitdown-*"+ext)
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}
	defer tmpFile.Close()

	written, err := io.Copy(tmpFile, io.LimitReader(resp.Body, maxFileSize+1))
	if err != nil {
		os.Remove(tmpFile.Name())
		return "", fmt.Errorf("failed to save file: %w", err)
	}
	if written > maxFileSize {
		os.Remove(tmpFile.Name())
		return "", ErrFileTooLarge
	}

	return tmpFile.Name(), nil
}
