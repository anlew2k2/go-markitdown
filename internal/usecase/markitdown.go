package usecase

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/go-markitdown/internal/services"
	"github.com/go-markitdown/internal/utils"
)

const maxFileSize = 16 * 1024 * 1024 // 16MB

var (
	ErrUnsupportedFormat = errors.New("unsupported file format")
	ErrFileTooLarge      = errors.New("file exceeds the 16MB size limit")
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
