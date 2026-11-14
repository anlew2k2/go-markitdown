package usecase

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"

	"github.com/anthony-lewis/go-markitdown/internal/services"
)

const maxFileSize = 16 * 1024 * 1024 // 16MB

var allowedExtensions = map[string]bool{
	".doc":  true,
	".docx": true,
	".xls":  true,
	".xlsx": true,
	".csv":  true,
	".pptx": true,
	".html": true,
	".eml":  true,
	".pdf":  true,
}

var (
	ErrUnsupportedFormat = errors.New("unsupported file format")
	ErrFileTooLarge      = errors.New("file exceeds the 16MB size limit")
)

// ConvertURLToMarkdown downloads the file at fileURL, validates its format
// and size, converts it to markdown and returns the result.
func ConvertURLToMarkdown(fileURL string) (string, error) {
	ext, err := extensionOf(fileURL)
	if err != nil {
		return "", err
	}
	if !allowedExtensions[ext] {
		return "", ErrUnsupportedFormat
	}

	filePath, err := downloadFile(fileURL, ext)
	if err != nil {
		return "", err
	}
	defer os.Remove(filePath)

	return services.ConvertToMarkdown(filePath)
}

func extensionOf(fileURL string) (string, error) {
	parsed, err := url.Parse(fileURL)
	if err != nil {
		return "", fmt.Errorf("invalid url: %w", err)
	}
	return strings.ToLower(path.Ext(parsed.Path)), nil
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
