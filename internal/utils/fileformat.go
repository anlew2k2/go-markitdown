package utils

import (
	"net/url"
	"path"
	"strings"
)

// AllowedExtensions lists the extensions (including the leading dot,
// lowercase) that every binary in this module is allowed to operate on.
var AllowedExtensions = map[string]bool{
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

// IsAllowedExtension reports whether ext (including the leading dot) is a
// supported format.
func IsAllowedExtension(ext string) bool {
	return AllowedExtensions[strings.ToLower(ext)]
}

// ExtensionOf extracts the lowercase file extension (including the leading
// dot) from a URL or file path, ignoring any query string.
func ExtensionOf(rawURL string) (string, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	return strings.ToLower(path.Ext(parsed.Path)), nil
}
