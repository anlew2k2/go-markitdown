package services

import "fmt"

// ConvertToMarkdown runs the markitdown CLI against a local file and returns
// the resulting markdown.
func ConvertToMarkdown(filePath string) (string, error) {
	return RunCommand(fmt.Sprintf("markitdown %s", filePath))
}
