package services

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"
)

// ConvertToMarkdown runs the markitdown CLI against a local file and returns
// the resulting markdown.
func ConvertToMarkdown(filePath string) (string, error) {
	return RunCommand(fmt.Sprintf("markitdown %s", filePath))
}

// limitedWriter retains at most limit bytes written to it, silently
// discarding the rest while still reporting success, so capturing stderr
// never blocks or grows unbounded on a chatty subprocess.
type limitedWriter struct {
	buf   bytes.Buffer
	limit int
}

func (w *limitedWriter) Write(p []byte) (int, error) {
	if remaining := w.limit - w.buf.Len(); remaining > 0 {
		if len(p) > remaining {
			w.buf.Write(p[:remaining])
		} else {
			w.buf.Write(p)
		}
	}
	return len(p), nil
}

// ConvertToMarkdownFile runs the markitdown CLI against inputPath, streaming
// its stdout straight to outputPath instead of buffering it in memory, and
// captures at most stderrLimit bytes of stderr for error reporting. The
// process — and its full process group, so a child it spawns can't outlive
// it — is killed as soon as ctx is cancelled or expires.
func ConvertToMarkdownFile(ctx context.Context, inputPath, outputPath string, stderrLimit int) error {
	outFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("create markdown output: %w", err)
	}
	defer outFile.Close()

	var stderr limitedWriter
	stderr.limit = stderrLimit

	cmd := exec.CommandContext(ctx, "markitdown", inputPath)
	cmd.Stdout = outFile
	cmd.Stderr = &stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	cmd.WaitDelay = 2 * time.Second

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("markitdown failed: %w (stderr: %s)", err, stderr.buf.String())
	}
	return nil
}
