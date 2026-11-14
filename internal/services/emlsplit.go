package services

import (
	"bufio"
	"context"
	"fmt"
	stdhtml "html"
	"io"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/emersion/go-message"
	_ "github.com/emersion/go-message/charset" // registers message.CharsetReader for non-UTF-8 bodies

	"github.com/go-markitdown/internal/utils"
)

const (
	// maxEMLMIMEDepth bounds multipart nesting so a crafted message can't
	// exhaust the stack or memory through unbounded recursion.
	maxEMLMIMEDepth = 20

	// maxEMLParts bounds the total number of MIME parts visited across the
	// whole message, guarding against a part-count explosion.
	maxEMLParts = 1000

	// maxEMLBodySize bounds how much of a single text/html or text/plain
	// body part is read; it's a sanity ceiling, not the chunk size.
	maxEMLBodySize = 128 * 1024 * 1024

	// maxEMLAttachmentSize bounds how much of a single attachment is
	// streamed to disk.
	maxEMLAttachmentSize = 256 * 1024 * 1024

	// maxEMLHeaderValueLen bounds how much of a single preserved header
	// value is rendered; excess is visibly marked, never silently dropped.
	maxEMLHeaderValueLen = 8 * 1024
)

// splitEML renders an email's headers and chosen body as a stream of HTML
// chunks (via the HTML chunk writer) and streams every attachment straight
// to MinIO, splitting it further if it is itself oversized. No single part
// is ever decoded fully into memory beyond what go-message itself buffers
// for one part at a time.
func splitEML(ctx context.Context, localPath string, maxPartSize int64, objectPrefix string) ([]string, error) {
	f, err := os.Open(localPath)
	if err != nil {
		return nil, fmt.Errorf("open eml: %w", err)
	}
	defer f.Close()

	msg, err := message.Read(f) // applies go-message's default 1MB header-size limit
	if err != nil && !message.IsUnknownCharset(err) {
		return nil, fmt.Errorf("parse eml: %w", err)
	}

	w := &emlSplitter{
		ctx:          ctx,
		maxPartSize:  maxPartSize,
		objectPrefix: objectPrefix,
	}

	bodyReader, hasBody, err := w.resolveBody(msg, 0)
	if err != nil {
		return nil, err
	}

	headerPrologue := strings.NewReader(renderEMLHeaderHTML(&msg.Header))

	var readers []io.Reader
	readers = append(readers,
		strings.NewReader("<!doctype html><html><body>"),
		headerPrologue,
	)
	if hasBody {
		readers = append(readers, bodyReader)
	}
	readers = append(readers, strings.NewReader("</body></html>"))

	chunkWriter := newHTMLChunkWriter(maxPartSize, func(partNum int, r io.Reader, size int64) error {
		objectName := fmt.Sprintf("%s/body-%03d.html", objectPrefix, partNum)
		url, err := UploadObject(ctx, objectName, r, size, "text/html; charset=utf-8")
		if err != nil {
			return err
		}
		w.urls = append(w.urls, url)
		return nil
	})
	if err := chunkWriter.WriteFrom(io.MultiReader(readers...)); err != nil {
		return nil, fmt.Errorf("split eml body: %w", err)
	}

	return append(w.urls, w.attachmentURLs...), nil
}

type emlSplitter struct {
	ctx          context.Context
	maxPartSize  int64
	objectPrefix string

	partsVisited   int
	attachNum      int
	urls           []string
	attachmentURLs []string
}

// resolveBody walks the entity tree. It returns a reader yielding the HTML
// representation of the single chosen body (preferring text/html over
// text/plain inside multipart/alternative, never concatenating
// alternatives of the same content), while dispatching every attachment it
// finds along the way. ok is false when the part contributes no renderable
// body (e.g. it was an attachment, or an alternative branch that lost out).
func (w *emlSplitter) resolveBody(e *message.Entity, depth int) (io.Reader, bool, error) {
	if depth > maxEMLMIMEDepth {
		return nil, false, fmt.Errorf("eml MIME nesting exceeds depth %d", maxEMLMIMEDepth)
	}

	disp, dispParams, _ := e.Header.ContentDisposition()
	mediaType, ctParams, _ := e.Header.ContentType()
	filename := attachmentFilename(dispParams, ctParams)

	if strings.EqualFold(disp, "attachment") {
		return nil, false, w.dispatchAttachment(e, filename)
	}

	if strings.HasPrefix(mediaType, "multipart/") {
		return w.resolveMultipart(e, mediaType, depth)
	}

	switch {
	case strings.HasPrefix(mediaType, "text/html"):
		return io.LimitReader(e.Body, maxEMLBodySize+1), true, nil
	case strings.HasPrefix(mediaType, "text/plain"):
		return plainTextToHTML(io.LimitReader(e.Body, maxEMLBodySize+1)), true, nil
	default:
		// Not displayable body text (image, pdf, octet-stream, ...): treat
		// as an attachment even without an explicit "attachment"
		// disposition, so nothing is silently dropped.
		return nil, false, w.dispatchAttachment(e, filename)
	}
}

func (w *emlSplitter) resolveMultipart(e *message.Entity, mediaType string, depth int) (io.Reader, bool, error) {
	mr := e.MultipartReader()
	if mr == nil {
		return nil, false, fmt.Errorf("content-type %q claims multipart but has no boundary", mediaType)
	}
	defer mr.Close()

	alternative := mediaType == "multipart/alternative"

	var chosen io.Reader
	chosenOK := false
	chosenIsHTML := false

	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, false, fmt.Errorf("read mime part: %w", err)
		}

		w.partsVisited++
		if w.partsVisited > maxEMLParts {
			return nil, false, fmt.Errorf("eml has more than %d MIME parts", maxEMLParts)
		}

		partMediaType, _, _ := part.Header.ContentType()
		r, ok, err := w.resolveBody(part, depth+1)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			continue
		}

		if alternative {
			// Keep the best candidate only (html beats plain text); never
			// emit more than one representation of the same content.
			isHTML := strings.HasPrefix(partMediaType, "text/html")
			if !chosenOK || (isHTML && !chosenIsHTML) {
				chosen, chosenOK, chosenIsHTML = r, true, isHTML
			}
			continue
		}

		// multipart/mixed, multipart/related, ...: siblings are distinct
		// content, concatenate in document order.
		if chosenOK {
			chosen = io.MultiReader(chosen, r)
		} else {
			chosen, chosenOK = r, true
		}
	}

	return chosen, chosenOK, nil
}

func (w *emlSplitter) dispatchAttachment(e *message.Entity, filename string) error {
	if e.Body == nil {
		return nil
	}

	ext := ""
	if filename != "" {
		ext = strings.ToLower(path.Ext(filename))
	}

	tmp, err := os.CreateTemp("", "eml-attachment-*"+ext)
	if err != nil {
		return fmt.Errorf("create attachment temp file: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)

	written, err := io.Copy(tmp, io.LimitReader(e.Body, maxEMLAttachmentSize+1))
	closeErr := tmp.Close()
	if err != nil {
		return fmt.Errorf("read attachment: %w", err)
	}
	if closeErr != nil {
		return fmt.Errorf("write attachment: %w", closeErr)
	}
	if written > maxEMLAttachmentSize {
		return fmt.Errorf("attachment %q exceeds %d bytes", filename, maxEMLAttachmentSize)
	}
	if written == 0 {
		return nil
	}

	w.attachNum++
	objectBase := fmt.Sprintf("%s/attachment-%03d", w.objectPrefix, w.attachNum)
	if filename != "" {
		objectBase += "-" + sanitizeObjectNameSegment(strings.TrimSuffix(filename, ext))
	}

	if ext != "" && utils.IsAllowedExtension(ext) && written > w.maxPartSize {
		urls, err := SplitAndUpload(w.ctx, tmpPath, ext, w.maxPartSize, objectBase)
		if err != nil {
			return fmt.Errorf("split attachment %q: %w", filename, err)
		}
		w.attachmentURLs = append(w.attachmentURLs, urls...)
		return nil
	}

	url, err := uploadLocalFile(w.ctx, tmpPath, objectBase+ext, attachmentContentType(ext))
	if err != nil {
		return fmt.Errorf("upload attachment %q: %w", filename, err)
	}
	w.attachmentURLs = append(w.attachmentURLs, url)
	return nil
}

func attachmentFilename(dispParams, ctParams map[string]string) string {
	if name := dispParams["filename"]; name != "" {
		return name
	}
	return ctParams["name"]
}

func attachmentContentType(ext string) string {
	switch ext {
	case ".pdf":
		return "application/pdf"
	case ".csv":
		return "text/csv"
	case ".html":
		return "text/html; charset=utf-8"
	default:
		return "application/octet-stream"
	}
}

func sanitizeObjectNameSegment(name string) string {
	var b strings.Builder
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.':
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
	}
	return b.String()
}

// renderEMLHeaderHTML renders a small, fixed set of headers once (not
// repeated per chunk, to avoid duplicating the same information across
// every part). Header.Text decodes RFC 2047 encoded-words, normalizing
// non-ASCII header values to UTF-8.
func renderEMLHeaderHTML(h *message.Header) string {
	fields := []string{"Subject", "From", "To", "Cc", "Date"}

	var b strings.Builder
	b.WriteString("<table>")
	for _, name := range fields {
		value, err := h.Text(name)
		if err != nil || value == "" {
			continue
		}
		value = truncateHeaderValue(value)
		b.WriteString("<tr><th>")
		b.WriteString(stdhtml.EscapeString(name))
		b.WriteString("</th><td>")
		b.WriteString(stdhtml.EscapeString(value))
		b.WriteString("</td></tr>")
	}
	b.WriteString("</table>")
	return b.String()
}

func truncateHeaderValue(v string) string {
	if len(v) <= maxEMLHeaderValueLen {
		return v
	}
	return v[:maxEMLHeaderValueLen] + "...[truncated, " + strconv.Itoa(len(v)-maxEMLHeaderValueLen) + " bytes omitted]"
}

// plainTextToHTML streams src, converting it paragraph-by-paragraph (blank
// lines separate paragraphs) into escaped HTML via an io.Pipe, so the
// conversion and the downstream HTML tokenizer apply natural backpressure
// on each other instead of either side buffering the whole body.
func plainTextToHTML(src io.Reader) io.Reader {
	pr, pw := io.Pipe()

	go func() {
		br := bufio.NewReader(src)
		var para strings.Builder

		flush := func() error {
			if para.Len() == 0 {
				return nil
			}
			if _, err := pw.Write([]byte("<p>" + para.String() + "</p>")); err != nil {
				return err
			}
			para.Reset()
			return nil
		}

		for {
			line, readErr := br.ReadString('\n')
			trimmed := strings.TrimRight(line, "\r\n")

			if strings.TrimSpace(trimmed) == "" {
				if err := flush(); err != nil {
					pw.CloseWithError(err)
					return
				}
			} else {
				if para.Len() > 0 {
					para.WriteString("<br>")
				}
				para.WriteString(stdhtml.EscapeString(trimmed))
			}

			if readErr != nil {
				if readErr != io.EOF {
					pw.CloseWithError(fmt.Errorf("read plain text body: %w", readErr))
					return
				}
				break
			}
		}

		if err := flush(); err != nil {
			pw.CloseWithError(err)
			return
		}
		pw.Close()
	}()

	return pr
}
