package services

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"golang.org/x/net/html"
)

const (
	// htmlMaxTokenBuf bounds how much a single token (e.g. one very long
	// attribute or run of text) may grow the tokenizer's internal buffer.
	htmlMaxTokenBuf = 2 << 20 // 2MiB

	// htmlMaxNestDepth bounds how many unclosed ancestor tags we track at
	// once; this both guards against pathological/malicious nesting and
	// caps how much markup has to be replayed when a chunk rotates.
	htmlMaxNestDepth = 512

	// htmlHardCapFactor is the safety valve: if a boundary tag never shows
	// up (e.g. one enormous <p> with no children), force a flush once the
	// current chunk reaches this multiple of the target size.
	htmlHardCapFactor = 4
)

// htmlBoundaryTags are safe points to end a chunk: closing one of them
// leaves reading order and document structure intact once still-open
// ancestors are reopened in the next chunk.
var htmlBoundaryTags = map[string]bool{
	"p": true, "div": true, "section": true, "article": true,
	"h1": true, "h2": true, "h3": true, "h4": true, "h5": true, "h6": true,
	"ul": true, "ol": true, "li": true,
	"table": true, "thead": true, "tbody": true, "tfoot": true, "tr": true,
	"blockquote": true, "pre": true, "figure": true,
	"header": true, "footer": true, "nav": true,
}

// htmlVoidElements never get a matching end tag per the HTML5 spec.
var htmlVoidElements = map[string]bool{
	"area": true, "base": true, "br": true, "col": true, "embed": true,
	"hr": true, "img": true, "input": true, "link": true, "meta": true,
	"param": true, "source": true, "track": true, "wbr": true,
}

// htmlStrippedElements are dropped entirely, including their content: they
// carry no reading-order content MarkItDown needs and can be arbitrarily
// large (inline scripts/styles).
var htmlStrippedElements = map[string]bool{
	"script": true, "style": true,
}

// htmlBinaryAttrs may embed a binary payload as a data: URI; such payloads
// are stripped so a single tag can't balloon a chunk.
var htmlBinaryAttrs = map[string]bool{
	"src": true, "href": true, "poster": true, "srcset": true, "data": true,
}

type openHTMLTag struct {
	name string
	raw  string // the sanitized opening tag markup, reused verbatim on reopen
}

// htmlChunkWriter tokenizes HTML as a stream (golang.org/x/net/html's
// tokenizer never buffers more than one token) and emits a sequence of
// well-formed, self-contained HTML fragments via emit, each close to
// maxChunkSize bytes. Splits only happen right after closing a block-level
// boundary tag; any ancestors still open at that point are closed in the
// finished chunk and reopened at the start of the next one, so every
// emitted fragment stays independently valid HTML.
type htmlChunkWriter struct {
	maxChunkSize int64
	emit         func(partNum int, r io.Reader, size int64) error

	buf       bytes.Buffer
	open      []openHTMLTag
	skipStack []string
	partNum   int
}

func newHTMLChunkWriter(maxChunkSize int64, emit func(partNum int, r io.Reader, size int64) error) *htmlChunkWriter {
	return &htmlChunkWriter{maxChunkSize: maxChunkSize, emit: emit}
}

// WriteFrom tokenizes src and emits chunks as it goes. src is read
// incrementally by the tokenizer; the source document is never buffered in
// full.
func (w *htmlChunkWriter) WriteFrom(src io.Reader) error {
	z := html.NewTokenizer(src)
	z.SetMaxBuf(htmlMaxTokenBuf)

	for {
		tt := z.Next()
		if tt == html.ErrorToken {
			if err := z.Err(); err != nil && err != io.EOF {
				return fmt.Errorf("tokenize html: %w", err)
			}
			break
		}

		tok := z.Token()
		var err error
		switch tt {
		case html.StartTagToken, html.SelfClosingTagToken:
			err = w.handleStartTag(tok, tt == html.SelfClosingTagToken)
		case html.EndTagToken:
			err = w.handleEndTag(tok)
		case html.TextToken:
			if len(w.skipStack) == 0 {
				err = w.append(tok.String())
			}
		case html.CommentToken, html.DoctypeToken:
			// dropped: neither carries reading-order content
		}
		if err != nil {
			return err
		}
	}

	return w.finish()
}

func (w *htmlChunkWriter) handleStartTag(tok html.Token, selfClosing bool) error {
	name := tok.Data

	if len(w.skipStack) > 0 {
		if htmlStrippedElements[name] {
			w.skipStack = append(w.skipStack, name)
		}
		return nil
	}

	if htmlStrippedElements[name] {
		if !selfClosing {
			w.skipStack = append(w.skipStack, name)
		}
		return nil
	}

	sanitizeHTMLAttrs(&tok)
	raw := tok.String()
	if err := w.append(raw); err != nil {
		return err
	}

	if !selfClosing && !htmlVoidElements[name] {
		if len(w.open) >= htmlMaxNestDepth {
			return fmt.Errorf("html nesting depth exceeds %d", htmlMaxNestDepth)
		}
		w.open = append(w.open, openHTMLTag{name: name, raw: raw})
	}
	return nil
}

func (w *htmlChunkWriter) handleEndTag(tok html.Token) error {
	name := tok.Data

	if len(w.skipStack) > 0 {
		if w.skipStack[len(w.skipStack)-1] == name {
			w.skipStack = w.skipStack[:len(w.skipStack)-1]
		}
		return nil
	}

	if err := w.append(tok.String()); err != nil {
		return err
	}

	closedBoundary := false
	for i := len(w.open) - 1; i >= 0; i-- {
		if w.open[i].name == name {
			w.open = w.open[:i]
			closedBoundary = htmlBoundaryTags[name]
			break
		}
	}

	if closedBoundary && int64(w.buf.Len()) >= w.maxChunkSize {
		return w.rotate()
	}
	return nil
}

// append writes s to the current chunk and force-rotates if the hard cap is
// hit, so a single oversized element can never grow the buffer unbounded.
func (w *htmlChunkWriter) append(s string) error {
	w.buf.WriteString(s)
	if int64(w.buf.Len()) >= w.maxChunkSize*htmlHardCapFactor {
		return w.rotate()
	}
	return nil
}

// rotate emits the current chunk (closing any open tags) and reopens them
// so the next chunk continues as a valid fragment with the same ancestry.
func (w *htmlChunkWriter) rotate() error {
	if err := w.emitCurrent(); err != nil {
		return err
	}
	for _, t := range w.open {
		w.buf.WriteString(t.raw)
	}
	return nil
}

// finish emits whatever remains as the final chunk, closing any open tags
// without reopening them.
func (w *htmlChunkWriter) finish() error {
	return w.emitCurrent()
}

func (w *htmlChunkWriter) emitCurrent() error {
	if w.buf.Len() == 0 {
		return nil
	}
	for i := len(w.open) - 1; i >= 0; i-- {
		w.buf.WriteString("</" + w.open[i].name + ">")
	}

	w.partNum++
	data := w.buf.Bytes()
	size := int64(len(data))
	if err := w.emit(w.partNum, bytes.NewReader(data), size); err != nil {
		return err
	}
	w.buf.Reset()
	return nil
}

func sanitizeHTMLAttrs(tok *html.Token) {
	if len(tok.Attr) == 0 {
		return
	}
	kept := tok.Attr[:0]
	for _, a := range tok.Attr {
		key := strings.ToLower(a.Key)
		if strings.HasPrefix(key, "on") {
			continue // inline event handler script
		}
		if htmlBinaryAttrs[key] && strings.HasPrefix(strings.TrimSpace(a.Val), "data:") {
			continue // embedded binary payload
		}
		kept = append(kept, a)
	}
	tok.Attr = kept
}

func splitHTML(ctx context.Context, localPath string, maxPartSize int64, objectPrefix string) ([]string, error) {
	src, err := os.Open(localPath)
	if err != nil {
		return nil, fmt.Errorf("open html: %w", err)
	}
	defer src.Close()

	var urls []string
	w := newHTMLChunkWriter(maxPartSize, func(partNum int, r io.Reader, size int64) error {
		objectName := fmt.Sprintf("%s/part-%03d.html", objectPrefix, partNum)
		url, err := UploadObject(ctx, objectName, r, size, "text/html; charset=utf-8")
		if err != nil {
			return err
		}
		urls = append(urls, url)
		return nil
	})

	if err := w.WriteFrom(src); err != nil {
		return nil, fmt.Errorf("split html: %w", err)
	}
	return urls, nil
}
