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

	// startOffset is this tag's opening markup's offset into the current
	// buf, kept up to date across every reset (rotate/rotateBefore). When
	// this tag is itself a boundary tag, it marks where its whole element
	// began, so handleEndTag can move the element to the next chunk
	// instead of letting it overshoot the one it was appended to.
	startOffset int
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

	startOffset := w.buf.Len()
	sanitizeHTMLAttrs(&tok)
	raw := tok.String()
	if err := w.append(raw); err != nil {
		return err
	}

	if !selfClosing && !htmlVoidElements[name] {
		if len(w.open) >= htmlMaxNestDepth {
			return fmt.Errorf("html nesting depth exceeds %d", htmlMaxNestDepth)
		}
		// Note: if append's hard-cap safety valve fired above, raw was
		// already flushed out as part of the emitted chunk and startOffset
		// no longer points at it — an existing limitation of that valve
		// (it can only fire on one uninterrupted run of htmlHardCapFactor×
		// maxChunkSize with no boundary tag in it at all, well beyond
		// anything a boundary-tag-driven split is meant to hit).
		w.open = append(w.open, openHTMLTag{name: name, raw: raw, startOffset: startOffset})
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

	matchIdx := -1
	for i := len(w.open) - 1; i >= 0; i-- {
		if w.open[i].name == name {
			matchIdx = i
			break
		}
	}

	if err := w.append(tok.String()); err != nil {
		return err
	}

	if matchIdx < 0 {
		return nil
	}

	// Read the mark after append (not before): append may have rotated
	// internally (hard cap) and rewritten every still-open ancestor's
	// startOffset to match the fresh buffer, so matchIdx stays a valid
	// index but its stored offset must be re-read now to stay accurate.
	mark := w.open[matchIdx].startOffset
	closedBoundary := htmlBoundaryTags[name]
	w.open = w.open[:matchIdx]

	if !closedBoundary {
		return nil
	}

	closeSize := w.openTagsCloseSize()
	if int64(w.buf.Len())+closeSize < w.maxChunkSize {
		return nil
	}

	// Content appended since the last check — possibly spanning more than
	// just this element, since opening a boundary tag (e.g. <tbody>) isn't
	// itself checked — is what pushed the chunk over the limit. Find the
	// closest safe split point at or before it and move everything from
	// there into the next chunk, instead of letting this one overshoot.
	if splitMark, keepOpen, ok := w.findRotationMark(mark); ok {
		w.open = w.open[:keepOpen]
		return w.rotateBefore(splitMark)
	}
	return w.rotate()
}

// findRotationMark looks for a safe place to end the current chunk at or
// before the just-closed boundary tag, preferring to keep as much of the
// buffer as possible: first the tag's own start (tagMark), then each
// still-open ancestor's start from innermost to outermost. keepOpen says
// how many leading entries of w.open remain genuinely open if this mark is
// used (to be closed in the emitted part and reopened in the next one);
// any ancestor beyond that is carried into the next chunk as part of the
// moved bytes, since its opening tag already appears there verbatim.
func (w *htmlChunkWriter) findRotationMark(tagMark int) (mark int, keepOpen int, ok bool) {
	if tagMark > 0 && int64(tagMark)+w.openTagsCloseSize() < w.maxChunkSize {
		return tagMark, len(w.open), true
	}
	for i := len(w.open) - 1; i >= 0; i-- {
		candidate := w.open[i].startOffset
		if candidate <= 0 {
			continue
		}
		var closeSize int64
		for _, t := range w.open[:i] {
			closeSize += int64(len(t.name)) + 3
		}
		if int64(candidate)+closeSize < w.maxChunkSize {
			return candidate, i, true
		}
	}
	return 0, 0, false
}

// append writes s to the current chunk and force-rotates if the hard cap is
// hit, so a single oversized element can never grow the buffer unbounded.
func (w *htmlChunkWriter) append(s string) error {
	w.buf.WriteString(s)
	if int64(w.buf.Len())+w.openTagsCloseSize() >= w.maxChunkSize*htmlHardCapFactor {
		return w.rotate()
	}
	return nil
}

// openTagsCloseSize returns how many bytes emitCurrent will add to close
// every still-open ancestor tag (e.g. "</table>"). Size checks before a
// rotate must include this, since emitCurrent appends those closing tags
// to the buffer right before measuring and emitting it.
func (w *htmlChunkWriter) openTagsCloseSize() int64 {
	var n int64
	for _, t := range w.open {
		n += int64(len(t.name)) + 3 // "</" + name + ">"
	}
	return n
}

// rotate emits the current chunk (closing any open tags) and reopens them
// so the next chunk continues as a valid fragment with the same ancestry.
func (w *htmlChunkWriter) rotate() error {
	if err := w.emitCurrent(); err != nil {
		return err
	}
	w.reopenAncestors()
	return nil
}

// rotateBefore splits the buffer at mark — the offset where the element
// that just overflowed the chunk began — emitting everything before it as
// the current chunk and carrying that element whole into the next one,
// instead of letting it inflate the chunk it was appended to.
func (w *htmlChunkWriter) rotateBefore(mark int) error {
	data := w.buf.Bytes()
	overflow := append([]byte(nil), data[mark:]...)
	w.buf.Truncate(mark)

	if err := w.emitCurrent(); err != nil {
		return err
	}
	w.reopenAncestors()
	w.buf.Write(overflow)
	return nil
}

// reopenAncestors rewrites every still-open ancestor's markup at the start
// of the (just-reset) buffer, refreshing each one's startOffset to match —
// rotate and rotateBefore both reset buf, which would otherwise leave
// those offsets pointing at positions from the previous chunk.
func (w *htmlChunkWriter) reopenAncestors() {
	for i := range w.open {
		w.open[i].startOffset = w.buf.Len()
		w.buf.WriteString(w.open[i].raw)
	}
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
		if size > maxPartSize {
			return fmt.Errorf("html chunk %d exceeds %d bytes limit (%d bytes)", partNum, maxPartSize, size)
		}
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
