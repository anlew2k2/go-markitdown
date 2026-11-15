package services

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	pdfapi "github.com/pdfcpu/pdfcpu/pkg/api"
	"github.com/pdfcpu/pdfcpu/pkg/pdfcpu/model"
)

// ErrOLENotSplittable is returned for legacy OLE-based formats (.doc,
// .xls) that need splitting (i.e. exceed the size threshold) but can't be:
// the compound file binary format has no mature pure-Go support, and
// hand-rolling a structural splitter for it isn't worth the effort these
// formats are legacy. Callers should surface this as a client-facing
// error rather than retrying.
var ErrOLENotSplittable = errors.New("legacy OLE document exceeds the size limit and cannot be split")

// SplitAndUpload splits the file at localPath (of the given extension) into
// parts no larger than maxPartSize bytes and uploads each part to MinIO
// under objectPrefix, returning the public URL of every part in order.
//
// PDF is split page-by-page (via pdfcpu), CSV row-by-row preserving the
// header, HTML token-by-token at block-element boundaries, and EML as
// streaming MIME (headers + chosen body rendered as HTML chunks,
// attachments dispatched to their own splitter) — all of these read from
// disk and stream straight to MinIO without holding the whole file in
// memory. docx/xlsx/pptx are OOXML (a zip of XML parts): they're split via
// archive/zip + encoding/xml — reusing every unchanged part of the package
// verbatim and only rewriting the part(s) that actually need to shrink
// (document.xml's paragraphs, a worksheet's rows, or the set of slides) —
// so every part stays a genuinely valid, independently openable document
// of the same format. Legacy OLE formats (doc, xls) have no mature pure-Go
// support for the compound file binary format, so they're rejected with
// ErrOLENotSplittable rather than attempted.
func SplitAndUpload(ctx context.Context, localPath, ext string, maxPartSize int64, objectPrefix string) ([]string, error) {
	switch ext {
	case ".pdf":
		return splitPDF(ctx, localPath, maxPartSize, objectPrefix)
	case ".csv":
		return splitCSV(ctx, localPath, maxPartSize, objectPrefix)
	case ".xlsx":
		return splitXLSX(ctx, localPath, maxPartSize, objectPrefix)
	case ".docx":
		return splitDOCX(ctx, localPath, maxPartSize, objectPrefix)
	case ".pptx":
		return splitPPTX(ctx, localPath, maxPartSize, objectPrefix)
	case ".html":
		return splitHTML(ctx, localPath, maxPartSize, objectPrefix)
	case ".eml":
		return splitEML(ctx, localPath, maxPartSize, objectPrefix)
	case ".doc", ".xls":
		return nil, ErrOLENotSplittable
	default:
		return splitBytes(ctx, localPath, ext, maxPartSize, objectPrefix)
	}
}

// pdfSpanStart extracts the starting page number from a pdfcpu span
// filename ("<base>_<from>.pdf" or "<base>_<from>-<thru>.pdf"), returning 0
// if the name doesn't match the expected pattern.
func pdfSpanStart(name string) int {
	trimmed := strings.TrimSuffix(name, ".pdf")
	idx := strings.LastIndex(trimmed, "_")
	if idx < 0 {
		return 0
	}
	tail := trimmed[idx+1:]
	if dash := strings.Index(tail, "-"); dash >= 0 {
		tail = tail[:dash]
	}
	n, err := strconv.Atoi(tail)
	if err != nil {
		return 0
	}
	return n
}

func splitPDF(ctx context.Context, localPath string, maxPartSize int64, objectPrefix string) ([]string, error) {
	info, err := os.Stat(localPath)
	if err != nil {
		return nil, fmt.Errorf("stat pdf: %w", err)
	}

	pageCount, err := pdfapi.PageCountFile(localPath)
	if err != nil {
		return nil, fmt.Errorf("count pdf pages: %w", err)
	}
	if pageCount == 0 {
		return nil, fmt.Errorf("pdf has no pages")
	}

	avgPageSize := info.Size() / int64(pageCount)
	if avgPageSize < 1 {
		avgPageSize = 1
	}
	span := int(maxPartSize / avgPageSize)
	if span < 1 {
		span = 1
	}

	outDir, err := os.MkdirTemp("", "split-pdf-*")
	if err != nil {
		return nil, fmt.Errorf("create temp dir: %w", err)
	}
	defer os.RemoveAll(outDir)

	conf := model.NewDefaultConfiguration()
	var entries []os.DirEntry

	// Pages aren't uniformly sized, so the span estimated from the average
	// may still produce an oversized part; shrink it and retry until every
	// part fits (or we've reached single-page parts).
	for {
		existing, _ := os.ReadDir(outDir)
		for _, e := range existing {
			os.Remove(filepath.Join(outDir, e.Name()))
		}

		if err := pdfapi.SplitFile(localPath, outDir, span, conf); err != nil {
			return nil, fmt.Errorf("split pdf: %w", err)
		}

		entries, err = os.ReadDir(outDir)
		if err != nil {
			return nil, fmt.Errorf("read split output: %w", err)
		}

		fitsAll := true
		for _, e := range entries {
			fi, err := e.Info()
			if err != nil {
				return nil, err
			}
			if fi.Size() > maxPartSize {
				fitsAll = false
				break
			}
		}
		if fitsAll {
			break
		}
		if span == 1 {
			return nil, fmt.Errorf("pdf contains page exceeding 4MB limit and cannot be split further")
		}
		span /= 2
		if span < 1 {
			span = 1
		}
	}

	// pdfcpu names spans "<base>_<from>.pdf" or "<base>_<from>-<thru>.pdf"
	// using the raw page number, not zero-padded, so a lexical sort on the
	// filename breaks page order past 9 parts (e.g. "_10" sorts before
	// "_2"). Sort by the numeric "from" page instead.
	sort.Slice(entries, func(i, j int) bool { return pdfSpanStart(entries[i].Name()) < pdfSpanStart(entries[j].Name()) })

	urls := make([]string, 0, len(entries))
	for i, e := range entries {
		objectName := fmt.Sprintf("%s/part-%03d.pdf", objectPrefix, i+1)
		url, err := uploadLocalFile(ctx, filepath.Join(outDir, e.Name()), objectName, "application/pdf")
		if err != nil {
			return nil, err
		}
		urls = append(urls, url)
	}
	return urls, nil
}

// csvRowSizer reuses one buffer and csv.Writer across every row of a single
// splitCSV invocation to measure encoded size, instead of allocating a fresh
// bytes.Buffer and csv.Writer per row.
type csvRowSizer struct {
	buf bytes.Buffer
	w   *csv.Writer
}

func newCSVRowSizer() *csvRowSizer {
	s := &csvRowSizer{}
	s.w = csv.NewWriter(&s.buf)
	return s
}

// size returns the exact number of bytes csv.Writer would emit for record,
// so a caller can decide whether it still fits in the current part before
// writing it (writing first and checking after would let the row that tips
// a part over the limit end up inside it). On error the sizer must not be
// reused.
func (s *csvRowSizer) size(record []string) (int64, error) {
	s.buf.Reset()
	if err := s.w.Write(record); err != nil {
		return 0, err
	}
	s.w.Flush()
	if err := s.w.Error(); err != nil {
		return 0, err
	}
	return int64(s.buf.Len()), nil
}

func splitCSV(ctx context.Context, localPath string, maxPartSize int64, objectPrefix string) ([]string, error) {
	src, err := os.Open(localPath)
	if err != nil {
		return nil, fmt.Errorf("open csv: %w", err)
	}
	defer src.Close()

	reader := csv.NewReader(bufio.NewReader(src))
	reader.FieldsPerRecord = -1

	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("read csv header: %w", err)
	}
	sizer := newCSVRowSizer()
	headerSize, err := sizer.size(header)
	if err != nil {
		return nil, fmt.Errorf("encode csv header: %w", err)
	}

	var urls []string
	partNum := 0
	var currentFile *os.File
	var currentWriter *csv.Writer
	var currentSize int64

	newPart := func() error {
		partNum++
		f, err := os.CreateTemp("", fmt.Sprintf("split-csv-%03d-*.csv", partNum))
		if err != nil {
			return err
		}
		w := csv.NewWriter(f)
		if err := w.Write(header); err != nil {
			return err
		}
		w.Flush()
		if err := w.Error(); err != nil {
			return err
		}
		currentFile, currentWriter, currentSize = f, w, headerSize
		return nil
	}

	uploadPart := func() error {
		currentWriter.Flush()
		if err := currentWriter.Error(); err != nil {
			currentFile.Close()
			os.Remove(currentFile.Name())
			return err
		}
		info, err := currentFile.Stat()
		if err != nil {
			currentFile.Close()
			os.Remove(currentFile.Name())
			return err
		}
		if info.Size() > maxPartSize {
			currentFile.Close()
			os.Remove(currentFile.Name())
			return fmt.Errorf("csv part exceeds 4MB limit and cannot be split further (%d bytes)", info.Size())
		}
		if _, err := currentFile.Seek(0, io.SeekStart); err != nil {
			currentFile.Close()
			os.Remove(currentFile.Name())
			return err
		}
		objectName := fmt.Sprintf("%s/part-%03d.csv", objectPrefix, partNum)
		url, err := UploadObject(ctx, objectName, currentFile, info.Size(), "text/csv")
		currentFile.Close()
		os.Remove(currentFile.Name())
		if err != nil {
			return err
		}
		urls = append(urls, url)
		return nil
	}

	if err := newPart(); err != nil {
		return nil, fmt.Errorf("start csv part: %w", err)
	}

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			currentFile.Close()
			os.Remove(currentFile.Name())
			return nil, fmt.Errorf("read csv row: %w", err)
		}

		rowSize, err := sizer.size(record)
		if err != nil {
			currentFile.Close()
			os.Remove(currentFile.Name())
			return nil, fmt.Errorf("encode csv row: %w", err)
		}

		// Roll over to a new part before writing this row if it would push
		// the current part over the limit — but only when the current part
		// already holds at least one data row, since a single row bigger
		// than maxPartSize can't be split any further and must go out alone.
		if currentSize > headerSize && currentSize+rowSize > maxPartSize {
			if err := uploadPart(); err != nil {
				return nil, fmt.Errorf("upload csv part: %w", err)
			}
			if err := newPart(); err != nil {
				return nil, fmt.Errorf("start csv part: %w", err)
			}
		}

		if err := currentWriter.Write(record); err != nil {
			currentFile.Close()
			os.Remove(currentFile.Name())
			return nil, fmt.Errorf("write csv row: %w", err)
		}
		currentSize += rowSize
	}

	if err := uploadPart(); err != nil {
		return nil, fmt.Errorf("upload final csv part: %w", err)
	}

	return urls, nil
}

func splitBytes(ctx context.Context, localPath, ext string, maxPartSize int64, objectPrefix string) ([]string, error) {
	src, err := os.Open(localPath)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}
	defer src.Close()

	var urls []string
	partNum := 0

	for {
		partNum++
		tmp, err := os.CreateTemp("", fmt.Sprintf("split-part-%03d-*%s", partNum, ext))
		if err != nil {
			return nil, fmt.Errorf("create part file: %w", err)
		}

		written, err := io.Copy(tmp, io.LimitReader(src, maxPartSize))
		if err != nil {
			tmp.Close()
			os.Remove(tmp.Name())
			return nil, fmt.Errorf("write part: %w", err)
		}
		if written == 0 {
			tmp.Close()
			os.Remove(tmp.Name())
			break
		}

		objectName := fmt.Sprintf("%s/part-%03d%s", objectPrefix, partNum, ext)
		url, err := uploadLocalFile(ctx, tmp.Name(), objectName, "application/octet-stream")
		tmp.Close()
		os.Remove(tmp.Name())
		if err != nil {
			return nil, fmt.Errorf("upload part: %w", err)
		}
		urls = append(urls, url)

		if written < maxPartSize {
			break
		}
	}

	return urls, nil
}

func uploadLocalFile(ctx context.Context, localPath, objectName, contentType string) (string, error) {
	f, err := os.Open(localPath)
	if err != nil {
		return "", fmt.Errorf("open part: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return "", fmt.Errorf("stat part: %w", err)
	}
	if info.Size() > 4*1024*1024 {
		return "", fmt.Errorf("part exceeds 4MB limit and cannot be split further (%d bytes)", info.Size())
	}

	return UploadObject(ctx, objectName, f, info.Size(), contentType)
}
