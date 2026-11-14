package services

import (
	"bufio"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

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
		if fitsAll || span == 1 {
			break
		}
		span /= 2
		if span < 1 {
			span = 1
		}
	}

	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })

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

	var urls []string
	partNum := 0

	newPart := func() (*os.File, *csv.Writer, error) {
		partNum++
		f, err := os.CreateTemp("", fmt.Sprintf("split-csv-%03d-*.csv", partNum))
		if err != nil {
			return nil, nil, err
		}
		w := csv.NewWriter(f)
		if err := w.Write(header); err != nil {
			return nil, nil, err
		}
		return f, w, nil
	}

	uploadPart := func(f *os.File, w *csv.Writer) error {
		w.Flush()
		if err := w.Error(); err != nil {
			f.Close()
			os.Remove(f.Name())
			return err
		}
		info, err := f.Stat()
		if err != nil {
			f.Close()
			os.Remove(f.Name())
			return err
		}
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			f.Close()
			os.Remove(f.Name())
			return err
		}
		objectName := fmt.Sprintf("%s/part-%03d.csv", objectPrefix, partNum)
		url, err := UploadObject(ctx, objectName, f, info.Size(), "text/csv")
		f.Close()
		os.Remove(f.Name())
		if err != nil {
			return err
		}
		urls = append(urls, url)
		return nil
	}

	currentFile, currentWriter, err := newPart()
	if err != nil {
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

		if err := currentWriter.Write(record); err != nil {
			currentFile.Close()
			os.Remove(currentFile.Name())
			return nil, fmt.Errorf("write csv row: %w", err)
		}
		currentWriter.Flush()

		info, err := currentFile.Stat()
		if err != nil {
			currentFile.Close()
			os.Remove(currentFile.Name())
			return nil, fmt.Errorf("stat csv part: %w", err)
		}

		if info.Size() >= maxPartSize {
			if err := uploadPart(currentFile, currentWriter); err != nil {
				return nil, fmt.Errorf("upload csv part: %w", err)
			}
			currentFile, currentWriter, err = newPart()
			if err != nil {
				return nil, fmt.Errorf("start csv part: %w", err)
			}
		}
	}

	if err := uploadPart(currentFile, currentWriter); err != nil {
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

	return UploadObject(ctx, objectName, f, info.Size(), contentType)
}
