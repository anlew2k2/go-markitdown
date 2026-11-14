package services

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"os"
)

const (
	docxDocumentPart = "word/document.xml"
	docxContentType  = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
)

// splitDOCX splits a .docx by its body's direct children (paragraphs and
// tables), the same unit Word itself treats as indivisible. word/document.xml
// is the only part that changes between output parts: every other part of
// the package (styles, numbering, media, headers/footers, rels, theme,
// content types, ...) is copied into every part verbatim via
// archive/zip raw copies, so no manifest ever needs rewriting. A trailing
// w:sectPr (the body's own section properties) isn't a paragraph or table;
// it's repeated at the end of every part so each stays a structurally
// valid document.
//
// This only helps when the bulk of the file is in document.xml itself; if
// an oversized .docx is oversized because of its embedded media instead,
// splitting the body does nothing to shrink it — same limitation as
// splitting a PDF with one enormous page.
func splitDOCX(ctx context.Context, localPath string, maxPartSize int64, objectPrefix string) ([]string, error) {
	f, err := os.Open(localPath)
	if err != nil {
		return nil, fmt.Errorf("open docx: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat docx: %w", err)
	}
	zr, err := zip.NewReader(f, info.Size())
	if err != nil {
		return nil, fmt.Errorf("read docx zip: %w", err)
	}

	docFile := zipFileByName(zr.File, docxDocumentPart)
	if docFile == nil {
		return nil, fmt.Errorf("docx missing %s", docxDocumentPart)
	}

	prefix, err := withZipEntry(docFile, func(r io.Reader) ([]byte, error) {
		return scanXMLPrefix(r, "body")
	})
	if err != nil {
		return nil, fmt.Errorf("scan docx body: %w", err)
	}

	cs, err := withZipEntry(docFile, func(r io.Reader) (childScan, error) {
		children, suffixStart, err := scanXMLChildren(r, "body")
		return childScan{children, suffixStart}, err
	})
	if err != nil {
		return nil, fmt.Errorf("scan docx children: %w", err)
	}
	children, suffixStart := cs.children, cs.suffixStart

	suffix, err := withZipEntry(docFile, func(r io.Reader) ([]byte, error) {
		return readXMLSuffix(r, suffixStart)
	})
	if err != nil {
		return nil, fmt.Errorf("read docx suffix: %w", err)
	}

	var sectPr []byte
	if n := len(children); n > 0 && children[n-1].name == "sectPr" {
		last := children[n-1]
		children = children[:n-1]
		sectPr, err = withZipEntry(docFile, func(r io.Reader) ([]byte, error) {
			return readXMLSpan(r, last.start, last.end)
		})
		if err != nil {
			return nil, fmt.Errorf("read docx sectPr: %w", err)
		}
	}

	overhead := int64(len(prefix) + len(sectPr) + len(suffix))
	groups := groupChildrenBySize(children, maxPartSize-overhead)
	if len(groups) == 0 {
		groups = []spanGroup{{start: 0, end: 0}}
	}

	sharedEntries := make([]*zip.File, 0, len(zr.File))
	for _, zf := range zr.File {
		if zf.Name != docxDocumentPart {
			sharedEntries = append(sharedEntries, zf)
		}
	}

	bodyReader, err := docFile.Open()
	if err != nil {
		return nil, fmt.Errorf("open docx body: %w", err)
	}
	defer bodyReader.Close()

	var urls []string
	partNum := 0
	err = streamSpanGroups(bodyReader, groups, func(_ int, body io.Reader) error {
		bodyBytes, err := io.ReadAll(body)
		if err != nil {
			return fmt.Errorf("read docx group: %w", err)
		}

		doc := make([]byte, 0, len(prefix)+len(bodyBytes)+len(sectPr)+len(suffix))
		doc = append(doc, prefix...)
		doc = append(doc, bodyBytes...)
		doc = append(doc, sectPr...)
		doc = append(doc, suffix...)

		partNum++
		url, err := writeOOXMLPart(ctx, sharedEntries, map[string][]byte{docxDocumentPart: doc}, objectPrefix, ".docx", docxContentType, partNum)
		if err != nil {
			return err
		}
		urls = append(urls, url)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("split docx: %w", err)
	}

	return urls, nil
}
