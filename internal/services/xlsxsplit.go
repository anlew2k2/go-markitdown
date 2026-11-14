package services

import (
	"archive/zip"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
)

const (
	xlsxWorkbookPart     = "xl/workbook.xml"
	xlsxWorkbookRelsPart = "xl/_rels/workbook.xml.rels"
	xlsxContentTypesPart = "[Content_Types].xml"
	xlsxCalcChainPart    = "xl/calcChain.xml"
	xlsxSinglesheetPart  = "xl/worksheets/sheet1.xml"
	xlsxContentType      = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
	xlsxFallbackWSType   = "application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"

	xlsxWorksheetRelType = "http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet"
	xlsxCalcChainRelType = "http://schemas.openxmlformats.org/officeDocument/2006/relationships/calcChain"
)

var (
	xlsxDimensionPattern = regexp.MustCompile(`<dimension ref="([A-Z]+)\d+:([A-Z]+)\d+"/>`)
	xlsxRowNumberPattern = regexp.MustCompile(`<row r="(\d+)"`)
	// The leading \s anchors this to an actual "r" attribute (row or cell
	// reference); without it, this would also match inside a shared
	// formula's unrelated "ref=" attribute (e.g. ref="A2:A100").
	xlsxRowRefPattern = regexp.MustCompile(`(\s)r="([A-Za-z]*)(\d+)"`)
)

// renumberRows shifts every row number and cell reference ("r" attribute,
// whether on a <row> or a <c>) in body down by delta, preserving any
// internal gaps (rows a sheet writer skipped because they were empty).
// Needed because a part only contains a contiguous slice of the original
// sheet's rows, but a row-oriented reader (openpyxl's read_only mode,
// which pandas — and so markitdown — uses) pads a sheet out to its
// highest row number: left unrenumbered, a late part's original row
// numbers (e.g. 50001-59999) would make it appear to have tens of
// thousands of blank leading rows instead of the handful it actually
// holds.
func renumberRows(body []byte, delta int) []byte {
	if delta == 0 {
		return body
	}
	return xlsxRowRefPattern.ReplaceAllFunc(body, func(m []byte) []byte {
		sub := xlsxRowRefPattern.FindSubmatch(m)
		n, err := strconv.Atoi(string(sub[3]))
		if err != nil {
			return m
		}
		return []byte(fmt.Sprintf(`%sr="%s%d"`, sub[1], sub[2], n-delta))
	})
}

// patchDimension rewrites a worksheet prefix's <dimension> row bounds (a
// hint consumers may use to size up front) to match the row numbers
// actually present in body, since body is a subset of the original sheet's
// rows and the original <dimension> still describes the whole thing.
// Row "r" attributes are preserved verbatim from the source (so a later
// part's rows legitimately start well above 1); only the dimension's row
// bounds are corrected here, never the rows themselves. If no dimension
// tag or no rows are found, prefix is returned unchanged.
func patchDimension(prefix, body []byte) []byte {
	dim := xlsxDimensionPattern.FindSubmatchIndex(prefix)
	rows := xlsxRowNumberPattern.FindAllSubmatch(body, -1)
	if dim == nil || len(rows) == 0 {
		return prefix
	}
	col1 := prefix[dim[2]:dim[3]]
	col2 := prefix[dim[4]:dim[5]]
	firstRow := rows[0][1]
	lastRow := rows[len(rows)-1][1]

	replacement := fmt.Sprintf(`<dimension ref="%s%s:%s%s"/>`, col1, firstRow, col2, lastRow)
	out := make([]byte, 0, len(prefix))
	out = append(out, prefix[:dim[0]]...)
	out = append(out, replacement...)
	out = append(out, prefix[dim[1]:]...)
	return out
}

type xlsxSheetRef struct {
	Name string `xml:"name,attr"`
	RID  string `xml:"http://schemas.openxmlformats.org/officeDocument/2006/relationships id,attr"`
}

type xlsxWorkbookXML struct {
	XMLName xml.Name `xml:"workbook"`
	Sheets  struct {
		Sheet []xlsxSheetRef `xml:"sheet"`
	} `xml:"sheets"`
}

// splitXLSX splits a workbook sheet-by-sheet, row-by-row: each output part
// is a minimal, single-sheet .xlsx containing a contiguous run of rows from
// one original sheet (the header row included naturally, since it's just
// the first row). Every other part of the package (styles, sharedStrings,
// theme, docProps, ...) is copied into every part verbatim, so a cell's
// shared-string or style index stays valid without rewriting the cells
// themselves. workbook.xml, its rels and [Content_Types].xml are rewritten
// per part to describe just the one sheet present; calcChain.xml is
// dropped since it describes formula evaluation order across the whole
// original workbook, which no longer applies to a partial sheet.
func splitXLSX(ctx context.Context, localPath string, maxPartSize int64, objectPrefix string) ([]string, error) {
	f, err := os.Open(localPath)
	if err != nil {
		return nil, fmt.Errorf("open xlsx: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat xlsx: %w", err)
	}
	zr, err := zip.NewReader(f, info.Size())
	if err != nil {
		return nil, fmt.Errorf("read xlsx zip: %w", err)
	}

	wbFile := zipFileByName(zr.File, xlsxWorkbookPart)
	if wbFile == nil {
		return nil, fmt.Errorf("xlsx missing %s", xlsxWorkbookPart)
	}
	wbBytes, err := readZipFile(wbFile)
	if err != nil {
		return nil, err
	}
	var wb xlsxWorkbookXML
	if err := xml.Unmarshal(wbBytes, &wb); err != nil {
		return nil, fmt.Errorf("parse workbook.xml: %w", err)
	}

	var rels []opcRelationship
	if relsFile := zipFileByName(zr.File, xlsxWorkbookRelsPart); relsFile != nil {
		relsBytes, err := readZipFile(relsFile)
		if err != nil {
			return nil, err
		}
		if rels, err = parseOPCRels(relsBytes); err != nil {
			return nil, fmt.Errorf("parse workbook rels: %w", err)
		}
	}
	relByID := make(map[string]opcRelationship, len(rels))
	for _, r := range rels {
		relByID[r.ID] = r
	}

	var ct *opcContentTypes
	if ctFile := zipFileByName(zr.File, xlsxContentTypesPart); ctFile != nil {
		ctBytes, err := readZipFile(ctFile)
		if err != nil {
			return nil, err
		}
		if ct, err = parseOPCContentTypes(ctBytes); err != nil {
			return nil, fmt.Errorf("parse content types: %w", err)
		}
	}

	worksheetPaths := make(map[string]bool, len(wb.Sheets.Sheet))
	for _, s := range wb.Sheets.Sheet {
		if rel, ok := relByID[s.RID]; ok {
			worksheetPaths[resolveOPCTarget("xl", rel.Target)] = true
		}
	}

	sharedEntries := make([]*zip.File, 0, len(zr.File))
	for _, zf := range zr.File {
		if zf.Name == xlsxWorkbookPart || zf.Name == xlsxWorkbookRelsPart ||
			zf.Name == xlsxContentTypesPart || zf.Name == xlsxCalcChainPart ||
			worksheetPaths[zf.Name] {
			continue
		}
		sharedEntries = append(sharedEntries, zf)
	}
	var sharedOverhead int64
	for _, zf := range sharedEntries {
		sharedOverhead += int64(zf.UncompressedSize64)
	}

	var keptRels []opcRelationship
	for _, r := range rels {
		if r.Type == xlsxWorksheetRelType || r.Type == xlsxCalcChainRelType {
			continue
		}
		keptRels = append(keptRels, r)
	}

	worksheetContentType := xlsxFallbackWSType
	if ct != nil {
		for name := range worksheetPaths {
			if t := contentTypeFor(ct, name); t != "" {
				worksheetContentType = t
				break
			}
		}
	}

	keepParts := make(map[string]bool, len(sharedEntries)+3)
	for _, zf := range sharedEntries {
		keepParts[zf.Name] = true
	}
	keepParts[xlsxWorkbookPart] = true
	keepParts[xlsxWorkbookRelsPart] = true
	keepParts[xlsxSinglesheetPart] = true

	var urls []string
	partNum := 0

	for _, sheet := range wb.Sheets.Sheet {
		rel, ok := relByID[sheet.RID]
		if !ok {
			continue
		}
		sheetFile := zipFileByName(zr.File, resolveOPCTarget("xl", rel.Target))
		if sheetFile == nil {
			continue
		}

		prefix, err := withZipEntry(sheetFile, func(r io.Reader) ([]byte, error) {
			return scanXMLPrefix(r, "sheetData")
		})
		if err != nil {
			return nil, fmt.Errorf("scan sheet %q: %w", sheet.Name, err)
		}

		cs, err := withZipEntry(sheetFile, func(r io.Reader) (childScan, error) {
			children, suffixStart, err := scanXMLChildren(r, "sheetData")
			return childScan{children, suffixStart}, err
		})
		if err != nil {
			return nil, fmt.Errorf("scan sheet %q rows: %w", sheet.Name, err)
		}

		suffix, err := withZipEntry(sheetFile, func(r io.Reader) ([]byte, error) {
			return readXMLSuffix(r, cs.suffixStart)
		})
		if err != nil {
			return nil, fmt.Errorf("read sheet %q suffix: %w", sheet.Name, err)
		}

		// The sheet's first row is its header; repeat it at the top of
		// every part (same convention as splitCSV) so a part other than
		// the first doesn't read as headerless, or worse, have its first
		// real data row mistaken for one by a column-oriented reader.
		var header []byte
		dataRows := cs.children
		if len(cs.children) > 0 {
			first := cs.children[0]
			header, err = withZipEntry(sheetFile, func(r io.Reader) ([]byte, error) {
				return readXMLSpan(r, first.start, first.end)
			})
			if err != nil {
				return nil, fmt.Errorf("read sheet %q header row: %w", sheet.Name, err)
			}
			dataRows = cs.children[1:]
		}

		wbTemplate := []byte(fmt.Sprintf(
			`<?xml version="1.0" encoding="UTF-8" standalone="yes"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="%s" sheetId="1" r:id="rIdSheet"/></sheets></workbook>`,
			escapeXMLAttr(sheet.Name)))

		partRels := append(append([]opcRelationship{}, keptRels...),
			opcRelationship{ID: "rIdSheet", Type: xlsxWorksheetRelType, Target: "worksheets/sheet1.xml"})
		wbRelsBytes := marshalOPCRels(partRels)

		var ctBytes []byte
		if ct != nil {
			ctBytes = filterContentTypes(ct, keepParts, []opcCTOverride{
				{PartName: "/" + xlsxSinglesheetPart, ContentType: worksheetContentType},
			})
		}

		overhead := sharedOverhead + int64(len(wbTemplate)+len(wbRelsBytes)+len(ctBytes)+len(prefix)+len(suffix)+len(header))
		groups := groupChildrenBySize(dataRows, maxPartSize-overhead)
		if len(groups) == 0 {
			continue
		}

		_, err = withZipEntry(sheetFile, func(r io.Reader) (struct{}, error) {
			streamErr := streamSpanGroups(r, groups, func(_ int, body io.Reader) error {
				bodyBytes, err := io.ReadAll(body)
				if err != nil {
					return fmt.Errorf("read sheet %q rows: %w", sheet.Name, err)
				}

				// Renumber this group's rows to start right after the
				// header (row 1), so they're contiguous instead of
				// resuming at their original, much higher row numbers.
				if m := xlsxRowNumberPattern.FindSubmatch(bodyBytes); m != nil {
					if firstRow, err := strconv.Atoi(string(m[1])); err == nil {
						bodyBytes = renumberRows(bodyBytes, firstRow-2)
					}
				}

				rows := make([]byte, 0, len(header)+len(bodyBytes))
				rows = append(rows, header...)
				rows = append(rows, bodyBytes...)

				partPrefix := patchDimension(prefix, rows)
				sheetXML := make([]byte, 0, len(partPrefix)+len(rows)+len(suffix))
				sheetXML = append(sheetXML, partPrefix...)
				sheetXML = append(sheetXML, rows...)
				sheetXML = append(sheetXML, suffix...)

				overrides := map[string][]byte{
					xlsxWorkbookPart:     wbTemplate,
					xlsxWorkbookRelsPart: wbRelsBytes,
					xlsxSinglesheetPart:  sheetXML,
				}
				if ctBytes != nil {
					overrides[xlsxContentTypesPart] = ctBytes
				}

				partNum++
				url, err := writeOOXMLPart(ctx, sharedEntries, overrides, objectPrefix, ".xlsx", xlsxContentType, partNum)
				if err != nil {
					return err
				}
				urls = append(urls, url)
				return nil
			})
			return struct{}{}, streamErr
		})
		if err != nil {
			return nil, fmt.Errorf("split sheet %q: %w", sheet.Name, err)
		}
	}

	return urls, nil
}
