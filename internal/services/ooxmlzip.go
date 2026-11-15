package services

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
)

// xmlChild describes one direct child element of a wrapper element (e.g.
// one <row> inside <sheetData>, or one <w:p>/<w:tbl> inside <w:body>), as a
// byte-offset span into the XML stream it was found in.
type xmlChild struct {
	name       string
	start, end int64
}

// scanXMLPrefix reads r only until it finds the opening tag of the first
// element named wrapperLocal (by local name, namespace prefix ignored) and
// returns every byte up to and including that opening tag. It stops as
// soon as the tag is found, so it never buffers more than that prefix.
func scanXMLPrefix(r io.Reader, wrapperLocal string) ([]byte, error) {
	var buf bytes.Buffer
	dec := xml.NewDecoder(io.TeeReader(r, &buf))
	for {
		tok, err := dec.RawToken()
		if err != nil {
			return nil, fmt.Errorf("scan xml prefix: %w", err)
		}
		if se, ok := tok.(xml.StartElement); ok && se.Name.Local == wrapperLocal {
			end := dec.InputOffset()
			return buf.Bytes()[:end], nil
		}
	}
}

// scanXMLChildren scans r (consumed until the wrapper element closes) and
// reports the byte-offset span of each of the wrapper's direct children, in
// document order, plus the byte offset at which the wrapper's own closing
// tag begins. It holds only the child offsets in memory, never the
// element content itself, so it stays cheap regardless of how large the
// wrapper's content is.
func scanXMLChildren(r io.Reader, wrapperLocal string) (children []xmlChild, suffixStart int64, err error) {
	dec := xml.NewDecoder(r)

	type frame struct{ name string }
	var stack []frame
	wrapperDepth := -1
	curChildStart := int64(-1)
	curChildName := ""

	prevEnd := int64(0)
	for {
		start := prevEnd
		tok, terr := dec.RawToken()
		if terr != nil {
			return nil, 0, fmt.Errorf("scan xml children: %w", terr)
		}
		end := dec.InputOffset()
		prevEnd = end

		switch t := tok.(type) {
		case xml.StartElement:
			stack = append(stack, frame{name: t.Name.Local})
			depth := len(stack)
			if wrapperDepth == -1 && t.Name.Local == wrapperLocal {
				wrapperDepth = depth
			} else if wrapperDepth != -1 && depth == wrapperDepth+1 && curChildStart == -1 {
				curChildStart = start
				curChildName = t.Name.Local
			}
		case xml.EndElement:
			depth := len(stack)
			if wrapperDepth != -1 && depth == wrapperDepth+1 && curChildStart != -1 {
				children = append(children, xmlChild{name: curChildName, start: curChildStart, end: end})
				curChildStart = -1
			}
			if wrapperDepth != -1 && depth == wrapperDepth {
				return children, start, nil
			}
			stack = stack[:len(stack)-1]
		}
	}
}

// readXMLSuffix reads r, discards everything before offset from, and
// returns the remainder (which is expected to be small: the wrapper's
// closing tag plus whatever little markup follows it to the end of the
// part).
func readXMLSuffix(r io.Reader, from int64) ([]byte, error) {
	if _, err := io.CopyN(io.Discard, r, from); err != nil {
		return nil, fmt.Errorf("skip to suffix: %w", err)
	}
	return io.ReadAll(r)
}

// readXMLSpan reads exactly the [start, end) byte range from a freshly
// opened reader over the same part.
func readXMLSpan(r io.Reader, start, end int64) ([]byte, error) {
	if _, err := io.CopyN(io.Discard, r, start); err != nil {
		return nil, fmt.Errorf("skip to span: %w", err)
	}
	buf := make([]byte, end-start)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("read span: %w", err)
	}
	return buf, nil
}

// spanGroup is a contiguous run of children (by index into the slice
// scanXMLChildren returned) whose combined byte length fits within a size
// budget.
type spanGroup struct {
	start, end int64 // byte offsets in the original stream: [start, end)
}

// groupChildrenBySize greedily packs consecutive children into groups whose
// combined byte length stays within budget, flushing a group as soon as the
// next child would push it over. A single child larger than budget becomes
// its own (oversized) group rather than being dropped.
func groupChildrenBySize(children []xmlChild, budget int64) []spanGroup {
	if budget < 1 {
		budget = 1
	}

	var groups []spanGroup
	var cur spanGroup
	open := false

	for _, c := range children {
		size := c.end - c.start
		if open && cur.end-cur.start+size > budget {
			groups = append(groups, cur)
			open = false
		}
		if !open {
			cur = spanGroup{start: c.start, end: c.end}
			open = true
		} else {
			cur.end = c.end
		}
	}
	if open {
		groups = append(groups, cur)
	}
	return groups
}

// streamSpanGroups makes one sequential pass over r, and for every group in
// groups (which must be in increasing, non-overlapping offset order) calls
// onGroup with exactly that byte range copied through. Bytes outside every
// group are discarded.
func streamSpanGroups(r io.Reader, groups []spanGroup, onGroup func(i int, body io.Reader) error) error {
	cursor := int64(0)
	for i, g := range groups {
		if gap := g.start - cursor; gap > 0 {
			if _, err := io.CopyN(io.Discard, r, gap); err != nil {
				return fmt.Errorf("skip to group %d: %w", i, err)
			}
		}
		if err := onGroup(i, io.LimitReader(r, g.end-g.start)); err != nil {
			return err
		}
		cursor = g.end
	}
	return nil
}

// copyZipEntryRaw copies a zip entry's compressed bytes verbatim from src
// into zw, without decompressing/recompressing, preserving its original
// compression method, CRC32 and sizes.
func copyZipEntryRaw(zw *zip.Writer, src *zip.File) error {
	rc, err := src.OpenRaw()
	if err != nil {
		return fmt.Errorf("open raw %s: %w", src.Name, err)
	}
	w, err := zw.CreateRaw(&src.FileHeader)
	if err != nil {
		return fmt.Errorf("create raw %s: %w", src.Name, err)
	}
	if _, err := io.Copy(w, rc); err != nil {
		return fmt.Errorf("copy raw %s: %w", src.Name, err)
	}
	return nil
}

// zipEntryOverhead estimates the bytes a raw-copied entry (see
// copyZipEntryRaw) actually contributes to an output archive: such entries
// are never re-inflated, so their payload is CompressedSize64 rather than
// UncompressedSize64, plus the fixed local (30B) and central directory (46B)
// file header bytes ZIP spends per entry and the two copies of its name
// those headers carry.
func zipEntryOverhead(zf *zip.File) int64 {
	const fixedHeaderBytes = 30 + 46
	return int64(zf.CompressedSize64) + fixedHeaderBytes + 2*int64(len(zf.Name))
}

// zipFileByName finds a zip entry by its exact path, or nil if absent.
func zipFileByName(files []*zip.File, name string) *zip.File {
	for _, f := range files {
		if f.Name == name {
			return f
		}
	}
	return nil
}

// withZipEntry opens a fresh reader on f, hands it to fn, and closes it
// before returning. A single zip entry's content needs several independent
// sequential passes (a prefix scan, a child-offset scan, a suffix read, a
// body stream); zip.File.Open can be called repeatedly to get a fresh
// reader for each one.
func withZipEntry[T any](f *zip.File, fn func(io.Reader) (T, error)) (T, error) {
	var zero T
	r, err := f.Open()
	if err != nil {
		return zero, err
	}
	defer r.Close()
	return fn(r)
}

// childScan is the paired result of scanXMLChildren, bundled so it can
// flow through withZipEntry's single-value signature.
type childScan struct {
	children    []xmlChild
	suffixStart int64
}

// readZipFile fully reads a small zip entry (a manifest-style part such as
// workbook.xml, a .rels file, or [Content_Types].xml — never the large
// content part being split) into memory.
func readZipFile(f *zip.File) ([]byte, error) {
	rc, err := f.Open()
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", f.Name, err)
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", f.Name, err)
	}
	return data, nil
}

// opcRelationship is one <Relationship> entry of an OPC .rels part.
type opcRelationship struct {
	ID         string `xml:"Id,attr"`
	Type       string `xml:"Type,attr"`
	Target     string `xml:"Target,attr"`
	TargetMode string `xml:"TargetMode,attr,omitempty"`
}

type opcRelationships struct {
	XMLName       xml.Name          `xml:"Relationships"`
	Relationships []opcRelationship `xml:"Relationship"`
}

func parseOPCRels(data []byte) ([]opcRelationship, error) {
	var rels opcRelationships
	if err := xml.Unmarshal(data, &rels); err != nil {
		return nil, fmt.Errorf("parse rels: %w", err)
	}
	return rels.Relationships, nil
}

// marshalOPCRels renders rels back into a .rels part. Built by hand rather
// than via xml.Marshal so every relationship always carries exactly the
// attributes OPC expects, in the conventional order.
func marshalOPCRels(rels []opcRelationship) []byte {
	var buf bytes.Buffer
	buf.WriteString(xml.Header)
	buf.WriteString(`<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">`)
	for _, r := range rels {
		buf.WriteString(`<Relationship Id="`)
		xml.EscapeText(&buf, []byte(r.ID))
		buf.WriteString(`" Type="`)
		xml.EscapeText(&buf, []byte(r.Type))
		buf.WriteString(`" Target="`)
		xml.EscapeText(&buf, []byte(r.Target))
		buf.WriteString(`"`)
		if r.TargetMode != "" {
			buf.WriteString(` TargetMode="`)
			xml.EscapeText(&buf, []byte(r.TargetMode))
			buf.WriteString(`"`)
		}
		buf.WriteString(`/>`)
	}
	buf.WriteString(`</Relationships>`)
	return buf.Bytes()
}

// resolveOPCTarget resolves a relationship Target against the folder that
// contains the part the .rels file belongs to (e.g. "xl" for
// xl/_rels/workbook.xml.rels, whose targets are rooted at "xl/").
func resolveOPCTarget(baseDir, target string) string {
	if strings.HasPrefix(target, "/") {
		return strings.TrimPrefix(target, "/")
	}
	return path.Join(baseDir, target)
}

// opcContentTypes is the parsed form of [Content_Types].xml.
type opcContentTypes struct {
	XMLName  xml.Name        `xml:"Types"`
	Default  []opcCTDefault  `xml:"Default"`
	Override []opcCTOverride `xml:"Override"`
}

type opcCTDefault struct {
	Extension   string `xml:"Extension,attr"`
	ContentType string `xml:"ContentType,attr"`
}

type opcCTOverride struct {
	PartName    string `xml:"PartName,attr"`
	ContentType string `xml:"ContentType,attr"`
}

func parseOPCContentTypes(data []byte) (*opcContentTypes, error) {
	var ct opcContentTypes
	if err := xml.Unmarshal(data, &ct); err != nil {
		return nil, fmt.Errorf("parse content types: %w", err)
	}
	return &ct, nil
}

// filterContentTypes rebuilds [Content_Types].xml keeping every Default
// (extension-based defaults are harmless even for an unused extension) but
// dropping any Override whose PartName isn't present in keepParts (so the
// manifest never references a part that got left out of the repackaged
// zip), plus appending extra Overrides for parts synthesized for this part
// (e.g. a renamed single worksheet).
func filterContentTypes(ct *opcContentTypes, keepParts map[string]bool, extra []opcCTOverride) []byte {
	var buf bytes.Buffer
	buf.WriteString(xml.Header)
	buf.WriteString(`<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">`)
	for _, d := range ct.Default {
		buf.WriteString(`<Default Extension="`)
		xml.EscapeText(&buf, []byte(d.Extension))
		buf.WriteString(`" ContentType="`)
		xml.EscapeText(&buf, []byte(d.ContentType))
		buf.WriteString(`"/>`)
	}
	for _, o := range ct.Override {
		if !keepParts[strings.TrimPrefix(o.PartName, "/")] {
			continue
		}
		writeCTOverride(&buf, o)
	}
	for _, o := range extra {
		writeCTOverride(&buf, o)
	}
	buf.WriteString(`</Types>`)
	return buf.Bytes()
}

func writeCTOverride(buf *bytes.Buffer, o opcCTOverride) {
	buf.WriteString(`<Override PartName="`)
	xml.EscapeText(buf, []byte(o.PartName))
	buf.WriteString(`" ContentType="`)
	xml.EscapeText(buf, []byte(o.ContentType))
	buf.WriteString(`"/>`)
}

// contentTypeFor looks up the ContentType an original manifest declared for
// partName (with or without its leading slash).
func contentTypeFor(ct *opcContentTypes, partName string) string {
	partName = "/" + strings.TrimPrefix(partName, "/")
	for _, o := range ct.Override {
		if o.PartName == partName {
			return o.ContentType
		}
	}
	return ""
}

func escapeXMLAttr(s string) string {
	var buf bytes.Buffer
	xml.EscapeText(&buf, []byte(s))
	return buf.String()
}

// writeOOXMLPart assembles one output OOXML package: every entry in
// sharedEntries is copied verbatim (raw, undecompressed-then-recompressed)
// from the original archive, and every path in overrides is written fresh
// with the given bytes. The result is uploaded to MinIO and its URL
// returned.
func writeOOXMLPart(ctx context.Context, sharedEntries []*zip.File, overrides map[string][]byte, objectPrefix, ext, contentType string, partNum int) (string, error) {
	tmp, err := os.CreateTemp("", fmt.Sprintf("split-part-%03d-*%s", partNum, ext))
	if err != nil {
		return "", fmt.Errorf("create part file: %w", err)
	}
	defer os.Remove(tmp.Name())

	zw := zip.NewWriter(tmp)
	for _, zf := range sharedEntries {
		if err := copyZipEntryRaw(zw, zf); err != nil {
			zw.Close()
			tmp.Close()
			return "", err
		}
	}
	for name, data := range overrides {
		w, err := zw.Create(name)
		if err != nil {
			zw.Close()
			tmp.Close()
			return "", fmt.Errorf("create %s: %w", name, err)
		}
		if _, err := w.Write(data); err != nil {
			zw.Close()
			tmp.Close()
			return "", fmt.Errorf("write %s: %w", name, err)
		}
	}
	if err := zw.Close(); err != nil {
		tmp.Close()
		return "", fmt.Errorf("finalize zip: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return "", fmt.Errorf("close part file: %w", err)
	}

	objectName := fmt.Sprintf("%s/part-%03d%s", objectPrefix, partNum, ext)
	return uploadLocalFile(ctx, tmp.Name(), objectName, contentType)
}
