package services

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
)

const (
	pptxPresentationPart = "ppt/presentation.xml"
	pptxPresRelsPart     = "ppt/_rels/presentation.xml.rels"
	pptxContentTypesPart = "[Content_Types].xml"
	pptxContentType      = "application/vnd.openxmlformats-officedocument.presentationml.presentation"
	pptxSlideRelType     = "http://schemas.openxmlformats.org/officeDocument/2006/relationships/slide"
)

// pptxSlideRIDPattern pulls the r:id out of one <p:sldId .../> entry; these
// are simple, attribute-only, self-closing elements, so a regexp over the
// element's own span is simpler and just as reliable as unmarshaling.
var pptxSlideRIDPattern = regexp.MustCompile(`r:id="([^"]+)"`)

type pptxSlideInfo struct {
	rID      string
	file     *zip.File
	relsFile *zip.File
	media    []*zip.File
	weight   int64
}

// splitPPTX splits a deck slide-by-slide: slides are already separate zip
// entries, so no XML needs to be cut mid-element — only bin-packed into
// groups whose combined size (slide + its own rels + the media it
// references) fits maxPartSize, preserving original slide order. Shared
// package parts (slide masters/layouts, theme, docProps, ...) are small and
// copied into every part verbatim; presentation.xml, its rels and
// [Content_Types].xml — all small manifests — are rewritten per part to
// describe only the slides present in it.
//
// Media referenced by more than one included slide is only ever written
// once per part (zip entries are deduplicated by name); media referenced
// across different parts is necessarily duplicated, same trade-off as
// xlsx's shared parts.
func splitPPTX(ctx context.Context, localPath string, maxPartSize int64, objectPrefix string) ([]string, error) {
	f, err := os.Open(localPath)
	if err != nil {
		return nil, fmt.Errorf("open pptx: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat pptx: %w", err)
	}
	zr, err := zip.NewReader(f, info.Size())
	if err != nil {
		return nil, fmt.Errorf("read pptx zip: %w", err)
	}

	presFile := zipFileByName(zr.File, pptxPresentationPart)
	if presFile == nil {
		return nil, fmt.Errorf("pptx missing %s", pptxPresentationPart)
	}
	presBytes, err := readZipFile(presFile)
	if err != nil {
		return nil, err
	}

	sldPrefix, err := scanXMLPrefix(bytes.NewReader(presBytes), "sldIdLst")
	if err != nil {
		return nil, fmt.Errorf("scan presentation slide list: %w", err)
	}
	sldChildren, sldSuffixStart, err := scanXMLChildren(bytes.NewReader(presBytes), "sldIdLst")
	if err != nil {
		return nil, fmt.Errorf("scan presentation slides: %w", err)
	}
	sldSuffix := presBytes[sldSuffixStart:]

	presRelsFile := zipFileByName(zr.File, pptxPresRelsPart)
	if presRelsFile == nil {
		return nil, fmt.Errorf("pptx missing %s", pptxPresRelsPart)
	}
	presRelsBytes, err := readZipFile(presRelsFile)
	if err != nil {
		return nil, err
	}
	presRels, err := parseOPCRels(presRelsBytes)
	if err != nil {
		return nil, fmt.Errorf("parse presentation rels: %w", err)
	}
	relByID := make(map[string]opcRelationship, len(presRels))
	for _, r := range presRels {
		relByID[r.ID] = r
	}

	var ct *opcContentTypes
	if ctFile := zipFileByName(zr.File, pptxContentTypesPart); ctFile != nil {
		ctBytes, err := readZipFile(ctFile)
		if err != nil {
			return nil, err
		}
		if ct, err = parseOPCContentTypes(ctBytes); err != nil {
			return nil, fmt.Errorf("parse content types: %w", err)
		}
	}

	slides := make([]pptxSlideInfo, len(sldChildren))
	for i, c := range sldChildren {
		m := pptxSlideRIDPattern.FindSubmatch(presBytes[c.start:c.end])
		if m == nil {
			return nil, fmt.Errorf("pptx slide entry %d missing r:id", i)
		}
		rID := string(m[1])

		rel, ok := relByID[rID]
		if !ok {
			return nil, fmt.Errorf("pptx slide rId %s has no relationship", rID)
		}
		slidePath := resolveOPCTarget("ppt", rel.Target)
		slideFile := zipFileByName(zr.File, slidePath)
		if slideFile == nil {
			return nil, fmt.Errorf("pptx missing slide part %s", slidePath)
		}

		relsPath := resolveOPCTarget("ppt/slides/_rels", strings.TrimPrefix(slidePath, "ppt/slides/")+".rels")
		var relsFile *zip.File
		var media []*zip.File
		if rf := zipFileByName(zr.File, relsPath); rf != nil {
			relsFile = rf
			rb, err := readZipFile(rf)
			if err != nil {
				return nil, err
			}
			slideRels, err := parseOPCRels(rb)
			if err != nil {
				return nil, fmt.Errorf("parse %s: %w", relsPath, err)
			}
			seen := make(map[string]bool)
			for _, r := range slideRels {
				if r.TargetMode == "External" {
					continue
				}
				mediaPath := resolveOPCTarget("ppt/slides", r.Target)
				if !strings.HasPrefix(mediaPath, "ppt/media/") || seen[mediaPath] {
					continue
				}
				seen[mediaPath] = true
				if mf := zipFileByName(zr.File, mediaPath); mf != nil {
					media = append(media, mf)
				}
			}
		}

		weight := int64(slideFile.UncompressedSize64)
		if relsFile != nil {
			weight += int64(relsFile.UncompressedSize64)
		}
		for _, mf := range media {
			weight += int64(mf.UncompressedSize64)
		}

		slides[i] = pptxSlideInfo{rID: rID, file: slideFile, relsFile: relsFile, media: media, weight: weight}
	}

	slideScoped := make(map[string]bool)
	for _, s := range slides {
		slideScoped[s.file.Name] = true
		if s.relsFile != nil {
			slideScoped[s.relsFile.Name] = true
		}
		for _, mf := range s.media {
			slideScoped[mf.Name] = true
		}
	}

	var sharedEntries []*zip.File
	var sharedOverhead int64
	for _, zf := range zr.File {
		if zf.Name == pptxPresentationPart || zf.Name == pptxPresRelsPart || zf.Name == pptxContentTypesPart || slideScoped[zf.Name] {
			continue
		}
		sharedEntries = append(sharedEntries, zf)
		sharedOverhead += int64(zf.UncompressedSize64)
	}

	weights := make([]int64, len(slides))
	for i, s := range slides {
		weights[i] = s.weight
	}
	groups := groupIndicesBySize(weights, maxPartSize-sharedOverhead)

	keepPartsBase := make(map[string]bool, len(sharedEntries)+2)
	for _, zf := range sharedEntries {
		keepPartsBase[zf.Name] = true
	}
	keepPartsBase[pptxPresentationPart] = true
	keepPartsBase[pptxPresRelsPart] = true

	var urls []string
	partNum := 0

	for _, idxs := range groups {
		lo, hi := idxs[0], idxs[len(idxs)-1]

		includedRIDs := make(map[string]bool, len(idxs))
		extraEntries := make(map[string]*zip.File)
		for _, i := range idxs {
			s := slides[i]
			includedRIDs[s.rID] = true
			extraEntries[s.file.Name] = s.file
			if s.relsFile != nil {
				extraEntries[s.relsFile.Name] = s.relsFile
			}
			for _, mf := range s.media {
				extraEntries[mf.Name] = mf
			}
		}

		partPres := make([]byte, 0, len(sldPrefix)+int(sldChildren[hi].end-sldChildren[lo].start)+len(sldSuffix))
		partPres = append(partPres, sldPrefix...)
		partPres = append(partPres, presBytes[sldChildren[lo].start:sldChildren[hi].end]...)
		partPres = append(partPres, sldSuffix...)

		var partRels []opcRelationship
		for _, r := range presRels {
			if r.Type == pptxSlideRelType && !includedRIDs[r.ID] {
				continue
			}
			partRels = append(partRels, r)
		}
		partRelsBytes := marshalOPCRels(partRels)

		keepParts := make(map[string]bool, len(keepPartsBase)+len(extraEntries))
		for k := range keepPartsBase {
			keepParts[k] = true
		}
		for name := range extraEntries {
			keepParts[name] = true
		}

		var ctBytes []byte
		if ct != nil {
			ctBytes = filterContentTypes(ct, keepParts, nil)
		}

		entries := make([]*zip.File, 0, len(sharedEntries)+len(extraEntries))
		entries = append(entries, sharedEntries...)
		for _, zf := range extraEntries {
			entries = append(entries, zf)
		}

		overrides := map[string][]byte{
			pptxPresentationPart: partPres,
			pptxPresRelsPart:     partRelsBytes,
		}
		if ctBytes != nil {
			overrides[pptxContentTypesPart] = ctBytes
		}

		partNum++
		url, err := writeOOXMLPart(ctx, entries, overrides, objectPrefix, ".pptx", pptxContentType, partNum)
		if err != nil {
			return nil, fmt.Errorf("write pptx part: %w", err)
		}
		urls = append(urls, url)
	}

	return urls, nil
}

// groupIndicesBySize greedily bin-packs 0..len(weights)-1 into
// contiguous runs whose combined weight stays within budget, preserving
// order (so the contiguous original-document run invariant holds for
// slicing the sldIdLst's byte range per group). A single item heavier than
// budget becomes its own oversized group rather than being dropped.
func groupIndicesBySize(weights []int64, budget int64) [][]int {
	if budget < 1 {
		budget = 1
	}

	var groups [][]int
	var cur []int
	var curWeight int64

	for i, w := range weights {
		if len(cur) > 0 && curWeight+w > budget {
			groups = append(groups, cur)
			cur = nil
			curWeight = 0
		}
		cur = append(cur, i)
		curWeight += w
	}
	if len(cur) > 0 {
		groups = append(groups, cur)
	}
	return groups
}
