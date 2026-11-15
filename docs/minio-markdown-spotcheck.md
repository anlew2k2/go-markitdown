# MinIO Markdown output spot-check

Date: 2026-11-15. Read-only check against local test MinIO at localhost:19000, bucket split-files; Redis mapping from local test stack. No object/state mutations or conversion reruns.

Listed 1,068 objects, including 609 Markdown objects; none of the listed Markdown objects had zero size. Random sampling stratified by source format, 2 samples/format, seed 20261115. All 609 Markdown objects could be mapped to a source part in the retrieved stream entries.

| Sample | Format | Verification | Result |
|---|---|---|---|
| 1 | csv | All 726 rows and cells equal, including header | PASS for stated check |
| 2 | csv | All 2,230 rows and cells equal, including header | PASS for stated check |
| 3 | docx | All 111,106 nonempty paragraphs found in source order | PASS for stated check |
| 4 | docx | All 22,364 nonempty paragraphs found in source order | PASS for stated check |
| 5 | pdf | Source text via pdfminer matches after whitespace normalization; page markers 161–200 | PASS for stated check |
| 6 | pdf | Source text via pdfminer matches after whitespace normalization; page markers 161–200 | PASS for stated check |
| 7 | xlsx | All 2,848 rows/cells equal with numeric representation normalization | PASS for stated check |
| 8 | xlsx | All 22,419 rows/cells equal with numeric representation normalization | PASS for stated check |

## Observations and limits

- Two randomly selected PDFs are separate requests with the same fixture/page range, so they do not provide distinct PDF content coverage. PDF text extraction is not a visual/layout audit and may share underlying extraction logic with MarkItDown.
- PDF outputs contain 39 form-feed characters (U+000C) per sample. Text matches; consider normalizing page separators for downstream consumers if needed.
- Sample 4 DOCX input: 1112582 bytes; Markdown: 51792917 bytes. A small compressed input can yield a much larger output; size-based input limits do not bound output memory.
- Compared each Markdown with its mapped source document/part, not all parts with the complete original document. This does not establish cross-part completeness, no duplicated boundaries, formatting fidelity, image/OCR correctness, or formula evaluation correctness.
- Existing objects are historical; backend/version provenance was not established. This is not evidence that the new persistent backend produced these objects or equals CLI.
- Evidence and downloaded samples: tmp/md-spotcheck/samples.json, checks.json, check.py and numbered input/output files. Raw evidence is local temporary data.

## Object references

### Sample 1

Markdown: `splits/64fe9ee1-961f-4a23-9d42-2e4d1b874fba/8d58ab30-eb18-43bc-b5ad-f671dd1e92ea/65658273-7390-4fb6-9f50-64917a3fc416/0105b9f6-f804-46a5-a8a2-ccc74172c336/part-001.md`

Source: `http://minio:9000/split-files/splits/64fe9ee1-961f-4a23-9d42-2e4d1b874fba/8d58ab30-eb18-43bc-b5ad-f671dd1e92ea/65658273-7390-4fb6-9f50-64917a3fc416/part-002.csv`

### Sample 2

Markdown: `splits/0237d4a1-fb57-4cef-b13f-da02643cba80/c31570bb-045a-449f-bc61-172a4f3e2424/db03878b-aee3-40cc-a0bb-69309257733e/fa399fb0-f8d4-402a-be5a-bf7bc11ff827/part-001.md`

Source: `http://minio:9000/split-files/splits/0237d4a1-fb57-4cef-b13f-da02643cba80/c31570bb-045a-449f-bc61-172a4f3e2424/db03878b-aee3-40cc-a0bb-69309257733e/part-002.csv`

### Sample 3

Markdown: `splits/75012de1-bb57-4a92-aac9-adc650964710/aa2bffc5-9cec-4111-93de-c52e90298b42/1823449a-6281-494b-944e-f5691fa93d7a/cc7e0637-da6e-4f26-ba0d-bea882c3880b/part-001.md`

Source: `http://minio:9000/split-files/splits/75012de1-bb57-4a92-aac9-adc650964710/aa2bffc5-9cec-4111-93de-c52e90298b42/1823449a-6281-494b-944e-f5691fa93d7a/part-002.docx`

### Sample 4

Markdown: `splits/89723f75-c876-4bfa-8d59-720a37c5369b/5d23e526-cc78-46e6-8790-d767288f2d5f/80293bc4-34a9-4388-8d47-a254942f7706/acc1f347-74bd-4cd6-822f-88ef61fe2382/part-000.md`

Source: `http://minio:9000/dataset/docx_small_1mib.docx`

### Sample 5

Markdown: `splits/ee424945-e344-4c4e-b1f9-7b802cd87791/f547bcc9-e8b0-4a47-bdf9-22aa2692bdb6/c02caca6-508f-4d6d-9fc9-2860e44cb0a0/8c5037ee-a09e-441f-80ef-d87d0d3cb422/part-004.md`

Source: `http://minio:9000/split-files/splits/ee424945-e344-4c4e-b1f9-7b802cd87791/f547bcc9-e8b0-4a47-bdf9-22aa2692bdb6/c02caca6-508f-4d6d-9fc9-2860e44cb0a0/part-005.pdf`

### Sample 6

Markdown: `splits/e747cda5-0114-4d0e-ba30-05238482d3ec/3db524f4-d287-42f3-8ca0-c5694588751b/9e817c9d-b960-4427-80ec-22d38c936802/15edca7a-3708-4b1c-afd2-4481d99bbb7e/part-004.md`

Source: `http://minio:9000/split-files/splits/e747cda5-0114-4d0e-ba30-05238482d3ec/3db524f4-d287-42f3-8ca0-c5694588751b/9e817c9d-b960-4427-80ec-22d38c936802/part-005.pdf`

### Sample 7

Markdown: `splits/3e4d3b98-a183-4d0b-ae88-3cb9af6e31b9/504e2746-5143-4f25-abae-03736a942a4d/7b40a761-f855-4b9c-8c01-7be3262da680/ac235fe2-eb40-4a18-a37b-32f2860a5ba7/part-000.md`

Source: `http://minio:9000/dataset/xlsx_small_100kb.xlsx`

### Sample 8

Markdown: `splits/862f0227-0982-4b48-aac8-8f84f8477487/6c569b86-9291-4619-953f-b860911d5413/58c33e9a-a3eb-4880-8da8-4f84981eff3c/35a925c6-277d-4727-816a-ddfd97f7881d/part-007.md`

Source: `http://minio:9000/split-files/splits/862f0227-0982-4b48-aac8-8f84f8477487/6c569b86-9291-4619-953f-b860911d5413/58c33e9a-a3eb-4880-8da8-4f84981eff3c/part-008.xlsx`
