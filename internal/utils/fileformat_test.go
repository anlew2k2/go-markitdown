package utils

import "testing"

func TestExtensionOf(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{name: "plain pdf", raw: "https://example.com/files/doc.pdf", want: ".pdf"},
		{name: "uppercase ext", raw: "http://minio:9000/split-files/part.PDF", want: ".pdf"},
		{name: "presigned minio url", raw: "http://minio:9000/split-files/splits/a/b/part-001.pdf?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=abc%2F20261126%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Expires=900", want: ".pdf"},
		{name: "docx with query", raw: "https://cdn.example.com/report.docx?download=1", want: ".docx"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ExtensionOf(tc.raw)
			if err != nil {
				t.Fatalf("ExtensionOf(%q) unexpected error: %v", tc.raw, err)
			}
			if got != tc.want {
				t.Fatalf("ExtensionOf(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}
