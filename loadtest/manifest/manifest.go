// Package manifest loads and saves the k6 load-test fixture manifest
// (k6/fixtures/manifest.example.json), shared between loadtest tools
// (verifyfixtures, drain) so the schema is defined exactly once.
package manifest

import (
	"encoding/json"
	"fmt"
	"os"
)

// Fixture describes one load-test document: where to fetch it, what size
// class it belongs to, and whether its bytes/checksum have actually been
// confirmed against the real object (as opposed to being a placeholder
// entry awaiting a real upload). See k6/fixtures/manifest.example.json for
// the full field contract and k6/README.md for how it's consumed.
type Fixture struct {
	ID            string  `json:"id"`
	Format        string  `json:"format"`
	SizeClass     string  `json:"size_class"` // "small" | "large"
	URL           string  `json:"url"`
	URLHost       string  `json:"url_host,omitempty"`
	Bytes         *int64  `json:"bytes"`
	SHA256        *string `json:"sha256"`
	ExpectedParts *int    `json:"expected_parts"`
	Verified      bool    `json:"verified"`
	VerifiedAt    string  `json:"verified_at,omitempty"`
	VerifiedBy    string  `json:"verified_by,omitempty"`
	Notes         string  `json:"notes,omitempty"`
}

// Manifest is the top-level fixture manifest document.
type Manifest struct {
	SchemaVersion      int       `json:"schema_version"`
	Bucket             string    `json:"bucket"`
	SizeThresholdBytes int64     `json:"size_threshold_bytes"`
	Notes              []string  `json:"notes,omitempty"`
	Fixtures           []Fixture `json:"fixtures"`
}

// Load reads and parses the manifest at path. It does not validate beyond
// JSON structure: callers that need a fixture to be usable should check
// Verified/Bytes/SHA256 themselves, since an invalid-but-parseable
// placeholder entry is expected to exist in the file.
func Load(path string) (*Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read manifest %s: %w", path, err)
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("parse manifest %s: %w", path, err)
	}
	if len(m.Fixtures) == 0 {
		return nil, fmt.Errorf("manifest %s has no fixtures", path)
	}
	return &m, nil
}

// Save writes m back to path as indented JSON, preserving field order via
// the struct tags above. Used by verifyfixtures -write to persist
// newly-confirmed bytes/sha256/verified values.
func Save(path string, m *Manifest) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write manifest %s: %w", path, err)
	}
	return nil
}

// ByID returns the fixture with the given id, if present.
func (m *Manifest) ByID(id string) (*Fixture, bool) {
	for i := range m.Fixtures {
		if m.Fixtures[i].ID == id {
			return &m.Fixtures[i], true
		}
	}
	return nil, false
}

// SizeClassFor classifies bytes against the manifest's threshold, matching
// internal/usecase/splitfile.go's maxSplitPartSize semantics exactly
// (<=  threshold stays unsplit).
func (m *Manifest) SizeClassFor(bytes int64) string {
	if bytes <= m.SizeThresholdBytes {
		return "small"
	}
	return "large"
}
