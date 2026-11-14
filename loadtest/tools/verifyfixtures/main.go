// Command verifyfixtures confirms that every fixture in a k6 load-test
// manifest actually exists in MinIO with the bytes/checksum/size_class the
// manifest claims, and (with -write) fills in real values for placeholder
// entries. It is a support tool, not a test runner: it is never invoked
// automatically, and this change does not execute it against any
// environment.
//
// Usage:
//
//	go run ./loadtest/tools/verifyfixtures -manifest k6/fixtures/manifest.example.json
//	go run ./loadtest/tools/verifyfixtures -manifest k6/fixtures/manifest.example.json -write
//
// By default each fixture is fetched via its url_host field (the
// host-reachable MinIO address, e.g. through deploy/docker-compose.test.yaml's
// published port) since this tool is meant to run from the operator's host,
// not from inside the docker-compose network. Pass -use-url to fetch via the
// container-network url field instead (e.g. when running this tool itself
// inside the compose network).
//
// This tool deliberately does NOT attempt to determine expected_parts: part
// count for a "large" fixture depends on how the Splitter actually cuts the
// real file (page/row/zip-part boundaries), which cannot be inferred from
// byte size. Fill expected_parts manually after a real smoke run by reading
// the `total` field of the resulting SUCCESS event on
// stream:doc-extractor:output.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/go-markitdown/loadtest/manifest"
)

func main() {
	manifestPath := flag.String("manifest", "k6/fixtures/manifest.example.json", "path to the fixture manifest JSON")
	write := flag.Bool("write", false, "persist confirmed bytes/sha256/size_class/verified back into the manifest file")
	useContainerURL := flag.Bool("use-url", false, "fetch via the container-network `url` field instead of `url_host`")
	timeout := flag.Duration("timeout", 2*time.Minute, "per-fixture HTTP timeout")
	flag.Parse()

	m, err := manifest.Load(*manifestPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "verifyfixtures: %v\n", err)
		os.Exit(1)
	}

	client := &http.Client{Timeout: *timeout}

	changed := false
	failures := 0
	for i := range m.Fixtures {
		f := &m.Fixtures[i]
		fetchURL := f.URLHost
		if *useContainerURL || fetchURL == "" {
			fetchURL = f.URL
		}
		if fetchURL == "" {
			fmt.Printf("SKIP  %-32s no url/url_host set\n", f.ID)
			failures++
			continue
		}

		bytesCount, sum, err := fetchAndHash(client, fetchURL)
		if err != nil {
			fmt.Printf("FAIL  %-32s %s: %v\n", f.ID, fetchURL, err)
			failures++
			continue
		}

		observedClass := m.SizeClassFor(bytesCount)
		status := "OK"
		if f.Bytes != nil && *f.Bytes != bytesCount {
			status = "MISMATCH(bytes)"
		}
		if f.SHA256 != nil && *f.SHA256 != sum {
			status = "MISMATCH(sha256)"
		}
		if f.SizeClass != observedClass {
			status = "MISMATCH(size_class)"
		}
		fmt.Printf("%-18s %-32s bytes=%d size_class=%s sha256=%s\n", status, f.ID, bytesCount, observedClass, sum)
		if status != "OK" {
			failures++
		}

		if *write {
			// A successful fetch+hash is itself the verification: it replaces
			// whatever bytes/sha256/size_class the manifest previously claimed
			// (a MISMATCH above means the old manifest entry was wrong, not
			// that this fixture failed verification).
			f.Bytes = &bytesCount
			f.SHA256 = &sum
			f.SizeClass = observedClass
			f.Verified = true
			f.VerifiedAt = time.Now().UTC().Format("2006-01-02")
			f.VerifiedBy = "loadtest/tools/verifyfixtures"
			changed = true
		}
	}

	if *write && changed {
		if err := manifest.Save(*manifestPath, m); err != nil {
			fmt.Fprintf(os.Stderr, "verifyfixtures: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("\nWrote updated manifest to %s\n", *manifestPath)
	}

	if failures > 0 {
		fmt.Fprintf(os.Stderr, "\nverifyfixtures: %d fixture(s) could not be confirmed; expected_parts was NOT touched for any fixture and must be filled in manually from a real smoke run.\n", failures)
		os.Exit(1)
	}
}

func fetchAndHash(client *http.Client, url string) (int64, string, error) {
	resp, err := client.Get(url)
	if err != nil {
		return 0, "", fmt.Errorf("GET: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, "", fmt.Errorf("GET returned status %d", resp.StatusCode)
	}

	h := sha256.New()
	n, err := io.Copy(h, resp.Body)
	if err != nil {
		return 0, "", fmt.Errorf("read body: %w", err)
	}
	return n, hex.EncodeToString(h.Sum(nil)), nil
}
