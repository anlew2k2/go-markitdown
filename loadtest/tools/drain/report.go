package main

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/go-markitdown/loadtest/manifest"
)

type reportInput struct {
	runID              string
	requests           map[string]*requestRecord
	admissionStops     []admissionStopEvent
	runStartLine       *k6LogLine
	fixtureIndex       map[string]*manifest.Fixture
	duplicates         []DuplicateTerminal
	inputDecodeErrors  int
	outputDecodeErrors int
	deadlineExceeded   bool
	window             time.Duration
	parseWarnings      []string
}

// FixtureBreakdown reports one row per (size_class, format): never per part
// (stream:doc-extractor:output carries exactly one terminal event per
// document regardless of how many parts it fanned out into).
type FixtureBreakdown struct {
	Format                string   `json:"format"`
	SizeClass             string   `json:"size_class"`
	Accepted              int      `json:"accepted"`
	Success               int      `json:"success"`
	Failed                int      `json:"failed"`
	Unresolved            int      `json:"unresolved"`
	SuccessBytesTotal     int64    `json:"success_bytes_total"`
	SuccessMiB            float64  `json:"success_mib"`
	AuthoritativeE2EMsP50 *float64 `json:"authoritative_e2e_ms_p50,omitempty"`
	AuthoritativeE2EMsP95 *float64 `json:"authoritative_e2e_ms_p95,omitempty"`
	ClientE2EMsP50        *float64 `json:"client_e2e_ms_p50,omitempty"`
	ClientE2EMsP95        *float64 `json:"client_e2e_ms_p95,omitempty"`
}

type ThroughputWindow struct {
	WindowStartMs int64   `json:"window_start_ms"`
	WindowStart   string  `json:"window_start"`
	SuccessCount  int     `json:"success_count"`
	SuccessMiB    float64 `json:"success_mib"`
	DocsPerMin    float64 `json:"docs_per_min"`
	MiBPerMin     float64 `json:"mib_per_min"`
}

type UnresolvedRequest struct {
	RequestID  string  `json:"request_id"`
	FixtureID  string  `json:"fixture_id"`
	AgeSeconds float64 `json:"age_seconds"`
}

type DrainReport struct {
	RunID                 string               `json:"run_id"`
	GeneratedAt           string               `json:"generated_at"`
	DeadlineExceeded      bool                 `json:"deadline_exceeded"`
	TotalIntended         int                  `json:"total_intended"`
	TotalAccepted         int                  `json:"total_accepted"`
	TotalNotConfirmed     int                  `json:"total_not_confirmed_on_input_stream"`
	TotalSuccess          int                  `json:"total_success"`
	TotalFailed           int                  `json:"total_failed"`
	TotalUnresolved       int                  `json:"total_unresolved"`
	AcceptedEquationHolds bool                 `json:"accepted_equation_holds"`
	DuplicateTerminals    []DuplicateTerminal  `json:"duplicate_terminal_requests"`
	InputDecodeErrors     int                  `json:"input_stream_decode_errors"`
	OutputDecodeErrors    int                  `json:"output_stream_decode_errors"`
	AdmissionStopEvents   []admissionStopEvent `json:"admission_stop_events"`
	Breakdown             []FixtureBreakdown   `json:"breakdown_by_size_class_format"`
	ThroughputWindows     []ThroughputWindow   `json:"throughput_windows"`
	UnresolvedDetail      []UnresolvedRequest  `json:"unresolved_detail"`
	ParseWarnings         []string             `json:"parse_warnings,omitempty"`
	K6RunStart            *k6LogLine           `json:"k6_run_start,omitempty"`
	Notes                 []string             `json:"notes"`
}

func buildReport(in reportInput) DrainReport {
	report := DrainReport{
		RunID:               in.runID,
		GeneratedAt:         time.Now().UTC().Format(time.RFC3339),
		DeadlineExceeded:    in.deadlineExceeded,
		DuplicateTerminals:  in.duplicates,
		InputDecodeErrors:   in.inputDecodeErrors,
		OutputDecodeErrors:  in.outputDecodeErrors,
		AdmissionStopEvents: in.admissionStops,
		ParseWarnings:       in.parseWarnings,
		K6RunStart:          in.runStartLine,
	}

	type cellKey struct{ sizeClass, format string }
	cells := map[cellKey]*FixtureBreakdown{}
	var authE2EByCell = map[cellKey][]float64{}
	var clientE2EByCell = map[cellKey][]float64{}

	nowMs := time.Now().UnixMilli()
	for _, r := range in.requests {
		if r.SawIntent {
			report.TotalIntended++
		}
		if !r.AcceptedFinal {
			if r.SawIntent {
				report.TotalNotConfirmed++
			}
			continue
		}
		report.TotalAccepted++

		fixture, known := in.fixtureIndex[r.FixtureID]
		sizeClass, format := "unknown", "unknown"
		if known {
			sizeClass, format = fixture.SizeClass, fixture.Format
		} else {
			report.Notes = append(report.Notes, fmt.Sprintf("request_id=%s references fixture_id=%q not found in the manifest; counted under size_class/format=unknown", r.RequestID, r.FixtureID))
		}
		key := cellKey{sizeClass, format}
		cell, ok := cells[key]
		if !ok {
			cell = &FixtureBreakdown{SizeClass: sizeClass, Format: format}
			cells[key] = cell
		}
		cell.Accepted++

		switch r.FinalClass {
		case "SUCCESS":
			cell.Success++
			report.TotalSuccess++
			if known && fixture.Bytes != nil {
				cell.SuccessBytesTotal += *fixture.Bytes
			}
			if r.InputStreamID != "" && r.TerminalStreamID != "" {
				e2e := float64(streamIDMillis(r.TerminalStreamID) - streamIDMillis(r.InputStreamID))
				authE2EByCell[key] = append(authE2EByCell[key], e2e)
			}
			if r.ClientE2EMs != nil {
				clientE2EByCell[key] = append(clientE2EByCell[key], float64(*r.ClientE2EMs))
			}
		case "FAILED":
			cell.Failed++
			report.TotalFailed++
		case "UNRESOLVED":
			cell.Unresolved++
			report.TotalUnresolved++
			ageSeconds := float64(nowMs-r.IntentMs) / 1000.0
			report.UnresolvedDetail = append(report.UnresolvedDetail, UnresolvedRequest{
				RequestID: r.RequestID, FixtureID: r.FixtureID, AgeSeconds: ageSeconds,
			})
		}
	}

	for key, cell := range cells {
		cell.SuccessMiB = float64(cell.SuccessBytesTotal) / (1024 * 1024)
		if samples := authE2EByCell[key]; len(samples) > 0 {
			sort.Float64s(samples)
			p50, p95 := percentile(samples, 50), percentile(samples, 95)
			cell.AuthoritativeE2EMsP50, cell.AuthoritativeE2EMsP95 = &p50, &p95
		}
		if samples := clientE2EByCell[key]; len(samples) > 0 {
			sort.Float64s(samples)
			p50, p95 := percentile(samples, 50), percentile(samples, 95)
			cell.ClientE2EMsP50, cell.ClientE2EMsP95 = &p50, &p95
		}
		report.Breakdown = append(report.Breakdown, *cell)
	}
	sort.Slice(report.Breakdown, func(i, j int) bool {
		if report.Breakdown[i].SizeClass != report.Breakdown[j].SizeClass {
			return report.Breakdown[i].SizeClass < report.Breakdown[j].SizeClass
		}
		return report.Breakdown[i].Format < report.Breakdown[j].Format
	})

	report.ThroughputWindows = buildThroughputWindows(in.requests, in.fixtureIndex, in.window)
	report.AcceptedEquationHolds = report.TotalAccepted == report.TotalSuccess+report.TotalFailed+report.TotalUnresolved

	report.Notes = append(report.Notes,
		"authoritative_e2e_ms_* uses the output terminal stream ID timestamp minus the input stream ID timestamp (both Redis-side, millisecond resolution). client_e2e_ms_* is k6's own poll-observed duration and includes client/network/poll-interval overhead on top of the authoritative figure.",
		"throughput_windows bucket SUCCESS completions by the OUTPUT stream's timestamp (completion-time window), not by k6's poll time and not by the arrival/stage the request was originally published in -- a request published near the end of one stage can complete inside the next stage's window.",
		"Report success latency only next to this cell's failed/unresolved counts in the same row; a clean p95 computed in isolation is survivorship-biased when failures/timeouts are non-trivial.",
		"Each row is one document (one terminal output event), never one row per part, regardless of how many parts a large document fanned out into.",
	)
	if !report.AcceptedEquationHolds {
		report.Notes = append(report.Notes, "WARNING: accepted != success+failed+unresolved -- investigate before trusting this report (should hold by construction).")
	}
	if report.DeadlineExceeded {
		report.Notes = append(report.Notes, "Drain deadline was reached with requests still unresolved: see unresolved_detail. These are NOT failures -- the backend may complete them later; re-run with a longer -deadline or check worker health/logs directly.")
	}
	if len(report.DuplicateTerminals) > 0 {
		report.Notes = append(report.Notes, "Duplicate terminal events were found for one or more request_ids (should never happen under the Lua scripts' single-writer semantics) -- this indicates a bug and was NOT silently deduplicated away; see duplicate_terminal_requests.")
	}

	return report
}

func buildThroughputWindows(requests map[string]*requestRecord, fixtureIndex map[string]*manifest.Fixture, window time.Duration) []ThroughputWindow {
	windowMs := window.Milliseconds()
	if windowMs <= 0 {
		windowMs = 60000
	}
	type agg struct {
		count int
		bytes int64
	}
	byBucket := map[int64]*agg{}
	for _, r := range requests {
		if r.FinalClass != "SUCCESS" || r.TerminalStreamID == "" {
			continue
		}
		ms := streamIDMillis(r.TerminalStreamID)
		bucket := (ms / windowMs) * windowMs
		a, ok := byBucket[bucket]
		if !ok {
			a = &agg{}
			byBucket[bucket] = a
		}
		a.count++
		if f, known := fixtureIndex[r.FixtureID]; known && f.Bytes != nil {
			a.bytes += *f.Bytes
		}
	}

	buckets := make([]int64, 0, len(byBucket))
	for b := range byBucket {
		buckets = append(buckets, b)
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i] < buckets[j] })

	windowMinutes := window.Minutes()
	windows := make([]ThroughputWindow, 0, len(buckets))
	for _, b := range buckets {
		a := byBucket[b]
		mib := float64(a.bytes) / (1024 * 1024)
		windows = append(windows, ThroughputWindow{
			WindowStartMs: b,
			WindowStart:   time.UnixMilli(b).UTC().Format(time.RFC3339),
			SuccessCount:  a.count,
			SuccessMiB:    mib,
			DocsPerMin:    float64(a.count) / windowMinutes,
			MiBPerMin:     mib / windowMinutes,
		})
	}
	return windows
}

func renderMarkdown(r DrainReport) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Drain & reconciliation report: %s\n\n", r.RunID)
	fmt.Fprintf(&b, "Generated: %s. Deadline exceeded: %v.\n\n", r.GeneratedAt, r.DeadlineExceeded)
	fmt.Fprintf(&b, "RAM/CPU/OOM/replica-count columns are NOT produced by this tool -- merge them in manually from "+
		"`loadtest/tools/resources` output and the run's env config before using this as the final results table "+
		"in docs/load-test-plan-event-sourcing.md section 8.\n\n")

	fmt.Fprintf(&b, "## Totals\n\n")
	fmt.Fprintf(&b, "| intended | accepted | not confirmed on input stream | SUCCESS | FAILED | unresolved | accepted == success+failed+unresolved |\n")
	fmt.Fprintf(&b, "|---:|---:|---:|---:|---:|---:|:---:|\n")
	fmt.Fprintf(&b, "| %d | %d | %d | %d | %d | %d | %v |\n\n", r.TotalIntended, r.TotalAccepted, r.TotalNotConfirmed, r.TotalSuccess, r.TotalFailed, r.TotalUnresolved, r.AcceptedEquationHolds)

	fmt.Fprintf(&b, "## Breakdown by size_class x format\n\n")
	fmt.Fprintf(&b, "| size_class | format | accepted | SUCCESS | FAILED | unresolved | SUCCESS MiB | E2E p50/p95 (authoritative, ms) | E2E p50/p95 (client-observed, ms) |\n")
	fmt.Fprintf(&b, "|---|---|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, c := range r.Breakdown {
		fmt.Fprintf(&b, "| %s | %s | %d | %d | %d | %d | %.2f | %s | %s |\n",
			c.SizeClass, c.Format, c.Accepted, c.Success, c.Failed, c.Unresolved, c.SuccessMiB,
			p50p95(c.AuthoritativeE2EMsP50, c.AuthoritativeE2EMsP95), p50p95(c.ClientE2EMsP50, c.ClientE2EMsP95))
	}
	b.WriteString("\n")

	fmt.Fprintf(&b, "## Throughput windows (completion-time, SUCCESS only)\n\n")
	fmt.Fprintf(&b, "| window start (UTC) | SUCCESS count | SUCCESS MiB | docs/min | MiB/min |\n")
	fmt.Fprintf(&b, "|---|---:|---:|---:|---:|\n")
	for _, w := range r.ThroughputWindows {
		fmt.Fprintf(&b, "| %s | %d | %.2f | %.2f | %.2f |\n", w.WindowStart, w.SuccessCount, w.SuccessMiB, w.DocsPerMin, w.MiBPerMin)
	}
	b.WriteString("\n")

	if len(r.UnresolvedDetail) > 0 {
		fmt.Fprintf(&b, "## Unresolved requests\n\n")
		fmt.Fprintf(&b, "| request_id | fixture_id | age (s) |\n|---|---|---:|\n")
		for _, u := range r.UnresolvedDetail {
			fmt.Fprintf(&b, "| %s | %s | %.1f |\n", u.RequestID, u.FixtureID, u.AgeSeconds)
		}
		b.WriteString("\n")
	}

	if len(r.DuplicateTerminals) > 0 {
		fmt.Fprintf(&b, "## Duplicate terminal events (bug signal)\n\n")
		fmt.Fprintf(&b, "| request_id | stream_ids | statuses |\n|---|---|---|\n")
		for _, d := range r.DuplicateTerminals {
			fmt.Fprintf(&b, "| %s | %s | %s |\n", d.RequestID, strings.Join(d.StreamIDs, ", "), strings.Join(d.Statuses, ", "))
		}
		b.WriteString("\n")
	}

	if len(r.AdmissionStopEvents) > 0 {
		fmt.Fprintf(&b, "## Admission-stop events\n\n")
		fmt.Fprintf(&b, "Exclude time after the FIRST stop event below from any \"arrival matched target\" claim.\n\n")
		fmt.Fprintf(&b, "| timestamp | iteration | reason |\n|---|---:|---|\n")
		for _, s := range r.AdmissionStopEvents {
			fmt.Fprintf(&b, "| %s | %d | %s |\n", s.Timestamp, s.Iteration, s.Reason)
		}
		b.WriteString("\n")
	}

	if len(r.ParseWarnings) > 0 {
		fmt.Fprintf(&b, "## Parse warnings\n\n")
		for _, w := range r.ParseWarnings {
			fmt.Fprintf(&b, "- %s\n", w)
		}
		b.WriteString("\n")
	}

	fmt.Fprintf(&b, "## Notes\n\n")
	for _, n := range r.Notes {
		fmt.Fprintf(&b, "- %s\n", n)
	}
	return b.String()
}

func p50p95(p50, p95 *float64) string {
	if p50 == nil || p95 == nil {
		return "n/a"
	}
	return fmt.Sprintf("%.0f / %.0f", *p50, *p95)
}
