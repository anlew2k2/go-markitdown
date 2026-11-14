package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/go-markitdown/internal/pipeline"
	"github.com/go-markitdown/loadtest/manifest"
)

// k6LogLine is a permissive envelope over every structured JSON line
// load_test.js prints (INTENT / ACCEPTED / PUBLISH_UNKNOWN / OUTCOME /
// ADMISSION_STOPPED / RUN_START / RUN_END). Unused fields for a given event
// type are simply left zero-valued; encoding/json ignores fields the line
// doesn't have.
type k6LogLine struct {
	Event       string `json:"event"`
	RunID       string `json:"run_id"`
	Timestamp   string `json:"timestamp"`
	RequestID   string `json:"request_id"`
	FixtureID   string `json:"fixture_id"`
	StreamID    string `json:"stream_id"`
	Outcome     string `json:"outcome"`
	Reason      string `json:"reason"`
	Iteration   int64  `json:"iteration"`
	IntentTime  int64  `json:"intent_time_ms"`
	PublishTime int64  `json:"publish_duration_ms"`
	Dataset     *struct {
		Format    string `json:"format"`
		SizeClass string `json:"size_class"`
		Bytes     int64  `json:"bytes"`
	} `json:"dataset"`
	Polling *struct {
		PollCount          int    `json:"poll_count"`
		ReadErrors         int    `json:"read_errors"`
		LastObservedStatus string `json:"last_observed_status"`
		E2EDurationMs      *int64 `json:"e2e_duration_ms"`
	} `json:"polling"`
}

// requestRecord accumulates everything known about one request_id across
// the k6 log and, after reconciliation, the input/output streams
// themselves.
type requestRecord struct {
	RequestID  string
	FixtureID  string
	IntentMs   int64
	SawIntent  bool
	SawAccept  bool
	SawUnknown bool

	ClientOutcome string // COMPLETED | FAILED | OBSERVATION_TIMEOUT (from k6's own poll, informational only)
	ClientE2EMs   *int64

	// Filled by reconcileAgainstInputStream. The input stream, not k6's own
	// ACCEPTED/PUBLISH_UNKNOWN log line, is authoritative for whether Redis
	// actually received the request (see docs/load-test-plan-event-sourcing.md
	// section 5).
	InputStreamID  string
	AcceptedFinal  bool
	AcceptedSource string // "input_stream" | "not_found_on_input_stream"

	// Filled by classifyTerminals.
	TerminalStatus   string // "" | SUCCESS | FAILED
	TerminalStreamID string
	FinalClass       string // SUCCESS | FAILED | UNRESOLVED (only set for AcceptedFinal requests)
}

func runDrain(args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	addr := fs.String("redis-addr", "127.0.0.1:6379", "Redis host:port")
	password := fs.String("redis-password", "", "Redis password (never logged)")
	db := fs.Int("redis-db", 0, "Redis DB index")
	runID := fs.String("run-id", "", "run id to reconcile (must match the RUN_ID passed to k6 and to `drain record`)")
	startFile := fs.String("start-file", "", "path to the snapshot written by `drain record`")
	manifestPath := fs.String("manifest", "k6/fixtures/manifest.example.json", "path to the fixture manifest JSON")
	k6Log := fs.String("k6-log", "", "path to k6's captured stdout (JSON lines; non-JSON lines are skipped)")
	deadline := fs.Duration("deadline", 20*time.Minute, "how long to keep polling the output stream for unresolved accepted requests before giving up")
	pollInterval := fs.Duration("poll-interval", 5*time.Second, "delay between output-stream poll batches")
	window := fs.Duration("window", 60*time.Second, "completion-time window size for the throughput series")
	out := fs.String("out", "", "output JSON report path")
	outMd := fs.String("out-md", "", "optional output Markdown report path")
	fs.Parse(args)

	if *runID == "" || *startFile == "" || *k6Log == "" || *out == "" {
		fatal("run: -run-id, -start-file, -k6-log and -out are required")
	}

	snap, err := loadSnapshot(*startFile)
	if err != nil {
		fatal("load start-file: %v", err)
	}
	if snap.RunID != "" && snap.RunID != *runID {
		fmt.Fprintf(os.Stderr, "drain run: WARNING start-file run_id=%q does not match -run-id=%q; proceeding with -run-id\n", snap.RunID, *runID)
	}

	man, err := manifest.Load(*manifestPath)
	if err != nil {
		fatal("load manifest: %v", err)
	}
	fixtureIndex := map[string]*manifest.Fixture{}
	for i := range man.Fixtures {
		fixtureIndex[man.Fixtures[i].ID] = &man.Fixtures[i]
	}

	requests, admissionStops, runStartLine, parseWarnings, err := parseK6Log(*k6Log, *runID)
	if err != nil {
		fatal("parse k6 log: %v", err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: *addr, Password: *password, DB: *db})
	defer rdb.Close()
	ctx := context.Background()

	// Reconcile acceptance against the input stream itself: the input
	// stream is ground truth for "did Redis receive this request", which is
	// what a lost XADD reply (PUBLISH_UNKNOWN) cannot tell us on its own.
	inputIndex, inputDecodeErrors, err := scanRequestIDIndex(ctx, rdb, pipeline.StreamInput, snap.InputStartID, decodeInputRequestID)
	if err != nil {
		fatal("scan input stream: %v", err)
	}
	for _, r := range requests {
		if id, ok := inputIndex[r.RequestID]; ok {
			r.InputStreamID = id
			r.AcceptedFinal = true
			r.AcceptedSource = "input_stream"
		} else {
			r.AcceptedSource = "not_found_on_input_stream"
		}
	}

	// Poll the output stream, independent of k6's own deadline, until every
	// accepted request has a terminal event or our own -deadline elapses.
	// Duplicate terminals are detected BEFORE any dedup/classification.
	deadlineAt := time.Now().Add(*deadline)
	lastSeenOutputID := snap.OutputStartID
	terminalsByRequest := map[string][]outputTerminal{}
	outputDecodeErrors := 0
	deadlineExceeded := false

	for {
		var batch []redis.XMessage
		if err := drainAvailable(ctx, rdb, pipeline.StreamOutput, &lastSeenOutputID, &batch); err != nil {
			fatal("read output stream: %v", err)
		}
		for _, msg := range batch {
			term, ok := decodeOutputTerminal(msg)
			if !ok {
				outputDecodeErrors++
				continue
			}
			terminalsByRequest[term.RequestID] = append(terminalsByRequest[term.RequestID], term)
		}

		if countUnresolved(requests, terminalsByRequest) == 0 {
			break
		}
		if time.Now().After(deadlineAt) {
			deadlineExceeded = true
			break
		}
		time.Sleep(*pollInterval)
	}

	duplicates := classifyTerminals(requests, terminalsByRequest)

	report := buildReport(reportInput{
		runID:              *runID,
		requests:           requests,
		admissionStops:     admissionStops,
		runStartLine:       runStartLine,
		fixtureIndex:       fixtureIndex,
		duplicates:         duplicates,
		inputDecodeErrors:  inputDecodeErrors,
		outputDecodeErrors: outputDecodeErrors,
		deadlineExceeded:   deadlineExceeded,
		window:             *window,
		parseWarnings:      parseWarnings,
	})

	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		fatal("marshal report: %v", err)
	}
	if err := os.WriteFile(*out, append(data, '\n'), 0o644); err != nil {
		fatal("write report: %v", err)
	}
	fmt.Printf("drain run: wrote %s\n", *out)

	if *outMd != "" {
		if err := os.WriteFile(*outMd, []byte(renderMarkdown(report)), 0o644); err != nil {
			fatal("write markdown report: %v", err)
		}
		fmt.Printf("drain run: wrote %s\n", *outMd)
	}

	fmt.Printf(
		"accepted=%d success=%d failed=%d unresolved=%d duplicate_terminal_requests=%d deadline_exceeded=%v\n",
		report.TotalAccepted, report.TotalSuccess, report.TotalFailed, report.TotalUnresolved, len(report.DuplicateTerminals), deadlineExceeded,
	)
}

func loadSnapshot(path string) (StreamSnapshot, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return StreamSnapshot{}, err
	}
	var s StreamSnapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return StreamSnapshot{}, err
	}
	return s, nil
}

// parseK6Log reads k6's captured stdout line by line. Lines that are not
// valid JSON (k6's own banners/progress output) or that belong to a
// different run_id are skipped, not treated as errors.
func parseK6Log(path, runID string) (map[string]*requestRecord, []admissionStopEvent, *k6LogLine, []string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	defer f.Close()

	requests := map[string]*requestRecord{}
	var admissionStops []admissionStopEvent
	var runStartLine *k6LogLine
	var warnings []string

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || line[0] != '{' {
			continue
		}
		var rec k6LogLine
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			continue
		}
		if rec.RunID != runID {
			continue
		}

		switch rec.Event {
		case "RUN_START":
			cp := rec
			runStartLine = &cp
		case "ADMISSION_STOPPED":
			admissionStops = append(admissionStops, admissionStopEvent{Timestamp: rec.Timestamp, Iteration: rec.Iteration, Reason: rec.Reason})
		case "INTENT":
			r := getOrCreate(requests, rec.RequestID)
			r.SawIntent = true
			r.IntentMs = rec.IntentTime
			r.FixtureID = rec.FixtureID
		case "ACCEPTED":
			r := getOrCreate(requests, rec.RequestID)
			r.SawAccept = true
			if r.FixtureID == "" {
				r.FixtureID = rec.FixtureID
			}
		case "PUBLISH_UNKNOWN":
			r := getOrCreate(requests, rec.RequestID)
			r.SawUnknown = true
			if r.FixtureID == "" {
				r.FixtureID = rec.FixtureID
			}
		case "OUTCOME":
			r := getOrCreate(requests, rec.RequestID)
			r.ClientOutcome = rec.Outcome
			if rec.Polling != nil {
				r.ClientE2EMs = rec.Polling.E2EDurationMs
			}
			if r.FixtureID == "" {
				r.FixtureID = rec.FixtureID
			}
		}
	}
	if err := scanner.Err(); err != nil {
		warnings = append(warnings, fmt.Sprintf("k6 log scan stopped early at line %d: %v", lineNo, err))
	}

	// Every request_id must have an INTENT line; one without it (e.g. an
	// ACCEPTED/OUTCOME line surviving from a truncated log) cannot be
	// trusted for intent_time_ms and is reported, not silently dropped.
	for id, r := range requests {
		if !r.SawIntent {
			warnings = append(warnings, fmt.Sprintf("request_id=%s has log lines but no INTENT record (truncated log?)", id))
		}
	}

	return requests, admissionStops, runStartLine, warnings, nil
}

func getOrCreate(m map[string]*requestRecord, id string) *requestRecord {
	r, ok := m[id]
	if !ok {
		r = &requestRecord{RequestID: id}
		m[id] = r
	}
	return r
}

type admissionStopEvent struct {
	Timestamp string `json:"timestamp"`
	Iteration int64  `json:"iteration"`
	Reason    string `json:"reason"`
}

type outputTerminal struct {
	StreamID    string
	RequestID   string
	Status      string // SUCCESS | FAILED
	Total       int
	TimestampMs int64
}

// decodeInputRequestID extracts the request_id from one input-stream entry.
func decodeInputRequestID(msg redis.XMessage) (string, bool) {
	data, _ := msg.Values["data"].(string)
	ev, err := pipeline.ParseInputEvent(data)
	if err != nil {
		return "", false
	}
	return ev.RequestID, true
}

// decodeOutputTerminal extracts the terminal status payload from one
// output-stream entry, reusing pipeline.OutputEvent so the field names can
// never drift from the authoritative contract in internal/pipeline/events.go.
func decodeOutputTerminal(msg redis.XMessage) (outputTerminal, bool) {
	data, _ := msg.Values["data"].(string)
	var ev pipeline.OutputEvent
	if err := json.Unmarshal([]byte(data), &ev); err != nil {
		return outputTerminal{}, false
	}
	if ev.RequestID == "" || (ev.Status != pipeline.OutputStatusSuccess && ev.Status != pipeline.OutputStatusFailed) {
		return outputTerminal{}, false
	}
	return outputTerminal{
		StreamID:    msg.ID,
		RequestID:   ev.RequestID,
		Status:      ev.Status,
		Total:       ev.Total,
		TimestampMs: streamIDMillis(msg.ID),
	}, true
}

// scanRequestIDIndex fully drains `stream` from (exclusive) startID to the
// current tail once, decoding each entry with decode, and returns a
// request_id -> stream_id index plus a count of entries that failed to
// decode (malformed payloads some other producer wrote are reported, not
// silently ignored).
func scanRequestIDIndex(ctx context.Context, rdb *redis.Client, stream, startID string, decode func(redis.XMessage) (string, bool)) (map[string]string, int, error) {
	var all []redis.XMessage
	if err := drainAvailable(ctx, rdb, stream, &startID, &all); err != nil {
		return nil, 0, err
	}
	index := map[string]string{}
	decodeErrors := 0
	for _, msg := range all {
		id, ok := decode(msg)
		if !ok {
			decodeErrors++
			continue
		}
		if _, exists := index[id]; !exists {
			index[id] = msg.ID
		}
	}
	return index, decodeErrors, nil
}

// drainAvailable repeatedly XRANGEs stream from (exclusive) *lastSeenID
// forward, appending every entry to *sink and advancing *lastSeenID, until a
// batch comes back smaller than the batch size (i.e. caught up to the
// current tail). It does not block/wait for new entries: callers that need
// to keep polling call this again after a delay.
func drainAvailable(ctx context.Context, rdb *redis.Client, stream string, lastSeenID *string, sink *[]redis.XMessage) error {
	const batchSize = 500
	for {
		batch, err := rdb.XRangeN(ctx, stream, "("+*lastSeenID, "+", batchSize).Result()
		if err != nil && err != redis.Nil {
			return err
		}
		if len(batch) == 0 {
			return nil
		}
		*sink = append(*sink, batch...)
		*lastSeenID = batch[len(batch)-1].ID
		if len(batch) < batchSize {
			return nil
		}
	}
}

// streamIDMillis parses the millisecond-timestamp component of a Redis
// stream ID ("<ms>-<seq>"). Returns 0 if the ID cannot be parsed (should not
// happen for a real Redis-generated ID).
func streamIDMillis(id string) int64 {
	idx := strings.IndexByte(id, '-')
	if idx < 0 {
		return 0
	}
	ms, err := strconv.ParseInt(id[:idx], 10, 64)
	if err != nil {
		return 0
	}
	return ms
}

// countUnresolved reports how many requests that are (currently believed)
// accepted still have no terminal event, so the polling loop knows when it
// can stop early instead of always running the full -deadline.
func countUnresolved(requests map[string]*requestRecord, terminalsByRequest map[string][]outputTerminal) int {
	unresolved := 0
	for _, r := range requests {
		if !r.AcceptedFinal {
			continue
		}
		if len(terminalsByRequest[r.RequestID]) == 0 {
			unresolved++
		}
	}
	return unresolved
}

// classifyTerminals assigns each accepted request its final SUCCESS/FAILED/
// UNRESOLVED classification. Duplicate terminal events for the same
// request_id (which should never happen given the Lua scripts' single-writer
// semantics, but is checked rather than assumed) are reported BEFORE the
// canonical one (lowest stream ID, i.e. the actual winner under Redis'
// single-threaded script execution) is picked.
func classifyTerminals(requests map[string]*requestRecord, terminalsByRequest map[string][]outputTerminal) []DuplicateTerminal {
	var duplicates []DuplicateTerminal
	for id, terms := range terminalsByRequest {
		if len(terms) > 1 {
			sort.Slice(terms, func(i, j int) bool { return terms[i].StreamID < terms[j].StreamID })
			var statuses []string
			var ids []string
			for _, t := range terms {
				statuses = append(statuses, t.Status)
				ids = append(ids, t.StreamID)
			}
			duplicates = append(duplicates, DuplicateTerminal{RequestID: id, StreamIDs: ids, Statuses: statuses})
		}
	}

	for _, r := range requests {
		if !r.AcceptedFinal {
			continue
		}
		terms := terminalsByRequest[r.RequestID]
		if len(terms) == 0 {
			r.FinalClass = "UNRESOLVED"
			continue
		}
		sort.Slice(terms, func(i, j int) bool { return terms[i].StreamID < terms[j].StreamID })
		canonical := terms[0]
		r.TerminalStatus = canonical.Status
		r.TerminalStreamID = canonical.StreamID
		if canonical.Status == pipeline.OutputStatusSuccess {
			r.FinalClass = "SUCCESS"
		} else {
			r.FinalClass = "FAILED"
		}
	}
	return duplicates
}

type DuplicateTerminal struct {
	RequestID string   `json:"request_id"`
	StreamIDs []string `json:"stream_ids"`
	Statuses  []string `json:"statuses"`
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
