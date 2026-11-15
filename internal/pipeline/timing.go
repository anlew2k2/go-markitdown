package pipeline

import (
	"encoding/json"
	"log"
	"strconv"
	"strings"
	"time"
)

// Phase names used by structured timing logs.
//
// PhaseSplit covers split+per-part-upload as one figure, not two: splitter.go
// never separates them further because services.SplitAndUpload interleaves
// encoding and uploading each part (see internal/services/splitter.go) —
// doing so would require restructuring every format splitter into a
// produce/upload pipeline, which is out of scope here (see
// docs/throughput-source-review.md item 7). PhaseConvert and PhaseUpload are
// extractor-only and genuinely separate. PhaseTotal spans the whole handle()
// call (download..ack), excluding PhaseQueueWait.
const (
	PhaseQueueWait = "queue_wait"
	PhaseDownload  = "download"
	PhaseSplit     = "split_upload"
	PhaseConvert   = "convert"
	PhaseUpload    = "upload"
	PhaseTotal     = "total"
)

// Outcome values for structured timing logs. OutcomeIncomplete marks a job
// left pending for retry or shutdown recovery — neither a business success
// nor a business failure — so it is never confused with either in offline
// analysis.
const (
	OutcomeSuccess    = "success"
	OutcomeFailure    = "failure"
	OutcomeIncomplete = "incomplete"
)

// Stage names.
const (
	StageSplitter  = "splitter"
	StageExtractor = "extractor"
)

// timingEntry is one structured timing log line, emitted as "TIMING
// <json>". Fields are deliberately limited to identifiers, stage/phase
// labels, durations and outcomes: never a file_path, signed URL or document
// content, per the logging requirement this type exists to satisfy.
type timingEntry struct {
	Stage           string `json:"stage"`
	Consumer        string `json:"consumer"`
	RequestID       string `json:"request_id"`
	Offset          *int   `json:"offset,omitempty"`
	Phase           string `json:"phase"`
	DurationMS      int64  `json:"duration_ms,omitempty"`
	DurationUnknown bool   `json:"duration_unknown,omitempty"`
	Outcome         string `json:"outcome"`
	Error           string `json:"error,omitempty"`
	Note            string `json:"note,omitempty"`
}

func emitTiming(e timingEntry) {
	data, err := json.Marshal(e)
	if err != nil {
		log.Printf("pipeline: TIMING marshal error: %v", err)
		return
	}
	log.Printf("TIMING %s", data)
}

const errMsgLimit = 500

func truncateErr(err error) string {
	msg := err.Error()
	if len(msg) > errMsgLimit {
		return msg[:errMsgLimit]
	}
	return msg
}

// logQueueWait records how long msgID sat in its stream before this
// consumer picked it up, derived from the millisecond timestamp Redis
// embeds in every stream ID (see queueWait). If that can't be parsed,
// DurationUnknown is set instead of silently reporting zero.
func logQueueWait(stage, consumer, requestID string, offset *int, msgID string) {
	e := timingEntry{Stage: stage, Consumer: consumer, RequestID: requestID, Offset: offset, Phase: PhaseQueueWait, Outcome: OutcomeSuccess}
	if qw, ok := queueWait(msgID); ok {
		e.DurationMS = qw.Milliseconds()
	} else {
		e.DurationUnknown = true
		e.Note = "stream id timestamp unparseable; queue_wait unknown, not reported as zero"
	}
	emitTiming(e)
}

// logPhase records one sub-step (download/split_upload/convert/upload) of a
// job, keyed by whether it errored.
func logPhase(stage, consumer, requestID string, offset *int, phase string, dur time.Duration, err error) {
	e := timingEntry{Stage: stage, Consumer: consumer, RequestID: requestID, Offset: offset, Phase: phase, DurationMS: dur.Milliseconds(), Outcome: OutcomeSuccess}
	if err != nil {
		e.Outcome = OutcomeFailure
		e.Error = truncateErr(err)
	}
	emitTiming(e)
}

// logJob records the whole-job (PhaseTotal) figure for one handle() call,
// with an explicit outcome (success/failure/incomplete) chosen by the
// caller rather than inferred from an error value, since an "incomplete"
// job (shutdown, infra retry) is not itself an error the operator should
// read as a business failure.
func logJob(stage, consumer, requestID string, offset *int, outcome string, dur time.Duration, note string) {
	emitTiming(timingEntry{Stage: stage, Consumer: consumer, RequestID: requestID, Offset: offset, Phase: PhaseTotal, DurationMS: dur.Milliseconds(), Outcome: outcome, Note: note})
}

// streamIDTimestamp extracts the millisecond timestamp Redis assigned a
// stream entry (the part of a "<ms>-<seq>" ID before the "-").
func streamIDTimestamp(id string) (time.Time, bool) {
	msPart := id
	if i := strings.IndexByte(id, '-'); i >= 0 {
		msPart = id[:i]
	}
	ms, err := strconv.ParseInt(msPart, 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	return time.UnixMilli(ms), true
}

// queueWait reports how long a message with the given stream ID sat between
// being added to its stream and being picked up now.
//
// This reuses the Redis-assigned ID timestamp rather than a separately
// stamped enqueue field: it is accurate to Redis's own clock and the
// millisecond resolution of stream IDs, but assumes the Redis server clock
// and this worker's clock are reasonably in sync (true in every environment
// this pipeline currently runs in: one Docker host/VM) and does not account
// for any queuing on the Redis side before the ID was assigned. If id can't
// be parsed, ok is false — callers must not substitute zero.
func queueWait(id string) (time.Duration, bool) {
	t, ok := streamIDTimestamp(id)
	if !ok {
		return 0, false
	}
	return time.Since(t), true
}
