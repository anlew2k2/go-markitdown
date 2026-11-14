// Command drain is the post-run drain & reconciliation tool for the k6
// event-driven-pipeline load test. It is a support tool only: this change
// does not execute it, and it must never be run against shared Redis/MinIO
// state without the operator's explicit intent.
//
// It exists because k6's gracefulStop only waits for VUs that are still
// polling when arrivals stop; once a VU's own MAX_OBSERVATION_SECONDS
// deadline passes it stops watching even though the backend request may
// still be PROCESSING. gracefulStop ending is not evidence the backend has
// drained (see docs/load-test-review-event-sourcing.md P1). This tool keeps
// reading stream:doc-extractor:output on its own schedule, independent of
// k6, until every accepted request reaches a terminal state or its own
// -deadline elapses.
//
// Usage (two phases around one k6 invocation):
//
//	# before starting k6:
//	go run ./loadtest/tools/drain record -redis-addr localhost:16379 -run-id "$RUN_ID" \
//	    -out docs/test-results/event-sourcing/<run>/manifests/stream-start.json
//
//	# after k6's gracefulStop window has elapsed:
//	go run ./loadtest/tools/drain run -redis-addr localhost:16379 -run-id "$RUN_ID" \
//	    -start-file docs/test-results/event-sourcing/<run>/manifests/stream-start.json \
//	    -manifest k6/fixtures/manifest.example.json \
//	    -k6-log docs/test-results/event-sourcing/<run>/logs/k6-stdout.jsonl \
//	    -deadline 20m \
//	    -out docs/test-results/event-sourcing/<run>/manifests/drain-report.json \
//	    -out-md docs/test-results/event-sourcing/<run>/manifests/drain-report.md
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/go-markitdown/internal/pipeline"
)

// StreamSnapshot records the last stream ID of each of the three pipeline
// streams at a point in time, so a later `run` invocation knows exactly
// where this benchmark run's traffic begins (never relying on `$`, which
// cannot be replayed, or on 0-0 once the streams already carry older runs'
// entries).
type StreamSnapshot struct {
	RunID           string `json:"run_id"`
	CapturedAt      string `json:"captured_at"`
	InputStream     string `json:"input_stream"`
	InputStartID    string `json:"input_start_id"`
	SplittedStream  string `json:"splitted_stream"`
	SplittedStartID string `json:"splitted_start_id"`
	OutputStream    string `json:"output_stream"`
	OutputStartID   string `json:"output_start_id"`
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "record":
		runRecord(os.Args[2:])
	case "run":
		runDrain(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: drain record -redis-addr <host:port> -run-id <id> -out <file>")
	fmt.Fprintln(os.Stderr, "       drain run -redis-addr <host:port> -run-id <id> -start-file <file> -manifest <file> -k6-log <file> -out <file> [-out-md <file>] [-deadline 20m]")
}

func runRecord(args []string) {
	fs := flag.NewFlagSet("record", flag.ExitOnError)
	addr := fs.String("redis-addr", "127.0.0.1:6379", "Redis host:port (no credentials in this flag; use -redis-password)")
	password := fs.String("redis-password", "", "Redis password (never logged)")
	db := fs.Int("redis-db", 0, "Redis DB index")
	runID := fs.String("run-id", "", "run id to record this snapshot under (must match the RUN_ID passed to k6)")
	out := fs.String("out", "", "output JSON path for the stream snapshot")
	fs.Parse(args)

	if *runID == "" || *out == "" {
		fmt.Fprintln(os.Stderr, "drain record: -run-id and -out are required")
		os.Exit(2)
	}

	rdb := redis.NewClient(&redis.Options{Addr: *addr, Password: *password, DB: *db})
	defer rdb.Close()
	ctx := context.Background()

	snap := StreamSnapshot{
		RunID:          *runID,
		CapturedAt:     time.Now().UTC().Format(time.RFC3339),
		InputStream:    pipeline.StreamInput,
		SplittedStream: pipeline.StreamSplitted,
		OutputStream:   pipeline.StreamOutput,
	}
	var err error
	if snap.InputStartID, err = lastStreamID(ctx, rdb, pipeline.StreamInput); err != nil {
		fatal("read input stream tail: %v", err)
	}
	if snap.SplittedStartID, err = lastStreamID(ctx, rdb, pipeline.StreamSplitted); err != nil {
		fatal("read splitted stream tail: %v", err)
	}
	if snap.OutputStartID, err = lastStreamID(ctx, rdb, pipeline.StreamOutput); err != nil {
		fatal("read output stream tail: %v", err)
	}

	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		fatal("marshal snapshot: %v", err)
	}
	if err := os.WriteFile(*out, append(data, '\n'), 0o644); err != nil {
		fatal("write snapshot: %v", err)
	}
	fmt.Printf("drain record: wrote %s (input=%s splitted=%s output=%s)\n", *out, snap.InputStartID, snap.SplittedStartID, snap.OutputStartID)
}

// lastStreamID returns the ID of the most recent entry on stream, or "0-0"
// if the stream does not exist or is empty (XRANGE/XREVRANGE on a missing
// stream simply returns no entries rather than erroring).
func lastStreamID(ctx context.Context, rdb *redis.Client, stream string) (string, error) {
	msgs, err := rdb.XRevRangeN(ctx, stream, "+", "-", 1).Result()
	if err != nil && err != redis.Nil {
		return "", err
	}
	if len(msgs) == 0 {
		return "0-0", nil
	}
	return msgs[0].ID, nil
}

func fatal(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "drain: "+format+"\n", args...)
	os.Exit(1)
}
