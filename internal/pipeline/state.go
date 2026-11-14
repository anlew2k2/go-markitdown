package pipeline

import (
	"context"
	_ "embed"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed lua/init_and_publish.lua
var initAndPublishSrc string

//go:embed lua/complete_part.lua
var completePartSrc string

//go:embed lua/fail_request.lua
var failRequestSrc string

var (
	initAndPublishScript = redis.NewScript(initAndPublishSrc)
	completePartScript   = redis.NewScript(completePartSrc)
	failRequestScript    = redis.NewScript(failRequestSrc)
)

// Redis key prefixes for per-request state, per section 4.1 of the
// requirement doc.
const (
	statePrefix     = "doc-extractor:state:"
	completedPrefix = "doc-extractor:completed:"
	partsPrefix     = "doc-extractor:parts:"
)

func stateKey(requestID string) string     { return statePrefix + requestID }
func completedKey(requestID string) string { return completedPrefix + requestID }
func partsKey(requestID string) string     { return partsPrefix + requestID }

// Result codes returned by the Lua scripts; see the corresponding .lua file
// in internal/pipeline/lua for the full contract of each.
const (
	ResultCreated       = "CREATED"
	ResultAlreadyExists = "ALREADY_EXISTS"
	ResultConflict      = "CONFLICT"
	ResultRecorded      = "RECORDED"
	ResultFinal         = "FINAL"
	ResultDuplicate     = "DUPLICATE"
	ResultMissing       = "MISSING"
	ResultTerminal      = "TERMINAL"
	ResultInvalid       = "INVALID"
	ResultFailed        = "FAILED"
	ResultNoop          = "NOOP"
)

// InitAndPublish atomically creates the request's state (if absent) and
// publishes one splitted event per entry of partURLs, in order (offset =
// index into partURLs).
func InitAndPublish(ctx context.Context, rdb *redis.Client, in InputEvent, partURLs []string, ttl time.Duration) (string, error) {
	now := time.Now().Unix()
	argv := make([]interface{}, 0, 7+len(partURLs))
	argv = append(argv,
		in.RequestID, in.ConversationID, in.MessageID, in.FilePath,
		len(partURLs), int64(ttl.Seconds()), now,
	)
	for _, u := range partURLs {
		argv = append(argv, u)
	}
	keys := []string{stateKey(in.RequestID), StreamSplitted}
	res, err := initAndPublishScript.Run(ctx, rdb, keys, argv...).Result()
	if err != nil {
		return "", err
	}
	status, _ := res.(string)
	return status, nil
}

// CompletePart atomically records ev.Offset as done and, if it was the last
// outstanding offset for ev.RequestID, publishes the SUCCESS output event.
func CompletePart(ctx context.Context, rdb *redis.Client, ev SplittedEvent, markdownURL string, ttl time.Duration) (string, error) {
	now := time.Now().Unix()
	keys := []string{stateKey(ev.RequestID), completedKey(ev.RequestID), partsKey(ev.RequestID), StreamOutput}
	argv := []interface{}{
		ev.RequestID, ev.ConversationID, ev.MessageID,
		ev.Total, ev.Offset, markdownURL,
		int64(ttl.Seconds()), now,
	}
	res, err := completePartScript.Run(ctx, rdb, keys, argv...).Result()
	if err != nil {
		return "", err
	}
	status, _ := res.(string)
	return status, nil
}

// FailSplitter atomically marks in.RequestID as FAILED and publishes the
// FAILED output event, but only if no state exists yet for it (i.e. the
// Splitter failed before ever publishing a part).
func FailSplitter(ctx context.Context, rdb *redis.Client, in InputEvent, reason string, ttl time.Duration) (string, error) {
	return runFailRequest(ctx, rdb, "splitter", in.RequestID, in.ConversationID, in.MessageID, 0, FailedOffsetNone, reason, ttl)
}

// FailExtractor atomically transitions ev.RequestID from PROCESSING to
// FAILED and publishes the FAILED output event, unless the request is
// already terminal or ev.Offset was already recorded as completed.
func FailExtractor(ctx context.Context, rdb *redis.Client, ev SplittedEvent, reason string, ttl time.Duration) (string, error) {
	return runFailRequest(ctx, rdb, "extractor", ev.RequestID, ev.ConversationID, ev.MessageID, ev.Total, ev.Offset, reason, ttl)
}

func runFailRequest(ctx context.Context, rdb *redis.Client, mode, requestID, conversationID, messageID string, total, offset int, reason string, ttl time.Duration) (string, error) {
	now := time.Now().Unix()
	keys := []string{stateKey(requestID), completedKey(requestID), StreamOutput}
	argv := []interface{}{mode, requestID, conversationID, messageID, total, offset, reason, int64(ttl.Seconds()), now}
	res, err := failRequestScript.Run(ctx, rdb, keys, argv...).Result()
	if err != nil {
		return "", err
	}
	status, _ := res.(string)
	return status, nil
}
