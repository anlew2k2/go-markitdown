package pipeline

import (
	"context"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/go-markitdown/internal/usecase"
)

// SplitterWorker consumes stream:doc-extractor:input, splits each document
// via usecase.SplitFile and atomically publishes the resulting parts to
// stream:doc-extractor:splitted.
type SplitterWorker struct {
	rdb        *redis.Client
	consumer   string
	jobTimeout time.Duration
	ttl        time.Duration
}

// NewSplitterWorker builds a SplitterWorker. jobTimeout bounds one
// download+split+upload job (usecase.SplitFile).
func NewSplitterWorker(rdb *redis.Client, consumer string, jobTimeout time.Duration) *SplitterWorker {
	return &SplitterWorker{rdb: rdb, consumer: consumer, jobTimeout: jobTimeout, ttl: DefaultStateTTL}
}

// Run drains pending and then new input messages until ctx is cancelled.
func (w *SplitterWorker) Run(ctx context.Context) error {
	return RunConsumer(ctx, w.rdb, StreamInput, GroupSplitter, w.consumer, w.handle)
}

func (w *SplitterWorker) handle(ctx context.Context, msg Message) error {
	in, err := ParseInputEvent(msg.Data)
	if err != nil {
		log.Printf("splitter: malformed input message %s, acking: %v", msg.ID, err)
		return nil
	}
	if err := in.Validate(); err != nil {
		log.Printf("splitter: invalid input message %s, acking: %v", msg.ID, err)
		return nil
	}
	requestID, errR := uuid.Parse(in.RequestID)
	conversationID, errC := uuid.Parse(in.ConversationID)
	messageID, errM := uuid.Parse(in.MessageID)
	if errR != nil || errC != nil || errM != nil {
		log.Printf("splitter: invalid uuid in message %s, acking", msg.ID)
		return nil
	}

	exists, err := w.rdb.Exists(ctx, stateKey(in.RequestID)).Result()
	if err != nil {
		return err
	}
	if exists == 1 {
		log.Printf("splitter: request_id=%s already has state, skipping re-split", in.RequestID)
		return nil
	}

	jobCtx, cancel := context.WithTimeout(ctx, w.jobTimeout)
	urls, splitErr := usecase.SplitFile(jobCtx, usecase.SplitFileInput{
		RequestID:      requestID,
		ConversationID: conversationID,
		MessageID:      messageID,
		FilePath:       in.FilePath,
	})
	cancel()

	if splitErr != nil {
		if ctx.Err() != nil {
			// Shutdown in progress, not a genuine split failure: leave the
			// message pending for the next run instead of publishing FAILED.
			return ctx.Err()
		}
		log.Printf("splitter: split failed for request_id=%s: %v", in.RequestID, splitErr)
		status, err := retryScript(ctx, func() (string, error) {
			return FailSplitter(ctx, w.rdb, in, splitErr.Error(), w.ttl)
		})
		if err != nil {
			return err
		}
		log.Printf("splitter: request_id=%s fail_request -> %s", in.RequestID, status)
		return nil
	}

	status, err := retryScript(ctx, func() (string, error) {
		return InitAndPublish(ctx, w.rdb, in, urls, w.ttl)
	})
	if err != nil {
		return err
	}

	switch status {
	case ResultCreated:
		log.Printf("splitter: request_id=%s published %d part(s)", in.RequestID, len(urls))
	case ResultAlreadyExists:
		log.Printf("splitter: request_id=%s already initialized, skipping duplicate publish", in.RequestID)
	case ResultConflict:
		log.Printf("splitter: request_id=%s conflicts with an existing request of different identity, acking without change", in.RequestID)
	default:
		log.Printf("splitter: request_id=%s unexpected init_and_publish status %q", in.RequestID, status)
	}
	return nil
}
