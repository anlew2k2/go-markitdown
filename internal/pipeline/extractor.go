package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/go-markitdown/internal/services"
	"github.com/go-markitdown/internal/usecase"
)

const (
	// failFastPollInterval is how often an in-flight extraction checks
	// whether a sibling offset has already marked the request FAILED.
	failFastPollInterval = 500 * time.Millisecond

	// httpDownloadTimeout bounds downloading one part before conversion.
	httpDownloadTimeout = 2 * time.Minute

	// markdownStderrLimit caps how much of the markitdown subprocess's
	// stderr is retained for error reporting.
	markdownStderrLimit = 64 * 1024
)

// ExtractorWorker consumes stream:doc-extractor:splitted, converts each part
// to Markdown via the markitdown CLI, uploads the result to MinIO and
// atomically records completion (or failure) in Redis.
type ExtractorWorker struct {
	rdb            *redis.Client
	consumer       string
	jobTimeout     time.Duration
	convertTimeout time.Duration
	ttl            time.Duration
}

// NewExtractorWorker builds an ExtractorWorker. jobTimeout bounds one whole
// part (download+convert+upload); convertTimeout bounds the markitdown
// subprocess specifically.
func NewExtractorWorker(rdb *redis.Client, consumer string, jobTimeout, convertTimeout time.Duration) *ExtractorWorker {
	return &ExtractorWorker{
		rdb:            rdb,
		consumer:       consumer,
		jobTimeout:     jobTimeout,
		convertTimeout: convertTimeout,
		ttl:            DefaultStateTTL,
	}
}

// Run drains pending and then new splitted messages until ctx is cancelled.
func (w *ExtractorWorker) Run(ctx context.Context) error {
	return RunConsumer(ctx, w.rdb, StreamSplitted, GroupExtractor, w.consumer, w.handle)
}

func (w *ExtractorWorker) handle(ctx context.Context, msg Message) error {
	ev, err := ParseSplittedEvent(msg.Data)
	if err != nil {
		log.Printf("extractor: malformed splitted message %s, acking: %v", msg.ID, err)
		return nil
	}
	if err := ev.Validate(); err != nil {
		log.Printf("extractor: invalid splitted message %s, acking: %v", msg.ID, err)
		return nil
	}

	status, err := w.rdb.HGet(ctx, stateKey(ev.RequestID), "status").Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			log.Printf("extractor: missing state for request_id=%s offset=%d, acking", ev.RequestID, ev.Offset)
			return nil
		}
		return err
	}
	if status == StatusFailed || status == StatusCompleted {
		log.Printf("extractor: request_id=%s offset=%d already %s, skipping", ev.RequestID, ev.Offset, status)
		return nil
	}

	already, err := w.rdb.SIsMember(ctx, completedKey(ev.RequestID), ev.Offset).Result()
	if err != nil {
		return err
	}
	if already {
		log.Printf("extractor: request_id=%s offset=%d already completed, skipping", ev.RequestID, ev.Offset)
		return nil
	}

	jobCtx, cancelJob := context.WithTimeout(ctx, w.jobTimeout)
	defer cancelJob()

	pollCtx, stopPoll := context.WithCancel(ctx)
	go w.watchFailFast(pollCtx, ev.RequestID, cancelJob)

	mdPath, convErr := w.convert(jobCtx, ev.FilePath)
	if convErr != nil {
		stopPoll()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		log.Printf("extractor: conversion failed for request_id=%s offset=%d: %v", ev.RequestID, ev.Offset, convErr)
		return w.failAndAck(ctx, ev, convErr.Error())
	}
	defer os.Remove(mdPath)

	mdURL, upErr := w.upload(jobCtx, ev, mdPath)
	stopPoll()
	if upErr != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		log.Printf("extractor: upload failed for request_id=%s offset=%d: %v", ev.RequestID, ev.Offset, upErr)
		return w.failAndAck(ctx, ev, upErr.Error())
	}

	completeStatus, err := retryScript(ctx, func() (string, error) {
		return CompletePart(ctx, w.rdb, ev, mdURL, w.ttl)
	})
	if err != nil {
		return err
	}

	switch completeStatus {
	case ResultRecorded:
		log.Printf("extractor: request_id=%s offset=%d recorded", ev.RequestID, ev.Offset)
	case ResultFinal:
		log.Printf("extractor: request_id=%s offset=%d completed request (SUCCESS published)", ev.RequestID, ev.Offset)
	case ResultDuplicate:
		log.Printf("extractor: request_id=%s offset=%d duplicate completion, ignored", ev.RequestID, ev.Offset)
	case ResultTerminal:
		log.Printf("extractor: request_id=%s offset=%d arrived after terminal state, ignored", ev.RequestID, ev.Offset)
	case ResultMissing, ResultInvalid:
		log.Printf("extractor: request_id=%s offset=%d complete_part returned %s, acking", ev.RequestID, ev.Offset, completeStatus)
	default:
		log.Printf("extractor: request_id=%s offset=%d unexpected complete_part status %q", ev.RequestID, ev.Offset, completeStatus)
	}
	return nil
}

func (w *ExtractorWorker) failAndAck(ctx context.Context, ev SplittedEvent, reason string) error {
	status, err := retryScript(ctx, func() (string, error) {
		return FailExtractor(ctx, w.rdb, ev, reason, w.ttl)
	})
	if err != nil {
		return err
	}
	log.Printf("extractor: request_id=%s offset=%d fail_request -> %s", ev.RequestID, ev.Offset, status)
	return nil
}

// watchFailFast polls the request's state every 500ms and cancels the job
// the moment a sibling offset marks it FAILED, so an in-flight download or
// markitdown subprocess is killed promptly instead of running to
// completion for nothing. It is intentionally tolerant of transient Redis
// errors (it just tries again next tick): losing fail-fast during a brief
// Redis blip is not a correctness issue, only a delay.
func (w *ExtractorWorker) watchFailFast(ctx context.Context, requestID string, cancel context.CancelFunc) {
	ticker := time.NewTicker(failFastPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			status, err := w.rdb.HGet(ctx, stateKey(requestID), "status").Result()
			if err != nil {
				continue
			}
			if status == StatusFailed {
				cancel()
				return
			}
		}
	}
}

// convert downloads fileURL and runs markitdown against it, writing the
// resulting Markdown to a fresh temp file (never held as a string in
// memory) whose path is returned for the caller to upload and remove.
func (w *ExtractorWorker) convert(ctx context.Context, fileURL string) (string, error) {
	out, err := os.CreateTemp("", "extractor-*.md")
	if err != nil {
		return "", fmt.Errorf("create markdown temp file: %w", err)
	}
	outPath := out.Name()
	out.Close()

	if err := usecase.ConvertURLToMarkdownFile(ctx, fileURL, outPath, httpDownloadTimeout, w.convertTimeout, markdownStderrLimit); err != nil {
		os.Remove(outPath)
		return "", err
	}
	return outPath, nil
}

func (w *ExtractorWorker) upload(ctx context.Context, ev SplittedEvent, mdPath string) (string, error) {
	f, err := os.Open(mdPath)
	if err != nil {
		return "", fmt.Errorf("open markdown output: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return "", fmt.Errorf("stat markdown output: %w", err)
	}

	objectName := fmt.Sprintf("splits/%s/%s/%s/%s/part-%03d.md",
		ev.ConversationID, ev.MessageID, ev.RequestID, uuid.NewString(), ev.Offset)
	return services.UploadObject(ctx, objectName, f, info.Size(), "text/markdown")
}
