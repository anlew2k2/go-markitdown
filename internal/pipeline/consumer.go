package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// consumerBlock is how long a single XREADGROUP for new messages blocks
// waiting for one to arrive before looping to re-check ctx.
const consumerBlock = 2 * time.Second

// retryBackoff is the fixed delay between retries of a failed Redis call
// (read, ack, or business script), both in the consumer loop and in the
// workers themselves.
const retryBackoff = 1 * time.Second

// Message is a single Redis Streams entry carrying a JSON payload in its
// "data" field.
type Message struct {
	ID   string
	Data string
}

// Handler processes one message and returns nil once it is safe to ACK.
// It should only return a non-nil error for unexpected infrastructure
// failures (a Redis call that could not complete); the caller then retries
// the SAME message, with backoff, until it succeeds or ctx is cancelled.
// Ordinary business failures (download/convert/upload errors, a sibling
// FAILED, a duplicate/terminal state) must be resolved by the handler
// itself — including publishing a FAILED output event when appropriate —
// and reported as success (nil) so the message gets ACKed.
type Handler func(ctx context.Context, msg Message) error

// RunConsumer drains this consumer's own pending entries first (messages
// delivered to it by a previous run of the same CONSUMER_NAME that were
// never ACKed), then reads new messages one at a time via
// XREADGROUP ... ">" until ctx is cancelled.
func RunConsumer(ctx context.Context, rdb *redis.Client, stream, group, consumer string, handle Handler) error {
	if err := drainPending(ctx, rdb, stream, group, consumer, handle); err != nil {
		return err
	}
	return readNew(ctx, rdb, stream, group, consumer, handle)
}

// RunConsumerPool runs concurrency independent, bounded worker loops
// against the same stream/group, each as its own stable, numbered
// sub-consumer ("<consumerBase>-slot-<i>", i in [0, concurrency)). Every
// slot is a full, unmodified RunConsumer loop, so a job is only ever read,
// processed (download/convert/upload) and ACKed end-to-end by one slot at a
// time: concurrency slots means at most concurrency jobs in flight for this
// process, never more, and never less bounded than before — no goroutine or
// dequeue beyond these fixed loops. Values < 1 are treated as 1.
//
// Before starting the active slots, it drains pending entries left behind
// by any consumer identity that used to belong to this consumerBase but no
// longer does: either a retired slot (index >= concurrency, e.g. after a
// concurrency downgrade) or the bare consumerBase identity itself (the
// pre-concurrency naming scheme, for a one-time upgrade from a deployment
// that predates WORKER_CONCURRENCY). This only recognizes identities
// derived from consumerBase; it cannot discover pending left by a consumer
// name that changed for some other reason (e.g. CONSUMER_NAME/hostname
// changed) — that remains the same known POC limitation documented in
// docs/pod-resource-benchmark-plan.md ("POC không tự reclaim consumer đã
// biến mất").
func RunConsumerPool(ctx context.Context, rdb *redis.Client, stream, group, consumerBase string, concurrency int, handle Handler) error {
	if concurrency < 1 {
		concurrency = 1
	}
	if err := drainRetiredSlots(ctx, rdb, stream, group, consumerBase, concurrency, handle); err != nil {
		return err
	}

	errCh := make(chan error, concurrency)
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		slot := fmt.Sprintf("%s-slot-%d", consumerBase, i)
		wg.Add(1)
		go func(consumer string) {
			defer wg.Done()
			errCh <- RunConsumer(ctx, rdb, stream, group, consumer, handle)
		}(slot)
	}
	wg.Wait()
	close(errCh)

	var first error
	for err := range errCh {
		if err != nil && !errors.Is(err, context.Canceled) && first == nil {
			first = err
		}
	}
	return first
}

// drainRetiredSlots looks up every consumer Redis currently knows about for
// stream/group and, for each one that is a retired identity of
// consumerBase (see RunConsumerPool) with pending entries, drains it via
// the same drainPending used for a slot's own startup recovery. Best-effort:
// if XINFO CONSUMERS itself fails, it logs and continues rather than
// blocking worker startup on it.
func drainRetiredSlots(ctx context.Context, rdb *redis.Client, stream, group, consumerBase string, concurrency int, handle Handler) error {
	infos, err := rdb.XInfoConsumers(ctx, stream, group).Result()
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		log.Printf("pipeline: xinfo consumers on %s/%s failed, skipping retired-slot drain: %v", group, stream, err)
		return nil
	}

	slotPrefix := consumerBase + "-slot-"
	for _, info := range infos {
		if info.Pending == 0 {
			continue
		}
		name := info.Name
		retired := name == consumerBase // pre-concurrency bare identity
		if !retired && strings.HasPrefix(name, slotPrefix) {
			if idx, err := strconv.Atoi(strings.TrimPrefix(name, slotPrefix)); err == nil && idx >= concurrency {
				retired = true
			}
		}
		if !retired {
			continue
		}
		log.Printf("pipeline: draining %d pending message(s) left by retired consumer %q on %s (current concurrency=%d)", info.Pending, name, stream, concurrency)
		if err := drainPending(ctx, rdb, stream, group, name, handle); err != nil {
			return err
		}
	}
	return nil
}

func drainPending(ctx context.Context, rdb *redis.Client, stream, group, consumer string, handle Handler) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		res, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    group,
			Consumer: consumer,
			Streams:  []string{stream, "0"},
			Count:    1,
		}).Result()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Printf("pipeline: read own pending on %s failed, retrying: %v", stream, err)
			if !sleep(ctx, retryBackoff) {
				return ctx.Err()
			}
			continue
		}
		if len(res) == 0 || len(res[0].Messages) == 0 {
			return nil
		}
		for _, m := range res[0].Messages {
			if err := processOne(ctx, rdb, stream, group, m, handle); err != nil {
				return err
			}
		}
	}
}

func readNew(ctx context.Context, rdb *redis.Client, stream, group, consumer string, handle Handler) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		res, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    group,
			Consumer: consumer,
			Streams:  []string{stream, ">"},
			Count:    1,
			Block:    consumerBlock,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue // block timeout, no new messages
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Printf("pipeline: read new on %s failed, retrying: %v", stream, err)
			if !sleep(ctx, retryBackoff) {
				return ctx.Err()
			}
			continue
		}
		for _, s := range res {
			for _, m := range s.Messages {
				if err := processOne(ctx, rdb, stream, group, m, handle); err != nil {
					return err
				}
			}
		}
	}
}

// processOne runs handle against m, retrying with backoff on infrastructure
// errors until it succeeds or ctx is cancelled, then ACKs it. Keeping the
// retry here (rather than moving on to the next message) matches the POC
// requirement that a transient Redis error must not lose or skip a job.
func processOne(ctx context.Context, rdb *redis.Client, stream, group string, m redis.XMessage, handle Handler) error {
	msg := toMessage(m)
	for {
		err := handle(ctx, msg)
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		log.Printf("pipeline: handler error for %s %s, retrying: %v", stream, m.ID, err)
		if !sleep(ctx, retryBackoff) {
			return ctx.Err()
		}
	}
	for {
		err := rdb.XAck(ctx, stream, group, m.ID).Err()
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		log.Printf("pipeline: ack failed for %s %s, retrying: %v", stream, m.ID, err)
		if !sleep(ctx, retryBackoff) {
			return ctx.Err()
		}
	}
	return nil
}

func toMessage(m redis.XMessage) Message {
	data, _ := m.Values["data"].(string)
	return Message{ID: m.ID, Data: data}
}

// sleep waits for d or until ctx is cancelled, reporting which happened.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// retryScript runs fn (a single Lua script call) and retries it with
// backoff on error until it succeeds or ctx is cancelled. It is used to
// commit a business outcome (publish/complete/fail) without redoing the
// expensive work (split, download, convert) that produced it just because
// of a transient Redis error.
func retryScript(ctx context.Context, fn func() (string, error)) (string, error) {
	for {
		status, err := fn()
		if err == nil {
			return status, nil
		}
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		log.Printf("pipeline: redis script call failed, retrying: %v", err)
		if !sleep(ctx, retryBackoff) {
			return "", ctx.Err()
		}
	}
}
