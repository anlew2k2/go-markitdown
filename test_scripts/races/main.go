package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/go-markitdown/internal/pipeline"
)

func main() {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:16379",
	})
	defer rdb.Close()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("failed to connect to redis: %v", err)
	}

	evidenceDir := "docs/test-results/event-sourcing/run-001/redis"
	_ = os.MkdirAll(evidenceDir, 0755)

	report := make(map[string]interface{})
	allPassed := true

	runStep := func(name string, fn func() (map[string]interface{}, error)) {
		fmt.Printf("=== Running TC-006 Step: %s ===\n", name)
		data, err := fn()
		if err != nil {
			fmt.Printf("FAIL: %s: %v\n", name, err)
			report[name] = map[string]interface{}{"status": "FAIL", "error": err.Error(), "data": data}
			allPassed = false
		} else {
			fmt.Printf("PASS: %s\n", name)
			report[name] = map[string]interface{}{"status": "PASS", "data": data}
		}
	}

	// 1. Reject N > 1000 before writes
	runStep("reject_n_gt_1000", func() (map[string]interface{}, error) {
		reqID := uuid.NewString()
		urls := make([]string, 1001)
		for i := range urls {
			urls[i] = fmt.Sprintf("http://minio:9000/part-%d", i)
		}
		in := pipeline.InputEvent{
			RequestID:      reqID,
			ConversationID: uuid.NewString(),
			MessageID:      uuid.NewString(),
			FilePath:       "http://minio:9000/dataset/sample.pdf",
		}
		_, err := pipeline.InitAndPublish(ctx, rdb, in, urls, 24*time.Hour)
		if err == nil {
			return nil, fmt.Errorf("expected error for N=1001, got nil")
		}
		// Verify no keys created in Redis
		exists, err2 := rdb.Exists(ctx, "doc-extractor:state:"+reqID).Result()
		if err2 != nil {
			return nil, err2
		}
		if exists != 0 {
			return nil, fmt.Errorf("state key was created despite error")
		}
		return map[string]interface{}{"error_received": err.Error(), "state_exists": exists}, nil
	})

	// 2. Init twice with identical identity
	runStep("init_twice_dedup", func() (map[string]interface{}, error) {
		reqID := uuid.NewString()
		convID := uuid.NewString()
		msgID := uuid.NewString()
		in := pipeline.InputEvent{
			RequestID:      reqID,
			ConversationID: convID,
			MessageID:      msgID,
			FilePath:       "http://minio:9000/dataset/sample.pdf",
		}
		urls := []string{"http://minio:9000/part-0.pdf", "http://minio:9000/part-1.pdf"}

		// Clean streams before step or record len
		initLen, _ := rdb.XLen(ctx, pipeline.StreamSplitted).Result()

		res1, err := pipeline.InitAndPublish(ctx, rdb, in, urls, 24*time.Hour)
		if err != nil || res1 != pipeline.ResultCreated {
			return nil, fmt.Errorf("run 1 failed: res=%s err=%v", res1, err)
		}

		res2, err := pipeline.InitAndPublish(ctx, rdb, in, urls, 24*time.Hour)
		if err != nil || res2 != pipeline.ResultAlreadyExists {
			return nil, fmt.Errorf("run 2 failed: res=%s err=%v", res2, err)
		}

		afterLen, _ := rdb.XLen(ctx, pipeline.StreamSplitted).Result()
		addedEvents := afterLen - initLen
		if addedEvents != 2 {
			return nil, fmt.Errorf("expected exactly 2 events published, got %d", addedEvents)
		}

		return map[string]interface{}{
			"res1": res1, "res2": res2, "added_events": addedEvents,
		}, nil
	})

	// 3. Init conflict with different identity
	runStep("init_identity_conflict", func() (map[string]interface{}, error) {
		reqID := uuid.NewString()
		convID1 := uuid.NewString()
		msgID1 := uuid.NewString()
		in1 := pipeline.InputEvent{
			RequestID:      reqID,
			ConversationID: convID1,
			MessageID:      msgID1,
			FilePath:       "http://minio:9000/dataset/sample.pdf",
		}
		urls := []string{"http://minio:9000/part-0.pdf"}
		res1, err := pipeline.InitAndPublish(ctx, rdb, in1, urls, 24*time.Hour)
		if err != nil || res1 != pipeline.ResultCreated {
			return nil, fmt.Errorf("run 1 failed: res=%s err=%v", res1, err)
		}

		in2 := pipeline.InputEvent{
			RequestID:      reqID,
			ConversationID: uuid.NewString(), // different convID
			MessageID:      uuid.NewString(),
			FilePath:       "http://minio:9000/dataset/sample2.pdf",
		}
		res2, err := pipeline.InitAndPublish(ctx, rdb, in2, urls, 24*time.Hour)
		if err != nil || res2 != pipeline.ResultConflict {
			return nil, fmt.Errorf("run 2 expected CONFLICT, got res=%s err=%v", res2, err)
		}

		// Verify state untouched
		cid, _ := rdb.HGet(ctx, "doc-extractor:state:"+reqID, "conversation_id").Result()
		if cid != convID1 {
			return nil, fmt.Errorf("conversation_id was mutated to %s", cid)
		}

		return map[string]interface{}{"res1": res1, "res2": res2, "conversation_id": cid}, nil
	})

	// 4. Out of order completion and correct URL order in SUCCESS event
	runStep("out_of_order_completion_ordering", func() (map[string]interface{}, error) {
		reqID := uuid.NewString()
		convID := uuid.NewString()
		msgID := uuid.NewString()
		in := pipeline.InputEvent{
			RequestID:      reqID,
			ConversationID: convID,
			MessageID:      msgID,
			FilePath:       "http://minio:9000/sample.pdf",
		}
		urls := []string{"http://minio:9000/part-0.pdf", "http://minio:9000/part-1.pdf", "http://minio:9000/part-2.pdf"}
		_, err := pipeline.InitAndPublish(ctx, rdb, in, urls, 24*time.Hour)
		if err != nil {
			return nil, err
		}

		// Complete in reverse: offset 2, then offset 0, then offset 1 (final)
		ev2 := pipeline.SplittedEvent{RequestID: reqID, ConversationID: convID, MessageID: msgID, Offset: 2, Total: 3}
		ev0 := pipeline.SplittedEvent{RequestID: reqID, ConversationID: convID, MessageID: msgID, Offset: 0, Total: 3}
		ev1 := pipeline.SplittedEvent{RequestID: reqID, ConversationID: convID, MessageID: msgID, Offset: 1, Total: 3}

		r2, err := pipeline.CompletePart(ctx, rdb, ev2, "http://minio:9000/md-2.md", 24*time.Hour)
		if err != nil || r2 != pipeline.ResultRecorded {
			return nil, fmt.Errorf("offset 2: expected RECORDED, got %s err=%v", r2, err)
		}
		r0, err := pipeline.CompletePart(ctx, rdb, ev0, "http://minio:9000/md-0.md", 24*time.Hour)
		if err != nil || r0 != pipeline.ResultRecorded {
			return nil, fmt.Errorf("offset 0: expected RECORDED, got %s err=%v", r0, err)
		}
		r1, err := pipeline.CompletePart(ctx, rdb, ev1, "http://minio:9000/md-1.md", 24*time.Hour)
		if err != nil || r1 != pipeline.ResultFinal {
			return nil, fmt.Errorf("offset 1: expected FINAL, got %s err=%v", r1, err)
		}

		// Verify state status is COMPLETED
		st, _ := rdb.HGet(ctx, "doc-extractor:state:"+reqID, "status").Result()
		termID, _ := rdb.HGet(ctx, "doc-extractor:state:"+reqID, "terminal_event_id").Result()
		if st != pipeline.StatusCompleted || termID == "" {
			return nil, fmt.Errorf("status=%s termID=%s", st, termID)
		}

		// Read output event from terminal_event_id
		entries, err := rdb.XRange(ctx, pipeline.StreamOutput, termID, termID).Result()
		if err != nil || len(entries) == 0 {
			return nil, fmt.Errorf("failed to read output event: %v", err)
		}
		var outEv pipeline.OutputEvent
		if err := json.Unmarshal([]byte(entries[0].Values["data"].(string)), &outEv); err != nil {
			return nil, fmt.Errorf("unmarshal output event: %w", err)
		}

		if outEv.Status != "SUCCESS" || len(outEv.MarkdownURLs) != 3 {
			return nil, fmt.Errorf("output event mismatch: %+v", outEv)
		}
		// Assert strictly sorted by offset [md-0, md-1, md-2]
		if outEv.MarkdownURLs[0] != "http://minio:9000/md-0.md" ||
			outEv.MarkdownURLs[1] != "http://minio:9000/md-1.md" ||
			outEv.MarkdownURLs[2] != "http://minio:9000/md-2.md" {
			return nil, fmt.Errorf("URLs not in offset order: %v", outEv.MarkdownURLs)
		}

		return map[string]interface{}{
			"status": st, "urls": outEv.MarkdownURLs, "terminal_id": termID,
		}, nil
	})

	// 5. Concurrent final completions race
	runStep("concurrent_final_completions_race", func() (map[string]interface{}, error) {
		reqID := uuid.NewString()
		convID := uuid.NewString()
		msgID := uuid.NewString()
		in := pipeline.InputEvent{
			RequestID:      reqID,
			ConversationID: convID,
			MessageID:      msgID,
			FilePath:       "http://minio:9000/sample.pdf",
		}
		urls := []string{"http://minio:9000/part-0.pdf", "http://minio:9000/part-1.pdf"}
		_, _ = pipeline.InitAndPublish(ctx, rdb, in, urls, 24*time.Hour)

		// Record offset 0
		ev0 := pipeline.SplittedEvent{RequestID: reqID, ConversationID: convID, MessageID: msgID, Offset: 0, Total: 2}
		_, _ = pipeline.CompletePart(ctx, rdb, ev0, "http://minio:9000/md-0.md", 24*time.Hour)

		// 10 concurrent calls trying to complete offset 1
		concurrency := 10
		results := make([]string, concurrency)
		var wg sync.WaitGroup
		startGate := make(chan struct{})

		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				<-startGate
				ev1 := pipeline.SplittedEvent{RequestID: reqID, ConversationID: convID, MessageID: msgID, Offset: 1, Total: 2}
				mdURL := fmt.Sprintf("http://minio:9000/attempt-%d/md-1.md", idx)
				res, _ := pipeline.CompletePart(ctx, rdb, ev1, mdURL, 24*time.Hour)
				results[idx] = res
			}(i)
		}
		close(startGate)
		wg.Wait()

		finalCount := 0
		dupOrTermCount := 0
		for _, r := range results {
			if r == pipeline.ResultFinal {
				finalCount++
			} else if r == pipeline.ResultDuplicate || r == pipeline.ResultTerminal {
				dupOrTermCount++
			}
		}

		if finalCount != 1 {
			return nil, fmt.Errorf("expected exactly 1 FINAL, got %d (results: %v)", finalCount, results)
		}
		if finalCount+dupOrTermCount != concurrency {
			return nil, fmt.Errorf("unexpected results: %v", results)
		}

		return map[string]interface{}{
			"final_count": finalCount, "other_count": dupOrTermCount, "all_results": results,
		}, nil
	})

	// 6. Late failure after SUCCESS
	runStep("late_failure_after_success", func() (map[string]interface{}, error) {
		reqID := uuid.NewString()
		convID := uuid.NewString()
		msgID := uuid.NewString()
		in := pipeline.InputEvent{
			RequestID:      reqID,
			ConversationID: convID,
			MessageID:      msgID,
			FilePath:       "http://minio:9000/sample.pdf",
		}
		urls := []string{"http://minio:9000/part-0.pdf"}
		_, _ = pipeline.InitAndPublish(ctx, rdb, in, urls, 24*time.Hour)

		ev := pipeline.SplittedEvent{RequestID: reqID, ConversationID: convID, MessageID: msgID, Offset: 0, Total: 1}
		r1, err := pipeline.CompletePart(ctx, rdb, ev, "http://minio:9000/md-0.md", 24*time.Hour)
		if err != nil || r1 != pipeline.ResultFinal {
			return nil, fmt.Errorf("expected FINAL, got %s err=%v", r1, err)
		}

		// Try to fail the request now
		rf, err := pipeline.FailExtractor(ctx, rdb, ev, "belated failure", 24*time.Hour)
		if err != nil || rf != pipeline.ResultTerminal {
			return nil, fmt.Errorf("expected TERMINAL, got %s err=%v", rf, err)
		}

		st, _ := rdb.HGet(ctx, "doc-extractor:state:"+reqID, "status").Result()
		if st != pipeline.StatusCompleted {
			return nil, fmt.Errorf("status was changed to %s", st)
		}

		return map[string]interface{}{"complete_res": r1, "fail_res": rf, "final_status": st}, nil
	})

	// 7. Concurrent failures race
	runStep("concurrent_failures_race", func() (map[string]interface{}, error) {
		reqID := uuid.NewString()
		convID := uuid.NewString()
		msgID := uuid.NewString()
		in := pipeline.InputEvent{
			RequestID:      reqID,
			ConversationID: convID,
			MessageID:      msgID,
			FilePath:       "http://minio:9000/sample.pdf",
		}
		urls := []string{"http://minio:9000/part-0.pdf", "http://minio:9000/part-1.pdf"}
		_, _ = pipeline.InitAndPublish(ctx, rdb, in, urls, 24*time.Hour)

		concurrency := 10
		results := make([]string, concurrency)
		var wg sync.WaitGroup
		startGate := make(chan struct{})

		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				<-startGate
				ev := pipeline.SplittedEvent{RequestID: reqID, ConversationID: convID, MessageID: msgID, Offset: idx % 2, Total: 2}
				res, _ := pipeline.FailExtractor(ctx, rdb, ev, fmt.Sprintf("failure from goroutine %d", idx), 24*time.Hour)
				results[idx] = res
			}(i)
		}
		close(startGate)
		wg.Wait()

		failedCount := 0
		termCount := 0
		for _, r := range results {
			if r == pipeline.ResultFailed {
				failedCount++
			} else if r == pipeline.ResultTerminal {
				termCount++
			}
		}

		if failedCount != 1 {
			return nil, fmt.Errorf("expected exactly 1 FAILED, got %d (results: %v)", failedCount, results)
		}

		return map[string]interface{}{
			"failed_count": failedCount, "terminal_count": termCount, "all_results": results,
		}, nil
	})

	// 8. Late completion after FAILED
	runStep("late_completion_after_failed", func() (map[string]interface{}, error) {
		reqID := uuid.NewString()
		convID := uuid.NewString()
		msgID := uuid.NewString()
		in := pipeline.InputEvent{
			RequestID:      reqID,
			ConversationID: convID,
			MessageID:      msgID,
			FilePath:       "http://minio:9000/sample.pdf",
		}
		urls := []string{"http://minio:9000/part-0.pdf", "http://minio:9000/part-1.pdf"}
		_, _ = pipeline.InitAndPublish(ctx, rdb, in, urls, 24*time.Hour)

		ev0 := pipeline.SplittedEvent{RequestID: reqID, ConversationID: convID, MessageID: msgID, Offset: 0, Total: 2}
		rf, err := pipeline.FailExtractor(ctx, rdb, ev0, "part 0 broken", 24*time.Hour)
		if err != nil || rf != pipeline.ResultFailed {
			return nil, fmt.Errorf("expected FAILED, got %s err=%v", rf, err)
		}

		// Try to complete offset 1 late
		ev1 := pipeline.SplittedEvent{RequestID: reqID, ConversationID: convID, MessageID: msgID, Offset: 1, Total: 2}
		rc, err := pipeline.CompletePart(ctx, rdb, ev1, "http://minio:9000/md-1.md", 24*time.Hour)
		if err != nil || rc != pipeline.ResultTerminal {
			return nil, fmt.Errorf("expected TERMINAL, got %s err=%v", rc, err)
		}

		st, _ := rdb.HGet(ctx, "doc-extractor:state:"+reqID, "status").Result()
		if st != pipeline.StatusFailed {
			return nil, fmt.Errorf("status was changed to %s", st)
		}

		return map[string]interface{}{"fail_res": rf, "complete_res": rc, "final_status": st}, nil
	})

	// 9. Concurrent final completion vs failure race
	runStep("concurrent_final_completion_vs_failure_race", func() (map[string]interface{}, error) {
		reqID := uuid.NewString()
		convID := uuid.NewString()
		msgID := uuid.NewString()
		in := pipeline.InputEvent{
			RequestID:      reqID,
			ConversationID: convID,
			MessageID:      msgID,
			FilePath:       "http://minio:9000/sample.pdf",
		}
		urls := []string{"http://minio:9000/part-0.pdf", "http://minio:9000/part-1.pdf"}
		_, _ = pipeline.InitAndPublish(ctx, rdb, in, urls, 24*time.Hour)

		ev0 := pipeline.SplittedEvent{RequestID: reqID, ConversationID: convID, MessageID: msgID, Offset: 0, Total: 2}
		_, _ = pipeline.CompletePart(ctx, rdb, ev0, "http://minio:9000/md-0.md", 24*time.Hour)

		ev1 := pipeline.SplittedEvent{RequestID: reqID, ConversationID: convID, MessageID: msgID, Offset: 1, Total: 2}

		var compRes, failRes string
		var wg sync.WaitGroup
		startGate := make(chan struct{})

		wg.Add(2)
		go func() {
			defer wg.Done()
			<-startGate
			compRes, _ = pipeline.CompletePart(ctx, rdb, ev1, "http://minio:9000/md-1.md", 24*time.Hour)
		}()
		go func() {
			defer wg.Done()
			<-startGate
			failRes, _ = pipeline.FailExtractor(ctx, rdb, ev1, "corrupted while converting part 1", 24*time.Hour)
		}()
		close(startGate)
		wg.Wait()

		st, _ := rdb.HGet(ctx, "doc-extractor:state:"+reqID, "status").Result()
		termID, _ := rdb.HGet(ctx, "doc-extractor:state:"+reqID, "terminal_event_id").Result()

		// Assert: exactly one is winning (FINAL or FAILED), the other is TERMINAL
		if compRes == pipeline.ResultFinal && failRes == pipeline.ResultTerminal {
			if st != pipeline.StatusCompleted {
				return nil, fmt.Errorf("comp won but status is %s", st)
			}
		} else if failRes == pipeline.ResultFailed && compRes == pipeline.ResultTerminal {
			if st != pipeline.StatusFailed {
				return nil, fmt.Errorf("fail won but status is %s", st)
			}
		} else {
			return nil, fmt.Errorf("invalid race outcome: comp=%s fail=%s status=%s", compRes, failRes, st)
		}

		return map[string]interface{}{
			"comp_res": compRes, "fail_res": failRes, "winning_status": st, "terminal_id": termID,
		}, nil
	})

	// 10. Contract validation (missing state, invalid fields)
	runStep("contract_validation_rejections", func() (map[string]interface{}, error) {
		bogusID := uuid.NewString()
		ev := pipeline.SplittedEvent{
			RequestID:      bogusID,
			ConversationID: uuid.NewString(),
			MessageID:      uuid.NewString(),
			Offset:         0,
			Total:          1,
		}
		// Missing state
		rMissing, _ := pipeline.CompletePart(ctx, rdb, ev, "http://minio/md.md", 24*time.Hour)
		if rMissing != pipeline.ResultMissing {
			return nil, fmt.Errorf("expected MISSING, got %s", rMissing)
		}
		rMissingFail, _ := pipeline.FailExtractor(ctx, rdb, ev, "some err", 24*time.Hour)
		if rMissingFail != pipeline.ResultMissing {
			return nil, fmt.Errorf("expected MISSING, got %s", rMissingFail)
		}

		// Invalid identity against existing state
		realID := uuid.NewString()
		cID := uuid.NewString()
		mID := uuid.NewString()
		in := pipeline.InputEvent{RequestID: realID, ConversationID: cID, MessageID: mID, FilePath: "http://minio/f.pdf"}
		_, _ = pipeline.InitAndPublish(ctx, rdb, in, []string{"http://minio/p0.pdf"}, 24*time.Hour)

		evInvalidConv := pipeline.SplittedEvent{RequestID: realID, ConversationID: uuid.NewString(), MessageID: mID, Offset: 0, Total: 1}
		rInv, _ := pipeline.CompletePart(ctx, rdb, evInvalidConv, "http://minio/md.md", 24*time.Hour)
		if rInv != pipeline.ResultInvalid {
			return nil, fmt.Errorf("expected INVALID for wrong convID, got %s", rInv)
		}

		evInvalidTotal := pipeline.SplittedEvent{RequestID: realID, ConversationID: cID, MessageID: mID, Offset: 0, Total: 5}
		rInvTotal, _ := pipeline.CompletePart(ctx, rdb, evInvalidTotal, "http://minio/md.md", 24*time.Hour)
		if rInvTotal != pipeline.ResultInvalid {
			return nil, fmt.Errorf("expected INVALID for wrong total, got %s", rInvTotal)
		}

		return map[string]interface{}{
			"missing_complete": rMissing,
			"missing_fail":     rMissingFail,
			"invalid_conv":     rInv,
			"invalid_total":    rInvTotal,
		}, nil
	})

	// 11. TTL and expires_at consistency
	runStep("ttl_and_expires_at_consistency", func() (map[string]interface{}, error) {
		reqID := uuid.NewString()
		convID := uuid.NewString()
		msgID := uuid.NewString()
		in := pipeline.InputEvent{
			RequestID:      reqID,
			ConversationID: convID,
			MessageID:      msgID,
			FilePath:       "http://minio:9000/sample.pdf",
		}
		urls := []string{"http://minio:9000/part-0.pdf", "http://minio:9000/part-1.pdf"}
		ttlSec := int64(86400)
		_, err := pipeline.InitAndPublish(ctx, rdb, in, urls, time.Duration(ttlSec)*time.Second)
		if err != nil {
			return nil, err
		}

		stateKey := "doc-extractor:state:" + reqID
		compKey := "doc-extractor:completed:" + reqID
		partsKey := "doc-extractor:parts:" + reqID

		stateExpStr, _ := rdb.HGet(ctx, stateKey, "expires_at").Result()
		stateTTL, _ := rdb.TTL(ctx, stateKey).Result()

		// Record part 0
		ev0 := pipeline.SplittedEvent{RequestID: reqID, ConversationID: convID, MessageID: msgID, Offset: 0, Total: 2}
		_, err = pipeline.CompletePart(ctx, rdb, ev0, "http://minio:9000/md-0.md", time.Duration(ttlSec)*time.Second)
		if err != nil {
			return nil, err
		}

		partsTTL, _ := rdb.TTL(ctx, partsKey).Result()
		compTTL, _ := rdb.TTL(ctx, compKey).Result()

		// Sleep 2 seconds
		time.Sleep(2 * time.Second)

		// Duplicate part 0
		_, err = pipeline.CompletePart(ctx, rdb, ev0, "http://minio:9000/md-0.md", time.Duration(ttlSec)*time.Second)
		if err != nil {
			return nil, err
		}

		stateExpStrAfter, _ := rdb.HGet(ctx, stateKey, "expires_at").Result()
		stateTTLAfter, _ := rdb.TTL(ctx, stateKey).Result()

		if stateExpStr != stateExpStrAfter {
			return nil, fmt.Errorf("expires_at changed from %s to %s on duplicate", stateExpStr, stateExpStrAfter)
		}

		// Check that TTL decreased by ~2 seconds, not reset back to 86400
		if stateTTLAfter >= stateTTL {
			return nil, fmt.Errorf("state TTL increased/reset: before=%v after=%v", stateTTL, stateTTLAfter)
		}

		return map[string]interface{}{
			"state_expires_at": stateExpStr,
			"state_ttl_before": stateTTL.Seconds(),
			"parts_ttl":        partsTTL.Seconds(),
			"comp_ttl":         compTTL.Seconds(),
			"state_ttl_after":  stateTTLAfter.Seconds(),
			"ttl_decreased":    stateTTLAfter < stateTTL,
		}, nil
	})

	outBytes, _ := json.MarshalIndent(report, "", "  ")
	outputPath := filepath.Join(evidenceDir, "tc-006-results.json")
	_ = os.WriteFile(outputPath, outBytes, 0644)

	if allPassed {
		fmt.Printf("\n>>> TC-006 ALL STEPS PASSED. Saved to %s <<<\n", outputPath)
		os.Exit(0)
	} else {
		fmt.Printf("\n>>> TC-006 HAD FAILURES. Saved to %s <<<\n", outputPath)
		os.Exit(1)
	}
}
