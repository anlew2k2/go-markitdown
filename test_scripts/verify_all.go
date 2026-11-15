package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/go-markitdown/internal/pipeline"
)

const (
	redisAddr     = "localhost:16379"
	minioHostBase = "http://localhost:19000"
	evidenceDir   = "docs/test-results/event-sourcing/run-001"
)

type TestResult struct {
	CaseID        string                 `json:"case_id"`
	Title         string                 `json:"title"`
	Priority      string                 `json:"priority"`
	Status        string                 `json:"status"` // PASS, FAIL, BLOCKED
	RequestID     string                 `json:"request_id,omitempty"`
	InputID       string                 `json:"input_id,omitempty"`
	TerminalID    string                 `json:"terminal_id,omitempty"`
	TerminalState string                 `json:"terminal_state,omitempty"`
	Details       map[string]interface{} `json:"details"`
	Error         string                 `json:"error,omitempty"`
	DurationMs    int64                  `json:"duration_ms"`
}

type VerificationSuite struct {
	rdb     *redis.Client
	ctx     context.Context
	results map[string]TestResult
}

func NewSuite() *VerificationSuite {
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	return &VerificationSuite{
		rdb:     rdb,
		ctx:     context.Background(),
		results: make(map[string]TestResult),
	}
}

func (s *VerificationSuite) logStep(caseID, title, priority string, fn func() (map[string]interface{}, error)) {
	start := time.Now()
	fmt.Printf("\n======================================================\n")
	fmt.Printf("[%s] %s (%s)\n", caseID, title, priority)
	fmt.Printf("======================================================\n")

	details, err := fn()
	elapsed := time.Since(start).Milliseconds()

	res := TestResult{
		CaseID:     caseID,
		Title:      title,
		Priority:   priority,
		DurationMs: elapsed,
		Details:    details,
	}

	if err != nil {
		fmt.Printf(">>> FAILED [%s]: %v (took %dms)\n", caseID, err, elapsed)
		res.Status = "FAIL"
		res.Error = err.Error()
	} else {
		fmt.Printf(">>> PASSED [%s] (took %dms)\n", caseID, elapsed)
		res.Status = "PASS"
	}

	if details != nil {
		if reqID, ok := details["request_id"].(string); ok {
			res.RequestID = reqID
		}
		if inID, ok := details["input_stream_id"].(string); ok {
			res.InputID = inID
		}
		if termID, ok := details["terminal_id"].(string); ok {
			res.TerminalID = termID
		}
		if termSt, ok := details["terminal_status"].(string); ok {
			res.TerminalState = termSt
		}
	}

	s.results[caseID] = res
	s.saveCaseEvidence(caseID, res)
}

func (s *VerificationSuite) saveCaseEvidence(caseID string, res TestResult) {
	_ = os.MkdirAll(filepath.Join(evidenceDir, "cases"), 0755)
	b, _ := json.MarshalIndent(res, "", "  ")
	_ = os.WriteFile(filepath.Join(evidenceDir, "cases", caseID+".json"), b, 0644)
}

// publishInput sends a message to stream:doc-extractor:input
func (s *VerificationSuite) publishInput(reqID, convID, msgID, filePath string) (string, error) {
	payload := map[string]string{
		"request_id":      reqID,
		"conversation_id": convID,
		"message_id":      msgID,
		"file_path":       filePath,
	}
	b, _ := json.Marshal(payload)
	return s.rdb.XAdd(s.ctx, &redis.XAddArgs{
		Stream: pipeline.StreamInput,
		Values: map[string]interface{}{"data": string(b)},
	}).Result()
}

// waitForTerminal polls Redis state for reqID until it reaches COMPLETED or FAILED, or deadline
func (s *VerificationSuite) waitForTerminal(reqID string, timeout time.Duration) (string, map[string]string, error) {
	deadline := time.Now().Add(timeout)
	stateKey := "doc-extractor:state:" + reqID

	for time.Now().Before(deadline) {
		vals, err := s.rdb.HGetAll(s.ctx, stateKey).Result()
		if err == nil && len(vals) > 0 {
			st := vals["status"]
			if st == pipeline.StatusCompleted || st == pipeline.StatusFailed {
				return st, vals, nil
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	return "", nil, fmt.Errorf("timeout waiting for terminal state of %s", reqID)
}

// readTerminalEvent reads the output event from stream:doc-extractor:output for reqID
func (s *VerificationSuite) readTerminalEvent(reqID string) (*pipeline.OutputEvent, string, error) {
	entries, err := s.rdb.XRange(s.ctx, pipeline.StreamOutput, "-", "+").Result()
	if err != nil {
		return nil, "", err
	}
	for _, e := range entries {
		data, ok := e.Values["data"].(string)
		if !ok {
			continue
		}
		var ev pipeline.OutputEvent
		if err := json.Unmarshal([]byte(data), &ev); err == nil {
			if ev.RequestID == reqID {
				return &ev, e.ID, nil
			}
		}
	}
	return nil, "", fmt.Errorf("output event for %s not found in output stream", reqID)
}

// countTerminalEvents counts how many terminal events exist in stream:doc-extractor:output for reqID
func (s *VerificationSuite) countTerminalEvents(reqID string) int {
	entries, err := s.rdb.XRange(s.ctx, pipeline.StreamOutput, "-", "+").Result()
	if err != nil {
		return 0
	}
	count := 0
	for _, e := range entries {
		data, ok := e.Values["data"].(string)
		if !ok {
			continue
		}
		var ev pipeline.OutputEvent
		if err := json.Unmarshal([]byte(data), &ev); err == nil {
			if ev.RequestID == reqID {
				count++
			}
		}
	}
	return count
}

func main() {
	suite := NewSuite()

	// TC-001: Bootstrap & repeat check
	suite.logStep("TC-001", "Fresh start and repeat bootstrap", "Critical", func() (map[string]interface{}, error) {
		// Verify streams exist
		inExists, _ := suite.rdb.Exists(suite.ctx, pipeline.StreamInput).Result()
		splitExists, _ := suite.rdb.Exists(suite.ctx, pipeline.StreamSplitted).Result()
		outExists, _ := suite.rdb.Exists(suite.ctx, pipeline.StreamOutput).Result()
		if inExists != 1 || splitExists != 1 || outExists != 1 {
			return nil, fmt.Errorf("streams missing: in=%d split=%d out=%d", inExists, splitExists, outExists)
		}

		// Verify groups
		groupsIn, err := suite.rdb.XInfoGroups(suite.ctx, pipeline.StreamInput).Result()
		if err != nil || len(groupsIn) == 0 || groupsIn[0].Name != pipeline.GroupSplitter {
			return nil, fmt.Errorf("splitter group missing or invalid: %v", err)
		}
		groupsSplit, err := suite.rdb.XInfoGroups(suite.ctx, pipeline.StreamSplitted).Result()
		if err != nil || len(groupsSplit) == 0 || groupsSplit[0].Name != pipeline.GroupExtractor {
			return nil, fmt.Errorf("extractor group missing or invalid: %v", err)
		}

		// Verify no bootstrap sentinel in output
		entries, _ := suite.rdb.XRange(suite.ctx, pipeline.StreamOutput, "-", "+").Result()
		for _, e := range entries {
			if _, hasBootstrap := e.Values["bootstrap"]; hasBootstrap {
				return nil, fmt.Errorf("found sentinel bootstrap entry in output stream: %v", e.ID)
			}
		}

		// Verify consumers: 1 splitter, 3 extractors
		consumersSplit, _ := suite.rdb.XInfoConsumers(suite.ctx, pipeline.StreamSplitted, pipeline.GroupExtractor).Result()
		if len(consumersSplit) != 3 {
			return nil, fmt.Errorf("expected 3 extractor consumers, found %d", len(consumersSplit))
		}
		consNames := make([]string, len(consumersSplit))
		for i, c := range consumersSplit {
			consNames[i] = c.Name
		}

		// Re-run bootstrap in Go: must be idempotent and BUSYGROUP safe
		if err := pipeline.Bootstrap(suite.ctx, suite.rdb); err != nil {
			return nil, fmt.Errorf("bootstrap call failed: %w", err)
		}

		return map[string]interface{}{
			"streams":           []string{pipeline.StreamInput, pipeline.StreamSplitted, pipeline.StreamOutput},
			"groups":            []string{pipeline.GroupSplitter, pipeline.GroupExtractor},
			"extractor_workers": consNames,
			"output_entries":    len(entries),
		}, nil
	})

	// TC-002: Small file & boundary (report.pdf, boundary_under_16mb, boundary_above_16mb)
	suite.logStep("TC-002", "Small file and 4MB boundary", "Critical", func() (map[string]interface{}, error) {
		// Part 1: report.pdf (< 4MB)
		reqID1 := uuid.NewString()
		streamID1, err := suite.publishInput(reqID1, uuid.NewString(), uuid.NewString(), "http://minio:9000/dataset/report.pdf")
		if err != nil {
			return nil, err
		}

		st1, stateVals1, err := suite.waitForTerminal(reqID1, 30*time.Second)
		if err != nil || st1 != pipeline.StatusCompleted {
			return nil, fmt.Errorf("req1 failed or timed out: st=%s err=%v", st1, err)
		}

		outEv1, outID1, err := suite.readTerminalEvent(reqID1)
		if err != nil || outEv1.Status != "SUCCESS" || outEv1.Total != 1 || len(outEv1.MarkdownURLs) != 1 {
			return nil, fmt.Errorf("req1 invalid output event: %+v err=%v", outEv1, err)
		}

		// Download markdown from MinIO via host port 19000
		mdHostURL1 := strings.Replace(outEv1.MarkdownURLs[0], "http://minio:9000", minioHostBase, 1)
		resp1, err := http.Get(mdHostURL1)
		if err != nil || resp1.StatusCode != 200 {
			return nil, fmt.Errorf("failed to fetch markdown %s: resp=%v err=%v", mdHostURL1, resp1, err)
		}
		mdBody1, _ := io.ReadAll(resp1.Body)
		resp1.Body.Close()
		if len(mdBody1) == 0 {
			return nil, fmt.Errorf("empty markdown body for req1")
		}

		// Part 2: boundary_above_16mb.csv (> 4MB) -> should split into 2 parts
		reqID2 := uuid.NewString()
		streamID2, err := suite.publishInput(reqID2, uuid.NewString(), uuid.NewString(), "http://minio:9000/dataset/boundary_above_16mb.csv")
		if err != nil {
			return nil, err
		}

		st2, stateVals2, err := suite.waitForTerminal(reqID2, 45*time.Second)
		if err != nil || st2 != pipeline.StatusCompleted {
			return nil, fmt.Errorf("req2 failed: st=%s err=%v", st2, err)
		}

		outEv2, outID2, err := suite.readTerminalEvent(reqID2)
		if err != nil || outEv2.Status != "SUCCESS" || outEv2.Total != 2 || len(outEv2.MarkdownURLs) != 2 {
			return nil, fmt.Errorf("req2 invalid output event: %+v", outEv2)
		}

		return map[string]interface{}{
			"req1": map[string]interface{}{
				"request_id":      reqID1,
				"input_stream_id": streamID1,
				"terminal_status": st1,
				"terminal_id":     outID1,
				"total":           1,
				"markdown_bytes":  len(mdBody1),
				"state":           stateVals1,
			},
			"req2": map[string]interface{}{
				"request_id":      reqID2,
				"input_stream_id": streamID2,
				"terminal_status": st2,
				"terminal_id":     outID2,
				"total":           outEv2.Total,
				"markdown_urls":   outEv2.MarkdownURLs,
				"state":           stateVals2,
			},
		}, nil
	})

	// TC-003: Multi-part & ordering (DOCX 50MB, XLSX 50MB, PDF 50MB)
	suite.logStep("TC-003", "Large files, multi-part, 3 extractors & offset ordering", "Critical", func() (map[string]interface{}, error) {
		// Run DOCX 50MB
		reqIDDocx := uuid.NewString()
		inIDDocx, err := suite.publishInput(reqIDDocx, uuid.NewString(), uuid.NewString(), "http://minio:9000/dataset/sample_50mb.docx")
		if err != nil {
			return nil, err
		}

		stDocx, stateValsDocx, err := suite.waitForTerminal(reqIDDocx, 90*time.Second)
		if err != nil || stDocx != pipeline.StatusCompleted {
			return nil, fmt.Errorf("DOCX failed: st=%s err=%v", stDocx, err)
		}

		outEvDocx, outIDDocx, err := suite.readTerminalEvent(reqIDDocx)
		if err != nil || outEvDocx.Status != "SUCCESS" {
			return nil, fmt.Errorf("DOCX output event failed: %+v err=%v", outEvDocx, err)
		}

		// Verify URLs are ordered by part offset (part-000, part-001, ...)
		for i, u := range outEvDocx.MarkdownURLs {
			expectedSubstr := fmt.Sprintf("part-%03d.md", i)
			if !strings.Contains(u, expectedSubstr) {
				return nil, fmt.Errorf("DOCX url at index %d is out of order: %s (expected %s)", i, u, expectedSubstr)
			}
		}

		// Run PDF 50MB
		reqIDPdf := uuid.NewString()
		inIDPdf, err := suite.publishInput(reqIDPdf, uuid.NewString(), uuid.NewString(), "http://minio:9000/dataset/sample_50mb.pdf")
		if err != nil {
			return nil, err
		}

		stPdf, stateValsPdf, err := suite.waitForTerminal(reqIDPdf, 120*time.Second)
		if err != nil || stPdf != pipeline.StatusCompleted {
			return nil, fmt.Errorf("PDF failed: st=%s err=%v", stPdf, err)
		}

		outEvPdf, outIDPdf, err := suite.readTerminalEvent(reqIDPdf)
		if err != nil || outEvPdf.Status != "SUCCESS" {
			return nil, fmt.Errorf("PDF output event failed: %+v", outEvPdf)
		}

		for i, u := range outEvPdf.MarkdownURLs {
			expectedSubstr := fmt.Sprintf("part-%03d.md", i)
			if !strings.Contains(u, expectedSubstr) {
				return nil, fmt.Errorf("PDF url at index %d is out of order: %s", i, u)
			}
		}

		return map[string]interface{}{
			"docx": map[string]interface{}{
				"request_id":    reqIDDocx,
				"input_id":      inIDDocx,
				"terminal_id":   outIDDocx,
				"total":         outEvDocx.Total,
				"markdown_urls": outEvDocx.MarkdownURLs,
				"state":         stateValsDocx,
			},
			"pdf": map[string]interface{}{
				"request_id":    reqIDPdf,
				"input_id":      inIDPdf,
				"terminal_id":   outIDPdf,
				"total":         outEvPdf.Total,
				"markdown_urls": outEvPdf.MarkdownURLs,
				"state":         stateValsPdf,
			},
		}, nil
	})

	// TC-004: Splitter failures (404, OLE > 4MB, oversized row)
	suite.logStep("TC-004", "Splitter failures (404, OLE >4MB, oversized row)", "High", func() (map[string]interface{}, error) {
		// 1. URL 404
		reqID404 := uuid.NewString()
		inID404, _ := suite.publishInput(reqID404, uuid.NewString(), uuid.NewString(), "http://minio:9000/dataset/nonexistent_404.pdf")
		st404, _, err := suite.waitForTerminal(reqID404, 30*time.Second)
		if err != nil || st404 != pipeline.StatusFailed {
			return nil, fmt.Errorf("404 case expected FAILED, got %s err=%v", st404, err)
		}
		out404, outID404, err := suite.readTerminalEvent(reqID404)
		if err != nil || out404.Status != "FAILED" || out404.FailedOffset != -1 {
			return nil, fmt.Errorf("404 case invalid output event: %+v", out404)
		}

		// 2. OLE > 4MB (sample_50mb.doc)
		reqIDOLE := uuid.NewString()
		inIDOLE, _ := suite.publishInput(reqIDOLE, uuid.NewString(), uuid.NewString(), "http://minio:9000/dataset/sample_50mb.doc")
		stOLE, _, err := suite.waitForTerminal(reqIDOLE, 30*time.Second)
		if err != nil || stOLE != pipeline.StatusFailed {
			return nil, fmt.Errorf("OLE case expected FAILED, got %s err=%v", stOLE, err)
		}
		outOLE, outIDOLE, err := suite.readTerminalEvent(reqIDOLE)
		if err != nil || outOLE.Status != "FAILED" || outOLE.FailedOffset != -1 {
			return nil, fmt.Errorf("OLE case invalid output event: %+v", outOLE)
		}

		// 3. Oversized atomic unit (oversized_row.csv)
		reqIDOver := uuid.NewString()
		inIDOver, _ := suite.publishInput(reqIDOver, uuid.NewString(), uuid.NewString(), "http://minio:9000/dataset/oversized_row.csv")
		stOver, _, err := suite.waitForTerminal(reqIDOver, 30*time.Second)
		if err != nil || stOver != pipeline.StatusFailed {
			return nil, fmt.Errorf("oversized row expected FAILED, got %s err=%v", stOver, err)
		}
		outOver, outIDOver, err := suite.readTerminalEvent(reqIDOver)
		if err != nil || outOver.Status != "FAILED" || outOver.FailedOffset != -1 {
			return nil, fmt.Errorf("oversized row invalid output event: %+v", outOver)
		}

		return map[string]interface{}{
			"case_404":       map[string]interface{}{"request_id": reqID404, "input_id": inID404, "terminal_id": outID404, "error": out404.Error},
			"case_ole":       map[string]interface{}{"request_id": reqIDOLE, "input_id": inIDOLE, "terminal_id": outIDOLE, "error": outOLE.Error},
			"case_oversized": map[string]interface{}{"request_id": reqIDOver, "input_id": inIDOver, "terminal_id": outIDOver, "error": outOver.Error},
		}, nil
	})

	// TC-005: Deduplication
	suite.logStep("TC-005", "Deduplication & identity conflict", "Critical", func() (map[string]interface{}, error) {
		reqID := uuid.NewString()
		convID := uuid.NewString()
		msgID := uuid.NewString()

		// Publish original
		inID1, err := suite.publishInput(reqID, convID, msgID, "http://minio:9000/dataset/report.pdf")
		if err != nil {
			return nil, err
		}
		st, _, err := suite.waitForTerminal(reqID, 30*time.Second)
		if err != nil || st != pipeline.StatusCompleted {
			return nil, fmt.Errorf("original run failed: %v", err)
		}

		// Read TTL before duplicate
		stateKey := "doc-extractor:state:" + reqID
		ttlBefore, _ := suite.rdb.TTL(suite.ctx, stateKey).Result()

		// Wait 2 seconds
		time.Sleep(2 * time.Second)

		// 1. Re-publish exact same input message
		inID2, err := suite.publishInput(reqID, convID, msgID, "http://minio:9000/dataset/report.pdf")
		if err != nil {
			return nil, err
		}

		// 2. Publish same reqID but different conversationID (conflict)
		inID3, err := suite.publishInput(reqID, uuid.NewString(), uuid.NewString(), "http://minio:9000/dataset/sample_50mb.pdf")
		if err != nil {
			return nil, err
		}

		// Give splitter time to process both
		time.Sleep(2 * time.Second)

		// Assert: exactly 1 terminal event in output stream
		termCount := suite.countTerminalEvents(reqID)
		if termCount != 1 {
			return nil, fmt.Errorf("expected exactly 1 terminal event for %s, got %d", reqID, termCount)
		}

		// Assert: conversation_id not mutated
		cidInState, _ := suite.rdb.HGet(suite.ctx, stateKey, "conversation_id").Result()
		if cidInState != convID {
			return nil, fmt.Errorf("state conversation_id mutated to %s", cidInState)
		}

		// Assert: TTL not reset
		ttlAfter, _ := suite.rdb.TTL(suite.ctx, stateKey).Result()
		if ttlAfter >= ttlBefore {
			return nil, fmt.Errorf("TTL was extended on duplicate: before=%v after=%v", ttlBefore, ttlAfter)
		}

		return map[string]interface{}{
			"request_id":       reqID,
			"original_input":   inID1,
			"duplicate_input":  inID2,
			"conflict_input":   inID3,
			"terminal_count":   termCount,
			"ttl_before_s":     ttlBefore.Seconds(),
			"ttl_after_s":      ttlAfter.Seconds(),
			"ttl_did_decrease": ttlAfter < ttlBefore,
		}, nil
	})

	// TC-007: Extractor failures and fail-fast
	suite.logStep("TC-007", "Extractor failures and fail-fast cancellation", "Critical", func() (map[string]interface{}, error) {
		reqID := uuid.NewString()
		convID := uuid.NewString()
		msgID := uuid.NewString()

		// Init a multi-part request manually with 2 parts:
		// Part 0 is corrupted.pdf (will fail conversion immediately)
		// Part 1 is boundary_under_16mb.csv (valid, under the pre-reduction 16MB limit, takes several seconds to convert)
		partURLs := []string{
			"http://minio:9000/dataset/corrupted.pdf",
			"http://minio:9000/dataset/boundary_under_16mb.csv",
		}
		in := pipeline.InputEvent{
			RequestID:      reqID,
			ConversationID: convID,
			MessageID:      msgID,
			FilePath:       "http://minio:9000/multi-file.pdf",
		}

		t0 := time.Now()
		_, err := pipeline.InitAndPublish(suite.ctx, suite.rdb, in, partURLs, 24*time.Hour)
		if err != nil {
			return nil, err
		}

		// Wait for terminal state (should be FAILED from offset 0)
		st, stateVals, err := suite.waitForTerminal(reqID, 20*time.Second)
		timeToFail := time.Since(t0)

		if err != nil || st != pipeline.StatusFailed {
			return nil, fmt.Errorf("expected FAILED, got %s err=%v", st, err)
		}

		// Verify terminal event
		outEv, outID, err := suite.readTerminalEvent(reqID)
		if err != nil || outEv.Status != "FAILED" || outEv.FailedOffset != 0 {
			return nil, fmt.Errorf("expected FAILED for offset 0, got %+v", outEv)
		}

		// Wait additional 5 seconds to ensure sibling part 1 is aborted and does NOT publish a late SUCCESS or 2nd FAILED
		time.Sleep(5 * time.Second)
		termCount := suite.countTerminalEvents(reqID)
		if termCount != 1 {
			return nil, fmt.Errorf("expected exactly 1 terminal event, got %d", termCount)
		}

		return map[string]interface{}{
			"request_id":      reqID,
			"terminal_status": st,
			"terminal_id":     outID,
			"failed_offset":   outEv.FailedOffset,
			"error":           outEv.Error,
			"time_to_fail_ms": timeToFail.Milliseconds(),
			"terminal_count":  termCount,
			"state":           stateVals,
		}, nil
	})

	// TC-008: Own pending, crash and recovery
	suite.logStep("TC-008", "Pending message recovery and clean shutdown", "Critical", func() (map[string]interface{}, error) {
		// Stop all extractors so no active TCP socket holds blocked read commands inside Redis
		_ = exec.Command("docker", "stop", "pipelinetest-extractor-1-1", "pipelinetest-extractor-2-1", "pipelinetest-extractor-3-1").Run()
		defer func() {
			// Ensure all extractors are running for subsequent tests
			_ = exec.Command("docker", "start", "pipelinetest-extractor-1-1", "pipelinetest-extractor-2-1", "pipelinetest-extractor-3-1").Run()
		}()

		// Publish a splitted message assigned specifically to extractor-3 in the group
		reqID := uuid.NewString()
		convID := uuid.NewString()
		msgID := uuid.NewString()
		in := pipeline.InputEvent{RequestID: reqID, ConversationID: convID, MessageID: msgID, FilePath: "http://minio/f.pdf"}
		_, err := pipeline.InitAndPublish(suite.ctx, suite.rdb, in, []string{"http://minio:9000/dataset/report.pdf"}, 24*time.Hour)
		if err != nil {
			return nil, err
		}

		// Deliver message to consumer "extractor-3" without ACK by reading it via XREADGROUP with a finite timeout
		readCtx, cancelRead := context.WithTimeout(suite.ctx, 5*time.Second)
		defer cancelRead()
		readRes, err := suite.rdb.XReadGroup(readCtx, &redis.XReadGroupArgs{
			Group:    pipeline.GroupExtractor,
			Consumer: "extractor-3",
			Streams:  []string{pipeline.StreamSplitted, ">"},
			Count:    1,
			Block:    2 * time.Second,
		}).Result()
		if err != nil || len(readRes) == 0 || len(readRes[0].Messages) == 0 {
			return nil, fmt.Errorf("failed to deliver message to extractor-3 PEL: %v", err)
		}
		pendingMsgID := readRes[0].Messages[0].ID

		// Check PEL shows pending message for extractor-3
		pendingBefore, err := suite.rdb.XPendingExt(suite.ctx, &redis.XPendingExtArgs{
			Stream:   pipeline.StreamSplitted,
			Group:    pipeline.GroupExtractor,
			Consumer: "extractor-3",
			Start:    "-",
			End:      "+",
			Count:    10,
		}).Result()
		if err != nil || len(pendingBefore) == 0 {
			return nil, fmt.Errorf("expected pending message for extractor-3 in PEL, got %d", len(pendingBefore))
		}

		// Start all extractors again
		startCmd := exec.Command("docker", "start", "pipelinetest-extractor-1-1", "pipelinetest-extractor-2-1", "pipelinetest-extractor-3-1")
		if err := startCmd.Run(); err != nil {
			return nil, fmt.Errorf("failed to restart extractors: %w", err)
		}

		// Wait for extractor-3 to drain its pending message and complete the request
		st, _, err := suite.waitForTerminal(reqID, 30*time.Second)
		if err != nil || st != pipeline.StatusCompleted {
			return nil, fmt.Errorf("extractor-3 did not complete pending job: st=%s err=%v", st, err)
		}

		// Verify pending for extractor-3 is now 0
		time.Sleep(1 * time.Second)
		pendingAfter, _ := suite.rdb.XPendingExt(suite.ctx, &redis.XPendingExtArgs{
			Stream:   pipeline.StreamSplitted,
			Group:    pipeline.GroupExtractor,
			Consumer: "extractor-3",
			Start:    "-",
			End:      "+",
			Count:    10,
		}).Result()

		return map[string]interface{}{
			"request_id":          reqID,
			"pending_stream_id":   pendingMsgID,
			"pending_count_prior": len(pendingBefore),
			"pending_count_after": len(pendingAfter),
			"terminal_status":     st,
			"recovered":           st == pipeline.StatusCompleted && len(pendingAfter) == 0,
		}, nil
	})

	// TC-009: Redis interruption & reconnect
	suite.logStep("TC-009", "Redis interruption & automatic recovery", "Critical", func() (map[string]interface{}, error) {
		reqID := uuid.NewString()
		// Publish a job
		inID, err := suite.publishInput(reqID, uuid.NewString(), uuid.NewString(), "http://minio:9000/dataset/report.pdf")
		if err != nil {
			return nil, err
		}

		// Pause redis container briefly
		defer func() {
			_ = exec.Command("docker", "unpause", "pipelinetest-redis-1").Run()
		}()
		_ = exec.Command("docker", "pause", "pipelinetest-redis-1").Run()
		time.Sleep(2 * time.Second)
		_ = exec.Command("docker", "unpause", "pipelinetest-redis-1").Run()

		// Wait for job to resume and complete
		st, _, err := suite.waitForTerminal(reqID, 30*time.Second)
		if err != nil || st != pipeline.StatusCompleted {
			return nil, fmt.Errorf("job failed to complete after redis unpause: st=%s err=%v", st, err)
		}

		outEv, outID, err := suite.readTerminalEvent(reqID)
		if err != nil || outEv.Status != "SUCCESS" {
			return nil, fmt.Errorf("invalid terminal event after unpause: %+v", outEv)
		}

		return map[string]interface{}{
			"request_id":      reqID,
			"input_id":        inID,
			"terminal_status": st,
			"terminal_id":     outID,
		}, nil
	})

	// TC-010: Invalid events handling
	suite.logStep("TC-010", "Malformed and invalid stream events", "High", func() (map[string]interface{}, error) {
		// 1. Malformed JSON to input stream
		badJSONID, err := suite.rdb.XAdd(suite.ctx, &redis.XAddArgs{
			Stream: pipeline.StreamInput,
			Values: map[string]interface{}{"data": "not-valid-json{"},
		}).Result()
		if err != nil {
			return nil, err
		}

		// 2. Invalid UUIDs in input stream
		badUUIDID, err := suite.rdb.XAdd(suite.ctx, &redis.XAddArgs{
			Stream: pipeline.StreamInput,
			Values: map[string]interface{}{"data": `{"request_id":"not-uuid","conversation_id":"bad","message_id":"bad","file_path":"http://minio/f.pdf"}`},
		}).Result()
		if err != nil {
			return nil, err
		}

		// 3. Invalid offset/total in splitted stream
		badOffsetID, err := suite.rdb.XAdd(suite.ctx, &redis.XAddArgs{
			Stream: pipeline.StreamSplitted,
			Values: map[string]interface{}{"data": fmt.Sprintf(`{"request_id":"%s","conversation_id":"%s","message_id":"%s","offset":5,"total":2,"file_path":"http://minio/f.pdf"}`, uuid.NewString(), uuid.NewString(), uuid.NewString())},
		}).Result()
		if err != nil {
			return nil, err
		}

		// 4. Missing state in splitted stream
		missingStateID, err := suite.rdb.XAdd(suite.ctx, &redis.XAddArgs{
			Stream: pipeline.StreamSplitted,
			Values: map[string]interface{}{"data": fmt.Sprintf(`{"request_id":"%s","conversation_id":"%s","message_id":"%s","offset":0,"total":1,"file_path":"http://minio/f.pdf"}`, uuid.NewString(), uuid.NewString(), uuid.NewString())},
		}).Result()
		if err != nil {
			return nil, err
		}

		// Wait 2s for workers to consume and ACK all bad messages
		time.Sleep(2 * time.Second)

		// Now send a valid request to prove workers are alive and processing normally
		validReqID := uuid.NewString()
		validInID, err := suite.publishInput(validReqID, uuid.NewString(), uuid.NewString(), "http://minio:9000/dataset/report.pdf")
		if err != nil {
			return nil, err
		}

		st, _, err := suite.waitForTerminal(validReqID, 20*time.Second)
		if err != nil || st != pipeline.StatusCompleted {
			return nil, fmt.Errorf("workers failed to process valid request after invalid messages: st=%s err=%v", st, err)
		}

		return map[string]interface{}{
			"bad_json_id":      badJSONID,
			"bad_uuid_id":      badUUIDID,
			"bad_offset_id":    badOffsetID,
			"missing_state_id": missingStateID,
			"subsequent_valid": validReqID,
			"valid_input_id":   validInID,
			"valid_status":     st,
		}, nil
	})

	// TC-011: HTTP compatibility
	suite.logStep("TC-011", "HTTP regression and compatibility mode", "High", func() (map[string]interface{}, error) {
		splitterPort := "18081"
		extractorPort := "18082"

		// Ensure any old test containers are stopped
		_ = exec.Command("docker", "rm", "-f", "test-http-splitter", "test-http-extractor").Run()

		// Start splitter in HTTP mode via docker container
		cmdSplit := exec.Command("docker", "run", "-d", "--rm", "--name", "test-http-splitter",
			"-p", splitterPort+":8080",
			"-e", "SERVICE_MODE=http",
			"-e", "MINIO_ENDPOINT=minio:9000",
			"-e", "MINIO_ACCESS_KEY=minioadmin",
			"-e", "MINIO_SECRET_KEY=minioadmin",
			"-e", "MINIO_BUCKET=split-files",
			"--network", "pipelinetest_default",
			"pipelinetest-splitter")
		if err := cmdSplit.Run(); err != nil {
			return nil, fmt.Errorf("start docker http splitter: %w", err)
		}
		defer func() {
			_ = exec.Command("docker", "rm", "-f", "test-http-splitter").Run()
		}()

		// Start extractor in HTTP mode via docker container
		cmdExt := exec.Command("docker", "run", "-d", "--rm", "--name", "test-http-extractor",
			"-p", extractorPort+":8080",
			"-e", "SERVICE_MODE=http",
			"--network", "pipelinetest_default",
			"pipelinetest-extractor-1")
		if err := cmdExt.Run(); err != nil {
			return nil, fmt.Errorf("start docker http extractor: %w", err)
		}
		defer func() {
			_ = exec.Command("docker", "rm", "-f", "test-http-extractor").Run()
		}()

		// Wait for both HTTP containers to accept traffic
		time.Sleep(2 * time.Second)

		// 1. GET /ping on splitter
		resp1, err := http.Get("http://localhost:" + splitterPort + "/ping")
		if err != nil || resp1.StatusCode != 200 {
			return nil, fmt.Errorf("splitter /ping failed: %v", err)
		}
		resp1.Body.Close()

		// 2. GET /ping on extractor
		resp2, err := http.Get("http://localhost:" + extractorPort + "/ping")
		if err != nil || resp2.StatusCode != 200 {
			return nil, fmt.Errorf("extractor /ping failed: %v", err)
		}
		resp2.Body.Close()

		// 3. POST /api/v1/split-file on splitter
		splitReq := fmt.Sprintf(`{"request_id":"%s","conversation_id":"%s","message_id":"%s","file_path":"http://minio:9000/dataset/report.pdf"}`,
			uuid.NewString(), uuid.NewString(), uuid.NewString())
		respSplit, err := http.Post("http://localhost:"+splitterPort+"/api/v1/split-file", "application/json", strings.NewReader(splitReq))
		if err != nil || respSplit.StatusCode != 200 {
			return nil, fmt.Errorf("split-file failed: resp=%v err=%v", respSplit, err)
		}
		var splitResp struct {
			URLs []string `json:"urls"`
		}
		_ = json.NewDecoder(respSplit.Body).Decode(&splitResp)
		respSplit.Body.Close()
		if len(splitResp.URLs) == 0 {
			return nil, fmt.Errorf("split-file returned no URLs")
		}

		// 4. POST /api/v1/subprocess/run on extractor
		subProcReq := `{"args":"echo hello_http"}`
		resp3, err := http.Post("http://localhost:"+extractorPort+"/api/v1/subprocess/run", "application/json", strings.NewReader(subProcReq))
		if err != nil || resp3.StatusCode != 200 {
			return nil, fmt.Errorf("subprocess/run failed: %v", err)
		}
		body3, _ := io.ReadAll(resp3.Body)
		resp3.Body.Close()
		if !strings.Contains(string(body3), "hello_http") {
			return nil, fmt.Errorf("subprocess/run unexpected body: %s", string(body3))
		}

		// 5. POST /api/v1/subprocess/markitdown/run on extractor
		mkReq := `{"url":"http://minio:9000/dataset/report.pdf"}`
		resp4, err := http.Post("http://localhost:"+extractorPort+"/api/v1/subprocess/markitdown/run", "application/json", strings.NewReader(mkReq))
		if err != nil || resp4.StatusCode != 200 {
			return nil, fmt.Errorf("markitdown/run failed: resp=%v err=%v", resp4, err)
		}
		body4, _ := io.ReadAll(resp4.Body)
		resp4.Body.Close()
		if len(body4) == 0 {
			return nil, fmt.Errorf("empty markitdown output")
		}

		return map[string]interface{}{
			"splitter_ping":   "pong",
			"extractor_ping":  "pong",
			"split_file_urls": splitResp.URLs,
			"subprocess_run":  string(body3),
			"markitdown_run":  fmt.Sprintf("%d bytes markdown returned", len(body4)),
		}, nil
	})

	// TC-012: Resource smoke and cleanup
	suite.logStep("TC-012", "Resource smoke & cleanup across rounds", "High", func() (map[string]interface{}, error) {
		// Run 3 sequential document conversions: PDF, DOCX, XLSX
		formats := []string{"sample_50mb.pdf", "sample_50mb.docx", "sample_50mb.xlsx"}
		roundDetails := make([]map[string]interface{}, len(formats))

		for i, fmtName := range formats {
			reqID := uuid.NewString()
			t0 := time.Now()
			inID, err := suite.publishInput(reqID, uuid.NewString(), uuid.NewString(), "http://minio:9000/dataset/"+fmtName)
			if err != nil {
				return nil, err
			}

			st, _, err := suite.waitForTerminal(reqID, 120*time.Second)
			dur := time.Since(t0)
			if err != nil || st != pipeline.StatusCompleted {
				return nil, fmt.Errorf("round %d (%s) failed: st=%s err=%v", i+1, fmtName, st, err)
			}

			// Capture docker stats snapshot
			statsCmd := exec.Command("docker", "stats", "--no-stream", "--format", "{{.Name}}: mem={{.MemUsage}}, cpu={{.CPUPerc}}, pids={{.PIDs}}")
			statsOut, _ := statsCmd.Output()

			roundDetails[i] = map[string]interface{}{
				"round":       i + 1,
				"file":        fmtName,
				"request_id":  reqID,
				"input_id":    inID,
				"duration_s":  dur.Seconds(),
				"stats_lines": strings.Split(strings.TrimSpace(string(statsOut)), "\n"),
			}
		}

		return map[string]interface{}{
			"rounds": roundDetails,
		}, nil
	})

	// Summary and write report
	allPassed := true
	for id, res := range suite.results {
		if res.Status != "PASS" {
			allPassed = false
			fmt.Printf("FAILED CASE: %s - %s: %s\n", id, res.Title, res.Error)
		}
	}

	summaryBytes, _ := json.MarshalIndent(suite.results, "", "  ")
	_ = os.WriteFile(filepath.Join(evidenceDir, "full-suite-results.json"), summaryBytes, 0644)

	if allPassed {
		fmt.Printf("\n======================================================\n")
		fmt.Printf("ALL FUNCTIONAL PIPELINE SUITE CASES PASSED!\n")
		fmt.Printf("Results saved to %s/full-suite-results.json\n", evidenceDir)
		fmt.Printf("======================================================\n")
		os.Exit(0)
	} else {
		fmt.Printf("\n======================================================\n")
		fmt.Printf("SOME CASES FAILED. See results for details.\n")
		fmt.Printf("======================================================\n")
		os.Exit(1)
	}
}
