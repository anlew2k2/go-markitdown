package services

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os/exec"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
)

// pythonBin is the interpreter used to run the persistent conversion
// worker script. Fixed, not configurable: the image this runs in
// (deploy/extractor.dockerfile, python:3.13-slim base) always provides it.
const pythonBin = "python3"

// maxProtocolLineBytes bounds one JSON-Lines protocol frame (request or
// response) so a malformed/runaway line can't grow a worker's read buffer
// without limit; requests and responses are both small (paths, an id, a
// bounded error string), so this is generous headroom, not a tight budget.
const maxProtocolLineBytes = 2 << 20 // 2 MiB

// PersistentConverter manages exactly n long-lived `python3
// markitdown_worker.py` subprocesses (see deploy/markitdown_worker.py for
// the protocol), each converting one file at a time, to avoid paying
// Python/MarkItDown startup and import cost on every single conversion (as
// the CLIConverter does). It implements Converter.
//
// Lifecycle: a worker is checked out of the pool for the duration of one
// Convert call and checked back in only after a valid response (success or
// a business-level conversion failure the worker itself reported) — never
// while a response might still be in flight. Any abnormal exit (ctx
// cancelled/expired, process crash, malformed/mismatched protocol response)
// kills that one worker's process group and starts a replacement in the
// background; it never silently serves from a smaller pool or falls back to
// the CLI. If a replacement itself fails to become ready (and the pool is
// not already shutting down), the backend marks itself unhealthy and calls
// onUnhealthy exactly once — intended to cancel the owning worker's root
// context so RunConsumerPool stops cleanly (same shutdown semantics as
// SIGTERM: in-flight jobs are left pending for recovery, never marked
// FAILED just because of this).
type PersistentConverter struct {
	scriptPath     string
	stderrLimit    int
	startupTimeout time.Duration

	poolCtx    context.Context
	poolCancel context.CancelFunc

	avail chan *pyWorker

	mu       sync.Mutex
	registry []*pyWorker

	closing   atomic.Bool
	closeOnce sync.Once

	unhealthy     atomic.Bool
	unhealthyOnce sync.Once
	onUnhealthy   func()
}

type pyWorker struct {
	cmd     *exec.Cmd
	stdin   io.WriteCloser
	lineCh  chan lineMsg
	exited  chan struct{}
	discard chan struct{}
	killOne sync.Once
	stderr  *limitedWriter
}

type lineMsg struct {
	text string
	err  error
}

type workerRequest struct {
	ID         string `json:"id"`
	InputPath  string `json:"input_path"`
	OutputPath string `json:"output_path"`
}

type workerResponse struct {
	ID        string `json:"id"`
	OK        bool   `json:"ok"`
	ConvertMS int64  `json:"convert_ms"`
	Error     string `json:"error"`
}

type workerReady struct {
	Type   string `json:"type"`
	InitMS int64  `json:"init_ms"`
}

// NewPersistentConverter spawns n worker processes (running scriptPath) and
// waits for every one of them to report ready within startupTimeout. If any
// worker fails to start or become ready, every already-spawned worker in
// this attempt is killed and an error is returned — the caller (extractor
// startup) must not begin consuming in that case ("fail the whole startup",
// per the implementation plan), not fall back to the CLI backend silently.
//
// ctx is this converter's lifetime context: cancelling it (process
// shutdown) kills every worker via Close. onUnhealthy, if non-nil, is
// called at most once if a worker dies later (after startup) and its
// replacement cannot be spawned while ctx is still live — the intended use
// is to cancel the caller's own worker-pool context so consumption stops.
func NewPersistentConverter(ctx context.Context, n int, scriptPath string, stderrLimit int, startupTimeout time.Duration, onUnhealthy func()) (*PersistentConverter, error) {
	if n < 1 {
		n = 1
	}
	poolCtx, poolCancel := context.WithCancel(ctx)
	pc := &PersistentConverter{
		scriptPath:     scriptPath,
		stderrLimit:    stderrLimit,
		startupTimeout: startupTimeout,
		poolCtx:        poolCtx,
		poolCancel:     poolCancel,
		avail:          make(chan *pyWorker, n),
		onUnhealthy:    onUnhealthy,
	}

	spawned := make([]*pyWorker, 0, n)
	for i := 0; i < n; i++ {
		w, err := pc.spawnWorker(poolCtx)
		if err != nil {
			for _, sw := range spawned {
				pc.killWorker(sw)
			}
			poolCancel()
			return nil, fmt.Errorf("persistent converter: start worker %d/%d: %w", i+1, n, err)
		}
		spawned = append(spawned, w)
	}

	pc.mu.Lock()
	pc.registry = spawned
	pc.mu.Unlock()
	for _, w := range spawned {
		pc.avail <- w
	}
	return pc, nil
}

// Convert implements Converter by checking out a worker, sending it one
// request and waiting for exactly one matching response, all bounded by
// ctx. A business-level conversion failure the worker itself reports (its
// process stays healthy) is returned as a plain error and the worker is
// checked back in; any other anomaly kills the worker, triggers a
// background replacement, and is also returned as an error so the caller's
// existing failure path (unchanged) handles it exactly like a CLIConverter
// error.
func (pc *PersistentConverter) Convert(ctx context.Context, inputPath, outputPath string) error {
	w, err := pc.checkout(ctx)
	if err != nil {
		return fmt.Errorf("persistent converter: %w", err)
	}

	reqID := uuid.NewString()
	payload, err := json.Marshal(workerRequest{ID: reqID, InputPath: inputPath, OutputPath: outputPath})
	if err != nil {
		pc.killAndReplace(w, err)
		return fmt.Errorf("persistent converter: encode request: %w", err)
	}
	payload = append(payload, '\n')

	if _, err := w.stdin.Write(payload); err != nil {
		pc.killAndReplace(w, err)
		return fmt.Errorf("persistent converter: write request: %w", err)
	}

	select {
	case <-ctx.Done():
		pc.killAndReplace(w, ctx.Err())
		return ctx.Err()
	case msg := <-w.lineCh:
		if msg.err != nil {
			pc.killAndReplace(w, msg.err)
			return fmt.Errorf("persistent converter: worker exited: %w (stderr: %s)", msg.err, w.stderr.buf.String())
		}
		var resp workerResponse
		if err := json.Unmarshal([]byte(msg.text), &resp); err != nil {
			pc.killAndReplace(w, err)
			return fmt.Errorf("persistent converter: malformed response %q: %w", msg.text, err)
		}
		if resp.ID != reqID {
			pc.killAndReplace(w, fmt.Errorf("response id %q, want %q", resp.ID, reqID))
			return fmt.Errorf("persistent converter: response id mismatch, worker desynced")
		}
		if !resp.OK {
			pc.checkin(w)
			errMsg := resp.Error
			if errMsg == "" {
				errMsg = "conversion failed (no error detail)"
			}
			return fmt.Errorf("markitdown (persistent) failed: %s", errMsg)
		}
		pc.checkin(w)
		return nil
	}
}

func (pc *PersistentConverter) checkout(ctx context.Context) (*pyWorker, error) {
	select {
	case w := <-pc.avail:
		return w, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-pc.poolCtx.Done():
		return nil, pc.poolCtx.Err()
	}
}

// checkin returns a worker that just completed a request cleanly (success
// or a business-level failure) to the pool. By construction a worker is
// held by exactly one caller at a time (either checked out or sitting in
// avail), so this never blocks.
func (pc *PersistentConverter) checkin(w *pyWorker) {
	pc.avail <- w
}

func (pc *PersistentConverter) killAndReplace(w *pyWorker, cause error) {
	pc.killWorker(w)
	if pc.closing.Load() {
		return
	}
	go pc.spawnReplacement(cause)
}

func (pc *PersistentConverter) spawnReplacement(cause error) {
	log.Printf("services: persistent converter worker died (%v), spawning replacement", cause)
	w, err := pc.spawnWorker(pc.poolCtx)
	if err != nil {
		if pc.poolCtx.Err() != nil {
			return // shutting down; not a health failure worth reporting
		}
		pc.markUnhealthy(err)
		return
	}
	pc.mu.Lock()
	pc.registry = append(pc.registry, w)
	pc.mu.Unlock()
	select {
	case pc.avail <- w:
	case <-pc.poolCtx.Done():
		pc.killWorker(w)
	}
}

func (pc *PersistentConverter) markUnhealthy(err error) {
	pc.unhealthyOnce.Do(func() {
		pc.unhealthy.Store(true)
		log.Printf("services: persistent converter backend unhealthy, stopping consumption: %v", err)
		if pc.onUnhealthy != nil {
			pc.onUnhealthy()
		}
	})
}

// Healthy reports whether the backend has not yet hit an unrecoverable
// replacement failure. Informational only (e.g. for a health endpoint);
// callers must not use it to gate correctness.
func (pc *PersistentConverter) Healthy() bool { return !pc.unhealthy.Load() }

// Close kills every worker this converter has ever spawned (idle or
// in-flight — an in-flight one is also being killed by its own Convert
// call noticing poolCtx/ctx cancellation, so this is a safety net, not the
// only path) and waits for all of them to be reaped. Idempotent and safe
// to call more than once; never spawns a replacement once called.
func (pc *PersistentConverter) Close() {
	pc.closeOnce.Do(func() {
		pc.closing.Store(true)
		pc.poolCancel()
		pc.mu.Lock()
		workers := append([]*pyWorker(nil), pc.registry...)
		pc.mu.Unlock()
		for _, w := range workers {
			pc.killWorker(w)
		}
	})
}

func (pc *PersistentConverter) killWorker(w *pyWorker) {
	w.killOne.Do(func() {
		close(w.discard)
		if w.cmd.Process != nil {
			_ = syscall.Kill(-w.cmd.Process.Pid, syscall.SIGKILL)
		}
	})
	<-w.exited
	_ = w.stdin.Close()
}

func (pc *PersistentConverter) spawnWorker(parent context.Context) (*pyWorker, error) {
	cmd := exec.Command(pythonBin, "-u", pc.scriptPath)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("stdout pipe: %w", err)
	}
	stderr := &limitedWriter{limit: pc.stderrLimit}
	cmd.Stderr = stderr

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start worker process: %w", err)
	}

	w := &pyWorker{
		cmd:     cmd,
		stdin:   stdin,
		lineCh:  make(chan lineMsg, 1),
		exited:  make(chan struct{}),
		discard: make(chan struct{}),
		stderr:  stderr,
	}

	go func() {
		w.cmd.Wait() //nolint:errcheck // exit status/err not needed; stderr buffer carries diagnostics
		close(w.exited)
	}()
	go readWorkerLines(w, stdout)

	readyCtx, cancel := context.WithTimeout(parent, pc.startupTimeout)
	defer cancel()

	select {
	case msg := <-w.lineCh:
		if msg.err != nil {
			pc.killWorker(w)
			return nil, fmt.Errorf("worker exited before ready: %w (stderr: %s)", msg.err, stderr.buf.String())
		}
		var ready workerReady
		if err := json.Unmarshal([]byte(msg.text), &ready); err != nil || ready.Type != "ready" {
			pc.killWorker(w)
			return nil, fmt.Errorf("unexpected startup message %q (stderr: %s)", msg.text, stderr.buf.String())
		}
		return w, nil
	case <-readyCtx.Done():
		pc.killWorker(w)
		return nil, fmt.Errorf("worker did not become ready within %s (stderr: %s)", pc.startupTimeout, stderr.buf.String())
	}
}

// readWorkerLines scans newline-delimited protocol frames off stdout and
// forwards each to w.lineCh, or a final lineMsg carrying the scan/EOF error
// once the process stops producing output. It always selects against
// w.discard (closed exactly once by killWorker) instead of sending
// unconditionally, so a worker that is killed while a message is in flight
// never leaves this goroutine blocked forever on a channel nobody reads
// again.
func readWorkerLines(w *pyWorker, stdout io.Reader) {
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 4096), maxProtocolLineBytes)
	for scanner.Scan() {
		select {
		case w.lineCh <- lineMsg{text: scanner.Text()}:
		case <-w.discard:
			return
		}
	}
	err := scanner.Err()
	if err == nil {
		err = io.ErrUnexpectedEOF
	}
	select {
	case w.lineCh <- lineMsg{err: err}:
	case <-w.discard:
	}
}
