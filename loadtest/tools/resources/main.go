// Command resources is the Docker/cgroup resource-and-OOM monitor for the
// event-driven-pipeline load test. It is a support tool only: this change
// does not execute it, and it has not been run against a live
// docker-compose stack in this session. Treat every collected field as
// best-effort; anything that cannot be confirmed is reported as "UNKNOWN",
// never defaulted to false/OK (see docs/load-test-plan-event-sourcing.md
// section 6).
//
// It deliberately does not add Prometheus/cAdvisor: it shells out to the
// `docker` CLI already on the operator's machine for stats/events/inspect,
// and attempts a best-effort cgroup v2 read per container via two methods
// (in order):
//
//  1. `docker exec <container> cat /sys/fs/cgroup/...` -- works when the
//     container image has a shell + coreutils (e.g. the extractor's
//     python:3.13-slim base).
//  2. A short-lived privileged helper container sharing the target's PID
//     namespace with the host's root filesystem bind-mounted read-only --
//     needed for a distroless/scratch image with no shell at all (e.g. the
//     splitter's gcr.io/distroless/static-debian12 base). This is the
//     documented macOS workaround for reading cgroup files that live inside
//     Docker Desktop's Linux VM, not on the host filesystem directly.
//
// Usage:
//
//	go run ./loadtest/tools/resources \
//	    -compose-file deploy/docker-compose.test.yaml \
//	    -services splitter,extractor-1,extractor-2,extractor-3,redis,minio \
//	    -out-dir docs/test-results/event-sourcing/<run>/resources
//
// Runs until interrupted (Ctrl-C / SIGTERM); start it before warm-up and
// keep it running through the end of drain, per the load test plan.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

func main() {
	composeFile := flag.String("compose-file", "deploy/docker-compose.test.yaml", "docker-compose file to resolve service -> container ID from")
	servicesCSV := flag.String("services", "", "comma-separated compose service names to monitor (required; e.g. splitter,extractor-1,extractor-2,redis,minio)")
	outDir := flag.String("out-dir", "", "directory to write stats.jsonl / events.jsonl / cgroup.jsonl / summary.json into (required)")
	cgroupInterval := flag.Duration("cgroup-interval", 2*time.Second, "cgroup poll interval")
	cgroupHelperImage := flag.String("cgroup-helper-image", "alpine:3.20", "image used for the privileged cgroup-read fallback")
	flag.Parse()

	if *servicesCSV == "" || *outDir == "" {
		fmt.Fprintln(os.Stderr, "resources: -services and -out-dir are required")
		os.Exit(2)
	}
	services := strings.Split(*servicesCSV, ",")
	for i := range services {
		services[i] = strings.TrimSpace(services[i])
	}

	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		fatal("create out-dir: %v", err)
	}

	containerIDs, err := resolveContainerIDs(*composeFile, services)
	if err != nil {
		fatal("resolve container IDs from compose: %v", err)
	}
	fmt.Printf("resources: monitoring %d container(s): %v\n", len(containerIDs), containerIDs)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); runDockerStats(ctx, containerIDs, filepath.Join(*outDir, "stats.jsonl")) }()
	go func() { defer wg.Done(); runDockerEvents(ctx, containerIDs, filepath.Join(*outDir, "events.jsonl")) }()
	go func() {
		defer wg.Done()
		runCgroupPoller(ctx, containerIDs, *cgroupHelperImage, *cgroupInterval, filepath.Join(*outDir, "cgroup.jsonl"))
	}()

	startedAt := time.Now().UTC()
	<-ctx.Done()
	fmt.Println("resources: stop signal received, finishing in-flight collectors...")
	wg.Wait()

	summary := map[string]interface{}{
		"started_at":    startedAt.Format(time.RFC3339),
		"stopped_at":    time.Now().UTC().Format(time.RFC3339),
		"compose_file":  *composeFile,
		"services":      services,
		"container_ids": containerIDs,
		"note":          "oom/die/restart detection lives in events.jsonl and cgroup.jsonl (memory.events oom_kill deltas), not in this summary -- see docs/load-test-plan-event-sourcing.md section 6 for how to read them. Exit code 137 alone is NOT proof of OOM.",
	}
	writeJSON(filepath.Join(*outDir, "summary.json"), summary)
	fmt.Println("resources: stopped")
}

func fatal(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "resources: "+format+"\n", args...)
	os.Exit(1)
}

func writeJSON(path string, v interface{}) {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "resources: marshal %s: %v\n", path, err)
		return
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "resources: write %s: %v\n", path, err)
	}
}

// resolveContainerIDs asks docker compose for each service's container ID
// rather than hardcoding names, so a scaled-out extractor fleet (e.g.
// deploy/docker-compose.test.yaml's extractor-1..3) is picked up correctly.
func resolveContainerIDs(composeFile string, services []string) ([]string, error) {
	var ids []string
	for _, svc := range services {
		out, err := exec.Command("docker", "compose", "-f", composeFile, "ps", "-q", svc).Output()
		if err != nil {
			return nil, fmt.Errorf("docker compose ps -q %s: %w", svc, err)
		}
		id := strings.TrimSpace(string(out))
		if id == "" {
			return nil, fmt.Errorf("service %q has no running container (is the stack up?)", svc)
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// ---------------------------------------------------------------------------
// docker stats: one continuous stream for every monitored container,
// avoiding the "N sequential docker stats calls + sleep" pattern that
// produces non-simultaneous CPU/RAM snapshots with a real cycle time longer
// than the intended interval (see docs/load-test-review-event-sourcing.md
// history section 5).
// ---------------------------------------------------------------------------
func runDockerStats(ctx context.Context, containerIDs []string, outPath string) {
	f, err := os.Create(outPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "resources: open %s: %v\n", outPath, err)
		return
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()

	args := append([]string{"stats", "--format", "{{json .}}"}, containerIDs...)
	cmd := exec.CommandContext(ctx, "docker", args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		fmt.Fprintf(os.Stderr, "resources: docker stats pipe: %v\n", err)
		return
	}
	if err := cmd.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "resources: docker stats start: %v\n", err)
		return
	}

	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		var parsed map[string]interface{}
		if err := json.Unmarshal([]byte(line), &parsed); err != nil {
			// `docker stats` output format/fields vary across Docker
			// versions; a line that doesn't parse is recorded verbatim
			// under a raw key so nothing is silently dropped, rather than
			// assumed malformed.
			parsed = map[string]interface{}{"raw": line, "parse_error": err.Error()}
		}
		parsed["collected_at"] = time.Now().UTC().Format(time.RFC3339Nano)
		if encoded, err := json.Marshal(parsed); err == nil {
			w.Write(encoded)
			w.WriteByte('\n')
			w.Flush()
		}
	}
	_ = cmd.Wait()
}

// ---------------------------------------------------------------------------
// docker events: continuous, filtered to the monitored containers plus the
// event types the load test plan cares about. Exit code / OOMKilled /
// restart count are cross-checked via `docker inspect` on every oom/die/
// restart/start event, since SIGKILL (exit 137) alone does not prove OOM
// and a Python child can be OOM-killed while the Go PID 1 stays alive.
// ---------------------------------------------------------------------------
func runDockerEvents(ctx context.Context, containerIDs []string, outPath string) {
	f, err := os.Create(outPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "resources: open %s: %v\n", outPath, err)
		return
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()

	args := []string{"events", "--format", "{{json .}}"}
	for _, id := range containerIDs {
		args = append(args, "--filter", "container="+id)
	}
	for _, ev := range []string{"oom", "die", "restart", "start"} {
		args = append(args, "--filter", "event="+ev)
	}
	cmd := exec.CommandContext(ctx, "docker", args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		fmt.Fprintf(os.Stderr, "resources: docker events pipe: %v\n", err)
		return
	}
	if err := cmd.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "resources: docker events start: %v\n", err)
		return
	}

	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		var parsed map[string]interface{}
		if err := json.Unmarshal([]byte(line), &parsed); err != nil {
			parsed = map[string]interface{}{"raw": line, "parse_error": err.Error()}
		}
		parsed["collected_at"] = time.Now().UTC().Format(time.RFC3339Nano)

		if id, ok := parsed["id"].(string); ok && id != "" {
			parsed["inspect_followup"] = inspectContainer(id)
		}

		if encoded, err := json.Marshal(parsed); err == nil {
			w.Write(encoded)
			w.WriteByte('\n')
			w.Flush()
		}
	}
	_ = cmd.Wait()
}

type inspectResult struct {
	OOMKilled    interface{} `json:"oom_killed"`
	ExitCode     interface{} `json:"exit_code"`
	RestartCount interface{} `json:"restart_count"`
	Error        string      `json:"error,omitempty"`
}

func inspectContainer(id string) inspectResult {
	out, err := exec.Command("docker", "inspect", id,
		"-f", `{{.State.OOMKilled}}|{{.State.ExitCode}}|{{.RestartCount}}`).Output()
	if err != nil {
		return inspectResult{OOMKilled: "UNKNOWN", ExitCode: "UNKNOWN", RestartCount: "UNKNOWN", Error: err.Error()}
	}
	parts := strings.SplitN(strings.TrimSpace(string(out)), "|", 3)
	if len(parts) != 3 {
		return inspectResult{OOMKilled: "UNKNOWN", ExitCode: "UNKNOWN", RestartCount: "UNKNOWN", Error: "unexpected docker inspect output: " + string(out)}
	}
	return inspectResult{OOMKilled: parts[0], ExitCode: parts[1], RestartCount: parts[2]}
}

// ---------------------------------------------------------------------------
// cgroup v2 polling: memory.current / memory.peak / memory.events /
// memory.swap.current / cpu.stat, read per container every
// -cgroup-interval. Every failure mode is reported as an explicit
// "UNKNOWN" + reason string in the output, never defaulted to a numeric
// zero or a false "ok", per the load test plan's explicit requirement.
// ---------------------------------------------------------------------------
type cgroupSample struct {
	CollectedAt string            `json:"collected_at"`
	ContainerID string            `json:"container_id"`
	Method      string            `json:"method"` // "docker_exec" | "privileged_helper" | "unavailable"
	Values      map[string]string `json:"values,omitempty"`
	Status      string            `json:"status"` // "ok" | "UNKNOWN"
	Reason      string            `json:"reason,omitempty"`
}

func runCgroupPoller(ctx context.Context, containerIDs []string, helperImage string, interval time.Duration, outPath string) {
	f, err := os.Create(outPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "resources: open %s: %v\n", outPath, err)
		return
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, id := range containerIDs {
				sample := readCgroupSample(ctx, id, helperImage)
				if encoded, err := json.Marshal(sample); err == nil {
					w.Write(encoded)
					w.WriteByte('\n')
				}
			}
			w.Flush()
		}
	}
}

const cgroupFiles = "memory.current memory.peak memory.events memory.swap.current cpu.stat"

func readCgroupSample(ctx context.Context, containerID, helperImage string) cgroupSample {
	now := time.Now().UTC().Format(time.RFC3339Nano)

	// Method 1: docker exec with a shell (works for shell-bearing images,
	// e.g. the extractor's python:3.13-slim base; fails fast for a
	// distroless/scratch image such as the splitter).
	if values, err := execCatCgroupFiles(ctx, containerID); err == nil {
		return cgroupSample{CollectedAt: now, ContainerID: containerID, Method: "docker_exec", Values: values, Status: "ok"}
	}

	// Method 2: privileged helper sharing the target's PID namespace, with
	// the host (i.e. Docker Desktop's Linux VM, on macOS) root filesystem
	// bind-mounted read-only. This is the standard workaround for reading
	// cgroup files when the target container has no shell/coreutils at all.
	values, err := privilegedHelperCgroupFiles(ctx, containerID, helperImage)
	if err != nil {
		return cgroupSample{
			CollectedAt: now, ContainerID: containerID, Method: "unavailable",
			Status: "UNKNOWN",
			Reason: "docker exec failed (no shell in target image?) AND privileged helper failed: " + err.Error(),
		}
	}
	return cgroupSample{CollectedAt: now, ContainerID: containerID, Method: "privileged_helper", Values: values, Status: "ok"}
}

func execCatCgroupFiles(ctx context.Context, containerID string) (map[string]string, error) {
	cmd := exec.CommandContext(ctx, "docker", "exec", containerID, "sh", "-c",
		`for f in `+cgroupFiles+`; do echo "== $f =="; cat "/sys/fs/cgroup/$f" 2>&1; done`)
	out, err := cmd.Output()
	if err != nil {
		return nil, err
	}
	return parseCatOutput(string(out)), nil
}

// privilegedHelperCgroupFiles resolves containerID's host PID via `docker
// inspect`, then runs a short-lived helper container that shares that PID's
// namespace and has the VM root bind-mounted at /hostfs, to resolve the
// unified cgroup v2 path from /proc/<pid>/cgroup and cat the files under it.
// This never touches the macOS host filesystem directly: on Docker Desktop,
// cgroups live inside the Linux VM, which only another container (or a
// privileged exec into one) can see.
func privilegedHelperCgroupFiles(ctx context.Context, containerID, helperImage string) (map[string]string, error) {
	pidOut, err := exec.CommandContext(ctx, "docker", "inspect", containerID, "-f", "{{.State.Pid}}").Output()
	if err != nil {
		return nil, fmt.Errorf("docker inspect pid: %w", err)
	}
	pid := strings.TrimSpace(string(pidOut))
	if pid == "" || pid == "0" {
		return nil, fmt.Errorf("container has no live PID (not running?)")
	}
	if _, err := strconv.Atoi(pid); err != nil {
		return nil, fmt.Errorf("unexpected PID from docker inspect: %q", pid)
	}

	script := fmt.Sprintf(`
set -e
cg=$(cat /proc/%s/cgroup | awk -F: '{print $3}' | head -n1)
if [ -z "$cg" ]; then echo "NO_CGROUP_PATH"; exit 1; fi
base="/hostfs/sys/fs/cgroup$cg"
for f in %s; do
  echo "== $f =="
  cat "$base/$f" 2>&1 || echo "MISSING"
done
`, pid, cgroupFiles)

	cmd := exec.CommandContext(ctx, "docker", "run", "--rm",
		"--pid=host", "--privileged",
		"-v", "/:/hostfs:ro",
		helperImage, "sh", "-c", script)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("privileged helper: %w (output: %s)", err, strings.TrimSpace(string(out)))
	}
	if strings.Contains(string(out), "NO_CGROUP_PATH") {
		return nil, fmt.Errorf("could not resolve unified cgroup path from /proc/%s/cgroup (cgroup v1 host, or unsupported layout)", pid)
	}
	return parseCatOutput(string(out)), nil
}

func parseCatOutput(out string) map[string]string {
	values := map[string]string{}
	var current string
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimRight(line, "\r")
		if strings.HasPrefix(line, "== ") && strings.HasSuffix(line, " ==") {
			current = strings.TrimSuffix(strings.TrimPrefix(line, "== "), " ==")
			continue
		}
		if current == "" {
			continue
		}
		if existing, ok := values[current]; ok {
			values[current] = existing + "\n" + line
		} else {
			values[current] = line
		}
	}
	return values
}
