# Event-Driven Document Pipeline Load Test

k6 load-test scaffold for the Event-Driven Document Pipeline (Redis Streams architecture), plus the support tooling under [`../loadtest/`](../loadtest) that performs post-run drain/reconciliation and resource/OOM monitoring. Written against:

- [requirement-event-sourcing.md](../docs/requirement-event-sourcing.md)
- [implementation-plan-event-sourcing.md](../docs/implementation-plan-event-sourcing.md)
- [load-test-plan-event-sourcing.md](../docs/load-test-plan-event-sourcing.md)
- [load-test-review-event-sourcing.md](../docs/load-test-review-event-sourcing.md)

**Status: fixture dataset prepared and verified; the k6 load test itself has not been run.** All 13 entries in `k6/fixtures/manifest.example.json` are uploaded to the live MinIO `dataset` bucket and `"verified": true` -- confirmed by fetching and hashing each object directly (`go run ./loadtest/tools/verifyfixtures -write`), not inherited from old evidence. The six small PDF/DOCX/XLSX fixtures and both boundary CSVs were generated with real, varied content by `tmp/generate_real_fixtures.py` (not padding) and uploaded with `go run ./loadtest/tools/seedfixtures`; re-run both if the MinIO volume is ever reset. What's still outstanding: `expected_parts` for every `large` fixture (requires an actual publish + smoke run through the Splitter/Extractor, not just an upload), and the k6 scenarios / `drain` / `resources` tools themselves have not been executed against a live run.

---

## 1. Architecture

```
k6/
  load_test.js          -- main scenario: one workload per invocation
  lib/
    manifest.js          -- fixture manifest loading + validation + pool selection
    rng.js                -- deterministic seeded shuffle / ratio interleave
    redisClient.js         -- REDIS_URL parsing
    stages.js               -- per-workload stage schedule + VU-allocation check
  fixtures/
    manifest.example.json -- fixture metadata (see section 3)

loadtest/
  manifest/manifest.go      -- Go port of the manifest schema, shared by the tools below
  tools/
    verifyfixtures/         -- confirms real bytes/sha256/size_class against MinIO, can -write them back
    drain/                  -- post-run drain & reconciliation (record / run subcommands)
    resources/               -- Docker stats/events + cgroup v2 monitor
```

1. **Arrival scheduling:** `ramping-arrival-rate`, one scenario per invocation, selected by `WORKLOAD` (`small`, `large`, or `mixed`). Baseline level is a hold only; every level after it gets its own `{ramp, hold}` pair at the same target -- never a ramp straight to the next target, and never a ramp-down to 0 used as "drain" (see `docs/load-test-plan-event-sourcing.md` section 3).
2. **Deterministic fixture scheduling:** fixture choice is a pure function of `exec.scenario.iterationInTest` (the scenario-wide sequence number, not a per-VU counter), so the exact sequence is reproducible from `WORKLOAD_SEED` + `MIX_SMALL_RATIO` and is logged in full at `RUN_START`.
3. **One RUN_ID per run, shared correctly:** generated once in `setup()` (which k6 runs exactly once, in its own VU) and passed to every VU via the `data` argument of `default()`. It is never computed at module top level, since k6 re-evaluates top-level code independently in every VU's JS runtime -- a top-level `Date.now()` fallback would silently give each VU a different id.
4. **Admission guardrail:** a Redis-backed outstanding-request counter and an external stop-flag key, checked before every publish (section 5).
5. **Direct stream ingestion + state polling:** unchanged in spirit from the original scaffold -- `XADD` to `stream:doc-extractor:input`, poll `doc-extractor:state:<request_id>` via `HGET` every `POLL_INTERVAL_MS` against an elapsed-time deadline.
6. **No authoritative throughput claim from k6 itself.** k6 only logs structured JSON lines to stdout. Authoritative accepted/SUCCESS/FAILED/unresolved counts, E2E latency, and throughput windows come from `loadtest/tools/drain`, which reads `stream:doc-extractor:output` directly and joins it against k6's log and the fixture manifest.

---

## 2. Event contract

Unchanged from the pipeline implementation (`internal/pipeline/events.go`, `internal/pipeline/redis.go`):

- **Input stream:** `stream:doc-extractor:input`, field `data`, JSON `{request_id, conversation_id, message_id, file_path}` (all three IDs must be valid UUIDv4; `file_path` must be an absolute http(s) URL reachable by the Splitter/Extractor containers).
- **State hash:** `doc-extractor:state:<request_id>`, field `status` in `PROCESSING | COMPLETED | FAILED`.
- **Output stream:** `stream:doc-extractor:output`, one terminal event per request with `status` in `SUCCESS | FAILED`.
- **Splitting threshold:** a file `<= 16 MiB` (`maxSplitPartSize` in `internal/usecase/splitfile.go`) is never split; the original URL is returned as the sole part. Only a file over that threshold is split, with part count depending on the document's internal structure, not just its byte size.

### Outcome classification (k6-side, provisional)

| Outcome | Meaning | k6 metric |
|---|---|---|
| `ADMISSION_STOPPED` | Guardrail blocked this iteration before publish | `admission_stopped` |
| `PUBLISH_UNKNOWN` | `XADD` reply lost/failed; Redis may still have received it | `publish_errors` |
| `COMPLETED` (outcome field) | State hash reached `COMPLETED` | `observed_successes`, `e2e_duration` |
| `FAILED` (outcome field) | State hash reached `FAILED` | `terminal_failures`, `failed_duration` |
| `OBSERVATION_TIMEOUT` | Deadline hit while still `PROCESSING`/pending; **not** cancelled on the backend, **not** a worker failure | `observation_timeouts` |

Every custom metric is tagged with only `{workload, size_class, format}` -- never `request_id` (that would create one time series per request). **Always read `e2e_duration`/`failed_duration` next to the failure/timeout/admission-stopped counts for the same tag combination**: a clean p95 computed only from `COMPLETED` samples is survivorship-biased if a meaningful share of that cohort failed, timed out, or was never admitted.

These k6-side outcomes are **provisional**. The authoritative classification is produced by `loadtest/tools/drain run` from `stream:doc-extractor:output` + the input stream, per section 5 below.

---

## 3. Fixture manifest

`k6/fixtures/manifest.example.json` is the schema and a concrete starting point -- not a finished, fully-verified dataset. Each fixture entry:

```json
{
  "id": "pdf-large-23mib-real",
  "format": "pdf",
  "size_class": "large",
  "url": "http://minio:9000/dataset/system-design-interview.pdf",
  "url_host": "http://localhost:19000/dataset/system-design-interview.pdf",
  "bytes": 23088726,
  "sha256": "d2bafdef...",
  "expected_parts": null,
  "verified": true,
  "verified_at": "2026-11-14",
  "verified_by": "docs/test-results/event-sourcing/run-001/manifests/fixture-manifest.json",
  "notes": "..."
}
```

- `size_class` is **small** (`<= 16 MiB`, unsplit) or **large** (`> 16 MiB`, split). It must reflect the real byte count, not a label chosen for convenience.
- `url` is the MinIO address reachable **inside the docker-compose network** -- this is what gets embedded in the input event's `file_path`, since the Splitter/Extractor containers fetch it, not the k6 host. `url_host` is the host-reachable equivalent (e.g. via `deploy/docker-compose.test.yaml`'s published MinIO port), used by `verifyfixtures` and for manual smoke checks.
- `bytes`/`sha256` must be the **real, measured** values for that specific object -- never one shared size for every file.
- `expected_parts` is `1` for every `small` fixture (a direct contract guarantee: `SplitFile` returns the original URL unsplit whenever the real file is `<= 16 MiB`). For `large` fixtures it must be `null` until actually observed from a real smoke run's `SUCCESS` output event (`total` field) -- **never** computed as `ceil(bytes / 16MiB)`, since structural splitting and re-encoding (PDF page spans, OOXML zip-part rewriting, CSV row boundaries) do not scale linearly with input size.
- `verified: false` entries are **placeholders**: the object does not exist in MinIO yet. `k6/load_test.js` refuses to use an unverified fixture by default (see `ALLOW_UNVERIFIED_FIXTURES` below) specifically so a capacity run can never silently rest on data that was never confirmed to exist.

Some entries in the shipped manifest reuse bytes/sha256 already recorded as evidence in `docs/test-results/event-sourcing/run-001/manifests/fixture-manifest.json` (not re-fetched or re-verified in the session that wrote this manifest). Two of those (`pdf-large-50mib-synthetic`, `docx-large-50mib-synthetic`, `xlsx-large-50mib-synthetic`) are flagged in their `notes` as synthetic/padded per `docs/load-test-review-event-sourcing.md` P2 -- their physical size is real, but they are not representative of a real document's extraction cost. Prefer `pdf-large-23mib-real` wherever a claim about real-document cost matters.

**Before running a real capacity test:**

1. Upload real-content files for every `"verified": false` placeholder to the `dataset` bucket at the object name given in its `url`/`url_host`.
2. Run the verifier (reads/hashes the real object; does not touch Redis or the pipeline):
   ```bash
   go run ./loadtest/tools/verifyfixtures -manifest k6/fixtures/manifest.example.json -write
   ```
3. For every `large` fixture you intend to use, publish it alone once, read the resulting `SUCCESS` event on `stream:doc-extractor:output`, and fill in `expected_parts` from its `total` field by hand.

A boundary-smoke run (fixtures just under/at/over 16 MiB) is done by pinning the fixture set explicitly:
```bash
k6 run -e WORKLOAD=small -e FIXTURE_IDS=csv-boundary-under-16mib k6/load_test.js
k6 run -e WORKLOAD=large -e FIXTURE_IDS=csv-boundary-above-16mib k6/load_test.js
```
Only a CSV boundary pair is verified so far; no PDF/DOCX/XLSX boundary pair exists yet (see the manifest's top-level `notes`).

---

## 4. Configuration reference

All settings are passed via `k6 run -e <KEY>=<VALUE>`. `<PREFIX>` below is `WORKLOAD` upper-cased (`SMALL`, `LARGE`, or `MIXED`); a `<PREFIX>_`-prefixed variable overrides the unprefixed one for that workload only.

### Required

| Variable | Values | Description |
|---|---|---|
| `WORKLOAD` | `small` \| `large` \| `mixed` | Which pool(s) and stage schedule to use. No default -- an invalid or missing value aborts immediately with a clear message. |

### Fixtures & scheduling

| Variable | Default | Description |
|---|---|---|
| `MANIFEST_PATH` | `./fixtures/manifest.example.json` | Fixture manifest, path relative to this script (k6 `open()` semantics). |
| `ALLOW_UNVERIFIED_FIXTURES` | `false` | Set `true` only for a smoke/dev run to allow placeholder (`verified:false`) fixtures into the pool. Never set for a run whose results will be reported as capacity. |
| `FIXTURE_IDS` | _(unset)_ | Comma-separated manifest ids to pin the exact fixture set (e.g. for boundary smoke), overriding the default size_class-based pool. |
| `WORKLOAD_SEED` | `20261114` | Seeds the deterministic shuffle of each size_class pool. Logged at `RUN_START` with the full resulting order, for exact reproducibility. |
| `MIX_SMALL_RATIO` | `0.8` | **Mixed only.** Fraction of documents, by count, that are small. This is an explicit assumption (no real traffic-mix data was available), not a measured ratio -- override it when you have one. |
| `MIXED_ORDER` | `interleaved` | **Mixed only.** `interleaved` spaces small/large evenly (exact ratio over any prefix, via a Bresenham-style interleave). `block` clusters all of a block's "large" slots at the front (see `MIXED_BLOCK_SIZE`), to deliberately probe head-of-line blocking (a large document occupying the single Splitter/Extractor process ahead of a run of smalls). |
| `MIXED_BLOCK_SIZE` | `10` | **Mixed + `MIXED_ORDER=block` only.** Iterations per block. |

### Arrival schedule (per-workload via `<PREFIX>_*`, or unprefixed as a shared default)

| Variable | Default | Description |
|---|---|---|
| `RATE_LEVELS` | `1,2,3,4,5` | Comma-separated arrival-rate levels (docs/min). First level is the baseline (hold only); every level after gets its own ramp+hold. `small` almost certainly saturates well above 5/min -- override `SMALL_RATE_LEVELS` once you have a baseline. |
| `STAGE_HOLD_DURATION` | `10m` | Hold duration per level. |
| `STAGE_RAMP_DURATION` | `30s` | Ramp duration between levels. |
| `STAGES_JSON` | _(unset)_ | Full override: a JSON array of `{target, duration}`, bypassing `RATE_LEVELS`/hold/ramp entirely. |
| `PRE_ALLOCATED_VUS` / `MAX_VUS` | `30` | VU pool size. **Validated against the schedule** -- see below. |
| `GRACEFUL_STOP` | `15m` | Window for in-flight iterations to finish polling after arrivals stop. This is **not** proof the backend has drained; see section 5. |
| `START_RATE` | first `RATE_LEVELS` entry | Initial arrival rate; rarely needs overriding directly. |

**VU sizing is enforced, not just suggested.** At init, the script computes `required = ceil(max_rate_level * (MAX_OBSERVATION_SECONDS / 60) * headroom)` (headroom defaults to `1.2`, override via `VU_HEADROOM_FACTOR`) and aborts with the exact number to set if `MAX_VUS` is below it. Set `SKIP_VU_CHECK=true` to intentionally under-provision (e.g. to exercise the guardrail itself) and expect `dropped_iterations > 0`.

### Redis / pipeline contract

| Variable | Default | Description |
|---|---|---|
| `REDIS_URL` | `redis://127.0.0.1:6379` | Never logged with credentials (passwords are redacted in every log line). |
| `REDIS_DIAL_TIMEOUT` / `REDIS_READ_TIMEOUT` / `REDIS_WRITE_TIMEOUT` | `5` (seconds) | Finite command timeouts, counted against the poll deadline. |
| `INPUT_STREAM` | `stream:doc-extractor:input` | |
| `STATE_KEY_PREFIX` | `doc-extractor:state:` | |
| `POLL_INTERVAL_MS` | `1000` | |
| `MAX_OBSERVATION_SECONDS` | `600` | Per-request elapsed-time deadline (not a loop-count). |

### Guardrail

| Variable | Default | Description |
|---|---|---|
| `OUTSTANDING_LIMIT` | `20` | New admissions stop once the run's outstanding-request counter reaches this. |
| `GUARDRAIL_KEY_PREFIX` | `loadtest:` | Redis key prefix for the per-run outstanding counter and stop flag. |

### Run identity

| Variable | Default | Description |
|---|---|---|
| `RUN_ID` | `run-<timestamp>` | Set this explicitly (e.g. from the shell invoking k6) so it also matches what you pass to `loadtest/tools/drain record`/`run`. |

---

## 5. Guardrail: stopping admission without losing observers

`OUTSTANDING_LIMIT` and an external stop flag are both backed by Redis keys scoped to `RUN_ID`, checked at the top of every iteration **before** any fixture is chosen or any `XADD` issued:

- `loadtest:<run_id>:outstanding` -- `INCR`ed right after a successful publish, `DECR`ed when this VU's own poll observes `COMPLETED`/`FAILED`. **Outstanding counts accepted-but-not-terminal requests, not VUs currently polling.** It is intentionally **not** decremented on `OBSERVATION_TIMEOUT` (the backend may still finish the request after the observer gives up) -- a run with many timeouts can permanently inflate this counter. That is a known, documented limitation of this simple Redis-counter approach, not a bug; reset it between stages with `redis-cli -u $REDIS_URL DEL loadtest:<run_id>:outstanding` if needed.
- `loadtest:<run_id>:stop_reason` -- an operator, or `loadtest/tools/resources` (manually, after inspecting its OOM/event output -- it does not write this key itself), can halt new admissions without touching k6:
  ```bash
  redis-cli -u $REDIS_URL SET "loadtest:<run_id>:stop_reason" "OOM_DETECTED_BY_OPERATOR" EX 86400
  ```

When either condition trips, the iteration logs `ADMISSION_STOPPED` and returns immediately -- no fixture is selected, no request is published. VUs that already hold an accepted, still-polling request are unaffected and keep observing until their own terminal/deadline. Time after the first `ADMISSION_STOPPED` event must be excluded from any "arrival matched target" claim (`loadtest/tools/drain` reports these events explicitly).

This intentionally avoids a metrics platform or extra service: it is one Redis counter plus one Redis string, read with the same client every VU already holds.

---

## 6. Running

Each invocation targets exactly one workload. `RUN_ID` should be generated once by the shell and reused across the `drain record`/`run` calls that bracket it.

```bash
export RUN_ID="run-$(date +%s)"

# 0. Snapshot stream positions BEFORE starting k6.
go run ./loadtest/tools/drain record -redis-addr localhost:16379 -run-id "$RUN_ID" \
    -out docs/test-results/event-sourcing/<run>/manifests/stream-start.json

# 1. (optional but recommended) start the resource monitor in the background,
#    before warm-up, and leave it running through the end of drain.
go run ./loadtest/tools/resources \
    -compose-file deploy/docker-compose.test.yaml \
    -services splitter,extractor-1,extractor-2,extractor-3,redis,minio \
    -out-dir docs/test-results/event-sourcing/<run>/resources &

# 2. Run k6, capturing stdout (its structured JSON log) to a file.
k6 run -e WORKLOAD=small -e RUN_ID="$RUN_ID" k6/load_test.js \
    2>&1 | tee docs/test-results/event-sourcing/<run>/logs/k6-stdout-small.jsonl

# 3. After GRACEFUL_STOP has elapsed, reconcile against the output stream.
#    This keeps polling independently of k6 until every accepted request is
#    terminal or its own -deadline elapses -- gracefulStop ending is NOT
#    proof the backend has drained.
go run ./loadtest/tools/drain run -redis-addr localhost:16379 -run-id "$RUN_ID" \
    -start-file docs/test-results/event-sourcing/<run>/manifests/stream-start.json \
    -manifest k6/fixtures/manifest.example.json \
    -k6-log docs/test-results/event-sourcing/<run>/logs/k6-stdout-small.jsonl \
    -deadline 20m \
    -out docs/test-results/event-sourcing/<run>/manifests/drain-report-small.json \
    -out-md docs/test-results/event-sourcing/<run>/manifests/drain-report-small.md

# 4. Stop the resource monitor (Ctrl-C / kill the background job) once drain is done.
```

### Small-only

```bash
k6 run -e WORKLOAD=small -e RUN_ID="$RUN_ID" k6/load_test.js
```

### Large-only

```bash
k6 run -e WORKLOAD=large -e RUN_ID="$RUN_ID" k6/load_test.js
```

### Mixed (80/20 small/large by document count, assumption -- see section 4)

```bash
k6 run -e WORKLOAD=mixed -e RUN_ID="$RUN_ID" k6/load_test.js
```

### Abbreviated smoke / validation run

To verify connectivity, event schema, manifest validation and the guardrail before a long run:
```bash
k6 run -e WORKLOAD=small -e RUN_ID="smoke-$(date +%s)" \
    -e RATE_LEVELS=1 -e STAGE_HOLD_DURATION=15s -e STAGE_RAMP_DURATION=5s \
    -e MAX_OBSERVATION_SECONDS=30 -e GRACEFUL_STOP=1m \
    k6/load_test.js
```

### Inspect configuration without running

```bash
k6 inspect k6/load_test.js
```

---

## 7. Known limitations

1. **Not executed.** Every script, manifest entry, and Go tool here was written and reviewed, not run against a live stack, as part of the session that produced them. Treat every number this section implies as a design target until an operator runs it.
2. **Placeholder fixtures.** Small DOCX/XLSX and all three PDF/DOCX/XLSX sizes except the ones flagged `verified: true` do not exist in MinIO yet; `ALLOW_UNVERIFIED_FIXTURES=false` (the default) refuses to run against them.
3. **Synthetic large fixtures.** The 50 MiB PDF/DOCX/XLSX samples are generated with padding/`ZIP_STORED` (see `docs/load-test-review-event-sourcing.md` P2); their physical size is real but extraction cost is not representative of an equivalent real document. `pdf-large-23mib-real` is the only verified large fixture with genuine content.
4. **`expected_parts` is unknown for every large fixture.** It must be filled in by hand from a real smoke run's output event; no automation here infers it from size.
5. **Outstanding-counter drift.** The admission guardrail's outstanding counter is never decremented on `OBSERVATION_TIMEOUT`, so a run with many timeouts can trip `OUTSTANDING_LIMIT` even after the backend has actually drained. Reset the key between stages if this happens.
6. **`loadtest/tools/drain`'s input-stream reconciliation scan is a single pass**, run once after the output-stream polling loop starts. If the pipeline retries a publish far outside the run's time window this tool does not account for, that is out of scope for a single benchmark run's reconciliation.
7. **`loadtest/tools/resources`'s cgroup collection is best-effort**, especially the privileged-helper fallback needed for the splitter's distroless image on Docker Desktop for macOS (cgroups live inside the Linux VM). Every failure is reported as an explicit `"status": "UNKNOWN"` with a reason -- never defaulted to a numeric zero or a false "ok" -- but the mechanism itself has not been validated against a real container in this session.
8. **No negative/recovery or boundary-smoke workload beyond the manual `FIXTURE_IDS` pin.** Corrupted files, mid-processing worker kill, and a dedicated PDF/DOCX/XLSX boundary pair are out of scope for this change; see `docs/load-test-review-event-sourcing.md` section 2 for that matrix.
9. **`docker stats --format '{{json .}}'` output shape can vary by Docker version.** `loadtest/tools/resources` records unparseable lines verbatim under a `raw` key rather than dropping them, but has not been checked against multiple Docker versions.
