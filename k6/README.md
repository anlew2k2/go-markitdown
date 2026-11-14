# Event-Driven Document Pipeline Load Test Scaffold

This directory contains the minimal k6 load-testing scaffold for the Event-Driven Document Pipeline (Redis Streams architecture).

The scaffold is designed to evaluate pipeline sustainable throughput, queue behavior, and system stability under strict resource limits (Splitter 512 MiB / Extractor 1 GiB), following the specifications in:
- [requirement-event-sourcing.md](../docs/requirement-event-sourcing.md)
- [implementation-plan-event-sourcing.md](../docs/implementation-plan-event-sourcing.md)
- [load-test-plan-event-sourcing.md](../docs/load-test-plan-event-sourcing.md)
- [load-test-review-event-sourcing.md](../docs/load-test-review-event-sourcing.md)

---

## 1. Overview & Architecture

The load test executes as an arrival-rate producer using `k6` and the `k6/x/redis` extension:
1. **Arrival Scheduling:** Uses `ramping-arrival-rate` with 10-minute hold plateaus at 1, 2, 3, 4, and 5 docs/min separated by 30-second ramps.
2. **True Drain Phase:** After the final 5 docs/min hold stage, arrival stops completely (no ramp down to 0). In-flight iterations continue polling during an explicit `gracefulStop` window (default 15 minutes).
3. **Dataset Rotation:** Cycles through PDF, DOCX, and XLSX test files in strict 1:1:1 sequence using `scenario.iterationInTest % 3` (avoiding local VU bias).
4. **Direct Stream Ingestion:** Publishes JSON payloads to Redis stream `stream:doc-extractor:input` under field `data`.
5. **State Polling & Deadlines:** Polls `doc-extractor:state:<request_id>` every 1 second until reaching `COMPLETED`, `FAILED`, or the elapsed-time deadline (default 10 minutes).
6. **Structured Telemetry:** Logs one structured JSON record per request containing request identities, dataset metadata, publish timing, stream ID, poll statistics, and observed outcome.

---

## 2. Event Contract & State Semantics

* **Input Stream:** `stream:doc-extractor:input`
  * Message field: `data`
  * Schema:
    ```json
    {
      "request_id": "<uuid-v4>",
      "conversation_id": "<uuid-v4>",
      "message_id": "<uuid-v4>",
      "file_path": "http://minio:9000/dataset/sample_50mb.pdf"
    }
    ```
* **State Hash:** `doc-extractor:state:<request_id>`
  * Field: `status`
  * Values: `PROCESSING` | `COMPLETED` | `FAILED`
* **Output Stream:** `stream:doc-extractor:output`
  * Values: `SUCCESS` | `FAILED`

### Outcome Classification
| Outcome | Description | k6 Metric |
|---|---|---|
| `COMPLETED` | State hash reached `COMPLETED`. Provisional observed success. | `observed_successes`, `e2e_duration` |
| `FAILED` | State hash reached `FAILED`. Terminal failure detected. | `terminal_failures` |
| `OBSERVATION_TIMEOUT` | Elapsed deadline expired while state was still `PROCESSING` or pending. **Note:** Observation timeout does not cancel the task on the worker or imply terminal failure. | `observation_timeouts` |
| `PUBLISH_UNKNOWN` | Redis `XADD` failed or reply was lost. Correlation IDs are preserved for offline input stream audit. | `publish_errors` |

> [!IMPORTANT]
> Observed k6 success counters are provisional. Authoritative throughput calculation requires offline reconciliation against `stream:doc-extractor:output` after the drain phase.

---

## 3. Prerequisites

Before running the load test:
1. **k6 with Redis Extension:** `k6` v2.3+ with `k6/x/redis` (e.g. installed via xk6 or automatic extension resolution).
2. **Pipeline Workers:** Splitter and Extractor daemons implemented and running in worker mode (per [implementation-plan-event-sourcing.md](../../docs/implementation-plan-event-sourcing.md)).
3. **Redis Instance:** Accessible standalone Redis instance (default `redis://127.0.0.1:6379`).
4. **MinIO Dataset:** MinIO bucket `dataset` seeded with test fixtures (`sample_50mb.pdf`, `sample_50mb.docx`, `sample_50mb.xlsx`).

---

## 4. Configuration Reference

All settings can be configured via environment variables passed to `k6 run -e <KEY>=<VALUE>`:

| Variable | Default Value | Description |
|---|---|---|
| `REDIS_URL` | `redis://127.0.0.1:6379` | Redis connection URL (`redis://[user:pass@]host:port[/db]`). |
| `REDIS_DIAL_TIMEOUT` | `5` | Dial timeout in seconds. |
| `REDIS_READ_TIMEOUT` | `5` | Socket read timeout in seconds. |
| `REDIS_WRITE_TIMEOUT` | `5` | Socket write timeout in seconds. |
| `INPUT_STREAM` | `stream:doc-extractor:input` | Redis input stream name. |
| `STATE_KEY_PREFIX` | `doc-extractor:state:` | Redis state key prefix for request tracking. |
| `DATASET_PDF_URL` | `http://minio:9000/dataset/sample_50mb.pdf` | MinIO URL for the 50MB PDF test file. |
| `DATASET_DOCX_URL` | `http://minio:9000/dataset/sample_50mb.docx` | MinIO URL for the 50MB DOCX test file. |
| `DATASET_XLSX_URL` | `http://minio:9000/dataset/sample_50mb.xlsx` | MinIO URL for the 50MB XLSX test file. |
| `DATASET_FILE_BYTES` | `52428800` | Approximate file size in bytes (50 MiB). |
| `POLL_INTERVAL_MS` | `1000` | Polling interval in milliseconds (default 1s). |
| `MAX_OBSERVATION_SECONDS` | `600` | Maximum elapsed polling deadline per request (default 10m). |
| `START_RATE` | `1` | Initial arrival rate per minute. |
| `STAGE_HOLD_DURATION` | `10m` | Hold duration for each plateau stage. |
| `STAGE_RAMP_DURATION` | `30s` | Ramp duration between rate levels. |
| `PRE_ALLOCATED_VUS` | `30` | Number of pre-allocated virtual users. |
| `MAX_VUS` | `30` | Maximum virtual users pool size. |
| `GRACEFUL_STOP` | `15m` | Drain window for in-flight requests after arrivals stop. |
| `RUN_ID` | `run-<timestamp>` | Unique run identifier across test executions. |
| `STAGES_JSON` | `""` | Optional JSON string overriding stage definition. |

---

## 5. Execution

### Standard Capacity Step-Up Benchmark (1–5 docs/min, ~52m + 15m drain)
```bash
k6 run k6/load_test.js
```

### Abbreviated Smoke / Validation Run (15s hold, 5s ramp)
To verify connectivity, event schemas, and polling flow before conducting a long test:
```bash
k6 run \
  -e STAGE_HOLD_DURATION=15s \
  -e STAGE_RAMP_DURATION=5s \
  -e MAX_OBSERVATION_SECONDS=30 \
  -e GRACEFUL_STOP=1m \
  k6/load_test.js
```

### Inspect Configuration and Scenarios
```bash
k6 inspect k6/load_test.js
```

---

## 6. Known Integration Gaps & Future Integration Points

The load-test scaffold intentionally focuses on request generation, Redis stream publishing, state polling, and client-side observation. The following integration points are scheduled for downstream implementation:

1. **Resource & OOM Monitoring Daemon:**
   - Continuous Docker stats stream recording timestamped CPU, RAM, and I/O across worker, Redis, and MinIO containers.
   - Continuous Docker events recording `oom`, `die`, `restart`, and `start`.
   - Direct cgroup v2 collection (`memory.current`, `memory.peak`, `memory.events`, `cpu.stat`) inside Docker VM to detect memory spikes and Python child process OOMs.

2. **Shared Stop / Admission Guardrails:**
   - Active cross-VU admission guardrail that stops new request generation if outstanding non-terminal requests exceed 20 or if worker death/OOM is signaled by the monitoring daemon.
   - In k6, VUs operate in isolated JS runtimes. A shared stop flag will be integrated either via Redis state (e.g. checking a circuit-breaker key) or via an external benchmark orchestrator.

3. **Output-Stream Reconciliation (Authoritative Throughput):**
   - An offline post-run reconciliation script that inspects `stream:doc-extractor:output` between pre-run start ID and post-drain end ID.
   - Matches terminal events against the k6 manifest logs by `request_id` to compute authoritative `docs_completed_per_min` and `MiB_processed_per_min`, verifying that every accepted request resolves to exactly one terminal event.

4. **Worker-Side Structured Timing Logs:**
   - Instrumentation within Splitter and Extractor workers (`t0` to `t5`) to record exact input queue wait time, splitter processing, part queue wait, and part conversion/commit duration.
   - The k6 scaffold strictly measures end-to-end client latency and does not fabricate worker-side timestamps.
