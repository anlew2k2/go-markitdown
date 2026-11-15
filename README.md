# go-markitdown

Document splitting and Markdown extraction, as two Go binaries:

- **splitter** (`cmd/splitter`) — splits a document (PDF, DOCX, XLSX, PPTX, CSV, HTML, EML) into parts no larger than 4MB and uploads each part to MinIO. Files already within 4MB are passed through unsplit.
- **extractor** (`cmd/extractor`) — runs the `markitdown` CLI against a document (or one part of one) and uploads the resulting Markdown to MinIO.

Each binary runs in one of two modes, selected by `SERVICE_MODE`:

- `http` (default) — the original synchronous REST API, unchanged.
- `worker` — an asynchronous Redis Streams pipeline: a client submits one event, 1 Splitter + N Extractors process it in the background, and a single terminal `SUCCESS`/`FAILED` event is published for the whole document. See [docs/requirement-event-sourcing.md](docs/requirement-event-sourcing.md) and [docs/implementation-plan-event-sourcing.md](docs/implementation-plan-event-sourcing.md) for the full design.

## HTTP mode

```sh
make splitter:run    # go run ./cmd/splitter, listens on :8080
make extractor:run   # go run ./cmd/extractor, listens on :8080
```

Endpoints:

| Method | Path | Binary | Body |
|---|---|---|---|
| GET | `/ping` | both | — |
| POST | `/api/v1/split-file` | splitter | `{"request_id","conversation_id","message_id","file_path"}` (UUIDs + an HTTP(S) URL) → `{"urls":[...]}` |
| POST | `/api/v1/subprocess/run` | extractor | `{"args":"<command line>"}` → `{"output":"..."}` |
| POST | `/api/v1/subprocess/markitdown/run` | extractor | `{"url":"<file url>"}` → `{"markdown":"..."}` (file capped at 4MB) |

## Event-driven pipeline (worker mode)

### Streams and consumer groups

| Stream | Consumer group | Produced by | Consumed by |
|---|---|---|---|
| `stream:doc-extractor:input` | `cg:doc-splitter` | client | splitter |
| `stream:doc-extractor:splitted` | `cg:doc-extractor` | splitter | extractor(s) |
| `stream:doc-extractor:output` | — | splitter / extractor | client / downstream |

All payloads are JSON in the stream entry's `data` field. Schemas are exactly as specified in [docs/requirement-event-sourcing.md §3](docs/requirement-event-sourcing.md).

### Redis as source of truth

Per `request_id`, three keys (TTL 24h, set when first created):

| Key | Contents |
|---|---|
| `doc-extractor:state:{request_id}` | Hash: `total`, `status` (`PROCESSING`\|`COMPLETED`\|`FAILED`), identity fields, `failed_reason`, `failed_offset`, `terminal_event_id` |
| `doc-extractor:completed:{request_id}` | Set of completed offsets |
| `doc-extractor:parts:{request_id}` | Hash `part:<offset>` → Markdown URL |

Three Lua scripts (`internal/pipeline/lua/`) make every state transition atomic with the event it triggers, so a worker dying mid-step can never leave Redis and the output stream inconsistent:

- `init_and_publish.lua` — Splitter: create state + publish all `N` part events in one commit.
- `complete_part.lua` — Extractor: record one offset; if it's the last one, publish `SUCCESS` in the same commit.
- `fail_request.lua` — either worker: flip to `FAILED` and publish exactly one `FAILED` event; a no-op if the request is already terminal (fail-fast: later failures and late successes can't un-terminate it).

### Environment variables

| Variable | Applies to | Default | Meaning |
|---|---|---|---|
| `SERVICE_MODE` | both | `http` | `http` or `worker` |
| `REDIS_ADDR` | worker | `redis:6379` | Redis host:port |
| `REDIS_PASSWORD` | worker | *(empty)* | Redis auth |
| `REDIS_DB` | worker | `0` | Redis logical DB |
| `CONSUMER_NAME` | worker | hostname | Stream consumer identity — **must be unique per replica** |
| `JOB_TIMEOUT` | worker | splitter `15m`, extractor `5m` | bounds one whole job (download+split+upload, or download+convert+upload) |
| `CONVERT_TIMEOUT` | worker (extractor) | `2m` | bounds the `markitdown` subprocess specifically |
| `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_BUCKET`, `MINIO_PUBLIC_URL`, `MINIO_USE_SSL` | both | see `internal/services/minio.go` | MinIO connection + public URL used to build part/markdown URLs |

In worker mode both binaries still serve `GET /ping` on `:8080` (for container healthchecks via native subcommand `./splitter healthcheck` or `./extractor healthcheck`) but none of the HTTP conversion endpoints.

### Running the demo

```sh
make deploy:up
# or, to run multiple Extractors in parallel:
docker compose -f deploy/docker-compose.yaml up --build -d --scale extractor=3
```

Compose creates the three streams/two consumer groups automatically on first startup — no manual `XGROUP CREATE` or bucket setup needed.

Submit a document (`file_path` must resolve **from inside the Docker network** — use the `minio` hostname, not `localhost`, since the splitter/extractor containers resolve it, not your shell):

```sh
docker compose -f deploy/docker-compose.yaml exec redis redis-cli XADD stream:doc-extractor:input '*' \
  data '{"request_id":"<uuid>","conversation_id":"<uuid>","message_id":"<uuid>","file_path":"http://minio:9000/split-files/sample.pdf"}'
```

Read results, from the beginning so no event is missed:

```sh
docker compose -f deploy/docker-compose.yaml exec redis redis-cli XREAD COUNT 10 STREAMS stream:doc-extractor:output 0-0
```

Inspect the state of one request:

```sh
docker compose -f deploy/docker-compose.yaml exec redis redis-cli HGETALL doc-extractor:state:<request_id>
docker compose -f deploy/docker-compose.yaml exec redis redis-cli SMEMBERS doc-extractor:completed:<request_id>
docker compose -f deploy/docker-compose.yaml exec redis redis-cli HGETALL doc-extractor:parts:<request_id>
```

The Markdown URLs returned in the `SUCCESS` event also use the `minio` hostname; substitute `localhost:9000` (or whatever host/port you've exposed MinIO on) to fetch them from your own machine.

### Recovery semantics (POC scope)

- Each worker drains its own pending entries (delivered to it but never ACKed) on startup before reading new ones — restarting a worker **with the same `CONSUMER_NAME`** resumes exactly where it left off.
- A worker never ACKs a message before its Redis commit (script call) succeeds; a transient Redis error is retried with backoff in place rather than skipped or dropped.
- If a consumer is gone for good and its pending entries need to move to a different one, stop every worker of that stage and run once:
  ```sh
  redis-cli XAUTOCLAIM stream:doc-extractor:splitted cg:doc-extractor <new-consumer> 0 0-0
  ```
  then restart the workers. There is no automatic lease/fencing/reclaim in this POC.
- On SIGTERM/SIGINT, a worker stops reading new messages and lets the in-flight message either finish or stay pending — shutdown never marks a request `FAILED`.
- As soon as any offset of a request is marked `FAILED`, every Extractor still working that request polls state every 500ms and aborts (killing its download/`markitdown` subprocess) instead of finishing for nothing.

### Known POC limits

Deliberately out of scope, per [docs/implementation-plan-event-sourcing.md §1](docs/implementation-plan-event-sourcing.md): leases, heartbeats, fencing tokens, a deadline sweeper, an outbox, a retry-budget framework, stream auto-trimming, Redis Cluster, and event-store/replay. The first business failure for a `request_id` is terminal.

## Build & run locally

```sh
make splitter:build && make extractor:build

SERVICE_MODE=worker REDIS_ADDR=localhost:6379 MINIO_ENDPOINT=localhost:9000 ./bin/splitter
SERVICE_MODE=worker REDIS_ADDR=localhost:6379 MINIO_ENDPOINT=localhost:9000 ./bin/extractor
```

`markitdown` (the Python CLI) must be on `PATH` for the extractor to convert anything; the extractor's Docker image installs it, see `deploy/extractor.dockerfile`.

## Docker

```sh
make splitter:docker:build && make splitter:docker:run
make extractor:docker:build && make extractor:docker:run
```

Or the full stack (Redis + MinIO + splitter + extractor + optional nginx):

```sh
make deploy:up
make deploy:down
```
