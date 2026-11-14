# Load Test Plan — Tìm throughput bền vững dưới giới hạn RAM

Cập nhật: 2026-11-14. Trạng thái: **kế hoạch; k6 script + tooling hậu kiểm đã được viết theo kế hoạch này (xem [k6/README.md](../k6/README.md) và `loadtest/tools/`), nhưng chưa chạy benchmark nào**.
Nguồn: [requirement](requirement-event-sourcing.md), [implementation plan](implementation-plan-event-sourcing.md), [review](load-test-review-event-sourcing.md).

Tài liệu này mô tả **ý định/kế hoạch**; phần triển khai cụ thể (script, manifest, công cụ) nằm ở [k6/](../k6) và [loadtest/](../loadtest), với README riêng liệt kê giới hạn đã biết. Khi hai tài liệu khác nhau về chi tiết thực thi (tên biến môi trường, tên file, v.v.), `k6/README.md` là nguồn đúng vì nó mô tả code đã viết; tài liệu này giữ vai trò mục tiêu/rationale.

## 1. Mục tiêu và phạm vi

Ưu tiên **RAM/CPU, không OOM và throughput hoàn thành thực tế**; latency và error rate dùng để giải thích kết quả và kiểm tra tính hợp lệ của mức tải.

Giữ cách làm đơn giản:

- k6 dùng `ramping-arrival-rate`, trực tiếp XADD Redis Streams và poll state mỗi **1 giây**.
- Dùng Docker stats, Docker events và cgroup counters để theo dõi tài nguyên. Không thêm Prometheus, cAdvisor, Pub/Sub hoặc collector service.
- Sau run, đọc output stream một lần theo khoảng stream ID để đối soát completion muộn. Đây là bước tổng hợp kết quả, không phải observer chạy liên tục.
- Chỉ thêm structured timing logs tối thiểu trong workers khi triển khai, không xây metrics platform.

## 2. Điều kiện trước khi chạy

1. Worker mode đã có trong code (`internal/pipeline/{splitter,extractor}.go`, hai entrypoint `cmd/{splitter,extractor}/main.go` với `SERVICE_MODE=worker`, consumer groups, Lua scripts) và Compose đã cấu hình `SERVICE_MODE: worker` (`deploy/docker-compose.yaml`, `deploy/docker-compose.test.yaml`). **Có code không đồng nghĩa deployment đã sẵn sàng**: trước benchmark vẫn phải build image thực tế, bật stack, và smoke input → split → extract → đúng một output SUCCESS/FAILED cho từng format trong manifest trước khi tin bất kỳ con số nào.
2. Cố định **1 Splitter + N Extractor** theo Compose đang dùng (`deploy/docker-compose.test.yaml` hiện định nghĩa 1 Splitter + 3 Extractor, mỗi Extractor 2 CPU / 1 GiB; Splitter 1 CPU / 512 MiB). Mỗi process xử lý một message tại một thời điểm. Nếu đổi số replica thì chạy benchmark riêng và ghi tổng quota.
3. Xác minh limits thực tế, worker mode, consumer groups và truy cập MinIO từ worker. Ghi image/version, Docker VM CPU/RAM/swap và cấu hình Redis persistence thực tế; không đổi cấu hình giữa các mức tải.
4. Môi trường benchmark không có traffic khác; input/splitted group lag và pending về 0 trước run. Không xóa dữ liệu dùng chung để đạt điều kiện này.
5. Dùng fixture manifest (`k6/fixtures/manifest.example.json`) với bytes/checksum riêng từng file thay vì một file tổng hợp 50 MiB dùng chung; smoke từng file thành công. Ghi bytes/checksum, số trang/row nếu có và số part thực tế (`total` từ output event, không suy từ kích thước). Chạy `go run ./loadtest/tools/verifyfixtures -write` để xác nhận bytes/checksum thật trước khi dùng một fixture cho benchmark chính thức (xem [k6/README.md](../k6/README.md) mục 3).

## 3. Workload: small-only, large-only, mixed

Theo review (`load-test-review-event-sourcing.md` mục 1–2), một bộ rate 1→5 docs/min với file 50 MiB dùng chung không đo được capacity file nhỏ và không tách được ảnh hưởng của mix. k6 script hiện hỗ trợ ba workload riêng qua `-e WORKLOAD=small|large|mixed` (xem [k6/README.md](../k6/README.md) mục 4), mỗi workload có rate/stage riêng cấu hình qua env (`RATE_LEVELS`, `STAGE_HOLD_DURATION`, `STAGE_RAMP_DURATION`, hoặc `<WORKLOAD>_*` để override riêng từng workload).

Trước run chính (bất kỳ workload nào), chạy warm-up riêng 3–5 phút rồi chờ drain. Không tính warm-up vào throughput.

### 3.1. Large-only (sweep thăm dò, như kế hoạch gốc)

| Bước | Arrival rate | Thời lượng | Mục đích |
|---|---:|---:|---|
| Baseline | 1 doc/min | Hold 10 phút | Đo footprint và thời gian từng format |
| Mức 2 | 2 docs/min | Ramp 30s + hold 10 phút | Kiểm tra queue/throughput |
| Mức 3 | 3 docs/min | Ramp 30s + hold 10 phút | Tiếp tục tăng tải |
| Mức 4 | 4 docs/min | Ramp 30s + hold 10 phút | Tìm vùng bão hòa |
| Mức 5 | 5 docs/min | Ramp 30s + hold 10 phút | Xác định có cần tăng tiếp |
| Drain | **Không nạp mới** | Quan sát tới khi hết outstanding hoặc hết deadline hậu kiểm | Đối soát kết quả qua `loadtest/tools/drain` |

`RATE_LEVELS=1,2,3,4,5` (mặc định của script) khớp bảng trên. Nếu 5 docs/min vẫn ổn định, tăng tiếp từng 1 doc/min trong run kế tiếp (`RATE_LEVELS=1,2,3,4,5,6,7`); chưa được kết luận 5 là maximum.

### 3.2. Small-only

File nhỏ (≤16 MiB, không qua Splitter) có chi phí mỗi document thấp hơn nhiều so với file lớn đã split; 1→5 docs/min gần như chắc chắn chưa chạm bão hòa. Chạy baseline nhỏ trước (ví dụ 5 docs/min) rồi tăng dần tới khi thấy dấu hiệu bão hòa (queue/E2E tăng, CPU Splitter hoặc Extractor sát quota) thay vì copy nguyên bộ 1–5 của large. Ví dụ cấu hình thăm dò ban đầu (không phải kết luận capacity):

```bash
k6 run -e WORKLOAD=small -e RATE_LEVELS=5,10,15,20,25 k6/load_test.js
```

Dùng `FIXTURE_IDS` để cố định một boundary fixture khi cần (mục 3.4).

### 3.3. Mixed

Tỷ lệ mặc định **80/20 small/large theo số document** (`MIX_SMALL_RATIO=0.8`) — đây là **giả định**, chưa có số liệu traffic thực; ghi rõ giả định này mỗi khi báo cáo kết quả mixed. Fixture được chọn theo lịch deterministic (xen kẽ theo tỷ lệ, hoặc dồn cụm `MIXED_ORDER=block` để kiểm tra head-of-line blocking khi một file lớn chiếm Splitter/Extractor trước một loạt file nhỏ), có seed (`WORKLOAD_SEED`), và được log đầy đủ ở `RUN_START` để tái lập.

Mục tiêu đo: latency/backlog của small khi có large chen vào, so với small-only ở cùng small-arrival-rate. Tổng throughput tốt không được che mất việc small bị chờ lâu — luôn báo riêng theo `size_class × format` (mục 5), không chỉ tổng số.

```bash
k6 run -e WORKLOAD=mixed -e MIX_SMALL_RATIO=0.8 k6/load_test.js
```

### 3.4. Boundary smoke quanh 16 MiB

Ngưỡng code là **16 × 1024 × 1024 bytes** (`maxSplitPartSize`, `internal/usecase/splitfile.go`). Đây là kiểm tra định tuyến (không cắt vs. có cắt), không phải một capacity sweep riêng: chạy với rate thấp, số mẫu nhỏ, mục tiêu là xác nhận hành vi đúng tại biên.

```bash
k6 run -e WORKLOAD=small -e RATE_LEVELS=1 -e FIXTURE_IDS=csv-boundary-under-16mib k6/load_test.js
k6 run -e WORKLOAD=large -e RATE_LEVELS=1 -e FIXTURE_IDS=csv-boundary-above-16mib k6/load_test.js
```

Hiện chỉ có một cặp fixture CSV đã xác minh sát biên (`k6/fixtures/manifest.example.json`: `csv-boundary-under-16mib` 16.777.200 bytes, `csv-boundary-above-16mib` 17.000.000 bytes, cả hai lấy từ bằng chứng đã có ở `docs/test-results/event-sourcing/run-001/`). Chưa có cặp PDF/DOCX/XLSX sát biên — cần tạo và xác minh bằng `verifyfixtures` trước khi mở rộng boundary smoke sang các format đó. Không bắt buộc file trên ngưỡng luôn ra nhiều part sau tái nén; chỉ xác nhận có đi qua Splitter.

### 3.5. Guardrail và hold xác nhận (áp dụng mọi workload)

- Bỏ hai phút đầu mỗi hold khỏi cửa sổ tính throughput; giữ toàn bộ dữ liệu để giải thích transition. Nếu queue chưa ổn định thì không gọi phần còn lại là steady state.
- Dừng nạp sớm nếu phát hiện OOM, worker chết, hoặc số request chưa terminal (outstanding, không phải số VU đang poll) vượt **`OUTSTANDING_LIMIT`** (mặc định 20). Đây là guardrail của benchmark, không phải giới hạn sản phẩm. k6 kiểm tra guardrail (bộ đếm Redis + cờ dừng ngoài) trước mỗi `XADD` và giữ cờ dừng chung theo `RUN_ID`; không abort toàn bộ k6 — VU đang poll request đã accepted vẫn tiếp tục quan sát. Operator hoặc `loadtest/tools/resources` có thể đặt cờ dừng thủ công qua Redis khi phát hiện OOM/worker chết (xem [k6/README.md](../k6/README.md) mục 5).
- Sau khi tìm vùng ngưỡng (từng workload), chạy **hold riêng 30 phút** tại mức bền vững dự kiến, bắt đầu từ queue rỗng để xác nhận capacity và xem xu hướng RAM. Nếu còn quá ít mẫu hoặc RAM chưa ổn định, kéo dài thay vì kết luận không leak.
- Dừng nạp (arrival) không đồng nghĩa đã drain: kết thúc `gracefulStop` chỉ là k6 ngừng chờ; dùng `loadtest/tools/drain run` để tiếp tục quan sát `stream:doc-extractor:output` tới khi mọi accepted request đã terminal hoặc hết deadline hậu kiểm riêng của công cụ đó (mục 5).

## 4. Thiết kế k6 (đã triển khai tại `k6/load_test.js` + `k6/lib/`)

Phần dưới mô tả hành vi đã viết; chi tiết biến môi trường đầy đủ ở [k6/README.md](../k6/README.md) mục 4.

**Cấu hình ban đầu:** `startRate` = mức đầu của `RATE_LEVELS` (mặc định `1`), `timeUnit='1m'`, `preAllocatedVUs`/`maxVUs` mặc định `30`, polling 1s, thời gian quan sát tối đa mỗi request **10 phút** (`MAX_OBSERVATION_SECONDS`), `gracefulStop='15m'`. **VU được kiểm tra tự động**: script tính `required = ceil(max_rate_level × (MAX_OBSERVATION_SECONDS/60) × 1.2)` và dừng với thông báo rõ nếu `MAX_VUS` thấp hơn, thay vì chỉ khuyến nghị; `SKIP_VU_CHECK=true` để cố ý bỏ qua.

**Luồng mỗi iteration:**

1. Kiểm tra guardrail (cờ dừng ngoài `loadtest:<run_id>:stop_reason` hoặc outstanding `loadtest:<run_id>:outstanding` ≥ `OUTSTANDING_LIMIT`) **trước khi** chọn fixture hay publish. Nếu dừng: ghi `ADMISSION_STOPPED` kèm lý do, không chọn fixture, không publish, VU khác đang poll request đã accepted vẫn tiếp tục.
2. Chọn fixture theo `exec.scenario.iterationInTest` (không phải `__ITER` cục bộ mỗi VU) qua `datasetForIteration()` trong `k6/load_test.js`: small/large chọn theo thứ tự xáo trộn có seed của manifest pool tương ứng (`WORKLOAD_SEED`); mixed chọn lớp small/large theo tỷ lệ `MIX_SMALL_RATIO` bằng thuật toán nội suy kiểu Bresenham (`isSmallSlot`/`isSmallSlotBlock` trong `k6/lib/rng.js`), rồi chọn fixture trong lớp đó theo cùng cơ chế xáo trộn có seed. Toàn bộ thứ tự + seed được log ở sự kiện `RUN_START`.
3. Tạo UUID hợp lệ cho request/conversation/message; `RUN_ID` được tạo **đúng một lần** trong `setup()` (chạy một lần duy nhất, không phải ở top-level script — top-level chạy lại độc lập trong mỗi VU runtime của k6, nên `Date.now()` ở đó sẽ cho mỗi VU một `run_id` khác nhau) và truyền cho mọi VU qua tham số `data` của `default()`. Ghi sự kiện `INTENT` (run_id, request_id, fixture_id, timestamp) **ngay trước** `XADD` để xử lý cả trường hợp mất reply.
4. Dùng `async` function và **await** `redisClient.sendCommand('XADD', ...)`. Lưu stream ID trả về và ghi sự kiện `ACCEPTED` ngay sau reply. Nếu mất reply/lỗi: ghi `PUBLISH_UNKNOWN`, không retry bằng request ID mới vì Redis có thể đã nhận — đối soát thuộc về `loadtest/tools/drain`.
5. Poll bằng **await** `HGET status` mỗi `POLL_INTERVAL_MS` đến `COMPLETED`/`FAILED` hoặc deadline theo `Date.now()`. Thời gian Redis call nằm trong deadline; command timeout hữu hạn (`REDIS_*_TIMEOUT`). Không dùng số vòng lặp thay cho elapsed time.
6. `COMPLETED` → ghi observed success + E2E + `DECR` outstanding; `FAILED` → ghi terminal failure + `DECR` outstanding. Hết deadline → ghi **`OBSERVATION_TIMEOUT`**, không coi là worker FAILED, không hủy task, **không `DECR` outstanding** (backend có thể vẫn đang xử lý — xem giới hạn đã biết ở k6/README.md mục 7). Redis read lỗi ghi riêng (`redis_read_errors`), chỉ tiếp tục retry trong deadline.
7. Trong mọi nhánh (trừ `ADMISSION_STOPPED`), log sự kiện `OUTCOME` cuối cùng cùng request ID, fixture_id, dataset. Không log mỗi lần poll.

**Metric giữ trong k6** (tag giới hạn `{workload, size_class, format}`, không dùng `request_id` làm tag): `accepted_publishes`, `publish_errors`, `observed_successes`, `terminal_failures`, `observation_timeouts`, `redis_read_errors`, `admission_stopped`, `e2e_duration` (Trend, chỉ COMPLETED), `failed_duration` (Trend, chỉ FAILED), và `dropped_iterations` (built-in k6). Không gộp thành một success-rate threshold. `dropped_iterations=0` là điều kiện xác nhận arrival đã đạt lịch; run có guardrail kích hoạt chỉ dùng các cửa sổ hoàn chỉnh trước khi dừng (xem `admission_stop_events` trong báo cáo của `loadtest/tools/drain`). **Luôn đọc `e2e_duration`/`failed_duration` cạnh số lượng FAILED/OBSERVATION_TIMEOUT/ADMISSION_STOPPED cùng tag** — p95 tính riêng từ mẫu COMPLETED bị thiên lệch sống sót nếu tỷ lệ fail/timeout đáng kể.

30 VU đang poll 1s tạo khoảng 30 HGET/s, cộng fail-fast polling của worker. Chỉ điều tra overhead nếu Redis CPU/latency bất thường; chưa cần đổi kiến trúc observer. Nếu nghi nhiễu, lặp lại một mức với `POLL_INTERVAL_MS=2000`.

## 5. Đo throughput và thời gian

### Throughput chính thức

Triển khai tại `loadtest/tools/drain` (subcommand `record` trước run, `run` sau run — xem [k6/README.md](../k6/README.md) mục 6). `record` lưu last ID của cả ba stream (stream rỗng dùng `0-0`) qua `XREVRANGE ... COUNT 1`; `run` đọc theo batch (`XRANGE` với COUNT, ID loại trừ) từ ID đã lưu tới hiện tại, tiếp tục poll độc lập với k6 tới khi mọi request đã accepted (đối chiếu qua input stream, không chỉ qua log k6) đạt terminal hoặc hết `-deadline` riêng của công cụ. Phát hiện terminal trùng theo request ID **trước khi** dedup (`duplicate_terminal_requests` trong báo cáo); trạng thái nội bộ `COMPLETED` tương ứng output `SUCCESS`.

- `docs_completed_per_min = số document SUCCESS commit trong cửa sổ / số phút của cửa sổ` — field `throughput_windows[].docs_per_min` trong báo cáo JSON/Markdown.
- `MiB_processed_per_min = tổng bytes gốc (từ manifest, theo fixture_id) của các document SUCCESS / 2^20 / số phút` — field `mib_per_min`.
- Cửa sổ throughput dùng timestamp stream ID của **output** để xếp completion vào cửa sổ wall-clock, không dùng thời điểm k6 nhận poll. Không lấy tổng Counter chia cả warm-up/drain và không lấy nghịch đảo latency.
- Báo cáo theo `size_class × format` (field `breakdown_by_size_class_format`, một dòng = một document, không cộng part); ghi arrival thực tế (từ `RUN_START`/stage config trong log k6) bên cạnh completion rate. Completion có thể thuộc arrival stage trước, nên run hold độc lập dùng để xác nhận capacity cuối cùng.
- Đối soát `accepted = SUCCESS + FAILED + unresolved` — field `accepted_equation_holds`, cảnh báo nếu sai. Observation timeout có thể trở thành SUCCESS muộn và không được đếm thêm như terminal failure (nó chỉ là outcome quan sát phía k6, không ảnh hưởng phân loại terminal của drain tool). Publish mất reply (`PUBLISH_UNKNOWN`) được đối chiếu input stream theo request ID (`total_not_confirmed_on_input_stream` nếu không tìm thấy) để xác định đã accepted hay chưa.
- Hết `-deadline` mà còn unresolved: `unresolved_detail` liệt kê request_id/fixture_id/tuổi; `deadline_exceeded=true` trong báo cáo — không đánh run đạt capacity khi còn unresolved không giải thích được.
- E2E báo **hai loại riêng biệt**: `authoritative_e2e_ms_*` (output stream ID timestamp − input stream ID timestamp, phía Redis) và `client_e2e_ms_*` (k6 tự đo, gồm cả network/poll overhead) — không gộp chung.

### Queue và processing

**Chưa triển khai**: `internal/pipeline/splitter.go` và `extractor.go` hiện chỉ có `log.Printf` theo dòng sự kiện (không phải structured timing log có timestamp t0–t5 theo request/offset/attempt). Phần dưới vẫn là đặc tả cần làm nếu muốn tách queue wait / processing time; cho tới khi đó chỉ dùng E2E (từ `loadtest/tools/drain`) và backlog (`XINFO GROUPS`/`XPENDING`, xem cuối mục này) — không gọi đó là "processing time" đã đo riêng. Đây KHÔNG thuộc phạm vi thay đổi của script/tooling trong `k6/` và `loadtest/`.

Trong workers, thêm structured logs tối thiểu theo request ID/offset/attempt:

| Chỉ số | Cách tính |
|---|---|
| Input queue wait | Splitter start − input enqueue timestamp từ stream ID |
| Splitter processing | Split/upload/fanout kết thúc − Splitter start |
| Part queue wait | Extractor start − part enqueue timestamp từ stream ID |
| Part processing | Part commit − Extractor start |
| E2E | Terminal output timestamp − input enqueue timestamp |

Dùng clock đồng bộ để so timestamp giữa process; duration nội bộ dùng monotonic clock. Processing ở đây bao gồm download/convert/upload/commit; chỉ tách thêm từng bước nếu cần điều tra bottleneck. Không cộng thời gian mọi part song song thành processing wall-clock của document. Nếu chưa có timing logs, chỉ báo E2E/backlog và ghi rõ chưa đo riêng queue/service time.

Mỗi khoảng 5s, ghi `XINFO GROUPS` lag và `XPENDING` của input/splitted. Lag là chưa deliver; pending có thể đang xử lý hoặc bị kẹt. **Không dùng XLEN làm queue backlog** vì XACK không xóa entry. Ghi thêm số request đã accepted nhưng chưa terminal để quan sát backlog document.

## 6. Giám sát tài nguyên và OOM

Triển khai tại `loadtest/tools/resources` (chưa được chạy trong phiên viết công cụ này). Bắt đầu monitor trước warm-up và giữ đến hết drain:

1. Dùng **một Docker stats stream** (`docker stats --format '{{json .}}'` cho mọi container cùng lúc, không gọi tuần tự) cho các container liên quan, lưu timestamp, CPU, RAM, PIDs, net/block I/O. Container IDs lấy qua `docker compose ps -q <service>`, không hardcode tên — kể cả khi scale nhiều Extractor (`deploy/docker-compose.test.yaml` có `extractor-1..3`). Theo dõi Redis/MinIO bên cạnh workers để nhận biết tranh chấp tài nguyên.
2. Ghi **Docker events liên tục** (`docker events --filter event=oom|die|restart|start`) cho `oom`, `die`, `restart`, `start`; mỗi event tự động kèm `docker inspect` lấy OOMKilled, exit code, restart count. Exit 137 chưa đủ chứng minh OOM. Thu thập lỗi phải ghi UNKNOWN, không mặc định false/OK — công cụ trả `"UNKNOWN"` kèm lý do khi `docker inspect` thất bại, không trả `false`/`0`.
3. Đọc cgroup v2 (`memory.current`, `memory.peak`, `memory.events`, `memory.swap.current`, `cpu.stat`) định kỳ (mặc định 2s) theo hai phương án nối tiếp: `docker exec <container> cat ...` (dùng được với image có shell, ví dụ Extractor trên nền `python:3.13-slim`), nếu thất bại thì dùng container tạm `--pid=host --privileged -v /:/hostfs:ro` để đọc qua `/proc/<pid>/cgroup` (bắt buộc với Splitter trên nền `gcr.io/distroless/static-debian12`, không có shell). Python child có thể bị OOM kill trong khi Go còn sống, nên không chỉ dựa `.State.OOMKilled`.

Trên macOS, cgroup nằm trong Linux VM của Docker; kiểm tra khả năng truy cập trước benchmark — cơ chế container đặc quyền ở trên là cách công cụ xử lý việc này, nhưng **chưa được xác minh chạy thật** trong phiên viết công cụ. Nếu không thu được peak/counter thì công cụ ghi `"status": "UNKNOWN"` kèm lý do; không khẳng định đã loại trừ micro-spike. Không thêm cAdvisor chỉ để chạy POC này. Docker stats có thể trừ file cache nên RAM hiển thị không thay thế total memory charge/peak của cgroup.

RAM phải dưới actual limit 512 MiB/1 GiB, tính cả Go và Python child; ghi peak và xu hướng baseline qua nhiều chu kỳ. Cảnh báo khi vượt 90% budget để điều tra, chưa tự coi đó là OOM. Không cần ép RAM về đúng giá trị trước warm-up vì heap/cache có thể giữ high watermark.

## 7. Cách đọc bottleneck

| Bằng chứng | Giả thuyết cần kiểm tra tiếp |
|---|---|
| Input lag tăng, Splitter CPU gần 100% | Splitter CPU-bound; đối chiếu processing time và I/O |
| Splitted lag tăng, Extractor CPU cao | Extractor giới hạn conversion/concurrency; khoảng 100% cũng có thể nghẽn một core dù quota 200% |
| OOM event hoặc cgroup oom_kill tăng | RAM limit; đối chiếu format, phase và Python child |
| CPU thấp nhưng queue/E2E tăng | Kiểm tra worker concurrency, task kẹt, Redis và download/upload; chưa kết luận MinIO |
| Redis RAM tăng | Kiểm tra retention, lag/pending và command latency; plan không auto-trim streams, state TTL 24h nên tăng RAM chưa chứng minh leak |

Chỉ kết luận bottleneck khi có cả queue/throughput và bằng chứng tài nguyên/thời gian xử lý phù hợp. CPU quota throttling và tranh chấp Docker VM cũng cần được phân biệt.

## 8. Tiêu chí và kết quả cần lưu

Tiêu chí dưới áp dụng **riêng cho từng workload** (small-only, large-only, mixed) và riêng cho boundary smoke (mục 3.4, chỉ cần đúng hành vi định tuyến, không cần bão hòa). **Mức throughput bền vững cao nhất đã kiểm chứng** của một workload phải đồng thời:

- Không OOM, worker chết hoặc restart bất thường (`loadtest/tools/resources`: `events.jsonl` không có `oom`/bất thường `die`, `cgroup.jsonl` không có `status: UNKNOWN` trong các cửa sổ cần dùng để kết luận); footprint không tăng không kiểm soát trong hold xác nhận.
- Arrival thực đạt target, không `dropped_iterations`; không có terminal failure, duplicate terminal (`drain` report: `duplicate_terminal_requests` rỗng) hoặc unresolved không giải thích được (`unresolved_detail` rỗng hoặc đã điều tra từng mục).
- Completion rate ổn định và backlog không có xu hướng tăng qua ít nhất ba cửa sổ liên tiếp (`throughput_windows` trong báo cáo `drain`); drain hết sau khi dừng nạp (`deadline_exceeded=false`).
- Với mixed: latency/backlog của nhóm `small` (trong `breakdown_by_size_class_format`) không xấu đi bất ngờ so với small-only ở cùng small-arrival-rate.

Nếu mức cao nhất chưa bão hòa, báo **capacity ít nhất đạt X docs/min**, không gọi đó là maximum. Run hữu hạn không chứng minh không có leak chậm hay xác suất lỗi bằng 0.

Lưu kết quả thực thi vào `docs/load-test-results-event-sourcing.md` khi chạy, kèm đường dẫn manifest, k6 log JSONL, báo cáo `drain` (JSON + Markdown), và output của `loadtest/tools/resources`. `drain run -out-md` đã sinh sẵn bảng breakdown theo `size_class × format` và throughput windows; **không tự sinh RAM/CPU/OOM/replica count** — những cột đó lấy thủ công từ `loadtest/tools/resources` và cấu hình Compose thực tế khi điền vào bảng kết quả tối thiểu dưới đây:

| Workload | Rate / replica count | Cửa sổ đo | SUCCESS/min | MiB/min | RAM peak Splitter/Extractor | CPU | OOM | Lag/pending đầu→cuối | FAILED / unresolved | Kết luận |
|---|---|---|---|---|---|---|---|---|---|---|

Không cleanup dữ liệu dùng chung. Chỉ dọn object/state thuộc run trên môi trường benchmark được phép; không FLUSHALL hay trim stream dùng chung. Các công cụ trong `loadtest/tools/` không bao giờ tự FLUSHALL/trim stream; `verifyfixtures` chỉ đọc (GET) object MinIO, không ghi/xóa.
