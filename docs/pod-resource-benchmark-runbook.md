# Pod resource benchmark runbook

Ngày: 2026-11-15. Trạng thái: **bounded concurrency, structured timing logs và compose override đã triển khai và smoke-verify thật trên `pipelinetest` (xem mục 6); kết quả đo single-pod Extractor (1 pod, nhiều mức CPU/RAM/concurrency, file 50 MiB) ở mục 7-9; chưa chạy sweep CPU/RAM/concurrency dài theo ma trận BR-02..BR-06 đầy đủ của [pod-resource-benchmark-plan.md](pod-resource-benchmark-plan.md).**

Tài liệu này là phần "cách chạy" cụ thể, bổ sung cho kế hoạch/rationale ở `pod-resource-benchmark-plan.md`. Khi hai tài liệu khác nhau về chi tiết thực thi (tên biến, lệnh), tài liệu này là nguồn đúng vì mô tả code/compose đã viết.

**Mục lục kết quả đo (báo cáo)**: mục 6 (smoke verify tổng quát), mục 7 (baseline 1 pod/1 CPU/512 MiB + k6 ngắn), mục 8 (sweep concurrency 1/2/4 ở 1 CPU và 2 CPU/1 GiB), mục 9 (lặp 3 lần: 1 CPU/256 MiB/c=1 so với 2 CPU/512 MiB/c=2, kết luận tối ưu theo mục tiêu latency vs mật độ pod). Toàn bộ raw data (timing log, `docker stats`/cgroup, commit/image id) nằm dưới `docs/test-results/pod-resources/<run-id>/`.

## 1. Những gì đã triển khai

- **Structured timing logs** (`internal/pipeline/timing.go`, instrumented trong `splitter.go`/`extractor.go`): mỗi job ghi các dòng `TIMING {...}` JSON — `queue_wait` (từ timestamp trong Redis stream ID, xem giới hạn ở mục 5), `download`, `split_upload` (Splitter, gộp vì `SplitAndUpload` interleave encode/upload từng part — xem `docs/throughput-source-review.md` mục 7), `convert` + `upload` (Extractor, tách riêng), và `total` (toàn job, không gồm `queue_wait`). Mỗi dòng có `stage`, `consumer` (= tên consumer/slot), `request_id`, `offset` (Extractor), `outcome` (`success`/`failure`/`incomplete`), `error` nếu có, `note`. Không log file_path, signed URL hay nội dung tài liệu.
- **`WORKER_CONCURRENCY`** (mặc định `1`, độc lập theo container Splitter/Extractor vì mỗi container đọc biến môi trường riêng): `internal/pipeline/redis.go` validate số nguyên dương, fail startup (`log.Fatalf`, exit 1) nếu sai. `internal/pipeline/consumer.go` (`RunConsumerPool`) chạy N loop `RunConsumer` độc lập, mỗi loop là một sub-consumer ổn định `<CONSUMER_NAME>-slot-<i>`, mỗi loop xử lý tuần tự download→convert/split→upload→ACK đúng như trước — không có goroutine/dequeue không giới hạn, không nhân concurrency ở tầng khác.
- **Recovery khi đổi concurrency hoặc nâng cấp từ bản cũ**: khi worker khởi động, `drainRetiredSlots` liệt kê mọi consumer đã biết (`XINFO CONSUMERS`) và drain (qua `drainPending` có sẵn) pending của: (a) tên consumer trần cũ (trước khi có `WORKER_CONCURRENCY`), (b) bất kỳ slot nào có index >= concurrency hiện tại (sau khi hạ concurrency). Đây KHÔNG phải cơ chế lease/reclaim mới — chỉ tái dùng `drainPending` đã có; nó không phát hiện được một CONSUMER_NAME đổi vì lý do khác (hostname đổi) — giới hạn POC này vẫn giữ nguyên như trước, xem mục 5.
- **Compose override** (`deploy/docker-compose.bench.yaml`): override `cpus`/`mem_limit`/`WORKER_CONCURRENCY` của `splitter`/`extractor-1..3` qua biến môi trường shell, không sửa `docker-compose.test.yaml`. Thêm `extractor-4` (profile `replica4`, không bật mặc định) cho so sánh N=4 pod.
- **Manifest 4 MiB**: `size_threshold_bytes` = 4194304; toàn bộ 15 fixture đã `verifyfixtures -write` lại (bytes/sha256 không đổi, `size_class` tính lại đúng ngưỡng mới); `expected_parts` của mọi fixture `large` đã được xác minh THẬT qua smoke run trên `pipelinetest` ngày 2026-11-15 (xem mục 6) — không còn giá trị suy đoán hoặc kế thừa từ ngưỡng 16 MiB cũ.

## 2. Chưa triển khai / còn thiếu

- Chưa chạy sweep CPU/RAM dài (BR-02), concurrency 1→2→4 dưới tải thật (BR-03), so sánh topology cùng tổng quota (BR-04), mixed/burst (BR-05), hay hold 30–60 phút + restart (BR-06). Mục 4 dưới đây cho lệnh cụ thể để tự chạy; không có số liệu sizing production trong tài liệu này.
- `pdf-small-8mib` (regression case) vẫn **FAILED** dưới ngưỡng 4 MiB: Splitter cắt đúng 3 part, nhưng 2/3 part vẫn vượt `convert_timeout=120s` riêng lẻ (markitdown bị SIGKILL). Hạ ngưỡng kích thước KHÔNG tự giải quyết chi phí hội tụ trên một trang PDF dày đặc — xem mục 6 và `docs/throughput-source-review.md` mục "2. Tách ngưỡng kích thước truyền tải khỏi chi phí xử lý PDF".
- Collector Kubernetes tương đương `loadtest/tools/resources` (hiện chỉ hỗ trợ Docker) chưa có.
- SLO E2E/service-time p95 và error budget chưa được chốt (vẫn như `pod-resource-benchmark-plan.md` mục 1 đã ghi).

## 3. Trước khi chạy bất kỳ sweep nào

1. Dùng project Compose riêng cho benchmark (vd. `pipelinetest`, khác với stack production/dev khác trên cùng máy — không dùng chung port/volume).
2. Rebuild image sau mỗi thay đổi code:
   ```bash
   docker compose -p pipelinetest -f deploy/docker-compose.test.yaml build splitter extractor-1 extractor-2 extractor-3
   ```
3. Ghi commit/image digest:
   ```bash
   git rev-parse HEAD
   docker image inspect pipelinetest-splitter pipelinetest-extractor-1 --format '{{.Id}}'
   ```
4. Xác nhận input/splitted group không còn lag/pending từ run trước:
   ```bash
   docker exec <redis-container> redis-cli XINFO GROUPS stream:doc-extractor:input
   docker exec <redis-container> redis-cli XINFO GROUPS stream:doc-extractor:splitted
   ```
5. Xác nhận fixture manifest còn khớp MinIO đang chạy:
   ```bash
   go run ./loadtest/tools/verifyfixtures -manifest k6/fixtures/manifest.example.json
   ```

## 4. Lệnh chạy theo từng phép đo

Biến `$RUN_ID`, `$RESULT_DIR` dùng xuyên suốt; `$REDIS_PORT`/`$MINIO_PORT` theo cổng publish thực tế của project Compose benchmark của bạn (ví dụ 16379/19000 nếu dùng `docker-compose.test.yaml` với project name mặc định).

```bash
export RUN_ID="run-$(date +%s)"
export RESULT_DIR="docs/test-results/pod-resources/$RUN_ID"
mkdir -p "$RESULT_DIR/manifests" "$RESULT_DIR/resources" "$RESULT_DIR/logs"
```

### 4.1. Baseline N=1 (concurrency=1, resource mặc định)

```bash
docker compose -p pipelinetest -f deploy/docker-compose.test.yaml -f deploy/docker-compose.bench.yaml \
  up -d redis minio splitter extractor-1 extractor-2 extractor-3

go run ./loadtest/tools/drain record -redis-addr localhost:$REDIS_PORT -run-id "$RUN_ID" \
  -out "$RESULT_DIR/manifests/stream-start.json"

go run ./loadtest/tools/resources \
  -compose-file deploy/docker-compose.test.yaml \
  -services splitter,extractor-1,extractor-2,extractor-3,redis,minio \
  -out-dir "$RESULT_DIR/resources" &
RESOURCES_PID=$!

k6 run -e WORKLOAD=large -e RUN_ID="$RUN_ID" k6/load_test.js \
  2>&1 | tee "$RESULT_DIR/logs/k6-large.jsonl"

go run ./loadtest/tools/drain run -redis-addr localhost:$REDIS_PORT -run-id "$RUN_ID" \
  -start-file "$RESULT_DIR/manifests/stream-start.json" \
  -manifest k6/fixtures/manifest.example.json \
  -k6-log "$RESULT_DIR/logs/k6-large.jsonl" \
  -deadline 20m \
  -out "$RESULT_DIR/manifests/drain-report.json" \
  -out-md "$RESULT_DIR/manifests/drain-report.md"

kill $RESOURCES_PID
```

Lưu lại cấu hình: `docker compose -p pipelinetest -f deploy/docker-compose.test.yaml -f deploy/docker-compose.bench.yaml config > "$RESULT_DIR/manifests/compose-config.yaml"` trước khi `up`, cùng `git rev-parse HEAD > "$RESULT_DIR/manifests/commit.txt"`.

### 4.2. Concurrency trong một pod: C=1 → 2 → 4 (BR-03)

Giữ CPU/RAM cố định, chỉ đổi `EXTRACTOR_CONCURRENCY`:

```bash
for C in 1 2 4; do
  EXTRACTOR_CONCURRENCY=$C docker compose -p pipelinetest \
    -f deploy/docker-compose.test.yaml -f deploy/docker-compose.bench.yaml \
    up -d --no-deps extractor-1 extractor-2 extractor-3
  # ... chạy k6 + drain + resources như mục 4.1, đặt RESULT_DIR riêng cho mỗi C
done
```

**Assertion bắt buộc trước khi đọc số liệu**: xác nhận log khởi động mỗi container in đúng `concurrency=$C` (`docker logs <container> | grep "worker mode started"`), và trong lúc tải cao có >1 `TIMING ... phase":"convert"` cùng offset khác nhau overlap theo timestamp (xem mục 6 làm ví dụ) — đừng suy ra concurrency thực từ CPU% một mình.

### 4.3. So sánh topology cùng tổng CPU/RAM (BR-04)

Ví dụ: "1 pod × 2 CPU/2 GiB, C=2" so với "2 pod × 1 CPU/1 GiB, C=1":

```bash
# Topology A: 1 pod, C=2, 2 CPU / 2 GiB dồn vào một container
EXTRACTOR_CPUS=2.0 EXTRACTOR_MEM=2g EXTRACTOR_CONCURRENCY=2 docker compose -p pipelinetest \
  -f deploy/docker-compose.test.yaml -f deploy/docker-compose.bench.yaml \
  up -d redis minio splitter extractor-1
docker compose -p pipelinetest -f deploy/docker-compose.test.yaml stop extractor-2 extractor-3
# ... chạy benchmark, giữ corpus/offered load giống Topology B

# Topology B: 2 pod, C=1 mỗi pod, 1 CPU / 1 GiB mỗi pod (tổng vẫn 2 CPU / 2 GiB)
EXTRACTOR_CPUS=1.0 EXTRACTOR_MEM=1g EXTRACTOR_CONCURRENCY=1 docker compose -p pipelinetest \
  -f deploy/docker-compose.test.yaml -f deploy/docker-compose.bench.yaml \
  up -d redis minio splitter extractor-1 extractor-2
docker compose -p pipelinetest -f deploy/docker-compose.test.yaml stop extractor-3
```

Topology N=4 pod (profile `replica4`):

```bash
EXTRACTOR_CPUS=1.0 EXTRACTOR_MEM=1g EXTRACTOR_CONCURRENCY=1 docker compose -p pipelinetest \
  -f deploy/docker-compose.test.yaml -f deploy/docker-compose.bench.yaml \
  --profile replica4 up -d redis minio splitter extractor-1 extractor-2 extractor-3 extractor-4
```

### 4.4. Đo riêng một stage (BR-01 phần "đo extractor riêng")

```bash
# 1. Dừng Extractor, để Splitter tạo backlog part hợp lệ từ input thật.
docker compose -p pipelinetest -f deploy/docker-compose.test.yaml stop extractor-1 extractor-2 extractor-3
# publish một số input thật (giới hạn số lượng để backlog không quá lớn)

# 2. Dừng Splitter, chỉ bật Extractor để đo riêng.
docker compose -p pipelinetest -f deploy/docker-compose.test.yaml stop splitter
docker compose -p pipelinetest -f deploy/docker-compose.test.yaml up -d extractor-1 extractor-2 extractor-3
# ... đo tài nguyên/timing trong lúc Extractor drain backlog

# 3. Bật lại Splitter sau khi đo xong.
docker compose -p pipelinetest -f deploy/docker-compose.test.yaml up -d splitter
```

Đo Splitter riêng: đảo lại thứ tự (dừng Extractor, chỉ publish vào input, đo Splitter; giới hạn corpus để splitted stream không tích lũy backlog không cần thiết).

## 5. Giới hạn timing log cần biết trước khi đọc số liệu

- `queue_wait` dùng timestamp Redis gán cho message ID (`<ms>-<seq>`), KHÔNG phải một trường enqueue riêng. Giả định đồng hồ Redis và worker đồng bộ (đúng trong mọi môi trường hiện tại: cùng Docker host/VM). Nếu ID không parse được, dòng log ghi `"duration_unknown": true`, không bao giờ suy ra 0.
- Splitter chỉ có hai phase: `download` và `split_upload` (gộp split+upload từng part) — xem lý do ở `internal/usecase/splitfile.go` (field `OnPhase`) và `docs/throughput-source-review.md` mục 7. Không tách được chi tiết hơn nếu không refactor splitter thành pipeline produce/upload riêng (ngoài phạm vi đợt này).
- `total` không gồm `queue_wait`; đây là thời gian handle() xử lý (download/convert/split→upload→commit), không phải E2E. E2E (từ `loadtest/tools/drain`) vẫn là số liệu có thẩm quyền cho SLO người dùng cuối; `total`/`queue_wait` dùng để tách service time khỏi thời gian chờ hàng đợi khi điều tra bottleneck.
- `outcome: "incomplete"` nghĩa là job chưa hoàn tất (lỗi hạ tầng sẽ retry, hoặc shutdown đang diễn ra) — không phải business failure; đừng gộp vào tỷ lệ lỗi.

## 6. Smoke verification đã chạy (2026-11-15, trên `pipelinetest`)

Thực hiện trực tiếp trên stack `pipelinetest` (rebuild từ commit tại thời điểm viết tài liệu này cộng các thay đổi trong đợt này), KHÔNG phải một load test dài:

| Kiểm tra | Kết quả |
|---|---|
| `WORKER_CONCURRENCY` không hợp lệ (`abc`, `0`) | Worker thoát với `log.Fatalf`, exit code 1, message rõ ràng. |
| `WORKER_CONCURRENCY=2` qua compose override thật (`EXTRACTOR_CONCURRENCY=2 docker compose ... up`) | Log khởi động in `concurrency=2`; xác nhận qua `TIMING` log 2 offset convert chồng lấn thời gian. |
| Consumer pool + recovery | `XINFO CONSUMERS` xác nhận danh tính mới `<consumer>-slot-0` tồn tại song song với danh tính cũ (`splitter-1`) không pending — không mất job, không lỗi khi khởi động lại với concurrency khác (0 pending nên không có gì để drain, nhưng code path `drainRetiredSlots` đã chạy không lỗi). |
| Structured timing logs | `TIMING {...}` xuất hiện đúng cho `queue_wait`/`download`/`split_upload` (Splitter) và `queue_wait`/`download`/`convert`/`upload`/`total` (Extractor) với `request_id`/`offset`/`outcome` đúng, không có file_path/URL trong log. |
| Regression fixture `pdf-small-8mib` dưới ngưỡng 4 MiB | Splitter cắt thành 3 part (xác nhận `total=3` trong state), nhưng **FAILED**: 2/3 part vẫn vượt `convert_timeout=120s` riêng lẻ. Hạ ngưỡng không tự giải quyết regression này — ghi nhận là giới hạn chưa khắc phục, không phải thành công. |
| Fixture manifest dưới ngưỡng 4 MiB | 15/15 fixture `verifyfixtures` OK (bytes/sha256 không đổi, `size_class` đúng ngưỡng mới); 9 fixture `large` đã publish thật và lấy `expected_parts` từ event SUCCESS thật (`pdf-small-8mib`=3 nhưng FAILED, `pdf-large-23mib-real`=12, `pdf-large-50mib-synthetic`=13, `docx-large-50mib-synthetic`=13, `xlsx-large-50mib-synthetic`=15, `csv-boundary-under-16mib`=4, `csv-boundary-above-16mib`=5, `csv-boundary-under-4mib`=1, `csv-boundary-above-4mib`=2). Không còn giá trị `expected_parts` suy đoán trong manifest. |
| `go build/vet ./...`, `gofmt -l .` | Sạch. |
| `k6 inspect` cho cả ba workload (`small`/`large`/`mixed`) với manifest mới | Pool build thành công, không lỗi manifest (chỉ cảnh báo VU sizing có sẵn từ trước, không liên quan thay đổi này). |

Những gì KHÔNG chạy trong đợt này: sweep CPU/RAM dài theo BR-02, hold 30–60 phút BR-06, k6 full workload run (chỉ dùng publish Redis trực tiếp + poll để smoke, không chạy toàn bộ k6 scenario) — vẫn cần thực hiện trước khi kết luận sizing production.

## 7. Single-pod 1 CPU / 512 MiB baseline, file 50 MiB (2026-11-15)

`deploy/docker-compose.bench.yaml` đổi default scenario thành **1 pod Extractor duy nhất** (`EXTRACTOR_CPUS`/`EXTRACTOR_MEM` mặc định `1.0`/`512m`, thay vì `2.0`/`1g` cho 3 pod trước đây — xem comment đầu file). Chạy trên `pipelinetest` thật, `extractor-2`/`extractor-3` dừng hẳn (`docker compose stop`), chỉ `splitter` (1 CPU/512 MiB, không đổi) + `extractor-1` (1 CPU/512 MiB, concurrency=1) hoạt động. Fixture: `pdf-large-50mib-synthetic` (`sample_50mb.pdf`, 52,372,933 bytes thật, 13 part — xem giới hạn "synthetic padding" ở manifest, không đại diện chi phí xử lý một PDF thật 50 MiB).

### 7.1. Một request thủ công (XADD trực tiếp, đo rõ từng phase)

| Chỉ số | Giá trị |
|---|---|
| Tổng thời gian E2E (input stream ID → output terminal stream ID) | **17.635 s** (`status=SUCCESS`, `total=13` part) |
| Splitter: download | 145 ms |
| Splitter: split+upload (13 part) | 783 ms |
| Extractor: convert, tổng 13 part (concurrency=1, tuần tự) | 16.570 s (trung bình 1.27 s/part, min 1.10 s, max 1.48 s) |
| Extractor: download/upload mỗi part (MinIO local) | ~4 ms / ~3 ms (không đáng kể) |
| CPU trung bình của `extractor-1` trong lúc convert (cgroup `cpu.stat usage_usec` / wall time) | **~80%** của quota 1 CPU (chéo kiểm bằng `docker stats CPUPerc` avg 79%, max 108.2%) |
| CPU throttling | `nr_throttled`/`nr_periods` = 121/182 (**~66% số period bị throttle** trong lúc convert) — pod gần như bão hòa CPU ở mức 1 CPU trong lúc xử lý; xem ghi chú đáng ngờ về `throttled_usec` tuyệt đối bên dưới |
| RAM peak container (`memory.current`/`docker stats MemUsage`, container-wide, gồm cả Python subprocess `markitdown`) | **~127 MiB** (cgroup 127.41 MiB, docker stats 123.5 MiB) — còn xa giới hạn 512 MiB, không OOM |
| OOM/die/restart | Không có (`events.jsonl` rỗng) |

Kết quả đầy đủ: `docs/test-results/pod-resources/run-1794743165-singlepod-50mb/` (`logs/*-timing.jsonl` cho structured timing log theo part, `resources/` cho `docker stats`/cgroup raw, `manifests/commit.txt`+`image-ids.txt`).

**Ghi chú đáng ngờ, không che giấu**: `cpu.stat`'s `throttled_usec` delta đo được (36.77 s) lớn hơn cả wall time cửa sổ đo (20.9 s) — về mặt lý thuyết cgroup v2 `throttled_usec` không thể vượt quá wall time của chính cgroup đó. Nghi là artifact của cách Docker Desktop macOS đọc cgroup qua VM (xem comment "best-effort" sẵn có trong `loadtest/tools/resources/main.go`), không phải một số đo tin cậy tuyệt đối. Tín hiệu tin cậy hơn và nhất quán giữa hai nguồn độc lập (`cpu.stat usage_usec` và `docker stats CPUPerc`) là **~80% CPU trung bình, đỉnh ~100-110%, với tỷ lệ throttle period cao (~66%)** — tức pod đang chạy gần kịch trần 1 CPU trong lúc convert, không còn nhiều dư địa trước khi convert chậm lại do throttling.

### 7.2. k6 load test ngắn (3 request liên tiếp, 1 doc/phút, cùng cấu hình 1 pod/1 CPU/512 MiB)

Theo yêu cầu bổ sung giữa phiên ("thực hiện load test với k6"): chạy `k6/load_test.js` thật (không phải publish tay) ở quy mô rút gọn — `WORKLOAD=large`, ghim `FIXTURE_IDS=pdf-large-50mib-synthetic` (đúng file 50 MiB đang xét), `RATE_LEVELS=1` (1 doc/phút), `STAGE_HOLD_DURATION=3m`, `MAX_OBSERVATION_SECONDS=120`, `GRACEFUL_STOP=2m` — **không phải** sweep `RATE_LEVELS=1,2,3,4,5` đầy đủ của BR-02 (sẽ mất ~50+ phút, ngoài phạm vi xác minh nhanh này).

Lệnh đã chạy:
```bash
RUN_ID="run-1794743429-k6-singlepod"
go run ./loadtest/tools/drain record -redis-addr localhost:16379 -run-id "$RUN_ID" -out .../stream-start.json
go run ./loadtest/tools/resources -compose-file deploy/docker-compose.test.yaml -services splitter,extractor-1 -cgroup-interval 1s -out-dir .../resources &
cd k6 && k6 run -e WORKLOAD=large -e RUN_ID="$RUN_ID" -e REDIS_URL=redis://127.0.0.1:16379 \
  -e FIXTURE_IDS=pdf-large-50mib-synthetic \
  -e RATE_LEVELS=1 -e STAGE_HOLD_DURATION=3m -e STAGE_RAMP_DURATION=10s \
  -e MAX_OBSERVATION_SECONDS=120 -e GRACEFUL_STOP=2m \
  load_test.js | tee .../logs/k6-large-singlepod.jsonl
go run ./loadtest/tools/drain run -redis-addr localhost:16379 -run-id "$RUN_ID" -start-file .../stream-start.json \
  -manifest k6/fixtures/manifest.example.json -k6-log .../logs/k6-large-singlepod.jsonl \
  -deadline 3m -out .../drain-report.json -out-md .../drain-report.md
```

Kết quả (từ `drain run`, nguồn đúng — xem `docs/test-results/pod-resources/run-1794743429-k6-singlepod/manifests/drain-report.md`):

| Chỉ số | Giá trị |
|---|---|
| accepted / SUCCESS / FAILED / unresolved | 3 / 3 / 0 / 0 |
| E2E p50/p95 (authoritative, Redis stream ID timestamps) | **17.33 s / 17.86 s** |
| E2E p50/p95 (client-observed, k6 poll) | 18.08 s / 18.09 s (≈ +0.7s so với authoritative — chi phí poll interval 1s, đúng như tài liệu đã ghi) |
| Throughput (docs/min, theo cửa sổ hoàn tất) | 1.00 docs/min mỗi cửa sổ 1 phút — khớp đúng tốc độ arrival 1/phút đã cấu hình, không có backlog/suy giảm |
| CPU `extractor-1` cả cửa sổ chạy (227 s, gồm cả thời gian rảnh giữa các request) | avg ~23% (bị pha loãng bởi thời gian rảnh), đỉnh ~100-110% khi đang convert — khớp với phép đo đơn lẻ ở mục 7.1 |
| RAM peak container cả cửa sổ | ~141 MiB (cgroup), ~126 MiB (`docker stats`) |
| OOM/die/restart | Không có |
| `redis_read_errors` (k6 metric) | 3 (1 lỗi đọc tạm thời mỗi iteration, tự retry, không ảnh hưởng outcome — không phải lỗi pipeline) |

Kết luận cho scenario 1 pod / 1 CPU / 512 MiB / concurrency=1 với file ~50 MiB: **xử lý được ổn định, không OOM, không FAILED**, nhưng **CPU là nút thắt rõ** (bão hòa gần 100% trong lúc convert, throttle thường xuyên) — convert tuần tự 13 part chiếm gần như toàn bộ thời gian E2E (16.57/17.6 s ≈ 94%). Tăng concurrency trên cùng 1 CPU sẽ không rút ngắn thời gian (CPU đã gần bão hòa, chỉ thêm context-switch/throttle), muốn giảm latency cho file lớn cỡ này cần thêm CPU hoặc thêm pod (xem BR-03/BR-04 ở mục 4.2/4.3) — đây vẫn là một điểm dữ liệu từ 1 run, không phải sizing production (số run nhỏ, 3 iteration).

## 8. Sweep concurrency trong một pod, cùng file 50 MiB (2026-11-15)

Yêu cầu bổ sung giữa phiên: so sánh concurrency trên 1 pod ở hai mức CPU/RAM khác nhau, cùng `sample_50mb.pdf` (13 part), mỗi cấu hình là **một request thủ công** (không phải k6), đo qua `TIMING` log + `loadtest/tools/resources`. Mỗi dòng dưới là 1 run.

| CPU | RAM limit | `WORKER_CONCURRENCY` | E2E total | Convert avg/part | CPU trung bình (số core dùng) | Tỷ lệ period bị throttle | RAM peak container | Run dir |
|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 512 Mi | 1 | 17.64 s | 1.27 s | ~0.80 / 1 (80%) | 66% (121/182) | ~127 MiB | `run-1794743165-singlepod-50mb` |
| 1 | 512 Mi | 2 | 18.95 s | 2.70 s | ~0.825 / 1 (82.5%) | 92.2% (178/193) | ~246 MiB | `run-1794743856-singlepod-c2-50mb` |
| 2 | 1 Gi | 1 | **13.78 s** | 0.98 s | ~1.07 / 2 | 29.5% (41/139) | ~129 MiB | `run-1794744042-singlepod-2cpu1g-c1` |
| 2 | 1 Gi | 2 | **10.43 s** | 1.41 s | ~1.44 / 2 | 66.7% (70/105) | ~248 MiB | `run-1794744127-singlepod-2cpu1g-c2` |
| 2 | 1 Gi | 4 | 11.44 s | 2.95 s | ~1.36 / 2 | 81.5% (97/119) | ~466 MiB | `run-1794744197-singlepod-2cpu1g-c4` |

Quan sát:

- **Thêm CPU (1→2) giúp cả khi concurrency vẫn =1**: convert/part giảm từ 1.27s xuống 0.98s (−23%), throttle giảm từ 66%→29.5%. Việc này xác nhận một tiến trình `markitdown` đơn luồng vẫn có thể burst dùng >100% của 1 lõi trong khoảnh khắc ngắn (thư viện PDF/parsing bên dưới không hoàn toàn single-threaded), nên bị throttle ngay cả ở concurrency=1 khi chỉ có 1 CPU.
- **Ở 2 CPU, concurrency=2 là điểm tốt nhất** trong sweep này: nhanh hơn concurrency=1 cùng CPU 9.3% (13.78s → 10.43s) và nhanh hơn baseline 1 CPU/concurrency=1 tới 41% (17.64s → 10.43s). Concurrency khớp với số CPU cấp cho pod mới thực sự tạo ra xử lý song song có lợi.
- **Ở 2 CPU, concurrency=4 lại chậm hơn concurrency=2** (11.44s > 10.43s): 4 tiến trình Python tranh nhau 2 lõi (oversubscription 2:1) khiến mỗi convert chậm đi (2.95s/part so với 1.41s) nhiều hơn phần lợi từ việc gộp sóng xử lý; throttle tăng lên 81.5%. RAM cũng tăng gần gấp đôi so với concurrency=2 (466 MiB so với 248 MiB) mà không đổi lại tốc độ.
- **Giới hạn của kết luận này**: đây là **một tài liệu 50 MiB, 13 part, xử lý đơn lẻ (không có traffic khác chồng lấn)** — concurrency=4 có thể vẫn có lợi trong kịch bản nhiều tài liệu đến đồng thời (concurrency lấp đầy khoảng `queue_wait`/I-O giữa các job khác nhau thay vì tranh CPU trong cùng một job), khác với thử nghiệm này nơi toàn bộ 13 part đều thuộc một request và cùng cạnh tranh CPU. Chưa test concurrency=3, chưa test với 2+ document đồng thời ở 2 CPU, và đây vẫn là 1 run/cấu hình — không phải sizing production.

## 9. Lặp lại 3 lần: 1 CPU/256 Mi/concurrency=1 vs 2 CPU/512 Mi/concurrency=2 (2026-11-15)

Yêu cầu bổ sung: lặp lại vài lần mỗi cấu hình để kiểm tra độ ổn định (không chỉ 1 run), cùng `sample_50mb.pdf`. **Ghi chú đơn vị**: yêu cầu gốc ghi "256GB/512GB" — máy Docker Desktop dùng để test chỉ có tổng 7.75 GiB RAM (`docker info --format '{{.MemTotal}}'`), không thể set mem_limit cỡ GB đó; đã xác nhận lại với người yêu cầu và chạy với **256 MiB / 512 MiB** (khớp thang đo RAM đã dùng ở mục 7-8).

| Cấu hình | Iter 1 | Iter 2 | Iter 3 | Median | RAM peak (cả 3 iter) | CPU trung bình (cả cửa sổ, gồm thời gian rảnh giữa các iter) | OOM/restart |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1 CPU / 256 Mi / concurrency=1 | 17.833 s | 17.689 s | 17.331 s | **17.689 s** | 131.3 MiB / 256 MiB (51%) | ~0.62 / 1 core | Không |
| 2 CPU / 512 Mi / concurrency=2 | 9.949 s | 9.301 s | 9.888 s | **9.888 s** | 244.8 MiB / 512 MiB (48%) | ~1.01 / 2 core | Không |

Run dir: `run-1794744607-singlepod-1cpu256m-c1/`, `run-1794744722-singlepod-2cpu512m-c2/` (dưới `docs/test-results/pod-resources/`).

**Kết quả ổn định qua 3 lần lặp** (độ lệch giữa min/max mỗi cấu hình < 3%), không có outlier, không OOM/restart ở cả hai mức RAM thấp hơn đã test trước đó (256 Mi và 512 Mi đều đủ — RAM peak thực tế chỉ ~50% giới hạn ở cả hai cấu hình).

**Cấu hình 2 CPU / 512 Mi / concurrency=2 nhanh hơn ~44% (median)** so với 1 CPU / 256 Mi / concurrency=1 (9.888s so với 17.689s). Đáng chú ý: tổng **CPU-giây tiêu thụ mỗi tài liệu gần như bằng nhau giữa hai cấu hình** (ước tính từ CPU trung bình × wall time cả cửa sổ / 3 iter: ~16.9 core-giây/doc ở cấu hình 1 CPU so với ~17.5 core-giây/doc ở cấu hình 2 CPU) — tức công việc CPU cần làm cho mỗi tài liệu không đổi, chỉ khác ở việc có đủ CPU để làm song song hay không. Nói cách khác, cấu hình 2 CPU/512 Mi/concurrency=2 không "rẻ hơn" về tổng CPU-work, nhưng **giảm gần một nửa độ trễ cho cùng một lượng công việc**, mà RAM dùng thực tế vẫn rất an toàn (48%).

**Kết luận "cấu hình nào tối ưu"**: phụ thuộc mục tiêu tối ưu.
- Nếu tối ưu **độ trễ per-document** (ví dụ SLA P95 cho file lớn): chọn **2 CPU / 512 Mi / concurrency=2** — nhanh hơn rõ rệt, RAM vẫn dư margin lớn, không đổi lại gì về CPU-work tổng.
- Nếu tối ưu **mật độ pod trên node** (đóng gói nhiều pod nhỏ/node, chấp nhận latency cao hơn cho file lớn): **1 CPU / 256 Mi / concurrency=1** vẫn là lựa chọn hợp lý — dùng ít tài nguyên hơn cho cùng tổng CPU-work, chỉ đổi lấy thời gian hoàn thành dài hơn cho riêng các tài liệu lớn (file nhỏ dưới ngưỡng 4 MiB không bị ảnh hưởng vì không qua Splitter).
- Không có cấu hình nào "miễn phí" nhanh hơn — tăng tốc đến từ tăng CPU thật, không phải từ bản thân concurrency hay RAM (xem mục 8: RAM tăng không giúp gì nếu CPU không đổi).

Giới hạn: 3 lần lặp mỗi cấu hình, 1 loại tài liệu (PDF 50 MiB synthetic), không có traffic đồng thời khác, không đo trên nhiều loại file/kích thước khác — chưa đủ để kết luận sizing production cho toàn bộ workload.
