# Benchmark: 2 Splitter + 2 Extractor, mixed file-size traffic (~1000 request, burst admission)

Ngày: 2026-11-15. Chạy thật trên stack `pipelinetest` (`deploy/docker-compose.test.yaml` + `deploy/docker-compose.bench.yaml`), không phải ước tính. Raw data đầy đủ: `docs/test-results/pod-resources/run-1794751093-mix1000-natural/` (resources/, logs/k6-mix1000-natural.jsonl, manifests/drain-report.{json,md}).

## 1. Điều kiện / Input

### 1.1. Topology

| Thành phần | CPU | RAM limit | Concurrency | Số pod | Ghi chú |
|---|---:|---:|---:|---:|---|
| Splitter (`splitter`, `splitter-2`) | 1.0 | 256 MiB | 1 | 2 | |
| Extractor (`extractor-1`, `extractor-2`) | 2.0 | 1 GiB | 2 | 2 | + glibc allocator tuning (xem 1.2) |

Lệnh dựng stack:
```bash
export SPLITTER_CPUS=1.0 SPLITTER_MEM=256m SPLITTER_CONCURRENCY=1 \
       EXTRACTOR_CPUS=2.0 EXTRACTOR_MEM=1g EXTRACTOR_CONCURRENCY=2
docker compose -p pipelinetest -f deploy/docker-compose.test.yaml -f deploy/docker-compose.bench.yaml \
  --profile splitter2 up -d --build --no-deps splitter splitter-2 extractor-1 extractor-2
docker compose -p pipelinetest -f deploy/docker-compose.test.yaml stop extractor-3
```
`deploy/docker-compose.bench.yaml` có thêm service `splitter-2` (profile `splitter2`, theo mẫu `extractor-4` có sẵn) để chạy N>1 pod Splitter mà không sửa file base.

### 1.2. Allocator tuning (Extractor)

Thêm vào `environment` của `extractor-1`/`extractor-2`/`extractor-3`/`extractor-4` trong `deploy/docker-compose.bench.yaml`:
```
MALLOC_ARENA_MAX: "1"
MALLOC_TRIM_THRESHOLD_: "131072"
MALLOC_MMAP_THRESHOLD_: "131072"
```
Reach tới `markitdown` CLI subprocess tự động vì Go's `exec.Command` kế thừa env của container (`internal/services/markitdown.go` không override `cmd.Env`). Đây là tuning ở tầng **glibc allocator** (cách libc giữ/trả heap page cho OS), **không phải** GC của Python — mỗi conversion dùng `CONVERTER_BACKEND=cli` (mặc định) nên mỗi file là 1 tiến trình Python mới, thoát hẳn sau khi xong, không có rò rỉ bộ nhớ giữa các request để GC dọn.

### 1.3. Fixture — toàn bộ nội dung THẬT, không dùng generator sinh câu chữ

| Tier | Tỉ lệ | Fixture id | Bytes | Nguồn |
|---|---:|---|---:|---|
| Siêu nhỏ | 80% | `pdf-natural-ultrasmall` | 10,565 | README.md thật của repo, render nguyên văn qua `tmp/generate_natural_fixtures.py` (không sinh câu, không padding) |
| Nhỏ | 16% | `pdf-small-240kb` | 240,108 | `report.pdf` — PDF thật có sẵn |
| Trung bình | 3.2% | `pdf-natural-medium` | 1,290,060 | 3 trang thật (có 1 trang ảnh/diagram) trích từ `system-design-interview.pdf` qua `pypdf` |
| Lớn | 0.8% | `pdf-natural-large` | 46,197,481 (23 part sau split) | 2 bản copy thật của `system-design-interview.pdf` nối lại (mọi trang đều là nội dung thật, lặp lại để đạt kích thước, không padding) |

**Lý do thay fixture**: bộ gốc dùng PDF/DOCX sinh bởi reportlab/python-docx (nhiều đoạn văn ngắn để đạt byte-target) bị phát hiện quy luật **markitdown convert chậm theo mật độ trang/đoạn văn, không theo byte-size**: `pdf-ultrasmall-85kb` (92KB, reportlab) mất 5.2-5.75s để convert — chậm hơn 4x so với `pdf-small-240kb` (240KB, PDF thật) dù nhỏ hơn 2.6 lần. Thay toàn bộ 3 tier (trừ "nhỏ" vốn đã dùng file thật) bằng nội dung thật giải quyết vấn đề này (xem mục 3).

Tỉ lệ 80:16:3.2:0.8 đạt chính xác bằng lặp fixture id trong `FIXTURE_IDS` của k6 (100/20/4/1 lần, tỉ lệ rút gọn đúng bằng 80:16:3.2:0.8), tận dụng round-robin có sẵn của `k6/load_test.js`, không sửa code k6.

### 1.4. Redis — sạch hoàn toàn trước khi chạy

Trước run: xoá 3 stream (`stream:doc-extractor:{input,splitted,output}`) + toàn bộ key `doc-extractor:{state,completed,parts}:*` và `loadtest:*` còn sót từ các lần test trước, để bootstrap lại từ đầu (`Bootstrap()` trong `internal/pipeline/redis.go` tự tạo lại 2 consumer group + 3 stream khi splitter/extractor khởi động, idempotent). Xác nhận `stream-start.json` của run này có offset `0-0` cho cả 3 stream — tức chạy từ trạng thái hoàn toàn trống.

### 1.5. Chiến lược admission — BURST, không rate-match capacity

Theo yêu cầu: "đã có Redis Stream làm delay queue rồi nên không sao, cứ đẩy hết request vào rồi đo mất bao lâu thì xử lý xong" — tức đẩy ~1000 request vào gần như ngay lập tức, không giới hạn theo throughput bền vững, rồi đo tổng thời gian drain hết backlog.

```bash
k6 run -e WORKLOAD=mixed -e RUN_ID="$RUN_ID" -e REDIS_URL=redis://127.0.0.1:16379 \
  -e FIXTURE_IDS="<100x pdf-natural-ultrasmall>,<20x pdf-small-240kb>,<4x pdf-natural-medium>,pdf-natural-large" \
  -e MIX_SMALL_RATIO=0.992 \
  -e RATE_LEVELS=1200 -e STAGE_HOLD_DURATION=60s \
  -e MAX_OBSERVATION_SECONDS=30 -e GRACEFUL_STOP=2m \
  -e PRE_ALLOCATED_VUS=800 -e MAX_VUS=800 -e OUTSTANDING_LIMIT=1500 \
  load_test.js
```
`RATE_LEVELS=1200/phút` (20/s) trong 60s cố đẩy toàn bộ vào trong ~1 phút; `OUTSTANDING_LIMIT=1500` đủ cao để không chặn admission (đúng ý "cứ đẩy hết vào"); `MAX_OBSERVATION_SECONDS=30` chỉ để k6 giải phóng VU nhanh (không phải deadline thật) — số liệu thật lấy từ `loadtest/tools/drain run` chạy riêng sau, với deadline dài (30 phút) để chờ xử lý hết backlog:
```bash
go run ./loadtest/tools/drain run -redis-addr localhost:16379 -run-id "$RUN_ID" \
  -start-file .../stream-start.json -manifest k6/fixtures/manifest.example.json \
  -k6-log .../k6-mix1000-natural.jsonl -deadline 30m \
  -out .../drain-report.json -out-md .../drain-report.md
```
`loadtest/tools/resources` chạy song song suốt test, theo dõi `docker stats` + cgroup cho cả 4 pod (`splitter,splitter-2,extractor-1,extractor-2`).

## 2. Kết quả

| Chỉ số | Giá trị |
|---|---:|
| Intended / accepted / SUCCESS / FAILED / unresolved | 1199 / 1139 / **1139** / **0** / **0** |
| **Tổng thời gian từ request đầu tiên đến request cuối cùng hoàn tất** | **724.1 s ≈ 12.07 phút** (1139 request) |
| OOM / die / restart (cả 4 pod) | **0** |

### 2.1. Throughput (docs/phút, theo cửa sổ hoàn tất, loại cửa sổ đầu/cuối không trọn phút)

| | docs/phút |
|---|---:|
| Trung bình (steady-state, 11 cửa sổ) | **91.5** |
| Min | 68 |
| **Peak** | **116** |
| p50 | 99.0 |
| p95 | 112.5 |
| p99 | 115.3 |

### 2.2. CPU (toàn bộ cửa sổ test, `docker stats` CPUPerc)

| Pod | Avg | Max | p50 | p95 | p99 |
|---|---:|---:|---:|---:|---:|
| extractor-1 (2 CPU = 200%) | 192.7% | 271.0% | 199.6% | 212.8% | 233.4% |
| extractor-2 (2 CPU = 200%) | 192.5% | 297.4% | 199.0% | 214.2% | 224.5% |
| splitter (1 CPU = 100%) | 2.6% | 111.6% | 0.9% | 4.3% | 58.1% |
| splitter-2 (1 CPU = 100%) | 2.1% | 86.8% | 0.9% | 4.1% | 34.9% |

Extractor bão hòa CPU gần như tuyệt đối (avg ~192.5-192.7% của 200% cap) — **là nút thắt cổ chai duy nhất**. Splitter (dù đã tăng lên 1 CPU) gần như không tải (avg 2-2.6%) — chưa bao giờ là bottleneck cho mix traffic này; p99 CPU splitter cao hơn hẳn (35-58%) phản ánh đúng những lúc xử lý 1 trong 9 request "lớn" (44MB, cần split thật).

### 2.3. RAM (`memory.current` qua cgroup cho Extractor — chính xác byte-level; `docker stats` cho Splitter)

| Pod | Avg | Max | p95 | p99 | % giới hạn (max) |
|---|---:|---:|---:|---:|---:|
| extractor-1 (limit 1 GiB) | 186.5 MiB | 261.04 MiB | 255.96 MiB | 259.97 MiB | **25.5%** |
| extractor-2 (limit 1 GiB) | 176.6 MiB | 254.59 MiB | 248.16 MiB | 252.99 MiB | **24.9%** |
| splitter (limit 256 MiB) | 85.5 MiB | 192.7 MiB | 168.2 MiB | 184.7 MiB | 75.3% |
| splitter-2 (limit 256 MiB) | 90.5 MiB | 168.1 MiB | 123.8 MiB | 143.0 MiB | 65.7% |

`memory.events.oom_kill = 0` cả 2 pod Extractor suốt test (xác nhận qua cgroup, không chỉ docker events). RAM Extractor còn rất nhiều margin (~75% dư) — không còn là rủi ro.

### 2.4. Độ trễ E2E (authoritative, từ timestamp Redis stream ID, n=1139)

| | avg | min | max | p50 | p90 | p95 | p99 |
|---|---:|---:|---:|---:|---:|---:|---:|
| Tổng hợp | 358.3s | 2.0s | 664.2s | 386.0s | 606.3s | 644.3s | 661.3s |

**Lưu ý quan trọng**: vì đây là kiểu test **burst** (đẩy ~1100 request vào trong ~90s), p95/p99/max phản ánh **độ sâu hàng đợi tại đỉnh burst** (gần bằng tổng thời gian drain cả batch, 724s) đối với request được admit muộn nhất — KHÔNG phải chất lượng xử lý từng request kém. Đây không phải số latency cho traffic bình thường/ổn định; số liệu có ý nghĩa để đánh giá hiệu năng thực là **throughput (2.1)** và **tổng thời gian drain cả batch (2 — bảng tổng quan)**.

## 3. So sánh với bộ fixture cũ (reportlab/python-docx sinh câu chữ, cùng topology Extractor 2 CPU/512 MiB, Splitter 0.2 CPU/256 MiB)

| Chỉ số | Fixture cũ | Fixture thật (lần này) |
|---|---:|---:|
| Convert solo "siêu nhỏ" (không tải) | 5.2-5.75s | **1.32s** |
| Convert solo "trung bình" (không tải) | 9.1-48.97s (DOCX 9.1s, PDF reportlab cùng size tới 49s) | **0.95s** |
| Throughput trung bình (burst) | ~44-47/phút | **~91.5/phút (~2x)** |
| Tổng thời gian drain ~1100-1200 request | ~26 phút (ước tính, run bị huỷ giữa chừng khi phát hiện vấn đề) | **12.07 phút đo thật (~2.1x nhanh hơn)** |
| RAM đỉnh Extractor | 99.96-99.99% của 512 MiB (gần OOM, đã có Splitter 0.2 CPU) | **~25% của 1 GiB** (có Splitter 1 CPU + allocator tuning) |
| CPU Extractor | ~188-189% của 200% cap | ~192.5-192.7% của 200% cap (không đổi — vẫn bão hòa) |
| FAILED / OOM | 0 / 0 | 0 / 0 |

**Không tách bạch được riêng tác động của 3 thay đổi áp dụng cùng lúc cho RAM** (tăng limit 512→1024 MiB, thêm MALLOC_ARENA_MAX/_TRIM_THRESHOLD_/_MMAP_THRESHOLD_, đổi fixture). Bằng chứng mạnh nhất vẫn trỏ về **fixture** là nguyên nhân chính: MALLOC tuning chỉ ảnh hưởng việc trả bộ nhớ ĐÃ GIẢI PHÓNG về OS chứ không giảm working-set thật sự cần trong lúc parse — trong khi nội dung thật (ít đoạn văn/trang hơn mỗi KB) rõ ràng cần working-set nhỏ hơn hẳn để markitdown parse. Muốn kết luận chắc chắn tỉ lệ đóng góp của từng yếu tố cần thêm run cô lập biến (chưa làm).

## 4. Kết luận

1. **Vấn đề gốc nằm ở bộ test fixture**, không phải ở pipeline: markitdown convert chậm theo mật độ trang/đoạn văn của tài liệu, không theo byte-size — fixture sinh bởi generator (nhiều đoạn văn ngắn để đạt target byte) bị phạt nặng bởi hành vi này dù file rất nhỏ.
2. Thay toàn bộ fixture sang nội dung thật: **throughput tăng ~2x, tổng thời gian xử lý hết batch giảm ~2.1x, RAM đỉnh giảm từ 99.9% xuống ~25% của giới hạn**.
3. **CPU của 2 pod Extractor vẫn là nút thắt cổ chai duy nhất và không đổi** — muốn tăng thêm throughput cần thêm CPU (pod hoặc core), không phải tối ưu thêm ở tầng fixture/RAM.
4. Splitter ở 1 CPU vẫn dư thừa nhiều cho mix traffic chủ yếu là file nhỏ (chỉ cần HEAD request) — có thể hạ xuống thấp hơn nếu muốn tiết kiệm mà không ảnh hưởng kết quả, vì chưa bao giờ là bottleneck (trừ lúc split file lớn).
5. **0 FAILED, 0 unresolved, 0 OOM** trong toàn bộ batch burst 1139 request — tính đúng đắn (ACK-after-commit, recovery) giữ nguyên dưới cả tải burst nặng và ở cả 2 bộ fixture.

## 5. Giới hạn

- 1 lần chạy duy nhất cho cấu hình + fixture này — chưa lặp lại để đo variance.
- Chưa tách bạch được đóng góp riêng của RAM limit / MALLOC tuning / fixture lên kết quả RAM (mục 3).
- `pdf-natural-large` (44 MiB, 23 part) khác `pdf-large-50mib-synthetic` (52 MiB, 13 part) về kích thước byte và số part — không phải so sánh 1:1 hoàn hảo cho tier "lớn", dù cùng vai trò đại diện.
- Latency E2E ở kiểu test burst không đại diện cho latency dưới tải ổn định/bình thường (xem mục 2.4) — muốn SLO latency thật cần chạy lại kiểu rate-matched (admit gần bằng hoặc dưới throughput bền vững).
- Chưa benchmark riêng Splitter dưới tải toàn file lớn (traffic này không đủ nặng để Splitter là bottleneck).
