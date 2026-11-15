# Load Test Results — Event-Driven Document Pipeline (run-002)

**Ngày thực hiện:** 2026-11-14  
**Môi trường:** Docker Compose stack `pipelinetest` (`deploy/docker-compose.test.yaml`) trên macOS (Docker VM 8 Cores / 8 GiB RAM).  
**Cấu hình stack:**
- `splitter`: 1 replica, 1 CPU / 512 MiB limit, `SERVICE_MODE=worker` (image nền `gcr.io/distroless/static-debian12`)
- `extractor-1`, `extractor-2`, `extractor-3`: 3 replicas, 2 CPU / 1 GiB limit mỗi container, `SERVICE_MODE=worker` (image nền `python:3.13-slim` + Go binary)
- `redis`: Redis 7.4 (port localhost:16379), Streams persistence
- `minio`: MinIO (localhost:19000, dataset bucket)
- k6 test runner: `k6 v2.4.0` (k6/x/redis extension `v0.4.5`)

---

## 1. Bảng tổng hợp kết quả (theo Mục 8 Load Test Plan)

| Workload | Rate / replica count | Cửa sổ đo | SUCCESS/min | MiB/min | RAM peak Splitter/Extractor | CPU peak | OOM | Lag/pending đầu→cuối | FAILED / unresolved | Kết luận |
|---|---|---|---:|---:|---|---|:---:|---|---|---|
| **Small-only** (`run-1794655173-small`) | 5→10 docs/min / 1 Splitter, 3 Extractors | 11:20:00Z–11:41:00Z (21m hold) | **5.00** bền vững (peak 9.00) | **18.48** bền vững (peak 19.85) | Splitter: 65.2 MiB (12.7%)<br>Extractors: 511.3 MiB (49.9%) | Splitter: 7.5%<br>Extractors: 154.6% | 0 | Input: 0/0 → 0/0<br>Splitted: 0/0 → 4/3 → 0/0 | 17 FAILED (100% là `pdf-small-8mib` timeout 120s) / 0 unresolved | **Throughput bền vững: 5.0 docs/min.** Đạt bão hòa CPU Extractor tại 10 docs/min; kích hoạt `OUTSTANDING_LIMIT:20` do 8MB PDF nghẽn worker Python >120s. |
| **Large-only** (`run-1794656632-large`) | 1→2 docs/min / 1 Splitter, 3 Extractors | 11:46:00Z–12:06:00Z (20m hold) | **2.00** bền vững (peak 3.00) | **78.53** bền vững (peak 106.51) | Splitter: 255.2 MiB (49.8%)<br>Extractors: 716.3 MiB (69.9%) | Splitter: 109.4%<br>Extractors: 152.5% | 0 | Input: 0/0 → 0/0<br>Splitted: 0/0 → 1/3 → 0/0 | 0 FAILED / 0 unresolved (100% SUCCESS, 33/33 docs) | **Throughput bền vững: 2.0 docs/min** (~80–106 MiB/min). 100% completion rate. Splitter chia đúng part (2–4 parts), 3 Extractors xử lý song song không backlog. |
| **Mixed (80/20)** (`run-1794658027-mixed`) | 1→2 docs/min / 1 Splitter, 3 Extractors | 12:08:00Z–12:28:00Z (20m hold) | **2.00** bền vững (peak 3.00) | **50.23** bền vững (peak 66.00) | Splitter: 149.5 MiB (29.2%)<br>Extractors: 485.7 MiB (47.4%) | Splitter: 80.4%<br>Extractors: 144.4% | 0 | Input: 0/0 → 0/0<br>Splitted: 0/0 → 1/3 → 0/0 | 3 FAILED (`pdf-small-8mib` timeout) / 0 unresolved (29/32 SUCCESS) | **Throughput bền vững: 2.0 docs/min** (~50–66 MiB/min). Không bị head-of-line blocking nghiêm trọng: DOCX nhỏ p50=7.9s, XLSX nhỏ p50=1.6s, CSV nhỏ p50=1.9s. |

---

## 2. Chi tiết từng Workload

### 2.1. Workload 1: Small-only (`run-1794655173-small`)

- **Tổng nạp:** 130 requests intended, 130 accepted, 0 lost publish.
- **Kết quả đối soát:** 113 SUCCESS, 17 FAILED, 0 unresolved (`accepted == success + failed + unresolved: true`).
- **Tài nguyên peak:**
  - `pipelinetest-splitter-1`: CPU 7.53%, RAM 65.16 MiB / 512 MiB (12.7%)
  - `pipelinetest-extractor-1-1`: CPU 152.47%, RAM 486.40 MiB / 1 GiB (47.5%)
  - `pipelinetest-extractor-2-1`: CPU 154.61%, RAM 511.30 MiB / 1 GiB (49.9%)
  - `pipelinetest-extractor-3-1`: CPU 150.00%, RAM 484.20 MiB / 1 GiB (47.3%)
  - `pipelinetest-redis-1`: CPU 5.69%, RAM 18.34 MiB
  - `pipelinetest-minio-1`: CPU 34.90%, RAM 381.00 MiB
  - Cgroup OOM kills: **0** across all containers.

#### Breakdown by Format (`size_class x format`)

| Format | Accepted | SUCCESS | FAILED | SUCCESS MiB | E2E p50 (Authoritative) | E2E p95 (Authoritative) | E2E p50 (Client k6) | E2E p95 (Client k6) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| **csv** (`csv-boundary-under-16mib`) | 16 | 16 | 0 | 256.00 MiB | 15.03 s | 81.80 s | 16.03 s | 82.16 s |
| **docx** (100KB, 1MB) | 32 | 32 | 0 | 18.61 MiB | 18.82 s | 91.42 s | 19.10 s | 92.16 s |
| **pdf** (240KB, 1MB, 8MB) | 49 | 32 | 17 | 20.48 MiB | 49.74 s | 109.74 s | 50.09 s | 110.20 s |
| **xlsx** (100KB, 1MB) | 33 | 33 | 0 | 20.35 MiB | 12.92 s | 72.09 s | 12.03 s | 72.17 s |

*Ghi chú:* Toàn bộ 17 lỗi FAILED đều là fixture `pdf-small-8mib` do Python `markitdown` chạy vượt quá 120s timeout của Extractor worker (`convert_timeout = 2m0s`), worker chủ động gửi SIGKILL và đánh dấu FAILED.

---

### 2.2. Workload 2: Large-only (`run-1794656632-large`)

- **Tổng nạp:** 33 requests intended, 33 accepted, 0 lost publish.
- **Kết quả đối soát:** 33 SUCCESS, 0 FAILED, 0 unresolved (`accepted == success + failed + unresolved: true`). Tỷ lệ hoàn thành **100%**.
- **Tổng dung lượng xử lý thành công:** 1,296.68 MiB (1.27 GiB).
- **Tài nguyên peak:**
  - `pipelinetest-splitter-1`: CPU 109.43%, RAM 255.20 MiB / 512 MiB (49.8%)
  - `pipelinetest-extractor-1-1`: CPU 152.53%, RAM 716.30 MiB / 1 GiB (69.9%)
  - `pipelinetest-extractor-2-1`: CPU 141.81%, RAM 708.80 MiB / 1 GiB (69.2%)
  - `pipelinetest-extractor-3-1`: CPU 143.48%, RAM 714.40 MiB / 1 GiB (69.8%)
  - `pipelinetest-redis-1`: CPU 9.24%, RAM 18.22 MiB
  - `pipelinetest-minio-1`: CPU 31.56%, RAM 527.20 MiB
  - Cgroup OOM kills: **0** across all containers.

#### Breakdown by Format (`size_class x format`)

| Format | Fixture ID | Accepted | SUCCESS | Parts | SUCCESS MiB | E2E p50 (Authoritative) | E2E p95 (Authoritative) |
|---|---|---:|---:|:---:|---:|---:|---:|
| **csv** | `csv-boundary-above-16mib` (17MB) | 6 | 6 | **2** | 97.27 MiB | 2.47 s | 2.54 s |
| **docx** | `docx-large-50mib-synthetic` | 7 | 7 | **4** | 349.99 MiB | 22.15 s | 23.10 s |
| **pdf** | `pdf-large-23mib-real` & `50mib-synthetic` | 13 | 13 | **3 & 4** | 453.81 MiB | 8.03 s | 15.48 s |
| **xlsx** | `xlsx-large-50mib-synthetic` | 7 | 7 | **4** | 395.61 MiB | 32.09 s | 35.50 s |

*Xác nhận expected_parts từ live SUCCESS events:*
- `pdf-large-23mib-real`: **3 parts** (E2E p50 = 8.03s, 3 extractors chạy song song).
- `docx-large-50mib-synthetic`: **4 parts** (E2E p50 = 22.15s).
- `xlsx-large-50mib-synthetic`: **4 parts** (E2E p50 = 32.09s).
- `pdf-large-50mib-synthetic`: **4 parts** (E2E p50 = 5.02s).
- `csv-boundary-above-16mib`: **2 parts** (E2E p50 = 2.47s).

---

### 2.3. Workload 3: Mixed (`run-1794658027-mixed`)

- **Tỷ lệ mix:** 80% small / 20% large, thuật toán xen kẽ đều Bresenham.
- **Tổng nạp:** 32 requests intended, 32 accepted, 0 lost publish.
- **Kết quả đối soát:** 29 SUCCESS, 3 FAILED (100% là `pdf-small-8mib`), 0 unresolved.
- **Tổng dung lượng xử lý thành công:** 409.99 MiB.
- **Tài nguyên peak:**
  - `pipelinetest-splitter-1`: CPU 80.43%, RAM 149.50 MiB / 512 MiB (29.2%)
  - `pipelinetest-extractor-1-1`: CPU 144.44%, RAM 485.70 MiB / 1 GiB (47.4%)
  - `pipelinetest-extractor-2-1`: CPU 134.29%, RAM 438.70 MiB / 1 GiB (42.8%)
  - `pipelinetest-extractor-3-1`: CPU 139.98%, RAM 457.50 MiB / 1 GiB (44.7%)
  - `pipelinetest-redis-1`: CPU 4.51%, RAM 18.20 MiB
  - `pipelinetest-minio-1`: CPU 25.83%, RAM 385.30 MiB
  - Cgroup OOM kills: **0** across all containers.

#### Breakdown by Format (`size_class x format`)

| Size Class | Format | Accepted | SUCCESS | FAILED | SUCCESS MiB | E2E p50 (Authoritative) | E2E p95 (Authoritative) |
|---|---|---:|---:|---:|---:|---:|---:|
| **large** | **docx** (50 MiB, 4 parts) | 7 | 7 | 0 | 349.99 MiB | 22.01 s | 36.85 s |
| **small** | **csv** (boundary under 16 MiB) | 3 | 3 | 0 | 48.00 MiB | 1.91 s | 1.98 s |
| **small** | **docx** (100KB, 1MB) | 7 | 7 | 0 | 4.55 MiB | 7.91 s | 8.35 s |
| **small** | **pdf** (240KB, 1MB, 8MB) | 9 | 6 | 3 | 3.84 MiB | 1.20 s | 47.60 s |
| **small** | **xlsx** (100KB, 1MB) | 6 | 6 | 0 | 3.61 MiB | 1.63 s | 9.92 s |

---

## 3. Phân tích Bottleneck & Đánh giá Kiến trúc

### 3.1. Điểm nghẽn chính: CPU Extractor & `markitdown` CLI Subprocess
- **Splitter hoàn toàn không phải nút thắt:** Splitter CPU chỉ tốn 7–109% (chỉ spike ngắn khi split và zip lại các file 50MB lớn), RAM peak 255 MiB (<50% budget 512 MiB), lag stream `stream:doc-extractor:input` luôn duy trì ở mức 0.
- **Extractor là bottleneck của hệ thống:**
  - Mỗi Extractor worker chạy tuần tự từng part (`SERVICE_MODE=worker`). Khi convert, worker gọi Python CLI subprocess `markitdown`.
  - Python process chỉ sử dụng tối đa 1 CPU core (~100–150% do Python runtime + I/O).
  - Với 3 Extractor replicas, hệ thống có tổng cộng 3 slots xử lý song song.
  - Các tài liệu văn phòng chuẩn (DOCX, XLSX, CSV, PDF nhỏ) hoàn thành rất nhanh:
    - CSV 16–17MB: ~2.0s – 2.5s
    - DOCX/XLSX 100KB: ~1.5s – 2.0s
    - DOCX/XLSX 1MB: ~8s – 12s
    - PDF thật 23MB (chia 3 part): ~8s – 9s
    - DOCX/XLSX 50MB (chia 4 part): ~22s – 36s
  - **Ngoại lệ nghiêm trọng:** Fixture `pdf-small-8mib.pdf` (file PDF nội dung văn bản dày đặc chưa qua split) khiến Python `markitdown` chạy vượt quá 120s. Worker kích hoạt timeout `convert_timeout = 2m0s`, gửi SIGKILL và đánh dấu FAILED.

### 3.2. Đánh giá tính an toàn bộ nhớ (RAM & OOM)
- **Không có bất kỳ sự cố OOM nào (0 OOM kills):**
  - Giới hạn RAM của Extractor là 1 GiB. Trong suốt quá trình tải nặng nhất (file 50MB, đồng thời 3 worker), RAM peak của Extractor cao nhất ghi nhận là **716.30 MiB (69.9%)**, còn dư hơn 300 MiB an toàn.
  - Sau khi kết thúc tải hoặc giữa các khoảng nghỉ, RAM của Extractor tự động giải phóng về baseline **~18–20 MiB** (Go runtime GC và Python subprocess đã terminate). Không có hiện tượng rò rỉ bộ nhớ (memory leak).
  - Splitter peak RAM là **255.20 MiB (49.8%)** trên limit 512 MiB, chủ yếu là buffer trong bộ nhớ khi re-encode zip/pdf parts.

### 3.3. Hiệu quả phân mảnh tài liệu lớn (Splitting Effect)
- Việc chia file lớn (>16 MiB) thành các part nhỏ và fanout vào `stream:doc-extractor:splitted` mang lại lợi ích hiệu năng vượt trội:
  - File `pdf-large-23mib-real` (23 MiB) được chia thành 3 parts và xử lý đồng thời bởi 3 extractors chỉ mất **8.03 giây** E2E.
  - Trong khi đó, file `pdf-small-8mib` (8 MiB, dưới ngưỡng 16 MiB nên không chia) xử lý đơn lẻ mất hơn **120 giây** và bị timeout.
  - **Khuyến nghị kiến trúc:** Có thể cân nhắc hạ ngưỡng splitting đối với file PDF từ 16 MiB xuống 4–8 MiB hoặc chia theo số trang để tránh các file PDF phức tạp chiếm giữ worker quá lâu.

---

## 4. Danh mục file bằng chứng đã tạo

Tất cả dữ liệu thô, log JSONL, báo cáo đối soát và chỉ số tài nguyên được lưu trữ tại `docs/test-results/event-sourcing/run-002/`:

```
docs/test-results/event-sourcing/run-002/
├── logs/
│   ├── k6-small.jsonl                 # Log sự kiện chi tiết k6 (workload small)
│   ├── k6-large.jsonl                 # Log sự kiện chi tiết k6 (workload large)
│   └── k6-mixed.jsonl                 # Log sự kiện chi tiết k6 (workload mixed)
├── manifests/
│   ├── stream-start-small.json        # Snapshot vị trí Redis streams trước small run
│   ├── stream-start-large.json        # Snapshot vị trí Redis streams trước large run
│   ├── stream-start-mixed.json        # Snapshot vị trí Redis streams trước mixed run
│   ├── drain-small.json               # Báo cáo đối soát JSON cho small run
│   ├── drain-small.md                 # Báo cáo đối soát Markdown cho small run
│   ├── drain-large.json               # Báo cáo đối soát JSON cho large run
│   ├── drain-large.md                 # Báo cáo đối soát Markdown cho large run
│   ├── drain-mixed.json               # Báo cáo đối soát JSON cho mixed run
│   └── drain-mixed.md                 # Báo cáo đối soát Markdown cho mixed run
├── resources-small/
│   ├── stats.jsonl                    # Docker stats time-series (CPU, RAM, Net I/O)
│   ├── cgroup.jsonl                   # Cgroup v2 memory peak, current, oom_kill metrics
│   ├── events.jsonl                   # Docker daemon events (oom, die, restart)
│   └── summary.json                   # Container ID và thời gian giám sát
├── resources-large/
│   ├── stats.jsonl
│   ├── cgroup.jsonl
│   ├── events.jsonl
│   └── summary.json
└── resources-mixed/
    ├── stats.jsonl
    ├── cgroup.jsonl
    ├── events.jsonl
    └── summary.json
```
