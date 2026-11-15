# Verification — tái sử dụng Python converter (persistent backend)

Ngày: 2026-11-15. Trạng thái: **implement xong Task 1–2 theo [python-worker-reuse-plan.md](python-worker-reuse-plan.md); verification tối thiểu (Task 3) đã chạy thật**, không phải load test/soak dài. `CONVERTER_BACKEND=cli` vẫn là mặc định; `persistent` là backend thử nghiệm, bật qua biến môi trường.

## Môi trường verify

- Commit nền: `03bdbe44e21787be8557f4f51ca908108b0788b2` cộng các thay đổi working-tree chưa commit của phiên này (structured timing logs, `WORKER_CONCURRENCY`, manifest 4 MiB, và phần converter backend này — xem `git status` tại thời điểm viết tài liệu).
- Image: `pipelinetest-extractor-1` rebuild tại chỗ từ `deploy/extractor.dockerfile` (base `python:3.13-slim`, `pip install markitdown[all]` không pin version — giới hạn đã biết, không mở rộng phạm vi ở đây), chạy trên Docker Desktop macOS arm64 (container `aarch64`).
- `markitdown` 0.1.9 (xác nhận qua `pip show markitdown` cả trên host lẫn trong container — khớp nhau).
- Go 1.27.1 darwin/arm64 (host, dùng để cross-compile `CGO_ENABLED=0 GOOS=linux GOARCH=arm64` cho phần đo bên trong container).
- Fixture: `tmp/report.pdf` (240 KB, PDF nội dung thật) cho equivalence + A/B; `tmp/docx_small_100kb.docx`, `tmp/xlsx_small_100kb.xlsx` cho equivalence thêm; `system-design-interview.pdf` (23 MB, MinIO fixture `pdf-large-23mib-real`, 12 part dưới ngưỡng 4 MiB) cho E2E qua Redis/MinIO thật.
- Stack: `pipelinetest` (`deploy/docker-compose.test.yaml`), container tạm (`docker run --network pipelinetest_default ...`) trỏ vào cùng Redis/MinIO để không cần sửa compose cho mỗi lần thử CONVERTER_BACKEND/WORKER_CONCURRENCY khác nhau.

## 1. Lifecycle và cấu hình

| Kiểm tra | Lệnh / cách làm | Kết quả |
|---|---|---|
| `CONVERTER_BACKEND` không hợp lệ fail startup | `docker run -e CONVERTER_BACKEND=bogus ...` | `extractor: config: invalid CONVERTER_BACKEND: "bogus" (want "cli" or "persistent")`, exit 1 |
| Backend `cli` không đổi hành vi | mặc định (không set biến) | log khởi động in `converter_backend=cli`; đường gọi vẫn qua `CLIConverter` → `ConvertToMarkdownFile` (một subprocess `markitdown` mỗi lần), không đổi |
| Backend `persistent` khởi động, log đúng | `docker run -e CONVERTER_BACKEND=persistent -e WORKER_CONCURRENCY=2 ...` | log: `concurrency=2 converter_backend=persistent`; 2 process Python sống được spawn trước khi worker bắt đầu consume |
| E2E thật qua Redis/MinIO | publish `system-design-interview.pdf` (12 part) vào `stream:doc-extractor:input` trên stack `pipelinetest`, backend `persistent` | `status=COMPLETED`, `total=12`; mọi `TIMING ... phase":"convert"` đều `outcome:"success"`, 2 part chạy chồng lấn thời gian (xác nhận concurrency=2 thật) |
| PID reuse (2 request liên tiếp cùng process) | harness Go nhẹ (`tmp/pcverify/main.go`, không commit — `tmp/` đã gitignore) gọi `Convert` 3 lần tuần tự qua pool N=2 | 3 conversion thành công; stdout cả 3 có cùng `init_ms` nguồn từ đúng 2 process đã spawn lúc `NewPersistentConverter`, không spawn thêm — xác nhận qua code path (checkout/checkin không bao giờ spawn mới trừ khi worker chết) |
| Conversion lỗi (business-level) không giết worker | harness: convert một path không tồn tại qua pool N=1, rồi convert file thật qua CÙNG worker | lần 1: lỗi `FileNotFoundError` trả về đúng qua error path, không panic; lần 2 (cùng worker, không spawn lại): thành công — xác nhận "catch exception, trả lỗi, worker nhận job sau" |
| Timeout/cancel kill đúng 1 worker, N=2 vẫn còn 2 | harness: 2 goroutine, job A dùng ctx 300ms trên PDF chậm hơn 300ms, job B không giới hạn chạy đồng thời | job A: lỗi `context deadline exceeded`, log `persistent converter worker died (context deadline exceeded), spawning replacement`; job B: thành công không bị ảnh hưởng; sau ~1s (đủ thời gian replacement spawn), 2 job mới đồng thời (C, D) đều thành công → pool tự phục hồi về đúng 2 worker |
| Script path sai fail startup (không fallback CLI) | `NewPersistentConverter(ctx, 1, "/nonexistent/script.py", ...)` | lỗi rõ ràng `worker exited before ready: ... can't open file`, không fallback, không tạo converter |
| `Close()` idempotent, reap hết | gọi `Close()` hai lần liên tiếp | không hang, không panic |
| SIGTERM không còn process Python | `docker run` backend `persistent` concurrency=2, `docker stop -t 10` | container thoát exit code 0 trong vài giây (không cần hết 10s grace), log in `extractor: shutdown complete`; container ở trạng thái `exited` (toàn bộ process trong pid namespace, gồm 2 Python worker, đã bị dọn theo) |

Harness xác minh (`tmp/pcverify/main.go`, `tmp/abmeasure/main.go`) nằm trong `tmp/` (đã `.gitignore`), không phải unit test mới, đúng theo yêu cầu "không thêm unit test". Reader có thể tái tạo bằng cách chạy lại chúng với `go run ./tmp/pcverify` / `go run ./tmp/abmeasure` nếu `tmp/` còn giữ các file này cục bộ.

## 2. Output tương đương CLI

So `python3 -m markitdown <file>` (đường CLI thật, cùng package `markitdown` 0.1.9) với output của backend `persistent` cho cùng input, trên host (có `python3`/`markitdown` package cài sẵn):

| File | Kết quả |
|---|---|
| `tmp/report.pdf` (PDF) | **Giống hệt byte-for-byte** sau khi sửa: wrapper ban đầu thiếu newline cuối cùng mà CLI có (CLI dùng `print(result.markdown)`, tự thêm `\n`) — đã sửa `deploy/markitdown_worker.py` ghi thêm `"\n"` sau `text_content` để khớp. Trước fix: lệch đúng 1 byte/1 dòng trắng cuối; sau fix: `diff` rỗng. |
| `tmp/docx_small_100kb.docx` (DOCX) | Giống hệt byte-for-byte |
| `tmp/xlsx_small_100kb.xlsx` (XLSX) | Giống hệt byte-for-byte |

Giới hạn: chỉ so 3 file nhỏ, không sweep toàn bộ format (PPTX/HTML/EML/CSV chưa so trực tiếp CLI-vs-persistent output — về nguyên tắc cùng gọi `MarkItDown.convert()` nên không có lý do khác biệt theo format, nhưng chưa đo thực tế).

## 3. Đo A/B (N=1, PDF nhỏ cố định, bên trong container thật)

Chạy **bên trong container `pipelinetest-extractor-1` thật** (binary Go cross-compile `CGO_ENABLED=0 GOOS=linux GOARCH=arm64`, copy vào `/app` bằng `docker cp`, input `tmp/report.pdf` cũng copy vào container) để CLI baseline dùng đúng `markitdown` thật trên PATH production, không phải wrapper giả trên host.

| Backend | n | median | min | max | tổng 10 call |
|---|---:|---:|---:|---:|---:|
| CLI (fresh subprocess mỗi lần) | 10 | 951.6 ms | 903.2 ms | 1176.2 ms | 9.682 s |
| Persistent (1 warm-up + 10 đo, cùng 1 process) | 10 | 333.1 ms | 328.2 ms | 360.0 ms | 3.365 s |

- Startup (spawn + ready) của persistent: 458.7 ms (một lần). Warm-up call riêng: 369.8 ms.
- **Tổng có tính startup**: CLI 9.682 s vs Persistent 458.7ms + 3.365s = 3.823 s cho cùng 10 conversion — giảm ~60% tổng thời gian kể cả cold start.
- **Median per-call giảm 65.0%** (951.6ms → 333.1ms), không chồng lắp giữa min/max hai nhóm (persistent max 360ms < CLI min 903ms) — chênh lệch rõ ràng và nhất quán, không cần lặp lại theo thứ tự đảo (tiêu chí trong plan chỉ yêu cầu lặp khi chênh lệch nhỏ/không nhất quán).
- RAM container sau chuỗi đo (2 worker persistent từ job E2E trước đó + tiến trình `abmeasure` tạm thời đã thoát): 282.7 MiB — chỉ là một điểm quan sát sau cùng, **không phải memory-leak test** (không có baseline trước/sau tách biệt cho riêng phép đo A/B này).
- Không sweep CPU/RAM, không test nhiều format trong phép đo A/B này, đúng theo giới hạn plan đã nêu.

## 4. Những gì CHƯA verify / giới hạn còn lại

- Chưa benchmark dài/soak với `WORKER_CONCURRENCY` > 2 hoặc nhiều giờ liên tục cho backend `persistent` — có thể có hành vi khác dưới tải bền vững (ví dụ RSS tăng dần của `markitdown`/dependency theo số job, retention bên trong thư viện) chưa được đo.
- Chưa so sánh output cho PPTX/HTML/EML/CSV hay file lớn (đã split) — chỉ test các file nhỏ nguyên bản.
- Chưa đo A/B với concurrency > 1 (N=2/4) dưới tải thật qua k6 — chỉ xác nhận đúng về mặt lifecycle (checkout/cancel/replace) ở N=2 bằng harness trực tiếp.
- Chưa pin version `markitdown` trong Dockerfile — A/B này chỉ hợp lệ cho version đang cài (0.1.9); nếu image rebuild với version khác cần đo lại, không giả định số liệu giữ nguyên.
- `markitdown` Python API vẫn giữ toàn bộ Markdown output trong RAM trước khi Go đọc file — backend `persistent` không đổi đặc tính này, chỉ tiết kiệm chi phí import/khởi tạo.
- Chưa test trường hợp pool mất healthy hoàn toàn (mọi replacement đều fail liên tiếp) dẫn đến `onUnhealthy`/dừng consumer trong môi trường container thật — chỉ test qua harness Go trực tiếp (script path sai). Hành vi "dừng consumer qua cancel context" đã verify ở tầng code, chưa verify thêm rằng Redis/stream state sau đó đúng pending-not-FAILED trong một container thật đang consume khi bị ép rơi vào trạng thái unhealthy.
- Không chạy trên kiến trúc x86_64 hay Kubernetes — chỉ Docker Desktop macOS arm64.

## 5. Kết luận tạm thời

Lifecycle, correctness (output tương đương, không giết nhầm worker khỏe, không fallback âm thầm, không leak process khi shutdown) đều đạt yêu cầu verify tối thiểu của plan. A/B cho thấy lợi ích rõ rệt ở N=1 cho PDF nhỏ (giảm ~60–65%) trên image/version hiện tại — đây là tín hiệu tích cực để cân nhắc bật `persistent` có kiểm soát (ví dụ A/B thêm trên tải thật qua k6, theo dõi RAM dài hạn) trước khi đổi mặc định; **không** đổi mặc định `CONVERTER_BACKEND=cli` trong đợt này.
