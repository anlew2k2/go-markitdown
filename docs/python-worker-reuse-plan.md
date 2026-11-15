# Implementation plan — tái sử dụng Python converter

Ngày: 2026-11-15. Trạng thái: **implement xong Task 1–3, verification ngắn đã chạy — xem [python-worker-reuse-results.md](python-worker-reuse-results.md)**. Phần dưới giữ nguyên làm tài liệu thiết kế gốc.

## Mục tiêu và phạm vi

Thêm backend Python process sống lâu để đo lợi ích bỏ startup/import/khởi tạo MarkItDown lặp lại. Giữ download vào file tạm, output Markdown ra file tạm, upload và Redis pipeline hiện có. Không chuyển sang streaming input, không thêm service/sidecar, không đổi converter PDF hoặc thiết kế lại consumer.

Đây là thử nghiệm A/B với CLI hiện tại, chưa có cam kết tăng throughput. Verification ngắn, không chạy load test/soak dài và không thêm unit test mới.

## Source hiện tại đã kiểm tra

- `internal/services/markitdown.go`: `ConvertToMarkdownFile` chạy CLI mới mỗi conversion, stdout ra output file, stderr có giới hạn; cancel kill process group.
- `internal/usecase/markitdown.go`: download có giới hạn 4 MiB, file tạm được cleanup; đã có observer cho download/convert.
- `internal/pipeline/extractor.go` và `consumer.go`: đã có bounded `WORKER_CONCURRENCY`, mỗi slot giữ toàn lifecycle job. Không implement lại pool consumer.
- `cmd/extractor/main.go`: là nơi quản lý lifecycle backend trong worker mode.
- `deploy/extractor.dockerfile`: Go binary và Python cùng container, nên có thể chia sẻ local paths; hiện cài `markitdown[all]` chưa pin version.

Các file trên đang có thay đổi trong working tree khi lên plan. Implement trên trạng thái hiện hành, không ghi đè các thay đổi đó.

## Quyết định triển khai

- Thêm cấu hình `CONVERTER_BACKEND=cli|persistent`, mặc định `cli` để so sánh và rollback. Chỉ áp dụng cho extractor worker mode; HTTP giữ hành vi hiện tại. Validate giá trị trước khi consume.
- Persistent backend quản lý đúng N Python process, N = `WORKER_CONCURRENCY`; mỗi process một conversion tại một thời điểm. Checkout/checkin bằng pool giới hạn, không thêm hàng đợi vô hạn hoặc hệ số concurrency mới.
- Go giữ download và upload; Python chỉ nhận local input/output path. Một instance MarkItDown/process được khởi tạo một lần và tái sử dụng.
- Backend có contract nhỏ `Convert(ctx, inputPath, outputPath) error` và lifecycle `Close`; truyền dependency vào worker/usecase thay vì global mutable backend. CLI adapter dùng lại function hiện tại.
- Timeout của conversion bao gồm chờ checkout, giao tiếp và chạy job. Startup ban đầu có deadline riêng hữu hạn (đề xuất 30 giây), lỗi thì dừng startup trước khi consume. Worker thay thế cũng có startup deadline, không retry startup vô hạn bên trong một conversion.

## Task 1 — Python wrapper và protocol

Thêm script Python được COPY vào image ở đường dẫn cố định, chạy bằng `python -u`. Dùng API local-file của đúng phiên bản MarkItDown trong image, không gọi CLI bên trong wrapper.

Protocol JSON Lines trên stdin/stdout:

- Ready: `{"type":"ready","init_ms":123}` sau khi import và khởi tạo xong.
- Request: `{"id":"unique-id","input_path":"...","output_path":"..."}`.
- Response: `{"id":"unique-id","ok":true,"convert_ms":123}` hoặc `{"id":"unique-id","ok":false,"error":"bounded diagnostic","convert_ms":123}`.

Response success chỉ gửi sau khi output đã ghi và đóng thành công. Markdown không truyền qua JSON. Stdout chỉ dành cho protocol: chuyển print/output của thư viện sang stderr trong quá trình import/init/convert; giữ riêng stream protocol để phát response. Go drain stderr liên tục vào buffer giới hạn, tránh pipe bị đầy. Giới hạn kích thước protocol frame và diagnostic; lỗi parse, ID không khớp hoặc EOF là worker bị hỏng.

Catch exception conversion thông thường, trả lỗi để job đi qua failure path hiện có; cho phép worker nhận job sau. Không tuyên bố constant-memory: API Python/converter có thể giữ toàn bộ Markdown trong RAM dù Go chỉ đọc file. So sánh output với CLI để phát hiện khác biệt formatting/options.

Pin cùng một version MarkItDown cho cả A và B dựa trên version đã xác minh hoạt động trong image hiện có; không tự đoán version hoặc nâng dependency ngoài phạm vi.

Hoàn tất khi: một PID xử lý liên tiếp hai request, chỉ có một ready, response khớp ID và output tương đương CLI.

## Task 2 — Go process pool và tích hợp extractor

Khởi tạo backend trong `runWorker`, sau khi dependency/bootstrap sẵn sàng và trước khi consume. Truyền backend đến conversion usecase, giữ nguyên validation, download, temp cleanup, timeout và timing observer. COPY script trong Dockerfile; ghi backend/version/concurrency trong startup log.

Lifecycle cần bảo đảm:

1. Checkout worker theo context; không dispatch nếu context đã hủy.
2. Mỗi worker chỉ có một request đang chạy. Serialize checkout/result/cancel để response cũ không bị gán cho job mới.
3. Khi context cancel, timeout, process crash hoặc protocol lỗi: kill toàn process group của worker đó, Wait/reap, đóng reader/writer và chờ các I/O goroutine kết thúc trước khi trả lỗi. Sau đó mới cho caller cleanup temp files.
4. Worker hỏng bị loại khỏi pool; chỉ thay bằng process đã ready. Nếu replacement startup thất bại, đánh backend không khỏe và dừng consumer bằng cách cancel root worker context, để job hiện hành giữ semantics shutdown/pending. Không tiếp tục consume bằng pool đã mất slot mà không báo lỗi, không hot-loop restart và không fallback CLI âm thầm.
5. Không tự chạy lại conversion trong pool; giữ failure/retry policy ở pipeline hiện tại. Shutdown root context hủy mọi conversion, Close idempotent và reap tất cả subprocess; không spawn replacement khi đang đóng.

Giữ ACK sau commit, fail-fast sibling và shutdown không đánh FAILED. Không thay consumer identity hoặc Lua scripts. Chỉ trả worker về pool sau khi nhận response hợp lệ và chắc chắn request đã kết thúc.

Hoàn tất khi: CLI vẫn chạy như cũ; persistent có tối đa N converter process; cancel một job không kill worker khác; shutdown không sót process hoặc goroutine I/O.

## Task 3 — Verification tối thiểu và A/B ngắn

Thực thi sau implementation trong local container riêng, không dùng production. Dùng cùng image, version, fixture, CPU/RAM, output path riêng mỗi lần. Không cần framework benchmark mới; một runner nhẹ gọi hai backend là đủ, có thể kiểm tra conversion trực tiếp để tách khỏi Redis/MinIO.

### Các check bắt buộc

1. Build image và chạy `go test ./...` với toolchain phù hợp nếu khả dụng; không thêm unit test mới.
2. Smoke một PDF nhỏ và một DOCX hoặc XLSX có sẵn qua cả hai backend. So sánh Markdown; khác biệt phải được giải thích hoặc sửa trước khi chấp nhận. Với persistent, chạy liên tiếp tối thiểu hai job trên cùng PID, ghi bằng chứng reuse.
3. Dùng file lỗi để kiểm tra response lỗi rồi gửi file hợp lệ vào cùng worker. Dùng deadline rất ngắn trên conversion đang chạy để kiểm tra kill/reap và request kế tiếp thành công trên worker thay thế. Không thêm sleep test vào production code.
4. Kiểm tra N = 2 bằng hai conversion đồng thời; hủy một job, job còn lại hoàn thành. Khi SIGTERM, không còn Python process thuộc backend. Một smoke E2E qua Redis/MinIO nếu stack local sẵn có để xác nhận SUCCESS và output upload; nếu thiếu stack thì báo chưa verify E2E.

### Phép đo A/B

- N = 1; chọn một PDF nhỏ cố định. CLI chạy 10 lần; persistent chạy 1 warm-up rồi 10 lần đo. Tất cả dùng file đã tải sẵn, không tính download/upload vào phép đo conversion.
- Ghi riêng startup + ready của persistent, latency end-to-end từng lần gọi Convert ở Go, và convert_ms phía Python. CLI baseline phải khởi động process mới mỗi lần đúng như production path.
- Báo median, min/max, sample count; không suy ra p95/p99 ổn định từ 10 mẫu. Tính mức giảm median và tổng thời gian gồm startup để không che chi phí cold start. Nếu làm được, ghi RAM container trước/sau chuỗi job; không gọi đây là memory-leak test.
- Đo A/B không có traffic khác; lặp thêm một lượt với thứ tự đảo nếu chênh lệch nhỏ/không nhất quán. Không cần sweep CPU/RAM, load dài hoặc nhiều format.

Không đặt ngưỡng speedup bắt buộc khi chưa có baseline. Nếu output/recovery đúng nhưng không nhanh hơn đáng kể, giữ mặc định CLI và báo số đo; không mở rộng tối ưu trong task này.

## Trình tự và bàn giao

Task 1 → Task 2 → Task 3. Không cần triển khai song song hoặc phân tách thêm service.

Cập nhật README với backend flag, lifecycle và cách rollback về CLI. Lưu verification ngắn ở `docs/python-worker-reuse-results.md`: image/version, lệnh chạy, fixture, output check, lifecycle check, bảng A/B và giới hạn chưa verify. Không ghi kết quả giả định thành PASS.

Không commit/push. Plan này chỉ là handoff triển khai; chưa thay code hoặc chạy verification.
