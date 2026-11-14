# Load Test Plan — Tìm throughput bền vững dưới giới hạn RAM

Cập nhật: 2026-11-14. Trạng thái: **kế hoạch, chưa chạy benchmark**.
Nguồn: [requirement](requirement-event-sourcing.md), [implementation plan](implementation-plan-event-sourcing.md), [review](load-test-review-event-sourcing.md).

## 1. Mục tiêu và phạm vi

Ưu tiên **RAM/CPU, không OOM và throughput hoàn thành thực tế**; latency và error rate dùng để giải thích kết quả và kiểm tra tính hợp lệ của mức tải.

Giữ cách làm đơn giản:

- k6 dùng `ramping-arrival-rate`, trực tiếp XADD Redis Streams và poll state mỗi **1 giây**.
- Dùng Docker stats, Docker events và cgroup counters để theo dõi tài nguyên. Không thêm Prometheus, cAdvisor, Pub/Sub hoặc collector service.
- Sau run, đọc output stream một lần theo khoảng stream ID để đối soát completion muộn. Đây là bước tổng hợp kết quả, không phải observer chạy liên tục.
- Chỉ thêm structured timing logs tối thiểu trong workers khi triển khai, không xây metrics platform.

## 2. Điều kiện trước khi chạy

1. Hoàn thành workers trong implementation plan; smoke input → split → extract → đúng một output SUCCESS/FAILED. Code hiện tại mới có HTTP handlers, chưa sẵn sàng benchmark Redis pipeline.
2. Cố định **1 Splitter + 1 Extractor** cho run đầu: Splitter 1 CPU / 512 MiB; Extractor 2 CPU / 1 GiB. Mỗi process xử lý một message tại một thời điểm. Nếu thử nhiều Extractor thì chạy benchmark riêng và ghi tổng quota.
3. Xác minh limits thực tế, worker mode, consumer groups và truy cập MinIO từ worker. Ghi image/version, Docker VM CPU/RAM/swap và cấu hình Redis persistence thực tế; không đổi cấu hình giữa các mức tải.
4. Môi trường benchmark không có traffic khác; input/splitted group lag và pending về 0 trước run. Không xóa dữ liệu dùng chung để đạt điều kiện này.
5. Dùng ba file `sample_50mb.pdf`, `.docx`, `.xlsx` trong MinIO bucket `dataset`; smoke từng file thành công. Ghi bytes/checksum, số trang/row nếu có và số part thực tế. File tổng hợp 50 MiB không đại diện cho mọi tài liệu thực.

## 3. Kịch bản tăng tải

Trước run chính, chạy warm-up riêng 3–5 phút rồi chờ drain. Không tính warm-up vào throughput.

| Bước | Arrival rate | Thời lượng | Mục đích |
|---|---:|---:|---|
| Baseline | 1 doc/min | Hold 10 phút | Đo footprint và thời gian từng format |
| Mức 2 | 2 docs/min | Ramp 30s + hold 10 phút | Kiểm tra queue/throughput |
| Mức 3 | 3 docs/min | Ramp 30s + hold 10 phút | Tiếp tục tăng tải |
| Mức 4 | 4 docs/min | Ramp 30s + hold 10 phút | Tìm vùng bão hòa |
| Mức 5 | 5 docs/min | Ramp 30s + hold 10 phút | Xác định có cần tăng tiếp |
| Drain | **Không nạp mới** | Đến khi hết outstanding, tối đa 15 phút | Đối soát kết quả |

Trong `ramping-arrival-rate`, mỗi mức sau baseline phải có **hai stage cùng target**: ví dụ `{target: 2, duration: '30s'}` rồi `{target: 2, duration: '10m'}`. Không dùng stage ramp xuống 0 làm drain vì vẫn phát request trong lúc giảm tải. Kết thúc lịch arrival ở cuối hold; chỉ cho iterations đang chạy hoàn thành bằng grace period.

- Bỏ hai phút đầu mỗi hold khỏi cửa sổ tính throughput; giữ toàn bộ dữ liệu để giải thích transition. Nếu queue chưa ổn định thì không gọi phần còn lại là steady state.
- Nếu 5 docs/min vẫn ổn định, tăng tiếp từng 1 doc/min trong run kế tiếp; chưa được kết luận 5 là maximum.
- Dừng nạp sớm nếu phát hiện OOM, worker chết, hoặc số request chưa terminal vượt **20**. Đây là guardrail của benchmark, không phải giới hạn sản phẩm. Khi code k6, kiểm tra guardrail trước mỗi XADD và giữ cờ dừng chung theo run; không abort toàn bộ k6 làm mất các VU đang chờ. Monitor có thể đặt cờ này khi phát hiện OOM/worker chết.
- Sau khi tìm vùng ngưỡng, chạy **hold riêng 30 phút** tại mức bền vững dự kiến, bắt đầu từ queue rỗng để xác nhận capacity và xem xu hướng RAM. Nếu còn quá ít mẫu hoặc RAM chưa ổn định, kéo dài thay vì kết luận không leak.

## 4. Thiết kế k6 tối thiểu

Đây là đặc tả để viết script, thay cho snippet chưa xử lý async và timeout của bản trước.

**Cấu hình ban đầu:** `startRate=1`, `timeUnit='1m'`, `preAllocatedVUs=30`, `maxVUs=30`, polling 1s, thời gian quan sát tối đa mỗi request **10 phút**, `gracefulStop='15m'`. Giới hạn VU này phù hợp run thăm dò đến 5 docs/min cùng guardrail outstanding; nếu tăng rate phải tính lại theo `rate × thời gian chờ` và kiểm tra `dropped_iterations`.

**Luồng mỗi iteration:**

1. Nếu run đã có cờ dừng nạp hoặc outstanding vượt guardrail thì không publish thêm. Thời gian sau cờ dừng không được tính là cửa sổ arrival đạt target.
2. Chọn file theo `scenario.iterationInTest % 3`, không dùng `__ITER` cục bộ mỗi VU. Gắn format và bytes vào manifest; mix là 1:1:1 theo thứ tự phát.
3. Tạo UUID hợp lệ cho request/conversation/message; request ID duy nhất giữa các run. Ghi `run_id`, request ID, file, timestamp ngay trước publish vào manifest/log để xử lý cả trường hợp mất reply.
4. Dùng `async` function và **await** `client.sendCommand('XADD', stream, '*', 'data', payload)`. Lưu stream ID trả về và timestamp nhận reply. Nếu mất reply, ghi `publish_unknown`, không retry bằng request ID mới vì Redis có thể đã nhận.
5. Poll bằng **await** HGET status mỗi 1s đến COMPLETED/FAILED hoặc deadline theo `Date.now()`. Thời gian Redis call cũng phải nằm trong deadline; cấu hình command timeout hữu hạn. Không dùng số vòng lặp thay cho elapsed time.
6. COMPLETED → ghi observed success và E2E; FAILED → ghi terminal failure. Hết deadline → ghi **observation_timeout**, không coi là worker FAILED, không hủy task. Redis read lỗi ghi riêng; chỉ tiếp tục retry trong deadline, không làm mất manifest.
7. Trong mọi nhánh, lưu kết quả quan sát cùng request ID. Không log mỗi lần poll.

**Các số liệu k6 cần giữ:** accepted publish, publish error/unknown, observed success, terminal failure, observation timeout, Redis read error, E2E duration và `dropped_iterations`. Không gộp tất cả thành một success-rate threshold. `dropped_iterations=0` là điều kiện xác nhận arrival đã đạt lịch; run có guardrail kích hoạt chỉ dùng các cửa sổ hoàn chỉnh trước khi dừng.

30 VU đang poll 1s tạo khoảng 30 HGET/s, cộng fail-fast polling của worker. Chỉ điều tra overhead nếu Redis CPU/latency bất thường; chưa cần đổi kiến trúc observer. Nếu nghi nhiễu, lặp lại một mức với polling 2s.

## 5. Đo throughput và thời gian

### Throughput chính thức

Trước run, lưu last ID của output stream (stream rỗng dùng `0-0`); sau drain lưu end ID. Đọc các entry trong khoảng đó, lọc request IDs thuộc manifest và đếm terminal **duy nhất theo request ID**. Phát hiện terminal trùng hoặc thiếu; trạng thái nội bộ COMPLETED tương ứng output SUCCESS.

- `docs_completed_per_min = số document SUCCESS commit trong cửa sổ / số phút của cửa sổ`.
- `MiB_processed_per_min = tổng bytes gốc của các document SUCCESS / 2^20 / số phút`.
- Dùng timestamp stream ID của output để xếp completion vào cửa sổ wall-clock, không dùng thời điểm k6 nhận poll. Không lấy tổng Counter chia cả warm-up/drain và không lấy nghịch đảo latency.
- Báo cáo theo từng plateau và format; ghi arrival thực tế bên cạnh completion rate. Completion có thể thuộc arrival stage trước, nên run hold độc lập dùng để xác nhận capacity cuối cùng.
- Đối soát `accepted = SUCCESS + FAILED + unresolved`. Observation timeout có thể trở thành SUCCESS muộn và không được đếm thêm như terminal failure. Publish mất reply phải được đối chiếu input stream theo request ID để xác định đã accepted hay chưa.
- Hết drain mà còn unresolved: báo số lượng/tuổi request; không đánh run đạt capacity.

### Queue và processing

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

Bắt đầu monitor trước warm-up và giữ đến hết drain:

1. Dùng **một Docker stats stream** cho các container liên quan, lưu timestamp, CPU, RAM, PIDs, net/block I/O. Lấy container IDs qua Compose, không hardcode tên. Theo dõi Redis/MinIO bên cạnh workers để nhận biết tranh chấp tài nguyên.
2. Ghi **Docker events liên tục** cho `oom`, `die`, `restart`, `start`; inspect bổ sung OOMKilled, exit code và restart count đầu/cuối run. Exit 137 chưa đủ chứng minh OOM. Thu thập lỗi phải ghi UNKNOWN, không mặc định false/OK.
3. Lưu cgroup `memory.current`, `memory.peak` nếu hỗ trợ, `memory.events` (delta `oom`, `oom_kill`, `max`), swap và `cpu.stat` đầu/cuối run; đọc định kỳ khoảng 2s khi khả thi. Lưu theo container ID/cgroup lifetime trước khi recreate. Python child có thể bị OOM kill trong khi Go còn sống, nên không chỉ dựa `.State.OOMKilled`.

Trên macOS, cgroup nằm trong Linux VM của Docker; kiểm tra khả năng truy cập trước benchmark. Nếu không thu được peak/counter thì phải ghi hạn chế quan sát, không khẳng định đã loại trừ micro-spike. Không thêm cAdvisor chỉ để chạy POC này. Docker stats có thể trừ file cache nên RAM hiển thị không thay thế total memory charge/peak của cgroup.

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

**Mức throughput bền vững cao nhất đã kiểm chứng** phải đồng thời:

- Không OOM, worker chết hoặc restart bất thường; footprint không tăng không kiểm soát trong hold xác nhận.
- Arrival thực đạt target, không dropped iterations; không có terminal failure, duplicate terminal hoặc unresolved không giải thích được.
- Completion rate ổn định và backlog không có xu hướng tăng qua ít nhất ba cửa sổ liên tiếp; drain hết sau khi dừng nạp.

Nếu mức cao nhất chưa bão hòa, báo **capacity ít nhất đạt X docs/min**, không gọi đó là maximum. Run hữu hạn không chứng minh không có leak chậm hay xác suất lỗi bằng 0.

Lưu kết quả thực thi vào `docs/load-test-results-event-sourcing.md` khi chạy, kèm đường dẫn manifest, output events, timing logs và resource/event logs. Bảng kết quả tối thiểu:

| Rate / replica count | Cửa sổ đo | SUCCESS/min | MiB/min | RAM peak Splitter/Extractor | CPU | OOM | Lag/pending đầu→cuối | FAILED / unresolved | Kết luận |
|---|---|---|---|---|---|---|---|---|---|

Không cleanup dữ liệu dùng chung. Chỉ dọn object/state thuộc run trên môi trường benchmark được phép; không FLUSHALL hay trim stream dùng chung.
