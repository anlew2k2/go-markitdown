# Implementation Plan — Redis Streams Pipeline POC

Cập nhật: 2026-11-14. Phạm vi: kế hoạch triển khai POC đã rút gọn theo chỉ đạo của người dùng; chưa triển khai code.
Nguồn: [requirement-event-sourcing.md](requirement-event-sourcing.md).

## 1. Phương án được chọn

Dùng Redis standalone, **3 streams, 2 consumer groups và 3 key trạng thái/request** như yêu cầu. Chạy **1 Splitter và nhiều Extractor**, mỗi process xử lý một message tại một thời điểm. Tái dùng logic split và MinIO hiện tại; bổ sung đường conversion có context, timeout và stdout ra file.

Chỉ thêm ba Lua script nghiệp vụ:

1. Khởi tạo state và publish N part events trong cùng script.
2. Ghi nhận part hoàn thành; nếu đủ N thì chuyển COMPLETED và publish SUCCESS trong cùng script.
3. Chuyển FAILED và publish FAILED trong cùng script.

Commit trạng thái và output cùng script để tránh worker chết giữa hai bước. Không thêm lease, heartbeat, fencing token, manifest key, deadline sweeper, outbox, retry framework, invalid stream hay metrics platform. Không thêm event store phục vụ replay lịch sử. Đây là event-driven POC theo flow tài liệu.

**Giới hạn POC được chọn:** input URL không đổi nội dung trong quá trình xử lý; request_id duy nhất cho mỗi lần chạy; toàn bộ demo hoàn thành trong 24 giờ; không tự động reclaim message đang được consumer khác xử lý. Khi worker crash, phục hồi pending theo mục 5. Không cam kết xử lý xuyên mất dữ liệu Redis hoặc dedup sau TTL. Các giới hạn này không chặn triển khai POC.

## 2. Hiện trạng code và thay đổi cần thiết

| File/thành phần | Hiện tại đã khảo sát | Thay đổi cho POC |
|---|---|---|
| `cmd/splitter/main.go` | Gin `/ping`, POST `/api/v1/split-file` | Thêm worker mode và graceful shutdown |
| `cmd/extractor/main.go` | Gin `/ping`, API subprocess/markitdown | Thêm worker mode và graceful shutdown |
| `internal/usecase/splitfile.go` | HEAD/GET, temp file, bypass <=16 MiB, gọi SplitAndUpload | Gắn context/timeout cho HTTP; giữ nghiệp vụ hiện tại |
| `internal/services/splitter.go` và các format splitter | Split/upload nhiều định dạng; OLE lớn không hỗ trợ | Kiểm tra size thực của part trước upload; bảo đảm thứ tự PDF theo trang |
| `internal/usecase/markitdown.go` | Download giới hạn 16 MiB; trả Markdown string | Thêm đường worker dùng temp Markdown file |
| `internal/services/markitdown.go`, `subprocess.go` | `CombinedOutput`, không timeout, stderr trộn stdout | Worker chạy markitdown trực tiếp bằng argv/context; stdout ra file, stderr giới hạn |
| `internal/services/minio.go` | Đã upload qua reader/context | Tái dùng; cả hai worker EnsureBucket khi khởi động |
| `deploy/docker-compose.yaml` | Đã có Redis, MinIO, hai service và nginx; splitter 512 MiB, extractor 1 GiB | Env worker, health/dependency readiness, Redis AOF; scale extractor không cần nginx |
| `deploy/*.dockerfile` | Hai image riêng, extractor có markitdown | Giữ build hiện tại, điều chỉnh health nếu cần |
| `go.mod`, `README.md` | Chưa Redis client; README không khớp commands hiện hữu | Thêm go-redis/v9; cập nhật cách chạy và demo |

Lưu ý khi triển khai: working tree có thay đổi sẵn ở `internal/services/{splitter,htmlsplit,emlsplit}.go` và `deploy/nginx.conf`; đọc diff và giữ các thay đổi đó. Không mặc định comments “streaming” chứng minh RAM ổn định: OOXML còn buffer và pdfcpu/Python cần đo thực tế.

## 3. Event contract và Redis schema

Giữ JSON schema trong requirement; ghi JSON vào field `data` của stream. Validate UUID, URL HTTP(S), total và offset. Offset bắt đầu từ 0; 16 MiB = 16 × 1024 × 1024 bytes.

| Key | Type / nội dung |
|---|---|
| `stream:doc-extractor:input` | Input; group `cg:doc-splitter` |
| `stream:doc-extractor:splitted` | Part events; group `cg:doc-extractor` |
| `stream:doc-extractor:output` | SUCCESS/FAILED; downstream tự quản lý việc đọc |
| `doc-extractor:state:{request_id}` | Hash: total, status, conversation_id, message_id, file_path, failed_reason, failed_offset, terminal_event_id, expires_at |
| `doc-extractor:completed:{request_id}` | Set các offset đã hoàn thành |
| `doc-extractor:parts:{request_id}` | Hash `part:<offset>` → Markdown URL |

`{request_id}` là placeholder trong tên key. Redis standalone, không triển khai Cluster.

TTL mặc định **86400 giây**, cùng `expires_at` cho ba key request; đặt EXPIREAT khi key được tạo. Completed/parts không cần tồn tại khi còn rỗng. Duplicate không reset TTL. Không expire global streams; không auto trim trong POC. Redis volume và AOF được bật; retention dài hạn nằm ngoài phạm vi demo.

State nội bộ COMPLETED tương ứng output SUCCESS. SUCCESS URLs được dựng theo `part:0` đến `part:N-1`, không dựa thứ tự HGETALL hay thứ tự worker kết thúc. Lỗi Splitter trước khi có part dùng `failed_offset=-1`; downstream demo theo quy ước này.

## 4. Ba Lua scripts

Thêm dưới `internal/pipeline/lua/`, embed và gọi bằng go-redis Script API. Validate payload, kiểu key và giới hạn trước write. POC giới hạn N <=1000 để fanout script hữu hạn; kiểm tra ngay khi tạo part để không split/upload vô hạn.

### `init_and_publish.lua`

- Nhận identity, ordered URLs, N; nếu state tồn tại: trả ALREADY_EXISTS, không reset hoặc phát lại part events. Input trùng ID khác identity thì log conflict và ACK, không thay đổi request gốc.
- Nếu state chưa có: ghi total=N/status=PROCESSING/identity/expires_at và XADD N events offset 0..N-1 vào splitted trong cùng script.
- Trả kết quả để Go ACK input. Nếu crash sau script trước ACK, redelivery chỉ ACK state đã tồn tại.
- Chỉ một Splitter chạy trong POC. Duplicate input có thể gây split lại trước commit nếu crash, nhưng không fanout lại sau commit.

### `complete_part.lua`

- State thiếu → MISSING; terminal → TERMINAL; sai identity/total/offset → INVALID; offset đã completed → DUPLICATE. Không mutation ở các nhánh này.
- Với PROCESSING hợp lệ: HSET URL và SADD offset. Duplicate không được ghi đè URL đã accepted.
- Nếu đây là offset cuối, kiểm tra đủ các URL/membership và dựng payload theo offset trước mutation; sau đó ghi completion, XADD SUCCESS và HSET status=COMPLETED/terminal_event_id trong cùng script.
- ACK sau RECORDED/FINAL/DUPLICATE/TERMINAL. Không XADD SUCCESS riêng từ Go.

### `fail_request.lua`

- Mode Splitter: chỉ tạo FAILED nếu state chưa tồn tại; state đã có là no-op, tránh split retry làm fail một pipeline đã phát part.
- Mode Extractor: chỉ đổi PROCESSING → FAILED khi identity/total/offset hợp lệ và offset chưa completed. Nếu terminal hoặc offset đã completed thì no-op.
- Lưu reason/failed_offset/timestamp, XADD FAILED và lưu terminal_event_id trong cùng script; TTL như trên.
- Nhiều part lỗi chỉ lần chuyển terminal đầu tiên phát FAILED. FAILED chặn mọi complete_part đến muộn. Nếu final SUCCESS đã commit trước, failure đến sau không đổi trạng thái.

**Giới hạn:** Lua có isolation nhưng không rollback khi runtime error. Trong POC, script/WRONGTYPE/OOM là lỗi dừng demo để sửa và chạy lại bằng request_id mới; không xây reconciler tự động. Lỗi mạng/lost reply thì retry cùng script, không ACK khi chưa biết kết quả. Không dùng HSETNX để thay field status vốn đã tồn tại.

## 5. Worker Daemon và pending messages

### Bootstrap chung

- Load config, PING Redis, EnsureBucket; `XGROUP CREATE <stream> <group> 0 MKSTREAM`, chỉ bỏ qua BUSYGROUP.
- Tạo output stream rỗng bằng Lua bootstrap EXISTS + XADD/XDEL sentinel trong một lần chạy; không để lại output giả. Không tạo group downstream.
- Consumer name từ `CONSUMER_NAME` hoặc hostname. Mỗi replica phải có tên khác nhau.
- Mỗi worker đọc `COUNT 1`, `BLOCK 2000`, xử lý xong mới đọc tiếp; không cần goroutine pool. Nhiều Extractor replicas tạo parallelism.
- Khi start, đọc pending của **chính consumer** bằng XREADGROUP ID `0`, xử lý hết rồi mới đọc `>`. ACK chỉ sau business commit hoặc xác nhận terminal/duplicate.

### Flow Splitter

Input → validate → kiểm tra state (đã tồn tại thì ACK/skip) → `usecase.SplitFile` → ordered URLs → init_and_publish → ACK.

File <=16 MiB: một event offset=0/total=1/file_path gốc. Lỗi download/split/upload hoặc timeout: fail_request mode Splitter → ACK. Không gọi HTTP handler nội bộ.

### Flow Extractor

Part → validate → đọc state → FAILED/COMPLETED/offset completed thì ACK/skip → download bounded → markitdown → upload Markdown → complete_part → ACK.

Lỗi download/convert/upload: fail_request mode Extractor → ACK sau script. State thiếu hoặc malformed: log rõ stream ID/reason và ACK để không lặp vô hạn; không suy ra request mới từ part lỗi. Quy tắc này dành cho demo trong TTL.

Object Markdown: `splits/<conv>/<msg>/<req>/<attempt-uuid>/part-<offset>.md`. UUID chỉ để hai attempts không ghi đè object của nhau, **không phải lease/fencing**. Redis chọn URL accepted đầu tiên; object không được chọn có thể còn lại trong bucket demo.

### Crash/restart đơn giản

- Redis tạm lỗi: backoff ngắn, giữ pending; sau reconnect retry commit hoặc đọc lại pending của chính consumer. Không chỉ chờ restart mới retry pending.
- Nếu cùng consumer name restart: đọc lại pending của chính nó như trên.
- Nếu phải đổi consumer: **dừng toàn bộ workers của stage đó**, chuyển pending bằng XAUTOCLAIM sang consumer phục hồi rồi chạy lại. Không auto-claim theo idle time trong khi worker khác còn đang chạy; không cần application lock.
- Không retry tự động lỗi nghiệp vụ; lỗi đầu tiên là FAILED theo requirement. Nếu một file làm worker crash/OOM lặp lại, dừng demo, sửa nguyên nhân hoặc chạy request mới; không thêm retry budget framework.
- SIGTERM: ngừng read, cancel context, cleanup temp và reap subprocess; không đánh FAILED do shutdown, giữ message chưa commit pending.

## 6. Fail-fast và giới hạn RAM

Mỗi Extractor đang xử lý poll state **500 ms** bằng một goroutine gắn với job context; FAILED thì cancel HTTP/MinIO/process. Kiểm tra state trước download và trước convert. Kết thúc job phải dừng poll goroutine. Redis mất kết nối thì cancel việc đang chạy, giữ pending để phục hồi.

Chạy markitdown bằng argv trực tiếp, stdout ra temp `.md`, stderr giới hạn 64 KiB; timeout conversion mặc định 2 phút. Kill/reap cả process group khi cancel để không để child sống sau fail-fast. Lỗi do sibling FAILED hoặc shutdown không publish FAILED thêm.

Nghiệm thu “dừng ngay” bằng tiêu chí thực tế: queued part không bắt đầu sau khi thấy FAILED; active process dừng mục tiêu <=2 giây sau state FAILED trong môi trường demo khỏe. Completion script vẫn chặn late results.

Tái dùng temp-file download/upload hiện tại; thêm context/timeout, luôn close/remove. Đo kích thước part thực trước upload, reject atomic unit không tách được dưới 16 MiB. Sửa thứ tự PDF nếu đang sort lexical. Không refactor toàn bộ format splitter trước khi có bằng chứng: chạy benchmark trước, chỉ sửa buffer/đường xử lý khiến AC-6 không đạt. RAM mục tiêu giữ theo Compose: splitter <512 MiB, extractor <1 GiB, bao gồm Python child. Streaming stdout không bảo đảm RAM nội bộ pdfcpu/markitdown; ghi FAIL nếu vượt budget.

## 7. Tasks và Acceptance Criteria

### T1 — Redis contracts và atomic state

**Thêm:** `internal/pipeline/events.go`, `redis.go`, `state.go`, `lua/{init_and_publish,complete_part,fail_request}.lua`.
**Sửa:** `go.mod`, `go.sum` để thêm go-redis/v9.

Typed events/validation → bootstrap streams/groups → ba scripts theo mục 4. Config tập trung trong pipeline: REDIS_ADDR/auth/DB khi cần, CONSUMER_NAME, SERVICE_MODE, JOB_TIMEOUT và CONVERT_TIMEOUT; các giá trị TTL/poll/count dùng default POC, không cần nhiều knobs. Giới hạn N=1000.

**AC:** đủ ba streams/hai groups; bootstrap lặp an toàn; duplicate input không nhân fanout; duplicate offset không tăng count/đổi URL; hai part cuối chỉ một SUCCESS đúng thứ tự; nhiều lỗi chỉ một FAILED; terminal không bị đảo. Mapped AC-1/4/5.

### T2 — Worker runtime và Splitter

**Thêm:** `internal/pipeline/consumer.go`, `splitter.go`.
**Sửa:** `cmd/splitter/main.go`, `internal/usecase/splitfile.go`, các splitter có kiểm tra size/order cần sửa.

Bootstrap → own pending → new events → SplitFile → init_and_publish/fail_request → ACK. HTTP downloads nhận context/timeout. Một Splitter process, job timeout 15 phút. Thêm worker mode; giữ HTTP mode hiện tại để tương thích, worker là default trong Compose.

**Dependencies:** T1.
**AC:** file nhỏ một event dùng URL gốc; file lớn đúng N offsets; mỗi part <=16 MiB; OLE lớn/lỗi download một FAILED; restart sau commit trước ACK không nhân fanout; shutdown giữ pending. Mapped AC-2/5.

### T3 — Extractor, Markdown upload và fail-fast

**Thêm:** `internal/pipeline/extractor.go`; helper process cancellation phù hợp OS nếu cần.
**Sửa:** `cmd/extractor/main.go`, `internal/usecase/markitdown.go`, `internal/services/markitdown.go`, `minio.go` khi cần. Giữ API generic subprocess hiện tại.

Own pending/new message → precheck → temp download → conversion stdout file → upload reader → complete/fail script → ACK. Extractor job timeout 5 phút; conversion 2 phút; HTTP download timeout 2 phút, tất cả bị giới hạn bởi job context. Poll FAILED 500 ms; temp Markdown file path không trả nội dung string trên đường worker. Giữ HTTP mode cũ.

**Dependencies:** T1 và runtime T2.
**AC:** nhiều replicas xử lý part song song; `.md` không chứa stderr; SUCCESS URLs đúng offset; lỗi một part chỉ một FAILED; active conversion bị kill/reap; late upload không đổi FAILED; Redis lỗi không ACK mất job. Mapped AC-3/4/5/6.

### T4 — Compose và hướng dẫn demo

**Sửa:** `deploy/docker-compose.yaml`, hai Dockerfiles nếu cần healthcheck, `README.md`; giữ nginx cho HTTP mode tùy chọn.

Cấu hình worker env, Redis AOF/volume và readiness dependencies, MinIO config, graceful stop. Giữ resource limits hiện tại. Không map host port riêng cho workers để scale Extractor; có `/ping` health trong worker mode. Nếu image distroless cần probe thì binary cung cấp healthcheck subcommand nhỏ; không thêm sidecar.

README có lệnh start, XADD input JSON, đọc output, scale extractor=3, phục hồi own pending/manual claim khi stage đã dừng. Demo sử dụng URL MinIO truy cập được từ các worker; không dùng URL hostname nội bộ từ host client nếu host không resolve được. Output demo đọc từ `0-0` để không bỏ lỡ event đã phát.

**Dependencies:** T2/T3 để smoke.
**AC:** Compose config hợp lệ; fresh start không cần tạo bucket/group bằng tay; 1 splitter + 3 extractors chạy được, pipeline không phụ thuộc nginx. Mapped AC-7.

### T5 — Verification POC

**Lưu kết quả:** `docs/test-results-event-sourcing.md` khi thực thi. Không thêm unit tests mới; dùng existing checks và functional scenarios.

**Dependencies:** T1–T4.
**AC:** chạy `go build ./...`, `go vet ./...`, existing tests nếu có; chạy các scenario dưới đây và ghi PASS/FAIL/bằng chứng. Chưa chạy thì không đánh PASS.

| Scenario | Kết quả cần chứng minh |
|---|---|
| Fresh Redis, start workers | Đủ streams/groups, không có output sentinel |
| File <=16 MiB và file >50 MiB | N đúng, offsets liên tục, part <=16 MiB |
| 3 Extractors, completion sai thứ tự | Một SUCCESS, đủ URLs theo offset |
| Duplicate input/part và hai final offsets cạnh tranh | Không nhân fanout/count/terminal output |
| Corrupted part hoặc conversion timeout | Một FAILED, queued skip, active process dừng trong mục tiêu 2 giây |
| Kill/restart worker trước ACK, cùng consumer name | Pending được xử lý lại, không thêm terminal event |
| Redis tạm gián đoạn | Không ACK mất việc; pending được retry sau reconnect |
| Lặp nhiều tài liệu, gồm PDF/OOXML lớn | Tổng RAM dưới container limits; temp files/goroutines không tích lũy |

Nếu RAM vượt budget hoặc part oversized, sửa đúng đường gây lỗi trong task T2/T3 rồi chạy lại scenario đó; không mở rộng sang hardening production.

## 8. Trình tự và trạng thái

T1 → T2 → T3 → T4 → T5. Có thể chuẩn bị conversion T3 song song logic Splitter, nhưng không cần thêm quy trình điều phối.

Plan này thay thế bản thiết kế và gap review phức tạp trước đó. Các gap TTL quá hạn, nguồn thay đổi, tự phục hồi OOM và retention dài hạn được xử lý bằng giới hạn demo rõ ràng; không thêm task hạ tầng cho chúng. Yêu cầu cốt lõi AC-1–AC-7 vẫn nằm trong tasks và scenarios trên.

Chưa sửa application code, chưa chạy build/integration/benchmark. Chỉ cập nhật tài liệu kế hoạch; triển khai là bước riêng.
