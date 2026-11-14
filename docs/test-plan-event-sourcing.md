# Functional Self-Test Plan — Redis Streams Pipeline POC

Ngày: 2026-11-14. Trạng thái: **DRAFT — chưa thực thi**.

## 1. Phạm vi và điều kiện đầu vào

Kiểm chứng T1–T4 trước [load test](load-test-plan-event-sourcing.md), theo [requirement](requirement-event-sourcing.md) và [simplified implementation plan](implementation-plan-event-sourcing.md). Khi khác nhau, dùng quyết định POC trong implementation plan. Đây là kiểm thử chức năng/integration, chưa đo capacity.

Checkout lúc lập plan chưa có `internal/pipeline`, ba Lua scripts hoặc worker mode; Compose và README chưa phản ánh pipeline. Vì vậy các hành vi dưới đây là **kỳ vọng theo thiết kế**, chưa phải hành vi implementation đã xác minh. Sau khi Claude hoàn tất, đối chiếu code/config thực tế trước khi chạy; ghi rõ mọi khác biệt ảnh hưởng assertions.

Không thêm unit tests, framework, leases, sweepers hoặc retry infrastructure. Fault injection dùng môi trường test riêng và công cụ sẵn có; không sửa application code để làm test pass. Không commit/deploy trong bước này.

## 2. Trình tự thực thi

1. Ghi commit, working-tree diff, Go/Docker versions và cấu hình; bảo toàn thay đổi có sẵn. Đối chiếu hoàn thành T1–T4.
2. Chạy `gofmt` check không ghi file, `go build ./...`, `go vet ./...`, existing tests bằng `go test ./...`; không tạo unit tests mới. Chạy `docker compose -f deploy/docker-compose.yaml config`. Đây là bước verification sau implementation, không phải thay đổi yêu cầu cũ đã cấm Claude chạy checks.
3. Khởi tạo Compose project/volumes riêng; build images rồi chạy **1 Splitter + 3 Extractors**, Redis, MinIO. Không cần nginx cho pipeline. Kiểm tra worker mode, readiness, consumer names duy nhất và actual memory limits.
4. Chạy TC-001–004 trước; nếu happy path lỗi thì dừng và sửa trước fault scenarios.
5. Chạy duplicate/concurrency, failure/cancellation, recovery, HTTP regression và resource smoke.
6. Lưu bằng chứng và verdict. Chỉ bắt đầu load test khi đạt gate ở mục 7.

Không chạy bất kỳ lệnh verification/runtime nào trong lúc chỉ lập plan này.

## 3. Fixtures và cách quan sát chung

- Dùng UUID hợp lệ và mới cho request/conversation/message của mỗi case; ghi manifest gồm case ID, IDs, source URL, bytes/checksum, thời điểm publish và input stream ID. Duplicate cases cố ý dùng lại cùng request ID.
- Fixtures: tài liệu nhỏ có nội dung biết trước; PDF >50 MiB có marker/trang để kiểm tra thứ tự; DOCX/XLSX lớn đại diện bộ load-test; file đúng 16 MiB và vừa vượt ngưỡng với định dạng được hỗ trợ; OLE >16 MiB; file hỏng; URL trả 404; file conversion đủ lâu cho cancellation. Fixture phải tải được từ workers. Không dùng padding tùy ý làm hỏng format để thử boundary.
- Đối với oversized atomic unit: chọn một đơn vị không thể chia nhỏ dưới 16 MiB theo splitter thực tế; ghi format và giới hạn được quan sát.
- Upload fixtures vào bucket test; xác nhận source URL/credentials theo README đã cập nhật. Không ghi secrets vào kết quả.
- Publish input bằng `XADD stream:doc-extractor:input * data <JSON>`; part events cũng dùng field `data`. Không tự đặt thêm contract ngoài tài liệu.
- Với mỗi request: đọc state hash, completed set, parts hash, output events và object Markdown; đối chiếu IDs và nội dung. Snapshot toàn bộ output từ `0-0` hoặc từ mốc trước case, lọc request ID; không dùng `$` vì có thể bỏ lỡ event.
- Quan sát `XINFO GROUPS`, `XINFO CONSUMERS`, `XPENDING`, worker logs, process tree, temp files và container resource/events. **XLEN không phải backlog**. Sau drain, lag/pending của messages hợp lệ thuộc case phải về 0.
- Poll tới điều kiện cụ thể, dùng deadline dựa trên JOB_TIMEOUT/CONVERT_TIMEOUT thực tế cộng thời gian ACK/quan sát. Ghi timeout quan sát riêng; không suy ra FAILED từ việc chờ quá lâu. Sau terminal vẫn chờ mọi attempt của case kết thúc trước khi đếm output cuối.
- Lưu fixture/setup steps và fault timing để có thể lặp lại. Test không chạm dữ liệu dùng chung; không FLUSHALL, xóa volume hay trim global streams để cleanup.

## 4. Danh sách kịch bản

| ID | Scenario | Priority | Kỳ vọng |
|---|---|---|---|
| TC-001 | Fresh start và bootstrap lặp | Critical | 3 streams, 2 groups, không output sentinel |
| TC-002 | File nhỏ và boundary 16 MiB | Critical | Bypass đúng ngưỡng, SUCCESS và Markdown đúng |
| TC-003 | File lớn, 3 Extractors, completion đảo thứ tự | Critical | Parts đúng size/order; một SUCCESS, URLs theo offset |
| TC-004 | Splitter failures | High | Một FAILED offset=-1; không fanout dở dang |
| TC-005 | Duplicate input/part, identity conflict | Critical | Không nhân fanout/count, không đổi URL hoặc TTL |
| TC-006 | Final completion/failure race | Critical | Một terminal event, terminal không bị đảo |
| TC-007 | Extractor failures và fail-fast | Critical | Một FAILED; queued skip; active child bị cancel/reap |
| TC-008 | Own pending, crash và SIGTERM | Critical | Phục hồi không mất job/nhân terminal |
| TC-009 | Redis gián đoạn | Critical | Không ACK mất việc, tự tiếp tục sau reconnect |
| TC-010 | Malformed/missing state | High | ACK/log rõ, không tạo request/terminal giả |
| TC-011 | HTTP regression | High | Các endpoint hiện hữu vẫn hoạt động trong HTTP mode |
| TC-012 | Resource/cleanup smoke | High | Không OOM, không tích lũy process/temp qua các lượt |

## 5. Test cases và chains

### TC-001 — Bootstrap

**Preconditions:** Redis và bucket test mới trong project riêng.

**Chain:** Start workers → kiểm tra streams/groups, bucket và health → restart workers → kiểm tra lại.

**Assertions:** Input group `cg:doc-splitter`, splitted group `cg:doc-extractor`, output stream tồn tại và không chứa sentinel; restart không lỗi BUSYGROUP, không phát output giả. Một Splitter, ba Extractors có consumer names khác nhau. Bucket sẵn sàng không cần tạo thủ công.

### TC-002 — Small file và boundary

**Preconditions:** Fixtures hợp lệ ở dưới, đúng và vừa trên 16 × 1024 × 1024 bytes.

**Chain:** Publish từng input → capture part events → chờ terminal → GET từng Markdown URL từ network phù hợp.

**Assertions:** File <=16 MiB có duy nhất offset=0, total=1, URL gốc. File >16 MiB đi đường split và các part thực tế <=16 MiB, hoặc FAILED có lý do rõ nếu atomic unit không thể tách. SUCCESS tương ứng state COMPLETED; completed count=total, đủ `part:0..N-1`; Markdown có marker mong đợi, không chứa stderr. Payload giữ đúng IDs, status và timestamp theo schema; chỉ một terminal event/request.

### TC-003 — Multi-part và thứ tự

**Preconditions:** PDF >50 MiB tạo nhiều part, có marker theo trang; ba Extractors. Chọn fixture có ít nhất 12 parts nếu khả thi để bắt lỗi sort lexical. Có case DOCX/XLSX đại diện bộ load-test.

**Chain:** Publish → kiểm tra fanout offset 0..N-1 → quan sát nhiều consumers xử lý đồng thời → chờ drain → đọc Markdown theo URL list.

**Assertions:** N khớp total ở state/events; không thiếu/trùng offset; actual part bytes <=16 MiB; marker nối theo offset giữ thứ tự tài liệu. URLs trong SUCCESS theo offset, không theo completion time. State terminal và output nhất quán.

**Notes:** Nếu run tự nhiên không có completion đảo thứ tự, giữ nguyên full-flow case và bổ sung isolated state/script case TC-006; không đánh ordering concurrency đã kiểm chứng chỉ từ một run tuần tự.

### TC-004 — Splitter failures

**Preconditions:** URL 404, OLE >16 MiB, atomic unit oversized; một case storage upload bị từ chối trong môi trường riêng.

**Chain:** Publish mỗi fixture với ID mới → chờ FAILED → inspect streams/state/storage.

**Assertions:** Đúng một FAILED với `failed_offset=-1`, reason có ý nghĩa, state FAILED; không publish part events của request khi split chưa hoàn tất. Input được ACK sau failure commit. Object đã upload trước lỗi có thể còn lại theo giới hạn POC; không yêu cầu rollback storage.

### TC-005 — Deduplication

**Preconditions:** Một request đang PROCESSING và một request đã terminal; có snapshot state/TTL/accepted URLs.

**Chain:** Publish lại input giống hệt → input cùng request ID nhưng identity khác → publish duplicate part trước/sau completion → chờ drain.

**Assertions:** Không tăng fanout sau init commit; duplicate part không tăng SCARD hoặc thay accepted URL; identity conflict không đổi request gốc và được log/ACK. Duplicate không reset expires_at; TTL giảm theo thời gian, không kéo dài. Các key được tạo có cùng expiry, mặc định khoảng 24h; completed/parts được phép chưa tồn tại khi rỗng. Chỉ một terminal event.

### TC-006 — Atomic transitions và races

**Preconditions:** Redis test riêng; đọc đúng KEYS/ARGV và return values của ba script sau triển khai. Dùng client có sẵn để gọi script trực tiếp, không thêm application hooks. Tạo IDs riêng và seed qua init script; không để workers consume fixture của isolated script cases.

**Chain:** Init hai lần → complete offset đảo thứ tự → gửi hai final completions đồng thời → thử failure sau SUCCESS. Với request mới: gửi hai failures đồng thời → complete muộn sau FAILED. Với request hai parts: record một part rồi cạnh tranh completion cuối với failure hợp lệ của part cuối. Lặp race có giới hạn, lưu kết quả từng lượt.

**Assertions:** Init chỉ phát N events một lần; duplicate không mutate. Hai finals chỉ tạo một SUCCESS, list theo offset. Hai failures chỉ tạo một FAILED. Race SUCCESS/FAILED cho phép bên commit terminal đầu tiên thắng; không bắt buộc một status cụ thể. State/terminal_event_id/output luôn khớp; không bao giờ có cả SUCCESS và FAILED cùng request. Late result không đổi terminal. Sai identity/total/offset, missing state và offset đã completed không gây mutation trái contract. Kiểm tra reject N>1000 trước writes bằng script contract, không cần tạo 1001 file thật.

**Notes:** Lua isolation không phải rollback runtime errors; WRONGTYPE/OOM ngoài recovery tự động POC. Case này kiểm tra transitions, không thay thế full-flow daemon tests.

### TC-007 — Extractor failures và cancellation

**Preconditions:** Init request nhiều parts; có part conversion chậm đang active và parts còn queued. Để fault xác định được, có thể upload rồi thay nội dung một part test bằng fixture hỏng trước khi start Extractors; giữ input gốc bất biến. Chuẩn bị riêng cases download 404, conversion lỗi, conversion timeout và Markdown upload thất bại.

**Chain:** Start Extractors → xác nhận slow conversion active → kích hoạt lỗi sibling → ghi thời điểm state FAILED và thời điểm process group dừng → chờ drain → kiểm tra late results.

**Assertions:** Một FAILED đúng offset, error và failed_at; queued parts thấy FAILED được ACK mà không convert. Active conversion mục tiêu dừng <=2 giây sau FAILED trên môi trường khỏe; không còn child/orphan, poll goroutine/temp được cleanup. Không publish FAILED thứ hai do sibling cancellation. Upload/completion đến muộn không đổi FAILED hoặc tạo SUCCESS. Lỗi download/convert/upload chỉ ACK sau failure script commit.

**Notes:** Cần bằng chứng process/timestamps; output FAILED riêng lẻ chưa đủ chứng minh fail-fast. Nếu không kiểm soát được timing, ghi BLOCKED cho assertion cancellation thay vì PASS suy đoán.

### TC-008 — Crash, pending và shutdown

**Preconditions:** Consumer name cố định; fixture đủ lâu để thấy pending. Thực hiện riêng cho Splitter và Extractor.

**Chain:** Kill process khi đang xử lý trước commit → kiểm tra pending → restart cùng name → chờ terminal. Lặp với SIGTERM. Kiểm tra thêm cửa sổ commit-trước-ACK: nếu bắt được bằng fault timing, restart và đối chiếu dedup; nếu không, ghi coverage gap, TC-005/006 chỉ là bằng chứng hỗ trợ.

**Assertions:** Crash không ACK mất message; own pending được xử lý trước new messages, không cần đổi name/manual claim. SIGTERM cancel/reap/cleanup, không tạo business FAILED vì shutdown. Sau recovery không nhân fanout/terminal; pending drain hết.

**Notes:** Không đòi tự reclaim từ consumer khác. Nếu cần phục hồi đổi name, dừng toàn bộ workers của stage trước manual XAUTOCLAIM theo README; không claim job active.

### TC-009 — Redis interruption

**Preconditions:** Redis test có persistence; request đang active. Fault chỉ tác động project test.

**Chain:** Làm Redis tạm không truy cập được trong lúc conversion/commit → quan sát cancellation → khôi phục Redis → giữ workers chạy và chờ recovery. Lặp ở gần terminal commit nếu có thể điều khiển timing.

**Assertions:** Không ACK khi commit chưa xác nhận; Redis read lỗi cancel active job; reconnect tự retry commit/own pending, không cần restart thủ công. Cuối cùng một terminal event, state khớp và không mất part. Lost-reply terminal retry không tạo event thứ hai nếu đã commit. Không yêu cầu sống sót qua mất dữ liệu Redis.

### TC-010 — Invalid events

**Preconditions:** Môi trường riêng, ID mới cho từng biến thể.

**Chain:** Publish malformed JSON, UUID/URL sai; part total/offset sai, identity mismatch; part hợp lệ nhưng state thiếu.

**Assertions:** Daemons tiếp tục xử lý request hợp lệ kế tiếp. Invalid messages được log với stream ID/reason và ACK theo POC; không tạo request mới từ part, không đổi state request khác, không phát terminal giả. Đối chiếu nhánh validation thực tế với thiết kế trước khi chốt assertions cho input không có identity hợp lệ.

### TC-011 — HTTP compatibility

**Preconditions:** Chạy HTTP mode với cấu hình riêng, theo README và request structs hiện hữu sau implementation.

**Chain:** GET `/ping` trên từng service → POST `/api/v1/split-file` → POST `/api/v1/subprocess/markitdown/run` với fixture nhỏ → smoke `/api/v1/subprocess/run` bằng lệnh vô hại theo allow/contract hiện hữu.

**Assertions:** Response status/body và conversion/splitting giữ contract cũ; worker mode không làm mất HTTP mode. So với baseline trước thay đổi hoặc existing tests, không dựa README cũ đang lệch code. Không tự invent request body.

### TC-012 — Resource smoke và cleanup

**Preconditions:** Actual limits Splitter 512 MiB, mỗi Extractor 1 GiB; monitor gồm Python children. Theo dõi Docker events, stats và cgroup peak/OOM counters nếu truy cập được trên Docker VM macOS.

**Chain:** Sau warm-up, chạy tuần tự 3–5 vòng bộ PDF/DOCX/XLSX lớn; không tạo arrival load → chờ drain mỗi vòng → ghi RAM peak, PIDs, temp files và cleanup sau lỗi/cancel.

**Assertions:** Không OOM/crash/restart bất thường; không tích lũy child/temp sau mỗi vòng; footprint không tăng không kiểm soát. Ghi peak dưới actual limits, không dùng Docker stats đơn lẻ để khẳng định không có micro-spike. Redis stream/state storage tăng theo retention POC không tự coi là memory leak.

**Notes:** Đây là smoke, không chứng minh không leak dài hạn hay throughput bền vững; phần đó thuộc load test. Không thu được cgroup peak/counters thì ghi hạn chế quan sát.

## 6. Coverage và giới hạn

| Requirement | Cases |
|---|---|
| AC-1 streams/groups | TC-001 |
| AC-2 split, offsets, size | TC-002/003/004 |
| AC-3 conversion/upload | TC-002/003/007 |
| AC-4 atomic aggregation/order | TC-003/005/006/008/009 |
| AC-5 failure/fail-fast | TC-004/006/007/008/009 |
| AC-6 memory/cleanup | TC-007/008/012; load test bổ sung |
| AC-7 Compose/replicas | TC-001/003 |
| HTTP compatibility | TC-011 |

Không yêu cầu production hardening: dedup sau TTL, replay lịch sử, Redis data loss, auto reclaim consumer khác, nguồn URL thay đổi, sweeper hoặc recovery OOM lặp. Race không tái hiện được phải ghi hạn chế; không mặc định PASS. Chưa thể chốt commands/config/script arguments chi tiết cho code chưa có trong checkout.

## 7. Gate trước load test và lưu kết quả

Tạo `docs/test-results-event-sourcing.md` **khi thực thi**, gồm commit/diff, môi trường/config, fixture manifest, từng TC với PASS/FAIL/BLOCKED/NOT RUN và đường dẫn logs/events/state/object evidence. Mọi failure ghi expected/actual, cách tái hiện và case cần rerun sau fix. Lưu bằng chứng dưới thư mục test-results theo convention repo hoặc `docs/test-results/event-sourcing/<run-id>/` nếu chưa có.

Chỉ chuyển sang load test khi:

- Build/vet/existing tests/Compose validation và image startup thành công.
- Tất cả Critical/High đạt; mọi assertion không thực hiện được được ghi rõ và giải quyết trước gate, đặc biệt cancellation, pending recovery và terminal races.
- Bộ fixtures load-test PDF/DOCX/XLSX thực sự SUCCESS, URLs đọc được và nội dung/thứ tự đúng.
- Không duplicate terminal, lost jobs, OOM, orphan processes hoặc backlog/pending chưa giải thích sau drain.
- Lưu báo cáo cùng bằng chứng; kết luận **sẵn sàng load test**, không kết luận production-ready hay đã biết capacity.

Cleanup chỉ xóa state/objects do run sở hữu trên môi trường riêng sau khi lưu evidence. Mỗi case failure/recovery phải khôi phục network/storage/workers trước case tiếp theo.
