# Benchmark tài nguyên mỗi pod và concurrency

Ngày: 2026-11-15. Trạng thái: **concurrency (`WORKER_CONCURRENCY`), structured timing logs và compose override đã triển khai và smoke-verify thật (xem [runbook](pod-resource-benchmark-runbook.md) mục 6); chưa chạy sweep CPU/RAM/concurrency dài theo ma trận dưới đây**. Phần "cách chạy" cụ thể (lệnh, kết quả smoke) đã chuyển sang [pod-resource-benchmark-runbook.md](pod-resource-benchmark-runbook.md); tài liệu này giữ vai trò kế hoạch/rationale.

## 1. Mục tiêu và cơ sở

Tìm cấu hình CPU/RAM nhỏ nhất đáp ứng throughput, latency và độ tin cậy mong muốn; xác định concurrency hữu ích cho từng loại worker. Không có một mức “RAM tối đa cho mọi file” nếu chưa giới hạn workload: bytes nén không giới hạn được số trang, cell, XML giải nén hay độ phức tạp nội dung. Kết luận chỉ áp dụng cho corpus và giới hạn input được công bố.

Source hiện tại:

- `internal/pipeline/consumer.go`: đọc `Count: 1`, gọi handler và ACK tuần tự. Hai entrypoint chỉ khởi tạo một worker/process. Một pod một process hiện có một job in-flight.
- Splitter xử lý **file gốc**; extractor xử lý **part**, không nhất thiết là cả file gốc. Luôn báo cả docs/min và parts/min.
- `internal/services/markitdown.go`: mỗi conversion tạo subprocess `markitdown`; stdout ghi ra file. RAM cần đo gồm Go, Python và các process con trong container, cùng các khoản memory được cgroup tính. Go heap không đại diện toàn bộ RAM pod.
- `internal/usecase/{splitfile,markitdown}.go`: ngưỡng hiện tại **4 MiB**. Manifest, phân loại small/large và expected_parts cũ cần xác minh lại; không suy luận routing từ nhãn fixture 16 MiB.
- [run-002](load-test-results-event-sourcing.md) ghi extractor peak khoảng 716 MiB với 2 CPU / 1 GiB, splitter khoảng 255 MiB với 1 CPU / 512 MiB. Đây là báo cáo lịch sử trước thay đổi ngưỡng; chưa kiểm chứng lại raw evidence trong đợt thiết kế này và không dùng làm sizing cuối cùng.

Phạm vi: worker mode, tách splitter/extractor; Redis/MinIO là dependency được theo dõi riêng. HTTP mode không thuộc benchmark này. Tài liệu bổ sung cho [load-test plan](load-test-plan-event-sourcing.md); giữ lại kế hoạch cũ làm bối cảnh, áp dụng ngưỡng 4 MiB trong đợt này.

## 2. Chuẩn bị chung

1. Dùng môi trường benchmark riêng. Ghi commit, image digest, kiến trúc CPU, phiên bản runtime, node/VM CPU/RAM/swap, requests/limits, timeout và Redis persistence. Docker trên macOS dùng làm thăm dò; xác nhận sizing trên Linux/Kubernetes tương đương production.
2. Cố định timeout hiện hành trong toàn bộ sweep. Không tăng timeout giữa các cấu hình để che regression.
3. Chọn corpus có checksum: PDF text dày/nhiều trang, PDF nhiều ảnh, DOCX nhiều bảng, XLSX nhiều cell/shared strings, CSV; ưu tiên file thực tế. Có file nhỏ, part gần 4 MiB, file gốc lớn cần nhiều part, và fixture `pdf-small-8mib` từng timeout. PDF scan chỉ đo chức năng hiện đang hỗ trợ, không mặc định có OCR.
4. Tạo cặp fixture dưới/trên 4 MiB. Smoke từng fixture qua pipeline để ghi routing, số part, tính hợp lệ của Markdown và terminal outcome. File lớn synthetic không thay thế corpus nội dung thật. Ghi page/cell count và bytes giải nén nếu thu thập được.
5. Dừng các replica cùng stage không thuộc phép đo. Đầu run không còn lag/pending từ run trước. Dùng request UUID và run ID mới; không xóa state dùng chung để làm sạch queue.
6. Đo idle 2 phút, warm-up 3–5 phút rồi drain, sau đó bắt đầu cửa sổ đo mới. Collector chạy xuyên suốt warm-up/load/drain. Container mới cho mỗi cấu hình để `memory.peak` không lẫn run trước.
7. Chốt trước SLO E2E p95, service-time p95 và error budget; hiện chưa có giá trị được xác nhận. Khi thiếu SLO, chỉ báo đường cong đánh đổi, không kết luận cấu hình đạt production.

## 3. Ma trận kịch bản

| ID | Kịch bản | Ưu tiên | Kết quả cần quan sát |
|---|---|---|---|
| BR-01 | Một job tại một thời điểm, từng fixture | Critical | Footprint và thời gian riêng theo format/độ phức tạp |
| BR-02 | Sweep CPU/RAM, concurrency = 1 | Critical | Cấu hình nhỏ nhất đạt yêu cầu, điểm bão hòa |
| BR-03 | Sweep concurrency = 1, 2, 4 trong một pod | High | Lợi ích throughput so với chi phí CPU/RAM và latency |
| BR-04 | Một pod nhiều slot so với nhiều pod một slot | High | So sánh công bằng theo tổng quota, hiệu suất và độ trễ |
| BR-05 | Mixed và burst file/part nặng đồng thời | Critical | Không OOM; file nhỏ không bị chờ vượt SLO |
| BR-06 | Hold dài và shutdown/restart khi có nhiều slot | High | Không tăng memory nền kéo dài, không mất job/terminal trùng |

### BR-01 — Footprint một job

**Chuỗi:** fixture đã smoke → publish input với ID mới → quan sát splitter → split/state → extractor convert/upload → đúng một output SUCCESS → xác minh output, ACK và cleanup file tạm.

Từng fixture chạy ít nhất 5 lần không chồng job để thăm dò, sau đó tăng mẫu đối với fixture biến động. Báo cold run và warm run riêng. Không công bố p99 từ số mẫu nhỏ.

Đo extractor riêng bằng cách dừng extractor, cho splitter chuẩn bị backlog part hợp lệ từ input thật, rồi dừng splitter và chỉ bật một extractor. Thu baseline tài nguyên sau khi chuẩn bị backlog. Không tự XADD part thiếu state. Đo splitter riêng khi extractor dừng, giới hạn corpus để không tạo backlog quá lớn; bật lại extractor để drain sau phép đo. Stage latency cần timestamp riêng; E2E có thời gian cố ý chờ ở bước chuẩn bị không phải latency extractor.

**Assertions:** mọi fixture hợp lệ đạt kết quả đã smoke, output không rỗng ngoài trường hợp nội dung cho phép, đủ offset, một terminal/request, không unresolved sau drain, không OOM/restart. Fixture từng timeout phải được báo riêng dù vẫn thất bại; không loại âm thầm khỏi sizing.

### BR-02 — Sweep resource với một slot

Giữ một pod của stage đang đo, concurrency = 1. Khởi đầu thăm dò (không phải cấu hình khuyến nghị):

| Stage | CPU sweep | RAM sweep |
|---|---|---|
| Splitter | 0.5, 1, 2 vCPU | 256, 512, 1024 MiB |
| Extractor | 0.5, 1, 2, 4 vCPU | 512, 1024, 2048 MiB |

Không chạy toàn bộ tích Descartes: trước hết dùng RAM đủ rộng trong ngân sách môi trường để sweep CPU; giữ CPU ở vùng throughput ngừng cải thiện đáng kể rồi giảm RAM từng mức. Khi chạm mức thiếu RAM, quay về cấu hình đạt gần nhất và tinh chỉnh quanh đó. Nếu mức đầu đã thiếu RAM, mở rộng dải có kiểm soát; không gọi mức cao nhất trong bảng là maximum.

Giữ corpus/order/seed cố định. Tăng offered load từng bước đến khi stage bận liên tục, quan sát queue và throughput. Mỗi mức hold ít nhất 10 phút; bỏ 2 phút chuyển tiếp khỏi throughput steady-state. Chỉ đọc CPU thấp khi có backlog mới có ý nghĩa phân biệt chờ I/O với thiếu tải. Nếu dependency hoặc stage khác giới hạn throughput, kết quả không xác định được capacity của pod đang đo.

**Assertions:** không OOM/restart/swap che thiếu RAM, không tăng failure/timeout do tài nguyên, throughput completion theo kịp arrivals ở mức được công bố bền vững, lag không tăng kéo dài. Sau dừng nạp, drain và đối soát accepted = success + failed + unresolved; unresolved phải bằng 0 để run đủ điều kiện sizing.

### BR-03 — Concurrency trong một pod

**Đã sẵn sàng:** `WORKER_CONCURRENCY` (mặc định 1, validate số nguyên dương, fail startup nếu sai) đã triển khai tại `internal/pipeline/{redis,consumer,splitter,extractor}.go` — xem [runbook](pod-resource-benchmark-runbook.md) mục 1 và 6 cho chi tiết cơ chế (N loop `RunConsumer` độc lập theo sub-consumer `<CONSUMER_NAME>-slot-<i>`, mỗi loop giữ nguyên lifecycle download→convert/split→upload→ACK tuần tự) và bằng chứng smoke (concurrency=2 xác nhận 2 offset convert chồng lấn thời gian thật). `deploy/docker-compose.bench.yaml` đặt `EXTRACTOR_CONCURRENCY`/`SPLITTER_CONCURRENCY` qua biến môi trường mà không sửa `docker-compose.test.yaml`. Chưa chạy sweep C=1→2→4 dưới tải thật (k6) — chỉ đã smoke một job 12-part dưới concurrency=2. Không dùng nhiều replica để giả lập nhiều slot/pod rồi báo như kết quả thực.

Tại cấu hình BR-02 đã đạt, so sánh C = 1 → 2 → 4, giữ CPU/RAM cố định trước để thấy có tận dụng được tài nguyên dư không. Sau đó chỉ tăng RAM/CPU cho cấu hình có triển vọng, báo riêng phép so sánh có tăng quota. Không thử C = 8 trừ khi C = 4 còn cải thiện rõ và còn ngân sách.

Mỗi slot extractor có thể giữ một subprocess; chi phí tăng chủ yếu ở conversion, không phải stack goroutine. C khởi đầu có thể quanh số vCPU cho workload thiên CPU, nhưng phải đo vì subprocess có thể tự tạo thread và workload có download/upload.

**Assertions:** concurrency thực không vượt C kể cả retry/download/upload; không đọc trước vô hạn; ACK chỉ sau commit; pending drain và retry giữ semantics hiện tại; đủ output và đúng một terminal/request; throughput, service time và resource được báo cùng nhau. Luôn thử C job nặng chạy đồng thời để đo RAM peak, không chỉ lấy peak một job nhân C.

### BR-04 — So sánh cùng tổng tài nguyên

Ví dụ sau khi pool sẵn sàng:

| Topology extractor | Tổng CPU/RAM | Tổng slot |
|---|---|---:|
| 1 pod × 2 CPU / 2 GiB, C = 2 | 2 CPU / 2 GiB | 2 |
| 2 pod × 1 CPU / 1 GiB, C = 1 | 2 CPU / 2 GiB | 2 |

Đây là ví dụ phép đo, không mặc định mỗi topology đủ RAM. Giữ splitter/dependency/quota tổng, corpus và tổng offered load tương đương; dùng cùng node class. Đo idle overhead, throughput, latency, RAM tổng và từng pod. Pod lớn có thể giảm overhead nền nhưng gom nhiều job cùng bị ảnh hưởng khi pod chết; nhiều pod cho scale và cô lập lỗi nhỏ hơn. Chọn theo số đo và yêu cầu vận hành.

### BR-05 — Mixed và burst

Chạy small-only, heavy-only, mixed; dùng mix thực tế nếu có, nếu chưa có dùng 80/20 theo số file và ghi rõ giả định. Xen kẽ và dồn cụm file nặng, có burst đủ lấp đầy C slot. Với file gốc nhiều part, ghi fan-out vì 1 doc/s có thể tạo nhiều hơn 1 part/s.

**Assertions:** RAM đủ khi các job nặng trùng thời điểm; E2E p95 của file nhỏ đạt SLO riêng, không để trung bình tổng che đói slot; queue ổn định ở tải được chọn; file thực tế thành công và output đúng. Burst có backlog tạm thời phải drain trong mục tiêu đã chốt trước, không được gọi là steady state khi backlog còn tăng.

### BR-06 — Hold và recovery

Giữ ứng viên cuối 30–60 phút ở tải bền vững dự kiến; lặp ít nhất 3 run độc lập. Sau drain chờ idle và so baseline RAM/file tạm. Nếu mẫu ít hoặc memory vẫn tăng, kéo dài quan sát; giữ cache/allocator chưa trả RAM không tự nó chứng minh leak.

Trên môi trường riêng, gửi SIGTERM khi C slot đang xử lý, restart với **cùng consumer identity** rồi drain pending. Hiện POC không tự reclaim consumer đã biến mất; không mặc định pod identity mới sẽ khôi phục được pending. Pool cần identity ổn định theo slot hoặc cơ chế recovery được thiết kế và kiểm chứng trước.

**Assertions:** không đánh FAILED chỉ vì shutdown, không sót subprocess, không vượt C khi restart, không mất job, đúng một terminal/request; không kết luận exactly-once computation vì retry có thể chạy lại conversion. Ghi object upload dư nếu có; không xóa output cần hậu kiểm.

## 4. Thu thập và điều kiện dừng

Tái sử dụng `k6/load_test.js`, hướng dẫn [k6/README.md](../k6/README.md), `loadtest/tools/resources` và `loadtest/tools/drain`. Cố định fixture bằng `FIXTURE_IDS`, arrival bằng `RATE_LEVELS`, hold bằng `STAGE_HOLD_DURATION`; cấu hình VU theo observation window và rate thực tế. Xác minh manifest 4 MiB trước khi dùng phân loại small/large của script.

Mỗi run lưu tại `docs/test-results/pod-resources/<run-id>/`: config/image/fixture manifest, k6 log, worker log, resources, stream-start và drain-report. Collector hiện hỗ trợ Docker; bản Kubernetes cần cách thu tương đương, không mặc định tooling hiện tại đọc được pod.

| Nhóm | Số liệu bắt buộc |
|---|---|
| Memory | Idle, steady p95, sampled max, cgroup memory.peak, memory.events/OOM delta, swap; gồm subprocess. Pod nhiều container phải cộng và ghi rõ từng container |
| CPU | CPU-seconds, average cores = delta usage / elapsed, throttled periods/time delta; CPU peak chỉ là phụ. Docker 100% tương ứng khoảng một logical CPU, không phải 100% quota pod |
| Hiệu năng | Successful docs/min, parts/min, MiB/min; service time và queue wait tách E2E p50/p95 theo fixture; sample count |
| Độ tin cậy | Timeout/failure theo nguyên nhân, restarts, lag/pending đầu/cuối và trend; accepted/terminal đối soát |
| Dependency/disk | Redis/MinIO latency/tài nguyên, network, temp/ephemeral-storage peak; tránh ghi nhầm nghẽn I/O thành thiếu CPU |

Nếu thiếu timestamp service start/end, chỉ báo E2E và ghi service-time/queue-wait UNKNOWN; cần bổ sung instrumentation trước khi kết luận nguyên nhân. Sample RAM khoảng 1 giây nếu collector chịu được; dùng cgroup peak để bắt spike giữa các mẫu, ghi overhead collector. CPU-seconds/success phải kèm failure rate và tính cả tài nguyên tiêu cho job thất bại; không tối ưu chỉ bằng tỷ lệ CPU/quota.

Dừng admission khi OOM/restart, memory gần limit kéo dài (ngưỡng thăm dò 90%), hoặc outstanding vượt guardrail đã đặt; giữ collector và drain tiếp. Với run chủ đích tìm giới hạn RAM, OOM là kết quả cấu hình không đạt, không phải thành công của workload. Cgroup không đọc được phải ghi UNKNOWN, không suy ra zero OOM.

## 5. Quy tắc chọn cấu hình và kết quả bàn giao

- Chọn mức gần điểm gãy: tăng CPU/C tiếp nhưng throughput tăng ít trong khi RAM hoặc p95 tăng nhiều. Có thể dùng cải thiện <10–15% làm tín hiệu dừng thăm dò; đây là tiêu chí đề xuất, không SLO đã được duyệt.
- Memory limit khởi đầu từ peak cgroup của workload C job nặng đồng thời + headroom khoảng 25–40%, rồi chạy lại BR-05/06. Không lấy p95 RAM làm hard limit. Công thức `idle + C × phần tăng RAM/job + margin` chỉ dùng dự trù, không thay số đo đồng thời.
- CPU request theo nhu cầu duy trì throughput và chính sách scheduling; kiểm chứng lại limit qua throttling/latency. RAM request theo footprint vận hành cần bảo đảm, cân nhắc gần limit cho workload cần bảo đảm dung lượng. Bảng sweep là quota thử nghiệm, không tự động trở thành requests production.
- Không suy ra capacity toàn hệ thống từ extractor riêng. Chạy E2E cuối với tỷ lệ splitter/extractor dự kiến; sizing hai stage độc lập.

Mẫu báo cáo cuối:

| Stage / corpus | CPU req/limit | RAM req/limit | C | SUCCESS docs/min; parts/min | E2E/service p95 | RAM peak | Throttling | Failure/OOM | Verdict |
|---|---|---|---:|---|---|---|---|---|---|
| Chưa đo | — | — | — | — | — | — | — | — | Chưa kết luận |

## 6. Khuyến nghị concurrency và khoảng trống

Nên giữ C = 1 làm baseline, đo C = 2 trước khi chọn nhiều slot/pod. Nếu chờ I/O đáng kể hoặc CPU/RAM còn dư, bounded concurrency có thể tăng throughput. Nếu conversion đã chiếm hết CPU hoặc peak RAM cao, tăng goroutine thường chỉ tăng tranh chấp và số job cùng rủi ro OOM. Khi CPU bão hòa, thử thêm pod hoặc tăng quota và so hiệu suất trên cùng tổng ngân sách.

Nếu triển khai pool: giới hạn toàn bộ lifecycle download → convert → upload → commit/ACK, không chỉ đoạn conversion; không dequeue vô hạn; giữ retry/idempotency/fail-fast; kiểm chứng duplicate đang in-flight vì precheck hiện tại không phải khóa xử lý độc quyền. Consumer identity/pending recovery phải rõ theo slot và replica. Không tự nhân pool ở cả consumer lẫn converter làm concurrency thực vượt cấu hình. Splitter và extractor có C độc lập.

2026-11-15 cập nhật: bounded pool cho C > 1 (`WORKER_CONCURRENCY`), timing instrumentation (structured `TIMING` logs) và corpus/expected_parts dưới ngưỡng 4 MiB đã triển khai và smoke-verify thật — xem [runbook](pod-resource-benchmark-runbook.md). Khoảng trống còn lại trước khi kết luận sizing production: SLO E2E/service-time p95 chưa chốt, sweep CPU/RAM/concurrency dài theo BR-02..BR-06 chưa chạy, collector Kubernetes nếu đo trên cluster (hiện chỉ Docker), và regression `pdf-small-8mib` dưới ngưỡng 4 MiB vẫn FAILED (xem runbook mục 6) — cần điều tra riêng (ví dụ split theo trang/budget công việc thay vì byte, theo `docs/throughput-source-review.md` mục 2) trước khi coi ngưỡng 4 MiB là đã giải quyết vấn đề timeout PDF.
