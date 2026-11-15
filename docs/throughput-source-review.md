# Review tối ưu throughput toàn hệ thống

Ngày: 2026-11-15. Trạng thái: đề xuất, chưa triển khai.

## Phạm vi và kết luận

Đã đọc hai entrypoint, toàn bộ `internal/pipeline`, Lua transitions, usecase, handler, các splitter PDF/CSV/DOCX/XLSX/PPTX/HTML/EML, MinIO/subprocess, Compose và bằng chứng load test run-002. Không sửa application code, không chạy lại load test trên stack đang hoạt động.

Ưu tiên cao nhất là giảm thời gian giữ slot của Extractor và loại bỏ các trường hợp Splitter bị giữ vô hạn. CSV có một quick win rõ ràng; OOXML có nhiều chi phí nhân theo số part. Tăng concurrency chỉ có lợi khi tài nguyên còn đủ; không thể suy ra mức tăng throughput từ số goroutine.

Facts từ code: mỗi replica worker chỉ có một job in-flight; conversion khởi động một Python CLI mới cho mỗi part; Splitter upload toàn bộ part xong mới publish fanout. Facts từ `run-002/manifests/drain-*.json`: small 113 SUCCESS / 17 FAILED / 130 accepted; large 33/0/33; mixed 29/3/32. Báo cáo load test quy các lỗi về PDF 8 MiB timeout 120s. Đây là bằng chứng lịch sử, không phải benchmark mới của review.

Không coi “5 docs/min” hoặc “2 docs/min” trong báo cáo cũ là maximum capacity đã chứng minh. Large test chưa bracket ngưỡng bão hòa; small/mixed có failure; CPU peak không thay cho CPU trung bình, throttling hoặc profile. So PDF 23 MiB với PDF 8 MiB khác nội dung không chứng minh splitting làm conversion nhanh hơn theo hệ số đó. E2E có queue wait, không dùng trực tiếp làm service time.

## Xếp hạng

Hiệu quả là đánh giá có điều kiện theo workload, chưa phải cam kết speedup. Độ dễ: 5 = dễ nhất; effort là ước lượng cho code và kiểm chứng có mục tiêu, chưa gồm soak/rollout. P1/P2 dùng cho lỗi có hậu quả thực tế; “Cơ hội” dùng cho tối ưu cần đo.

| # | Vấn đề / cơ hội | Hiệu quả dự kiến | Dễ /5 | Effort | Ưu tiên |
|---|---|---|---:|---|---|
| 1 | Invalid part count khiến retry vô hạn | Rất cao khi gặp: khôi phục cả slot Splitter | 5 | <1 ngày | P1 |
| 2 | PDF dưới 16 MiB vẫn rất đắt, giữ slot đến timeout | Rất cao với PDF nhiều text/trang | 3 | 1–3 ngày thử nghiệm | Cơ hội cao |
| 3 | Một job in-flight/replica, mọi phase chạy tuần tự | Cao nếu còn CPU/RAM hoặc nhiều I/O wait | 3 | 1–3 ngày | Cơ hội cao |
| 4 | CSV flush từng dòng + encode hai lần | Cao trong CSV splitter; toàn hệ thống tùy tỷ trọng CSV | 5 | <1 ngày | P2 |
| 5 | XLSX/PPTX budget theo uncompressed shared entries | Rất cao trên package có shared XML lớn | 2 | 2–4 ngày | P2 |
| 6 | Khởi động Python/khởi tạo converter mỗi part | Trung bình–cao với nhiều part nhỏ | 2 | 2–5 ngày | Cơ hội |
| 7 | Split/upload từng part tuần tự | Trung bình–cao khi upload chiếm nhiều service time | 3 | 1–3 ngày | Cơ hội |
| 8 | PDF thử lại bằng cách split lại cả tài liệu | Cao với PDF có page size không đồng đều | 2 | 2–4 ngày | P2 |
| 9 | OOXML giải nén lặp + nhiều buffer/copy | Trung bình; giúp tăng concurrency trong cùng RAM | 3 | 1–3 ngày | Cơ hội |
| 10 | Local split không tuân thủ deadline đầy đủ; download không cap | Cao khi có input quá lớn/chậm/pathological | 3 | 1–3 ngày | P2 |
| 11 | HTTP conversion không timeout/cancel/admission limit | Cao khi dùng HTTP dưới tải hoặc client hủy | 4 | 1–2 ngày | P1 trong HTTP mode |
| 12 | Redis streams giữ entry mãi sau ACK | Cao cho vận hành dài ngày, thấp ở benchmark ngắn | 3 | 1–2 ngày thiết kế/chỉnh | P2, giới hạn POC đã biết |

## Chi tiết và hướng sửa

### 1. Part count không hợp lệ có thể khóa vĩnh viễn một Splitter

Nguồn: `internal/pipeline/splitter.go:94`, `internal/pipeline/consumer.go:173`, `internal/pipeline/lua/init_and_publish.lua:43`, `internal/services/xlsxsplit.go:279`.

`InitAndPublish` chỉ chấp nhận 1..1000 URL. Sau split, Go không kiểm tra `len(urls)` trước khi gọi `retryScript`; mọi lỗi script đều được retry bằng parent context, không còn bị giới hạn bởi job timeout. Nếu có >1000 part, Redis luôn trả `invalid total`, worker không ACK và không đọc job kế tiếp. XLSX có thể trả 0 URL nếu không có data rows ở các sheet; đây cũng là lỗi cố định tương tự. Overhead budget âm tại mục 5 làm trường hợp >1000 part dễ xảy ra hơn.

Sửa tối thiểu: validate count trước script, đưa kết quả không hợp lệ qua `FailSplitter`; giới hạn part count ngay lúc tạo part để tránh split/upload vô ích. Retry chỉ lỗi transient; lỗi contract phải kết thúc có kiểm soát. Khi thêm precheck, nhớ trường hợp duplicate đã có state và giữ nguyên semantics terminal/idempotency.

Hiệu quả: một request xấu hiện có thể làm throughput replica Splitter bằng 0 cho đến khi can thiệp. Đây là ưu tiên sửa trước mọi tuning.

### 2. Tách ngưỡng kích thước truyền tải khỏi chi phí xử lý PDF

Nguồn: `internal/usecase/splitfile.go:46`, `internal/usecase/splitfile.go:60`, `internal/services/splitter.go:89`, `internal/pipeline/extractor.go:99`; dữ liệu run-002 và `docs/load-test-results-event-sourcing.md`.

Mọi file <=16 MiB được pass-through. Bytes không dự đoán tốt số trang, mật độ text hoặc layout complexity của PDF. Theo kết quả đã lưu, 17 PDF lỗi trong small run đều là fixture 8 MiB timeout 120s; mỗi lần như vậy chiếm một trong ba slot nhưng không tạo SUCCESS. Chỉ phần conversion timeout tương ứng khoảng 34 worker-minutes, chưa tính download/upload/queue.

Hướng ưu tiên: benchmark chính fixture lỗi với PDF splitting theo số trang/work budget, kể cả dưới 16 MiB; giữ 16 MiB là hard cap độc lập. Thử vài mức page span trên cùng input, đo tổng worker-seconds và SUCCESS/min cùng chất lượng/thứ tự output. Nếu phần lớn CPU nằm trong một converter path, profile rồi thử backend PDF khác bằng feature flag; kiểm tra license và chất lượng trước khi chọn.

Không mặc định hạ mọi format xuống 4 MiB. Nhiều part làm tăng startup/import, I/O và Redis events. Chia nhỏ có thể giảm latency nhờ parallelism nhưng không giảm tổng CPU; nếu chậm nằm trong một trang thì splitting theo trang cũng không giải quyết. Tách queue PDF đắt và queue tài liệu nhanh chỉ giúp cách ly/fairness; cần quota để tránh starvation, không tự tăng compute capacity. Chỉ tăng timeout có thể tăng số SUCCESS nhưng cũng giữ slot lâu hơn.

### 3. Bounded concurrency hoặc thêm replica, dựa trên tài nguyên thực

Nguồn: `internal/pipeline/consumer.go:87`, `internal/pipeline/consumer.go:108`, `cmd/extractor/main.go:105`, `deploy/docker-compose.yaml:3`.

`Count:1` và gọi `processOne` đồng bộ khiến một replica làm download → convert → upload → commit → ACK trước khi nhận job khác. Container Extractor có quota 2 CPU không có nghĩa hai conversion cùng chạy. Khi worker chờ network/disk, không có job khác tận dụng phần CPU rảnh.

Thử đơn giản nhất: thêm replica với consumer name riêng, giữ tài nguyên toàn stack trong budget. Nếu cần concurrency trong process, chạy số consumer loop hữu hạn với identity riêng hoặc dispatcher có giới hạn in-flight, giữ ACK sau commit, pending recovery và cancellation. Chỉ tăng COUNT mà vẫn xử lý tuần tự không giải quyết giới hạn này; batch quá lớn còn giữ job chờ trong PEL của một consumer.

Không bật concurrency=2 mặc định với 1 GiB RAM: peak Extractor cũ khoảng 716 MiB cho một job không chứng minh hai job an toàn. Có thể overlap download/upload có giới hạn trước khi tăng số subprocess, nhưng vẫn phải cap temp disk và bộ nhớ. Khi tăng Splitter concurrency/replica, hai duplicate request có thể cùng vượt precheck trước khi state được init và cùng ghi object path hiện mang tính deterministic; cần tránh duplicate work/ghi đè, chẳng hạn claim có owner và attempt prefix, không chỉ thêm goroutine.

Capacity gần đúng: min(C_s/E[T_s], C_e/E[tổng service time các part/document], giới hạn storage/network). Phải đo service time, không thay bằng E2E p50. Không cam kết tăng 2x khi gấp đôi slot.

### 4. CSV: bỏ flush từng dòng và tránh encode hai lần

Nguồn: `internal/services/splitter.go:184`, `internal/services/splitter.go:289`, `internal/services/splitter.go:314`.

Mỗi row được encode vào `bytes.Buffer` qua một `csv.Writer` mới chỉ để lấy size, rồi encode lại qua `currentWriter`. `currentWriter.Flush()` ngay mỗi row khiến buffering hầu như mất tác dụng, tạo write syscall theo từng dòng. Writer đo size cũng cấp phát buffer lặp lại.

Sửa nhỏ nhất: bỏ flush/error check mỗi row; kiểm tra Write error, giữ Flush/Error ở trước stat/seek/upload khi đóng part. Bước tiếp theo: tái sử dụng buffer + CSV writer, encode row một lần, dùng bytes đã encode để kiểm tra rollover rồi ghi qua buffered writer. Giữ nguyên quoting, multiline, header và exact size limit.

Đã chạy micro-experiment trên macOS arm64, Go 1.27.1, file temp, 300.000 row giống nhau, 15.900.000 output bytes, ba lần/mode. Mô phỏng current hot loop: 687–784ms; chỉ bỏ row flush: 227–293ms; median khoảng 2,6x nhanh hơn trong loop này. Cả hai còn khoảng 1.204 MiB cumulative allocations vì vẫn tạo size-writer mỗi row. Đây không phải peak RSS, không phải benchmark production `splitCSV` hoặc speedup toàn pipeline; chưa tính parsing, MinIO và Python. Thử encode một lần nhưng vẫn write trực tiếp từng row còn 642–709ms, xác nhận phải giữ buffering chứ không chỉ bỏ lần encode thứ hai. Experiment không lưu thêm test/application file.

### 5. OOXML: budget sai đơn vị có thể tạo hàng nghìn part

Nguồn: `internal/services/xlsxsplit.go:180`, `internal/services/xlsxsplit.go:278`, `internal/services/pptxsplit.go:157`, `internal/services/pptxsplit.go:186`, `internal/services/ooxmlzip.go:129`, `internal/services/ooxmlzip.go:180`.

Shared ZIP entries được `OpenRaw/CreateRaw` copy nguyên compressed bytes, nhưng XLSX/PPTX tính shared overhead bằng `UncompressedSize64`. Một sharedStrings.xml giải nén >16 MiB nhưng nén rất nhỏ có thể làm budget <=0. `groupChildrenBySize` ép budget về 1, dẫn đến gần một row/part. Mỗi part lại mang toàn bộ sharedStrings/styles/media và khởi động Python mới. Đây vừa lãng phí CPU/network vừa có thể kích hoạt lỗi #1. PPTX cũng cộng media weight theo từng slide, có thể tính lặp media dùng chung.

Sửa từng bước: shared raw entries dùng compressed size cộng ZIP overhead; nếu shared package tối thiểu thực sự vượt hard cap thì fail sớm. Nội dung XML viết lại phải ước tính compression riêng và kiểm tra kích thước archive thực sau finalize, retry giảm group có giới hạn nếu cần. Dùng thêm uncompressed/work budget độc lập để không tạo một part nén nhỏ nhưng cực đắt khi convert. Không chỉ đổi tất cả `UncompressedSize64` sang `CompressedSize64` rồi bỏ kiểm tra output.

Nếu profiling xác nhận sharedStrings/media lặp là chi phí chính, mới prune tài nguyên không dùng và remap indexes/relationships. Đây là thay đổi phức tạp hơn, cần kiểm chứng nội dung và tính hợp lệ OOXML.

### 6. Python converter tồn tại lâu hơn một part

Nguồn: `internal/services/markitdown.go:61`, `deploy/extractor.dockerfile:5`.

Mỗi part gọi `exec.CommandContext(..., "markitdown", ...)` mới. Startup, import và converter initialization được trả lại mỗi lần. Tác động lớn nhất với tài liệu nhỏ hoặc fanout nhiều part; chưa đo riêng startup nên không quy toàn bộ latency 1–2s cho import.

Trước tiên đo startup/import và conversion riêng trong image hiện tại. Sau đó thử pool Python process giữ converter instance, một job/process tại một thời điểm, protocol truyền path/result rõ ràng; timeout phải kill và respawn process, recycle theo số job/RSS để chặn memory retention. Giữ cách cô lập bằng subprocess hiện tại làm baseline. Pool bền vững phức tạp hơn giảm flush, vì runtime hiện được giải phóng sau mỗi job.

Đã đọc metadata trong container đang chạy: markitdown 0.1.9, pdfminer.six 20260107. Dockerfile không pin `markitdown[all]`, vì vậy cần ghi version/image digest để so A/B; không giả định các image build khác nhau có cùng converter.

### 7. Overlap split với upload bằng queue hữu hạn

Nguồn: `internal/services/splitter.go:169`, `internal/services/splitter.go:264`, `internal/services/docxsplit.go:120`, `internal/services/xlsxsplit.go:320`, `internal/services/htmlsplit.go:365`, `internal/pipeline/splitter.go:94`.

Hiện mỗi part được dựng và upload xong trước khi xử lý part kế tiếp. Network wait của upload giữ cả Splitter. Có thể producer tạo temp parts, queue bounded, 2–4 upload workers tùy đo đạc, kết quả gắn index và trả URLs đúng thứ tự; hủy toàn bộ khi một task lỗi, dọn temp khi mọi reader đã xong.

Giữ publish sau khi tất cả upload thành công trong bước tối ưu này. Publish từng part sớm thay đổi contract atomic init+fanout và cách xử lý split failure; đó là thiết kế khác, không phải quick fix. HTML/EML hiện truyền reader trỏ vào buffer được tái sử dụng, vì vậy không đưa reader đó vào goroutine bất đồng bộ nếu chưa chuyển ownership/copy/spool. Không spawn một goroutine cho mọi part.

Mức giảm splitter service time phụ thuộc tỷ lệ upload. Nếu upload chỉ 5% service time thì tối ưu riêng upload không tạo thay đổi lớn toàn hệ thống.

### 8. PDF oversized retry đang làm lại cả file

Nguồn: `internal/services/splitter.go:95`, `internal/services/splitter.go:122`, `internal/services/splitter.go:130`.

Code đọc page count, ước lượng span từ average bytes, split toàn file, kiểm tra output; chỉ một part quá lớn cũng xóa toàn output, giảm span một nửa và split lại toàn file. Tối đa khoảng O(log span) lần split toàn tài liệu; các page đã hợp lệ vẫn bị làm lại, và average gốc có thể không phản ánh shared objects/size sau repack.

Hướng sửa: giữ các range đã fit, chỉ bisect range oversized, preserve page order và hard cap. Khi thư viện cho phép, reuse parsed PDF context thay vì load/validate lại ở từng lần gọi. Đo trên PDF có page sizes lệch, không chỉ synthetic padding. Không tăng parallel PDF parsing trên Splitter 512 MiB trước khi đo heap/cgroup peak.

### 9. Giảm giải nén lặp và copy trong DOCX/XLSX

Nguồn: `internal/services/docxsplit.go:51`, `internal/services/docxsplit.go:67`, `internal/services/docxsplit.go:107`, `internal/services/xlsxsplit.go:231`, `internal/services/xlsxsplit.go:239`, `internal/services/xlsxsplit.go:286`, `internal/services/ooxmlzip.go:207`.

Entry XML được mở lại để scan children, đọc suffix (discard toàn bộ bytes phía trước), đọc sectPr/header và stream groups. Đây là nhiều pass O(S) qua cùng compressed entry; không phải O(S²) theo số part vì code đã có `streamSpanGroups`. Mỗi group còn `ReadAll`, copy sang doc/rows/sheetXML; XLSX có thêm regex renumber và collect matches. Part lớn tạo nhiều allocation/copy trước khi ZIP encode.

Thử gộp offset scan và lấy suffix; hoặc giải nén XML một lần ra temp file rồi dùng section reader, trade CPU lấy disk I/O/temp space. DOCX có thể viết prefix/body/sectPr/suffix trực tiếp vào ZIP entry thay vì ghép `[]byte`. XLSX cần xử lý renumber/dimension, nên tối ưu độc lập theo profile thay vì ép một refactor chung. Các row/paragraph đơn lẻ oversized phải được reject/cap trước ReadAll để tránh buffer vượt budget.

### 10. Deadline không chặn đầy đủ local CPU/disk; input download không giới hạn

Nguồn: `internal/usecase/splitfile.go:68`, `internal/usecase/splitfile.go:75`, `internal/usecase/splitfile.go:118`, `internal/services/splitter.go:122`, `internal/services/ooxmlzip.go:53`, `internal/services/htmlsplit.go:126`.

Worker có job context nhưng vòng parse/split local không kiểm tra ctx. PDF API không nhận context ở call site; cancel không dừng một `pdfapi.SplitFile` đang chạy. XML/CSV/HTML cũng có thể tiếp tục CPU/disk sau deadline đến khi gặp network call. Download Splitter dùng io.Copy toàn body không có tổng bytes cap; HEAD dùng cả job deadline thay vì budget riêng, server không hỗ trợ HEAD/chậm có thể giữ slot đáng kể.

Sửa: deadline ngắn riêng cho metadata/HEAD và fallback; cap total input bytes bằng policy rõ ràng, kiểm tra Content-Length và actual bytes; giới hạn expanded XML bytes/children; check ctx định kỳ trong local loops, giữa PDF attempts và trước tạo/upload part. Check ctx giữa hai PDF calls chỉ hỗ trợ cooperative cancellation; nếu cần hard deadline trong một PDF call, cô lập operation vào child process hoặc dùng API/library có cancellation. Không bỏ goroutine chạy CPU tiếp rồi coi là đã cancel.

### 11. HTTP mode: chặn tài nguyên bị giữ ngoài lifecycle request

Nguồn: `internal/handler/markitdown.go:29`, `internal/usecase/markitdown.go:116`, `internal/services/markitdown.go:16`, `internal/services/subprocess.go:17`.

HTTP conversion dùng `http.Get` không timeout, không nhận request context, `exec.Command` không deadline và `CombinedOutput` không cap; Gin có thể nhận nhiều request đồng thời, mỗi request tạo process/buffer riêng. Client disconnect vẫn không dừng download/conversion. Khi nhiều input chậm hoặc nhiều conversion đến cùng lúc, tài nguyên tích lũy và throughput hữu ích giảm mạnh/OOM.

Sửa: dùng đường conversion có context/timeout hiện có, truyền `c.Request.Context()`, thêm semaphore/bounded queue cho conversion và response overload rõ ràng. Bound stderr/output theo HTTP contract; response JSON markdown vẫn cần RAM tỷ lệ output nếu giữ contract hiện tại, không tuyên bố streaming toàn bộ chỉ vì subprocess stdout ra file. Áp dụng cancel/timeout/admission tương tự cho `/subprocess/run` nếu endpoint được dùng.

Finding này không giải thích throughput worker benchmark, vì worker dùng path khác đã có process-group cancellation và stderr cap.

### 12. Retention Redis ảnh hưởng throughput dài hạn

Nguồn: `internal/pipeline/consumer.go:136`, `internal/pipeline/lua/init_and_publish.lua:83`, `internal/pipeline/lua/complete_part.lua:115`, `README.md` mục Known POC limits.

`XACK` chỉ bỏ pending, không xóa stream entry. TTL 24h chỉ áp dụng state keys; input/splitted/output streams không có retention. Với traffic liên tục, memory, persistence và rewrite/recovery cost tăng theo lịch sử dù queue đã drain. Đây là giới hạn POC được ghi nhận, không khẳng định Redis đang là bottleneck ở run-002.

Chọn retention theo replay/downstream SLA và tiến độ mọi consumer group. Trim nền theo watermark an toàn cho undelivered + pending entries; output chỉ trim khi downstream đã tiêu thụ và hết retention đã thống nhất. Không dùng MAXLEN nhỏ tùy ý hoặc XDEL ngay ACK khi còn consumer khác cần entry. Theo dõi used_memory, stream length, oldest pending và AOF/rewrite latency.

## Những việc chưa đáng ưu tiên

- Giảm fail-fast polling 500ms: mỗi active Extractor khoảng 2 HGET/s; ba replica chỉ khoảng 6/s cộng prechecks. Với total=1 không có sibling nên có thể bỏ watcher, và HGET+SISMEMBER có thể pipeline, nhưng lợi ích nhỏ ở scale hiện tại. Chỉ batch/coalesce khi Redis measurements chứng minh cần.
- Tăng Redis connection pool/đổi broker: chưa có bằng chứng pool exhaustion hay Redis throughput ceiling. Lua đang giữ invariant atomic terminal/fanout; không bỏ tính atomic để tiết kiệm vài round trip.
- Cache toàn bộ conversion theo URL: URL có thể trỏ nội dung thay đổi; cache cần content version/hash và converter/options version. Chưa có số liệu repeat-content của production; dataset load test lặp không phải lý do để cache làm đẹp benchmark.
- Đổi temp files thành io.Pipe mọi nơi: đổi disk I/O lấy lifecycle/backpressure phức tạp; MinIO size, converter input và size validation hiện dựa trên file. Đo disk bottleneck trước.
- `EnsureBucket` mỗi split lớn thêm một request metadata dù startup worker đã ensure; có thể bỏ trong worker path/ensure-once có điều kiện recovery. Dễ sửa nhưng tác động thấp so với PDF, CSV và part count.

## Thứ tự thực hiện và cách xác nhận

1. Sửa #1; nếu chạy HTTP thì sửa #11 cùng đợt. Xác nhận input có 0/>1000 parts không giữ consumer vô hạn, request sau vẫn hoàn thành, terminal event không bị nhân đôi. Chặn part count trước khi upload hàng loạt.
2. Sửa #4 và đo CSV splitter riêng lẫn E2E; so output byte count/quoting/header/part boundaries. Micro-experiment ở trên chỉ là chỉ báo cho hot loop.
3. Đo cùng PDF 8 MiB trước/sau page splitting; profile conversion. Chọn phương án theo SUCCESS/min, worker CPU-seconds/document, timeout rate và output fidelity, không theo bytes hay latency đơn độc.
4. Thử số slots/replicas tăng từng bước trên cùng tổng CPU/RAM; kiểm tra cgroup peak, OOM events, CPU throttling, temp disk và queue slope. Không suy ra RAM an toàn từ Docker stats trung bình hoặc cộng/gấp đôi peak một cách cơ học.
5. Tối ưu #5, #8, #9 nếu splitter service time/CPU hoặc part count nổi lên; #6 nếu startup đáng kể; #7 nếu upload time đáng kể. Làm retention trước vận hành dài ngày.

Đo duration riêng cho HEAD, download, split, upload, conversion, Redis commit; có request/offset/attempt và enqueue/start/finish timestamps. Với document nhiều part, tổng worker-seconds đo compute cost, critical path đo E2E. Metrics chính: unique SUCCESS/min và successful original MiB/min ở cửa sổ wall-clock cố định, kèm accepted/FAILED/unresolved, queue lag/pending/oldest age. Failure nhanh không được tính thành throughput improvement.

A/B dùng cùng dataset hợp lệ, workload mix, image/version và resource budget. Giữ cả PDF khó, OOXML compressed thực tế, sharedStrings lớn, CSV nhiều short rows và HTML/EML/PPTX hiện chưa có trong capacity mix cũ. Sau warm-up, hold đủ dài để phân biệt backlog tăng với jitter; đưa cả phút không có SUCCESS vào denominator. Đếm toàn bộ terminal qua drain; dữ liệu lịch sử chỉ giúp chọn điểm thử, không chứng minh hệ số tăng.

## Kiểm chứng đã thực hiện trong review

- Môi trường thực: Darwin/macOS arm64, Go 1.27.1.
- `go test ./internal/... ./cmd/...`: exit 0; tất cả package báo `[no test files]`, vì vậy đây chỉ xác nhận compile/package checks, không chứng minh hành vi.
- `go vet ./internal/... ./cmd/...`: exit 0.
- CSV micro-experiment: ba sample/mode, kết quả và giới hạn tại #4; không tạo unit test mới.
- Đọc JSON reconciliation và log k6 run-002; không phát traffic, restart hay chỉnh cấu hình stack hiện có.
- Không chạy profiler hoặc load test mới; speedup production, CPU saturation và lợi ích warm pool/concurrency vẫn cần A/B.
