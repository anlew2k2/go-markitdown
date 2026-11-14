# Review Load Test Plan — Event-Driven Document Pipeline

Ngày: 2026-11-14. Trạng thái: **cần sửa trước benchmark**. Review tĩnh; chưa chạy k6, Docker hay xác minh object trên MinIO. Không sửa kế hoạch gốc/application code.

Nguồn nội bộ: [requirement](requirement-event-sourcing.md), [implementation plan](implementation-plan-event-sourcing.md), [load test plan](load-test-plan-event-sourcing.md), [Compose](../deploy/docker-compose.yaml), hai entrypoint `cmd/{splitter,extractor}/main.go`, `go.mod`, `tmp/generate_dataset.py`.

## 1. Kết luận và phát hiện ưu tiên

Ưu tiên RAM/OOM và throughput là phù hợp AC-6. Tuy nhiên script và cách quan sát hiện tại chưa đủ để công bố max sustainable throughput.

| Mức | Bằng chứng trong load plan | Rủi ro / chỉnh sửa |
|---|---|---|
| P1 | Dòng 120–142: function đồng bộ, không await Redis | Client hiện tại trả Promise; so sánh Promise với COMPLETED/FAILED không xác nhận terminal. Dùng async/await, bắt lỗi publish/read và pin phiên bản k6/extension. Cú pháp sendCommand variadic đang dùng là hợp lệ theo tài liệu hiện tại. |
| P1 | Dòng 121, 126–127: identity không phải UUID | Implementation plan yêu cầu validate UUID: dùng UUID hợp lệ cho request/conversation/message; request duy nhất giữa các run. |
| P1 | Dòng 99–105: mỗi target xuất hiện một lần | Đây là ramp giữa các target, không phải plateau; đoạn 5→0 vẫn nạp trung bình khoảng 2.5 docs/min, tức khoảng 7–8 docs trong 3 phút. Thêm ramp ngắn và hold cùng target; dừng producer hoàn toàn khi drain. |
| P1 | Dòng 89, 150–156: timeout 180s thành failed và ngừng theo dõi | Timeout bộ quan sát không hủy task; completion muộn bị bỏ sót, throughput thấp giả. Tách observation timeout, terminal FAILED, publish error; collector tiếp tục theo dõi đến deadline drain. |
| P1 | Dòng 173–180: snapshots và OOMKilled | Có thể mất peak và không phát hiện Python child bị OOM kill trong khi Go còn sống. Thu kernel counters/peak và Docker event liên tục. Không biến inspect lỗi thành false. |
| P2 | Dòng 122: __ITER modulo số file | __ITER cục bộ mỗi VU: VU mới đều bắt đầu PDF, gây lệch mix. Dùng scenario iterationInTest/global sequence với seed cố định; lưu mapping request→format. |
| P2 | Dòng 64–68: suy ra nguyên nhân từ CPU/RAM | CPU thấp chưa chứng minh MinIO nghẽn; Redis RAM tăng chưa chứng minh leak. Đổi thành giả thuyết và thêm bằng chứng phân biệt. |
| P2 | Dòng 110 vs 197: >95% và 100% | Thống nhất tiêu chí. Error rate vẫn là điều kiện hợp lệ của capacity: không tăng throughput bằng cách bỏ task/lỗi nhanh. |

Điều kiện chạy: entrypoints hiện tại chỉ chạy Gin HTTP; chưa có pipeline package/go-redis dependency. Compose hiện có giới hạn 1 CPU/512 MiB và 2 CPU/1 GiB nhưng chưa có worker env/readiness/AOF theo implementation plan. Chỉ chạy benchmark sau khi triển khai và smoke flow input→SUCCESS/FAILED thành công. Chốt 1 Splitter + số Extractor cố định; 2 CPU không đồng nghĩa hai part chạy song song vì plan quy định mỗi process xử lý một message.

## 2. Polling và thiết kế VU

Với V VU đang chờ, polling 500 ms tạo khoảng 2V HGET/s: 10–30 VU tương ứng 20–60/s, chưa tính polling fail-fast 500 ms của mỗi Extractor đang hoạt động. Đây chưa phải bằng chứng Redis quá tải; latency mạng, Lua fanout và tài nguyên VM có thể làm kết quả khác đi. Đo Redis command rate, CPU, command latency/SLOWLOG và CPU load generator; so sánh cùng tải với polling 0.5s và 2s để định lượng nhiễu. Không cần xây Pub/Sub chỉ để loại vài chục reads/s.

Nếu giữ VU chờ, ước lượng VU ≈ arrival_rate × thời gian iteration theo cùng đơn vị, thêm headroom từ baseline. 5 docs/min × 60s ≈ 5 VU; chờ 180s cần khoảng 15 VU. Không mặc định 10 preallocated đủ. Theo dõi dropped_iterations; nếu thiếu VU thì target arrival không được phát đủ, run không chứng minh capacity. Polling thêm sai số quan sát khoảng 0–500ms cộng RTT/scheduling. Dùng deadline theo thời gian thực thay vì đếm 360 vòng, vì Redis RTT cũng tiêu tốn thời gian.

Phương án phù hợp mục tiêu capacity: k6 chỉ await XADD rồi kết thúc iteration; một collector riêng dùng XREAD BLOCK có timeout hữu hạn, COUNT theo batch trên output stream. Collector chạy từ trước producer đến hết drain, lọc request thuộc run, dedup request_id, giữ cursor cuối. Chụp last stream ID trước run (hoặc đọc 0-0 và lọc); không dùng $ lặp lại vì có thể bỏ lỡ event giữa các lần đọc. Nếu dùng k6 collector, cần scenario/lifecycle riêng và kiểm chứng extension hỗ trợ response/block timeout; VU không chia sẻ JS mutable state để correlation, nên lưu manifest và join offline là đơn giản hơn.

Không để mỗi VU đọc cùng output stream: mỗi client XREAD nhận event của mọi request, tăng fanout. Không dùng chung consumer group giữa các VU rồi mong mỗi VU nhận đúng request của mình: group phân phối event cho consumer bất kỳ. Keyspace notification mặc định tắt, tạo thêm CPU, mất event khi disconnect và chỉ báo HSET chứ không trả status mới; vẫn cần HGET. Output stream đã là terminal contract bền vững hơn cho bài test.

## 3. Stage và thời gian quan sát

3 phút phù hợp tìm vùng capacity sơ bộ, không đủ chứng minh steady state hay không leak: ở 1 doc/min chỉ khoảng ba tài liệu, và với thời gian giả định 30–60s chỉ có vài chu kỳ. 30–60s chưa phải baseline đo được.

Đề xuất ban đầu, điều chỉnh theo kết quả thực tế:

1. Smoke từng format; warm-up 3–5 phút, tách khỏi số đo chính.
2. Mỗi mức: ramp 30–60s, hold 10–15 phút; loại transition khỏi cửa sổ steady. Giữ các target 1,2,3,4,5 để thăm dò, không gọi 5 là maximum nếu chưa có dấu bão hòa.
3. Xác nhận mức gần ngưỡng bằng run riêng từ backlog đã drain; ít nhất ba cửa sổ liên tiếp 2–3 phút có completion rate ổn định, backlog không có xu hướng tăng. Với sample quá ít, kéo dài hold; không kết luận p99 từ vài chục docs.
4. Soak 30–60 phút ở mức bền vững dự kiến để xem baseline RAM sau nhiều chu kỳ; leak chậm vẫn có thể chưa lộ. Phân biệt heap/cache high watermark với baseline tăng qua các chu kỳ tải/drain tương đương.
5. Dừng producer ngay; collector/monitor tiếp tục đến khi mọi request đã terminal và pending/lag về baseline, hoặc hết deadline đã công bố. Không mặc định ba phút đủ. Nếu backlog tăng, kết thúc bước overload theo giới hạn backlog/thời gian đã định trước rồi drain; trên capacity không tồn tại steady state ổn định.

## 4. Tách queue và processing, đếm throughput

Đề xuất instrumentation bổ sung, **chưa có trong contract hiện tại**: structured logs theo run/request/offset/attempt và timestamp millisecond. Không cần thêm metrics platform trong POC.

| Mốc | Nơi đo |
|---|---|
| t0 | Input được Redis chấp nhận; lưu XADD stream ID cùng thời điểm client trước/sau publish |
| t1 | Splitter bắt đầu xử lý input |
| t2,i | Part i được publish; lấy stream ID/timestamp của fanout |
| t3,i | Extractor bắt đầu xử lý part i |
| t4,i | Conversion bắt đầu/kết thúc, upload kết thúc và business commit part |
| t5 | Terminal SUCCESS/FAILED được commit vào output stream |

- queue_wait_input = t1 − t0.
- queue_wait_part_i = t3,i − t2,i.
- splitter_processing = kết thúc split/upload/fanout − t1.
- part_processing_i = part commit − t3,i; phân rã download/convert/upload/commit để chẩn đoán.
- E2E = t5 − t0; client-observed E2E đo từ trước XADD đến quan sát terminal, bao gồm network/polling delay.

Đồng bộ clock worker/collector/Redis; dùng monotonic clock cho duration nội bộ. Stream IDs cung cấp timestamp phía Redis cho enqueue/terminal nhưng không cung cấp worker start. Không dùng timestamp completed_at độ phân giải giây để phân tích subsecond.

Với parts song song, tổng processing của mọi part là worker-seconds, không phải wall-clock processing của document. Báo cáo per-stage/per-part queue và service time riêng; muốn phân rã E2E document phải dùng critical path của fanout/join, không cộng tất cả queue/service times. Nếu thiếu instrumentation thì công bố chỉ đo được E2E và backlog, không gọi đó là actual_processing_time.

Throughput đo trực tiếp từ số **request SUCCESS duy nhất commit trong cửa sổ wall-clock / độ dài cửa sổ**, không lấy nghịch đảo latency và không lấy tổng counter chia cả warm-up/drain. Ghi cả accepted input/s, SUCCESS/s, FAILED/s và outstanding. MB/min dùng tổng bytes gốc của docs SUCCESS, định nghĩa MB hay MiB rõ ràng; không đếm mỗi part là một document. Báo cáo theo format bên cạnh workload mix cố định. Completion trong cửa sổ có thể thuộc request stage trước: phân biệt completion-time window và arrival cohort, xác nhận capacity bằng hold độc lập.

Theo dõi XINFO GROUPS lag (chưa được deliver) và XPENDING (delivered chưa ACK), cùng oldest outstanding age. Pending có thể đang chạy hoặc bị kẹt; không coi mọi pending là đang xử lý. XLEN không phải backlog: XACK không xóa stream entry. Implementation plan cố ý không auto-trim global streams và TTL state là 24h, nên Redis memory tăng trong test là retention có thể dự kiến. Đối soát cuối: accepted = SUCCESS + FAILED + unresolved, mỗi request chỉ một terminal event; timeout quan sát là thuộc tính riêng, không cộng thêm như terminal failure.

## 5. RAM, CPU và OOM

Script gọi bốn docker stats tuần tự rồi inspect rồi sleep 2: chu kỳ thực dài hơn 2s và CPU/RAM không cùng snapshot. Dùng một stats stream cho tất cả worker container IDs lấy từ Compose; ghi timestamp chính xác. Cần theo dõi mọi replica và Redis/MinIO, không hardcode deploy-extractor-1.

Thu ba lớp bằng chứng:

1. Docker events liên tục, lưu oom/die/restart/start từ trước run; inspect bổ sung exit code/restart count. Exit 137 chỉ là SIGKILL, chưa đủ chứng minh OOM. Không dùng trap để bắt OOM trong process bị kill: SIGKILL không bắt được. Monitoring lỗi/mất mẫu phải là UNKNOWN, không phải OK.
2. Trong Linux cgroup v2 của container: memory.current, memory.peak nếu kernel hỗ trợ, memory.events (delta oom/oom_kill/max), memory.swap.current và cpu.stat (throttling). Đọc baseline/cuối run và lưu trước khi container bị recreate vì counters/peak thuộc lifetime cgroup. Nếu v1 dùng peak/counter tương ứng theo kernel. Tính cả Go, Python child và memory tính vào cgroup; child OOM có thể không làm PID 1 chết nên chỉ State.OOMKilled là thiếu.
3. Nếu sẵn cAdvisor: usage/working_set/RSS, max_usage nếu được export, container_oom_events_total, CPU usage/CFS throttled periods/seconds, filesystem/network metrics. Kiểm chứng metrics thực sự tồn tại với version/runtime, chu kỳ housekeeping và scrape; scrape 1s không tự khôi phục gauge peak bị mất. Kernel peak/counters + event collector mới bổ sung bằng chứng micro-spike.

Docker stats trên Linux trừ inactive file cache; số hiển thị không đồng nhất total cgroup charge dùng xét memory limit. So sánh bytes với actual memory.max: Compose 512m/1g tương ứng budget MiB/GiB. Không coi peak sát limit là an toàn dài hạn; có thể đặt cảnh báo headroom 10–20% như mục tiêu kỹ thuật đề xuất, chưa phải requirement mới.

Môi trường hiện tại là macOS: Linux cgroup nằm trong Docker VM, không nằm trực tiếp ở host /sys/fs/cgroup. Ghi VM CPU/RAM/swap, CPU architecture, Docker/kernel version, image digest và extractor replica count. Redis/MinIO hiện không có quota trong Compose; tranh chấp VM CPU/disk/cache có thể làm sai chẩn đoán. Giữ cấu hình giống nhau giữa các run, tránh load generator/monitor tranh chấp tài nguyên đáng kể.

CPU Extractor khoảng 100% vẫn có thể nghẽn một core dù quota 200%; kiểm tra CPU theo process/phase và worker concurrency. CPU thấp + latency cao cũng có thể do single worker, queue, Redis chậm hoặc task kẹt. Chỉ kết luận MinIO sau khi đo download/upload duration, I/O/latency và disk/network. Phân biệt CPU quota throttling với host contention.

## 6. Bộ scenario và điều kiện công bố kết quả

| ID | Scenario / priority | Assertion và bằng chứng |
|---|---|---|
| LT-01 | Smoke contract — Critical | UUID hợp lệ, awaited XADD trả ID, input tạo đúng N parts <=16 MiB và đúng một SUCCESS/FAILED; smoke SUCCESS kiểm tra URLs tồn tại/đúng thứ tự. Fixture hợp lệ, pipeline worker đã sẵn sàng. |
| LT-02 | Step-up capacity — Critical | Arrival thực khớp target, dropped_iterations=0, collector không mất terminal; mỗi plateau ghi SUCCESS/min, queue slope, RAM peak, OOM/cgroup delta, CPU/throttling. |
| LT-03 | Hold/soak gần ngưỡng — Critical | Không OOM, không restart bất thường, footprint baseline không tăng không kiểm soát, backlog không tăng dai dẳng; công bố số docs và thời gian quan sát. |
| LT-04 | Overload rồi drain — High | Nêu ngưỡng dừng; stop input thật, late completions vẫn được đếm; mọi accepted request được đối soát hoặc ghi unresolved; không coi observer timeout là worker FAILED. |
| LT-05 | Kiểm tra observer overhead — High | Cùng mix/rate/resource, so polling 0.5s với 2s hoặc collector; ghi ảnh hưởng throughput/Redis latency thay vì giả định polling là bottleneck. |

Preconditions chung: baseline backlog/PEL đã hiểu rõ, request mới theo run, fixture manifest có bytes/checksum/format/part count; URLs truy cập được từ worker. Không sửa/xóa object dataset. Cleanup chỉ object/state thuộc run trên môi trường benchmark được phép; không FLUSHALL hoặc trim stream dùng chung. Monitor tiếp tục qua drain; lưu manifest, raw terminal events, structured worker logs và resource samples để tái phân tích.

Dataset generator có PDF padding trong comment và OOXML ZIP_STORED; 50 MiB vật lý không đồng nghĩa 50 MiB nội dung trích xuất hoặc workload tài liệu thực. Xác nhận format validity bằng smoke, ghi pages/rows/uncompressed bytes/part count, giữ mix cố định; benchmark từng format trước khi tổng hợp. Không suy rộng PDF synthetic thành mọi PDF/OCR.

Chỉ công bố **mức throughput bền vững cao nhất đã kiểm chứng trong môi trường/mix này** khi: 0 OOM, không restart bất thường, workload hợp lệ, không dropped arrivals, không terminal business failure không giải thích được, không backlog tăng dai dẳng, bộ quan sát đầy đủ. 100% thành công trong mẫu hữu hạn không bảo đảm xác suất thành công tuyệt đối. Nếu mức cao nhất vẫn ổn định, kết quả là lower bound và cần tăng tải để bracket capacity; không gọi 5 docs/min là max theo cấu hình sẵn.

## 7. Nguồn kỹ thuật đã đối chiếu

- [k6 Redis Client: Promise API](https://grafana.com/docs/k6/latest/javascript-api/k6-x-redis/client/) và [sendCommand: variadic arguments](https://grafana.com/docs/k6/latest/javascript-api/k6-x-redis/client/client-sendcommand/).
- [k6 ramping-arrival-rate](https://grafana.com/docs/k6/latest/using-k6/scenarios/executors/ramping-arrival-rate/) và [gracefulStop mặc định 30s](https://grafana.com/docs/k6/latest/using-k6/scenarios/concepts/graceful-stop/). Cấu hình rõ grace/deadline theo thời gian observer; grace không phải bảo đảm backend drain.
- [Redis XREAD](https://redis.io/docs/latest/commands/xread/) và [keyspace notifications](https://redis.io/docs/latest/develop/pubsub/keyspace-notifications/).
- [Linux cgroup v2](https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html), [Docker events](https://docs.docker.com/reference/cli/docker/system/events/), [Docker stats và cache](https://docs.docker.com/reference/cli/docker/container/stats/).
- [cAdvisor metrics](https://github.com/google/cadvisor/blob/master/docs/storage/prometheus.md).
