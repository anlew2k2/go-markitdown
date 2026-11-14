# Extraction & aggregation flow

Các Extractor nhận từng part event từ `stream:doc-extractor:splitted` qua group `cg:doc-extractor`. Không cần chờ các phần trước hoàn tất.

```mermaid
sequenceDiagram
    participant R as Redis
    participant E as Extractor
    participant F as Part source
    participant CLI as markitdown CLI
    participant M as MinIO
    participant C as Client
    R->>E: Part event: request_id, offset, total, URL
    E->>R: Check status và completed offset
    alt Terminal request hoặc completed offset
        E->>R: ACK, bỏ qua
    else Cần xử lý
        E->>F: Download part vào temporary file, tối đa 16 MiB
        E->>CLI: Convert file sang Markdown
        CLI-->>E: Write Markdown vào temporary file
        E->>M: Upload Markdown
        M-->>E: Markdown URL
        E->>R: Lua record completed offset và URL
        alt Completed count chưa đạt total
            R-->>E: RECORDED
        else Last part completion
            R->>R: Set COMPLETED và publish SUCCESS
            R-->>C: SUCCESS event với ordered markdown_urls
        end
        E->>R: ACK part event
    end
```

Client đọc output stream; mũi tên trả kết quả thể hiện việc client đọc event, không phải HTTP callback.

## Aggregation không cần coordinator riêng

Redis dùng completed-offset set để deduplicate completion, tránh double counting. Khi số offset bằng `total`, Lua lấy URL theo thứ tự `0..total-1`, phát `SUCCESS` và lưu ID event cuối.

Ví dụ các phần hoàn thành theo thứ tự **2 → 0 → 1**, kết quả vẫn là **URL 0 → URL 1 → URL 2**. Danh sách này dành cho downstream đọc hoặc ghép; pipeline chưa merge Markdown content.

Upload diễn ra trước commit Redis. Nếu Redis gián đoạn sau khi upload, worker retry commit để không phải chuyển đổi lại ngay trong lần xử lý đó. Nếu tiến trình chết, lần chạy lại vẫn có thể tạo upload mới.

Source references: [ExtractorWorker](../internal/pipeline/extractor.go), [complete_part.lua](../internal/pipeline/lua/complete_part.lua).
