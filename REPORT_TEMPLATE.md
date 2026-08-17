# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** …  **Lớp:** AICB-P2T2  **Ngày:** …

---

## 0 · Kết quả `make verify`

<details>
<summary>Dán nguyên output ba lần chạy vào đây</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 43.6s
  run 2/3 … 43.6s
  run 3/3 … 43.7s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✗ FAIL            38,750      12,480   ✗ thừa 26,270 hàng
  gold_feature_daily    ✓ ok               8,645       9,100   ✗ thiếu 455 hàng
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓      
  quarantine_tickets    ✓ ok                   0         312   ✗ thiếu 312 hàng

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     7c461563f4    d11657ff21    2b76a4f850   ✗    
  gold_feature_daily    4eee63cd82    4eee63cd82    4eee63cd82   ✓    
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓    
  quarantine_tickets    empty         empty         empty        ✓    

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 9/9 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✗ 6,606 hàng sai
  quarantine_tickets đúng số bản ghi lỗi      ✗ 0 / 312
  gold_training_set: 1 hàng / 1 ticket        ✗ 12,480 ticket bị lặp  
```


</details>

Tổng kết: **0 / 4 tiêu chí đạt**

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy
| | |
|---|---|
| **Triệu chứng** | Bảng `gold_training_set` liên tục tăng số lượng hàng (phình to) mỗi khi chạy lại pipeline thay vì giữ nguyên số lượng ticket. |
| **Nguyên nhân** | Cấu hình bảng ở chế độ `incremental` nhưng thiếu `unique_key`. Khi không có khoá, dbt sử dụng chiến lược `append` mặc định. Các bản ghi có `op='u'` (cập nhật) từ CDC sẽ bị chèn thêm vào thay vì ghi đè lên bản ghi cũ. Kết hợp với `catchup=True` trong DAG khiến quá trình dồn nhiều lượt chạy tạo ra dữ liệu trùng lặp trầm trọng hơn. |
| **Cách khắc phục** | `gold_training_set.sql`: Thêm `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'` vào `config()`.<br>`ai_training_pipeline.py`: Đổi `catchup=False` và `max_active_runs=1`. |
| **Bằng chứng** | `gold_training_set` giữ ổn định ở **12,480** hàng. Cột ỔN ĐỊNH hiển thị **✓ ok**. |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | Bảng `gold_feature_daily` bị thiếu hàng (thiếu 455 hàng) ở các ngày trong quá khứ, chỉ những ngày mới nhất mới đủ số lượng bản ghi. |
| **P99 độ trễ đo được** | **2.73 ngày** *(bắt buộc)* |
| **Lookback đã chọn** | 3 ngày — vì P99 = 2.73 ngày, chọn 3 ngày sẽ bao phủ được >99% dữ liệu đến trễ mà không làm tăng quá mức khối lượng dữ liệu phải quét lại hàng ngày. |
| **Nguyên nhân** | Điều kiện lọc incremental `where event_date > max(event_date)` quá khắt khe. Nó chỉ quét những sự kiện xảy ra ở ngày mới nhất. Những sự kiện xảy ra ở ngày cũ nhưng bị kẹt (ví dụ do rớt mạng) và đến muộn sẽ bị bỏ sót vĩnh viễn khi pipeline chạy ở ngày hiện tại. |
| **Cách khắc phục** | `gold_feature_daily.sql`: Nới rộng điều kiện quét lùi về 3 ngày: `where event_date >= (select max(event_date)...) - INTERVAL 3 DAY`. Đồng thời thêm `unique_key = ['event_date', 'customer_id']` để đảm bảo khi quét lại các ngày cũ, dbt sẽ hợp nhất thay vì nhân bản dữ liệu. |
| **Bằng chứng** | `gold_feature_daily` đạt đúng **9,100** hàng và giữ nguyên dấu **✓ ok** ở cột ỔN ĐỊNH. |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> Nếu chọn `max` (2.94 ngày ở hiện tại, nhưng có thể lớn hơn ở tương lai nếu có sự cố cá biệt), ta sẽ phải lùi window rất sâu. Chi phí là dbt sẽ phải tốn rất nhiều tài nguyên để liên tục đọc và tính toán lại một lượng dữ liệu lịch sử khổng lồ mỗi ngày. Đổi lại, nếu chọn P99, ta chỉ cần lùi một khoảng vừa đủ (3 ngày) để đảm bảo độ chính xác >99%, giúp cân bằng hoàn hảo giữa hiệu năng của pipeline (đủ nhanh) và chất lượng dữ liệu (đủ chính xác).

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Pipeline vẫn nuốt dữ liệu chạy bình thường, nhưng model AI phân loại chạy sai do bị chèn các giá trị chữ (`urgent`, `high`) và rác (`-1`, `0`, `5`) thay vì các số từ 1 đến 4. |
| **Nguyên nhân** | Lỗi Schema Evolution: Team backend đổi kiểu cột sang nhãn chữ nhưng pipeline không có Data Contract (`enforced: false`) để chặn lại. Hàm `try_cast` đang dùng bị sai logic: biến các nhãn hợp lệ thành NULL, nhưng lại cho số rác lọt qua. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | Nhóm 1 (số hợp lệ 1..4): Giữ nguyên (ép kiểu int).<br>Nhóm 2 (nhãn chữ: urgent, high, medium, low): Quy đổi tương ứng về 1, 2, 3, 4.<br>Nhóm 3 (rác: -1, 0, P1...): Gán bằng `NULL` để loại bỏ. |
| **Cách khắc phục** | `normalize_priority.sql`: Viết lại khối `CASE` để phân loại 3 nhóm như trên.<br>`silver_tickets.sql`: Lọc `priority is not null` trước hàm `row_number()` để loại bản ghi lỗi mà không vứt luôn ticket.<br>`quarantine_tickets.sql`: Thêm `where priority is null` để hứng rác.<br>`schema.yml`: Bật `enforced: true` và test `accepted_values: [1, 2, 3, 4]`. |
| **Bằng chứng** | `quarantine_tickets` = **312** hàng · `dbt test` **9/9** pass |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để
pipeline dừng khi gặp bản ghi lỗi?

> - **Nên chặn ở tầng Silver.** Tầng Bronze là bản sao thô (raw) của hệ thống nguồn, có nhiệm vụ lưu giữ nguyên trạng lịch sử (tính EL - Extract, Load). Nếu chặn ở Bronze, ta sẽ làm mất dữ liệu gốc vĩnh viễn và không có cơ hội truy vết.
> - **Không dừng pipeline vì:** Một vài bản ghi lỗi (chiếm % cực nhỏ) không được phép làm đình trệ toàn bộ luồng dữ liệu (có thể chứa hàng triệu bản ghi sạch khác). Việc cách ly (Quarantine) giúp hệ thống vẫn tiếp tục phục vụ dữ liệu sạch cho báo cáo/AI, đồng thời gom dữ liệu bẩn vào một chỗ để kỹ sư dữ liệu xử lý sau (Fault Tolerance).

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | **Bài A** (Tối ưu truy vấn Dashboard) |
| **Nguyên nhân** | Lỗi Small-file problem: Dữ liệu bị xé nhỏ thành 5,000 file ngẫu nhiên, không được phân vùng (partition). Cú pháp truy vấn bọc cột thời gian trong hàm `strftime` khiến engine không tận dụng được thống kê min/max của Parquet, buộc phải quét toàn bộ 5 triệu dòng. |
| **Cách khắc phục** | Dùng `COPY TO` ghi lại dữ liệu: Phân vùng theo ngày (`PARTITION_BY event_date`). Sắp xếp các dòng của cùng khách hàng vào chung một khối (`ORDER BY customer_name`). Định cỡ khối hợp lý (`ROW_GROUP_SIZE 100000`). Sửa câu truy vấn trỏ vào cấu trúc dữ liệu mới để tận dụng Partition Pruning và Row Group Skipping. |
| **Bằng chứng** | `rows scanned` giảm từ 5,000,000 xuống **137,942** (tối ưu **36.2 lần**). Số file giảm từ 5,000 xuống **14**. Thời gian chạy giảm xuống chỉ còn 8.2ms. |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Model có dùng `incremental` không? Nếu có, khai báo `unique_key` và `incremental_strategy` đã đầy đủ chưa để đảm bảo tính ổn định (Idempotent)? |
| 2 | Mệnh đề lọc thời gian (lookback window) của model incremental có bao phủ được độ trễ thực tế (P99 delay) của dữ liệu nguồn không? |
| 3 | Các cột quan trọng đã có Data Contract chưa? Hệ thống có bắt lỗi và cách ly (quarantine) bản ghi bẩn thay vì cho sập toàn bộ pipeline không? |
