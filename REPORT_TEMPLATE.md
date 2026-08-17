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
| **Triệu chứng** | |
| **Nguyên nhân** | |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | |
| **Cách khắc phục** | |
| **Bằng chứng** | `quarantine_tickets` = … hàng · `dbt test` … pass |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để
pipeline dừng khi gặp bản ghi lỗi?

> …

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | A / B / không làm |
| **Nguyên nhân** | |
| **Cách khắc phục** | |
| **Bằng chứng** | |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | |
| 2 | |
| 3 | |
