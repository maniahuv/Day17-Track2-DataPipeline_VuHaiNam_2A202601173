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
| **Triệu chứng** | |
| **P99 độ trễ đo được** | **… ngày** *(bắt buộc)* |
| **Lookback đã chọn** | … ngày — vì … |
| **Nguyên nhân** | |
| **Cách khắc phục** | |
| **Bằng chứng** | trước: … hàng · sau: … hàng |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> …

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
