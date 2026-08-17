# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Vũ Hải Nam  **Lớp:** AICB-P2T2  **Ngày:** 17/8/2026

---

## 0 · Kết quả `make verify`

<details open>
<summary><b>Output sau khi sửa</b> — ba lượt chạy liên tiếp</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 25.8s
  run 2/3 … 26.8s
  run 3/3 … 28.1s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✓ 5,000,000 → 137,942 (36.2×, cần ≥ 10×)
    số file parquet                           ✓ 5,000 → 14
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

</details>

<details>
<summary>Output <b>trước khi sửa</b> — trạng thái lúc tiếp nhận bàn giao (để đối chiếu)</summary>

```
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

Tổng kết: **4 / 4 tiêu chí đạt**

> **Ghi chú môi trường.** Máy làm bài chạy Windows, console mặc định không phải
> UTF-8 nên các ký tự `✓ / ✗ / ━` của `verify.py` gây `UnicodeEncodeError`.
> Cách xử lý: **không** sửa `tools/verify.py` (file bị cấm sửa theo RUBRIC) —
> chỉ thêm một shim bật `PYTHONUTF8=1` ở đầu `tools/run_pipeline.py` (file được
> phép sửa). `tools/verify.py`, `tools/explain.py`, `tools/common.py`,
> `expected/` và `seed/generate.py` giữ nguyên 100% so với bản gốc.

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy
| | |
|---|---|
| **Triệu chứng** | Bảng `gold_training_set` liên tục tăng số lượng hàng (phình to) mỗi khi chạy lại pipeline thay vì giữ nguyên số lượng ticket. |
| **Nguyên nhân** | Cấu hình bảng ở chế độ `incremental` nhưng thiếu `unique_key`. Khi không có khoá, dbt sử dụng chiến lược `append` mặc định. Các bản ghi có `op='u'` (cập nhật) từ CDC sẽ bị chèn thêm vào thay vì ghi đè lên bản ghi cũ. Kết hợp với `catchup=True` trong DAG khiến quá trình dồn nhiều lượt chạy tạo ra dữ liệu trùng lặp trầm trọng hơn. |
| **Cách khắc phục** | `gold_training_set.sql`: Thêm `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'` vào `config()`.<br>`ai_training_pipeline.py`: Đổi `catchup=False` và `max_active_runs=1`. |
| **Bằng chứng** | `gold_training_set` giữ ổn định ở **12,480** hàng (trước: 38,750). Cột ỔN ĐỊNH **✓ ok**, checksum giống hệt nhau cả ba lượt: `8dd7c98653 / 8dd7c98653 / 8dd7c98653` (trước: `7c461563f4 / d11657ff21 / 2b76a4f850`). Dòng kiểm tra "1 hàng / 1 ticket" **✓ không lặp**. DAG: `catchup=False`, `max_active_runs=1` **✓**. |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | Bảng `gold_feature_daily` bị thiếu hàng (thiếu 455 hàng) ở các ngày trong quá khứ, chỉ những ngày mới nhất mới đủ số lượng bản ghi. |
| **P99 độ trễ đo được** | **2.73 ngày** *(bắt buộc)* — chính xác 2.726 ngày, đo trên toàn bộ 129.462 bản ghi `bronze_events` bằng `quantile_cont(date_diff('second', event_time, _ingested_at)/86400.0, 0.99)`. |
| **Phân bố độ trễ đầy đủ** | P50 = 0.128 ngày · P95 = 1.814 · **P99 = 2.726** · max = 2.945.<br>**5.05%** số bản ghi tới kho muộn hơn 1 ngày — khớp với triệu chứng "thiếu khoảng 5%" trong phiếu #1043, xác nhận đúng hướng chẩn đoán. |
| **Lookback đã chọn** | 3 ngày — vì P99 = 2.73 ngày, chọn 3 ngày sẽ bao phủ được >99% dữ liệu đến trễ mà không làm tăng quá mức khối lượng dữ liệu phải quét lại hàng ngày. |
| **Nguyên nhân** | Điều kiện lọc incremental `where event_date > max(event_date)` quá khắt khe. Nó chỉ quét những sự kiện xảy ra ở ngày mới nhất. Những sự kiện xảy ra ở ngày cũ nhưng bị kẹt (ví dụ do rớt mạng) và đến muộn sẽ bị bỏ sót vĩnh viễn khi pipeline chạy ở ngày hiện tại. |
| **Cách khắc phục** | `gold_feature_daily.sql`: Nới rộng điều kiện quét lùi về 3 ngày: `where event_date >= (select max(event_date)...) - INTERVAL 3 DAY`. Đồng thời thêm `unique_key = ['event_date', 'customer_id']` để đảm bảo khi quét lại các ngày cũ, dbt sẽ hợp nhất thay vì nhân bản dữ liệu. |
| **Bằng chứng** | `gold_feature_daily` đạt đúng **9,100** hàng (trước: 8,645 — thiếu 455) và giữ nguyên **✓ ok** ở cột ỔN ĐỊNH, checksum `3db448685c` đồng nhất cả ba lượt. Lookback 3 ngày > P99 2.726 ngày nên phủ hết phần đuôi trễ; `gold_training_set` và `gold_doc_chunks` không bị ảnh hưởng (checksum giữ nguyên) — thay đổi ở nhiệm vụ 2 không phá nhiệm vụ 1. |

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
| **Bằng chứng** | `quarantine_tickets` = **312** hàng (đúng grain: 312 hàng / 312 bản ghi CDC).<br>`silver_tickets` vẫn giữ đủ **12.480** ticket, `priority` 0 NULL và luôn ∈ 1..4 — lọc bản ghi hỏng TRƯỚC `row_number()` nên không mất ticket nào.<br>`dbt test` **11/11 pass** — tăng từ 9 test bản gốc lên 11 nhờ **2 test mới** thêm vào `priority`: `not_null` và `accepted_values: [1,2,3,4]`.<br>Phân loại lý do bị loại trong `reject_reason`: 118 "số ngoài khoảng 1..4" · 116 "chuỗi không hợp lệ" · 78 "bị bỏ trống". |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để
pipeline dừng khi gặp bản ghi lỗi?

> - **Nên chặn ở tầng Silver.** Tầng Bronze là bản sao thô (raw) của hệ thống nguồn, có nhiệm vụ lưu giữ nguyên trạng lịch sử (tính EL - Extract, Load). Nếu chặn ở Bronze, ta sẽ làm mất dữ liệu gốc vĩnh viễn và không có cơ hội truy vết.
> - **Không dừng pipeline vì:** Một vài bản ghi lỗi (chiếm % cực nhỏ) không được phép làm đình trệ toàn bộ luồng dữ liệu (có thể chứa hàng triệu bản ghi sạch khác). Việc cách ly (Quarantine) giúp hệ thống vẫn tiếp tục phục vụ dữ liệu sạch cho báo cáo/AI, đồng thời gom dữ liệu bẩn vào một chỗ để kỹ sư dữ liệu xử lý sau (Fault Tolerance).

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | **Bài A** (Tối ưu truy vấn) + **Bài B** (Consumer Crash Test) |
| **Nguyên nhân** | Bài A: Lỗi Small-file problem và thiếu phân vùng. Câu truy vấn dùng hàm `strftime` không sargable, làm mất khả năng Data Skipping.<br>Bài B: Hệ thống sử dụng cơ chế At-most-once (lưu offset trước khi ghi). Khi sập giữa chừng, tiến trình tái khởi động sẽ đọc lô tiếp theo, làm mất vĩnh viễn lô đang xử lý dở. |
| **Cách khắc phục** | Bài A: Dùng `COPY TO` ghi lại dữ liệu có Partition theo ngày, Order By để gom cụm khách hàng, và chia khối 100k dòng. Sửa câu truy vấn thành SARGable.<br>Bài B: Đảo ngược trình tự thành At-least-once (ghi dữ liệu xong mới lưu offset). Để khắc phục tình trạng lô bị ghi trùng, đổi cấu trúc bảng thêm `PRIMARY KEY(event_id)` và dùng lệnh `INSERT ... ON CONFLICT DO NOTHING` (Phép ghi Idempotent). Chọn `DO NOTHING` vì dữ liệu sự kiện là bất biến, lấy bản ghi đến trước tiên là đủ. |
| **Bằng chứng** | **Bài A** — ba con số đo bằng `make explain` (`threads = 1`):<br><br><table><tr><th></th><th>TRƯỚC</th><th>SAU</th></tr><tr><td><code>rows scanned</code></td><td>5,000,000</td><td><b>137,942</b> (giảm 36.2×, cần ≥ 10×)</td></tr><tr><td><code>files</code></td><td>5,000</td><td><b>14</b></td></tr><tr><td><code>rows on disk</code></td><td>130,683</td><td>130,683 (không đổi)</td></tr><tr><td><code>result hash</code></td><td>4379e4c5d9f3</td><td>4379e4c5d9f3 (<b>không đổi</b>)</td></tr></table>`rows on disk` giữ nguyên 130,683 trong khi `rows scanned` tụt 36 lần — chứng minh phần tiết kiệm đến hoàn toàn từ việc **bỏ qua file**, không phải từ việc xoá bớt dữ liệu. Khoảng cách 5,000,000 vs 130,683 lúc đầu chính là small-file problem hiện thành con số: 5.000 file × ~1.000 hàng đọc tối thiểu mỗi file.<br><br>**Bài B** — `make crash-test`: **ĐẠT ✓**. Kịch bản giết ở lô 7, offset đã commit 3,000; restart ghi nốt 17,000 → tổng **20,000 hàng / 20,000 event_id khác nhau**, khớp đúng lượt chạy không sự cố (không mất, không trùng). |

**Bài A — ba quyết định trong `tools/compact.py` và lý do:**

> - **`PARTITION_BY (event_date)`** — dashboard filter theo hai cột: `customer_name`
>   và ngày. Chỉ một trong hai được đưa vào đường dẫn. Chọn `event_date` vì nó có
>   **14 giá trị phân biệt** → 14 thư mục, mỗi thư mục ~9.300 hàng. Nếu partition
>   theo `customer_name` (**650 giá trị** — đã đếm) ta sẽ tạo 650 thư mục, mỗi thư
>   mục chỉ ~200 hàng — tức là tự tái tạo lại đúng small-file problem vừa đi sửa,
>   chỉ khác là đổi tên thư mục. **Đây là quyết định mang toàn bộ hiệu quả** (xem
>   đo lường bên dưới).
> - **`ORDER BY customer_name, event_time`** — phần lọc còn lại sau khi partition
>   đã cắt theo ngày là `customer_name = 'ACME'`, và nó chỉ có thể dựa vào min/max
>   statistics của row group. Statistics chỉ dùng được khi các hàng cùng một khách
>   hàng **nằm liền nhau**; thứ tự ngẫu nhiên thì mọi row group đều có min='A…',
>   max='Z…' và không row group nào bị loại.
> - **`ROW_GROUP_SIZE 100000`** — một ngày chỉ có ~9.300 hàng, **nhỏ hơn** cả
>   100.000 lẫn mặc định 122.880, nên trên thực tế mỗi partition vẫn gói gọn trong
>   **đúng một** row group (đã kiểm chứng: 14 file / 14 row group). Tham số này vì
>   vậy **không** thay đổi layout ở quy mô dữ liệu hiện tại; nó chỉ trở thành đòn
>   bẩy khi một partition lớn hơn nhiều lần con số đó.
>
> **Đo lường tách phần đóng góp của từng quyết định** (cùng `result hash`
> `4379e4c5d9f3` ở mọi biến thể, nên ngữ nghĩa query không đổi):
>
> | Biến thể layout | files | rows scanned |
> |---|---|---|
> | gốc: 5.000 file, không partition | 5,000 | 5,000,000 |
> | partition + order, `rgs=100000` *(bài nộp)* | 14 | **137,942** |
> | partition + order, `rgs=2000` | 14 | 137,144 |
> | partition, **không** order, `rgs=2000` | 14 | 138,670 |
>
> Kết luận trung thực rút ra từ bảng trên: ở tập dữ liệu này **gần như 100% mức
> giảm 36.2× đến từ việc gom 5.000 file xuống 14 file có partition theo ngày**.
> Đổi `row_group_size` chỉ dịch chuyển 0.6%, bỏ `ORDER BY` chỉ tệ đi 1.1% — vì
> `OPERATOR_ROWS_SCANNED` của DuckDB làm tròn lên theo *file*, nên khi chỉ còn 14
> file thì số hàng quét đã chạm sàn ~130.683 hàng thật trên đĩa và không còn chỗ
> để hai tham số kia phát huy. `ORDER BY` và `ROW_GROUP_SIZE` là lựa chọn đúng về
> nguyên tắc và sẽ có ý nghĩa khi một partition đủ lớn để chứa nhiều row group,
> nhưng ở quy mô lab này chúng chưa phải yếu tố quyết định.

**Bài B — `DO UPDATE` khác `DO NOTHING` ở điểm nào khi message được replay với nội dung đã đổi?**

> Khác nhau ở chỗ **bản ghi nào thắng khi có xung đột khoá**:
> - `DO NOTHING` → **bản đến trước thắng**. Message replay bị bỏ qua hoàn toàn,
>   nội dung mới không bao giờ vào bảng.
> - `DO UPDATE` → **bản đến sau thắng**. Hàng cũ bị ghi đè bằng nội dung mới.
>
> Cả hai đều idempotent về **số hàng** (chạy lại N lần vẫn 20.000 hàng), nên
> `make crash-test` pass với cả hai. Nhưng chúng khác nhau về **nội dung**, và
> lựa chọn đúng phụ thuộc bản chất dữ liệu:
> - Ở đây là **event log — dữ liệu bất biến**: một `event_id` đã xảy ra thì nội
>   dung không đổi, replay chỉ là bản sao y hệt. `DO NOTHING` đúng, lại rẻ hơn
>   vì không phát sinh phép ghi.
> - Nếu đây là **CDC của một entity** (như `bronze_tickets_cdc`), replay có thể
>   mang trạng thái *mới hơn*. Lúc đó `DO NOTHING` sẽ **đóng băng bản ghi ở trạng
>   thái cũ** và làm mất mọi cập nhật — phải dùng `DO UPDATE`. Đây chính là lý do
>   `gold_training_set` ở nhiệm vụ 1 dùng `incremental_strategy = 'merge'` (tương
>   đương `DO UPDATE`) chứ không phải bỏ qua bản trùng.

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Model có dùng `incremental` không? Nếu có, khai báo `unique_key` và `incremental_strategy` đã đầy đủ chưa để đảm bảo tính ổn định (Idempotent)? |
| 2 | Mệnh đề lọc thời gian (lookback window) của model incremental có bao phủ được độ trễ thực tế (P99 delay) của dữ liệu nguồn không? |
| 3 | Các cột quan trọng đã có Data Contract chưa? Hệ thống có bắt lỗi và cách ly (quarantine) bản ghi bẩn thay vì cho sập toàn bộ pipeline không? |
