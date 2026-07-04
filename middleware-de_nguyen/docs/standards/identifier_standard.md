# [TASK 1] Chuẩn định danh dữ liệu
* **STT (DA):** 16
* **Mô tả:** Xác định cách lưu định danh: source_system, source_object, source_record_id, master_record_id, datahub_id.
* **Assignee:** Lê Tuấn Đạt (DB) + Phan Trọng Nguyên (DE)
* **Quy tắc:**
  - `source_system`: e.g., 'EOS', 'ETS'
  - `source_object`: e.g., 'Organization', 'User'
  - `source_record_id`: String, ID nguyên bản từ hệ thống nguồn.
  - `master_record_id`: UUID, ID nội bộ Middleware để link trùng lặp nếu cần.
  - `datahub_id`: UUID, ID nhận về từ DataHub sau khi đồng bộ thành công.
