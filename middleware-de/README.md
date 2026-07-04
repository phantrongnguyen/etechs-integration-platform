# ETECHS Ingestion Middleware - Data Engineer (DE) Service

Dịch vụ `middleware-de` chịu trách nhiệm thu thập (ingestion), ánh xạ (mapping), đồng bộ (sync), đối soát (reconciliation) và vận hành các tác vụ dữ liệu nền giữa hệ thống nguồn EOS, database trung gian (Middleware) và đích đến (DataHub).

Dự án được viết bằng **Python** và sử dụng kiến trúc thiết kế dạng đường ống đồng bộ (pipeline orchestration).

---

## 1. Cấu trúc thư mục chi tiết & Phân công công việc

Dưới đây là sơ đồ thư mục và cách ánh xạ từng tác vụ của **Data Engineer (Phan Trọng Nguyên)**:

```text
middleware-de/
├── config/                  # Cấu hình hệ thống (DB, Redis, Logger...)
│   ├── settings.py          # Cấu hình tham số môi trường
│   └── logger.py            # Logger tập trung (structlog)
│
├── docs/                    # Tài liệu đặc tả chuẩn hóa dữ liệu
│   ├── standards/
│   │   ├── identifier_standard.md  # [TASK 1] Chuẩn định danh dữ liệu
│   │   ├── metadata_framework.md   # [TASK 2] Khung Metadata Framework
│   │   └── standardization_rules.md # [TASK 3] Rule chuẩn hóa dữ liệu
│   └── architecture/
│       └── diagram.drawio          # [TASK 4] Sơ đồ kiến trúc Middleware
│
├── src/                     # Mã nguồn ứng dụng chính
│   ├── main.py              # File khởi chạy ứng dụng
│   │
│   ├── core/                # Thành phần cốt lõi dùng chung (Framework Core)
│   │   ├── connectors/      # API Client gọi các hệ thống ngoài
│   │   │   ├── eos_client.py       # [TASK 28,30,32,34,36,38] API Client kết nối EOS
│   │   │   └── datahub_client.py   # [TASK 7] DataHub Connector base (gọi API DataHub)
│   │   │
│   │   ├── sync/            # Bộ máy quản trị tiến trình đồng bộ
│   │   │   ├── engine.py           # [TASK 5] State machine quản lý trạng thái đồng bộ
│   │   │   └── checkpoint.py       # Quản lý checkpoint của delta sync
│   │   │
│   │   ├── queue/           # Xử lý hàng đợi & Tác vụ bất đồng bộ
│   │   │   ├── celery_app.py       # [TASK 6] Khởi tạo Celery & Redis 7
│   │   │   └── tasks.py            # Định nghĩa các task chạy background
│   │   │
│   │   └── standardization/ # Logic tiền xử lý & chuẩn hóa
│   │       └── rules.py            # [TASK 3] Các hàm chuẩn hóa (Email, Phone, ISO Date)
│   │
│   ├── pipelines/           # Luồng Ingestion (EOS -> Middleware -> DataHub)
│   │   ├── base.py                 # Abstract Class cho Pipeline
│   │   │
│   │   ├── organization/
│   │   │   ├── pull.py             # [TASK 8,15,28] Trích xuất và sync Organization từ EOS
│   │   │   ├── checkpoint.py       # [TASK 29] Delta sync/checkpoint cho Organization
│   │   │   └── reverse.py          # [TASK 40] Chuẩn bị chiều master DataHub -> EOS
│   │   │
│   │   ├── user/
│   │   │   ├── pull.py             # [TASK 9,16,30] Trích xuất và sync User/Employee từ EOS
│   │   │   ├── checkpoint.py       # [TASK 31] Delta sync/checkpoint cho User/Employee
│   │   │   └── reverse.py          # [TASK 41] Chuẩn bị chiều master DataHub -> EOS
│   │   │
│   │   ├── project/
│   │   │   ├── pull.py             # [TASK 10,17,32] Trích xuất và sync Project từ EOS
│   │   │   └── checkpoint.py       # [TASK 33] Delta sync/checkpoint cho Project
│   │   │
│   │   ├── task/
│   │   │   ├── pull.py             # [TASK 11,18,34] Trích xuất và sync Task từ EOS
│   │   │   └── checkpoint.py       # [TASK 35] Delta sync/checkpoint cho Task
│   │   │
│   │   ├── work_result/
│   │   │   ├── pull.py             # [TASK 12,26,36] Trích xuất và sync Work Result từ EOS
│   │   │   └── checkpoint.py       # [TASK 37] Delta sync/checkpoint cho Work Result
│   │   │
│   │   └── performance/
│   │       ├── pull.py             # [TASK 13,19,38] Trích xuất và sync Performance từ EOS
│   │       └── checkpoint.py       # [TASK 39] Delta sync/checkpoint cho Performance
│   │
│   ├── reconciliation/      # Các tiến trình đối soát (Reconciliation)
│   │   ├── base.py                 # Runner đối soát chung
│   │   ├── organization.py         # [TASK 42,43] Count & Missing đối soát Organization
│   │   ├── user.py                 # [TASK 44,45] Count & Missing đối soát User/Employee
│   │   ├── task.py                 # [TASK 46,47] Count & Missing đối soát Task
│   │   ├── work_result.py          # [TASK 48,49] Count & Missing đối soát Work Result
│   │   └── performance.py          # [TASK 50,51] Count & Missing đối soát Performance
│   │
│   └── api/                 # Endpoint REST phục vụ quản trị thủ công
│       ├── router.py               # Định tuyến API
│       └── endpoints/              # [TASK 22-27] Các endpoint Admin-only chạy lại sync
│           ├── organization.py
│           ├── user.py
│           ├── project.py
│           ├── task.py
│           ├── work_result.py
│           └── performance.py
│
├── tests/                   # Kiểm thử tích hợp (Integration Tests)
│   ├── integration/
│   │   ├── test_eos_to_middleware.py     # [TASK 14] Test luồng EOS -> Middleware core
│   │   ├── test_middleware_to_datahub.py  # [TASK 20] Test luồng Middleware -> DataHub
│   │   └── test_e2e_sync.py              # [TASK 21] Test E2E EOS -> Middleware -> DataHub
```

---

## 2. Công nghệ sử dụng
* **Ngôn ngữ:** Python 3.11+
* **Hàng đợi tác vụ (Queue/Retry):** Celery + Redis 7
* **Framework API:** FastAPI (Dành cho các Admin Retry Endpoint) (Em thấy dùng FastAPI là đủ không cần Django)
* **Kiểm thử:** Pytest

---

## 3. Quy trình Triển khai và Phát triển của DE
1. **Hoàn thiện Đặc tả & Chuẩn hóa:** Hoàn thành các file thiết kế tại `docs/standards/`.
2. **Xây dựng Core Framework:** Cài đặt các kết nối cơ bản (`eos_client.py`, `datahub_client.py`), cơ chế chuyển trạng thái đồng bộ (`sync/engine.py`), và hàng đợi Celery (`queue/celery_app.py`).
3. **Phát triển Từng Pipeline (Phát triển theo vòng lặp):**
   - Viết logic kéo dữ liệu (Pull).
   - Thiết lập kiểm tra thay đổi dữ liệu (Checkpoint).
   - Tích hợp chạy thật với API và đẩy lên DataHub.
4. **Viết Job Đối soát (Reconciliation):** Triển khai đối soát số lượng và tìm bản ghi lệch tại `reconciliation/`.
5. **Viết Test:** Chạy tích hợp và kiểm thử E2E sử dụng pytest để đảm bảo luồng dữ liệu thông suốt, không bị mất mát hay sai lệch.
