# ETECHS Integration Platform (Ingestion Middleware)

![Architecture](https://img.shields.io/badge/Architecture-Hexagonal%20%2B%20DDD-blue.svg)
![Python](https://img.shields.io/badge/Python-3.11-green.svg)
![Django](https://img.shields.io/badge/Django-4.2-orange.svg)
![React](https://img.shields.io/badge/React-19-cyan.svg)
![TailwindCSS](https://img.shields.io/badge/TailwindCSS-v4-blueviolet.svg)
![Status](https://img.shields.io/badge/Status-Early%20Development-red.svg)

Hệ thống **ETECHS Integration Platform** là giải pháp trung gian tích hợp dữ liệu cấp doanh nghiệp, thu thập (ingest), ánh xạ (map), kiểm tra và đối soát (reconcile) dữ liệu từ nhiều hệ thống nguồn (CRM, ERP, HRM, POS) trước khi chuyển tiếp vào Data Lake và Data Warehouse (DWH).

---

## Mục lục

- [1. Tổng quan dự án](#1-tổng-quan-dự-án)
- [2. Kiến trúc tổng quan](#2-kiến-trúc-tổng-quan)
- [3. Cấu trúc thư mục](#3-cấu-trúc-thư-mục)
- [4. Backend Middleware](#4-backend-middleware)
- [5. Frontend Console](#5-frontend-console)
- [6. DevOps & Hạ tầng](#6-devops--hạ-tầng)
- [7. Kho dữ liệu (DWH)](#7-kho-dữ-liệu-dwh)
- [8. Đánh giá mức độ hoàn thiện](#8-đánh-giá-mức-độ-hoàn-thiện)
- [9. Lộ trình phát triển](#9-lộ-trình-phát-triển)
- [10. Hướng dẫn khởi chạy](#10-hướng-dẫn-khởi-chạy)

---

## 1. Tổng quan dự án

### Thông tin cơ bản

| Mục | Chi tiết |
|------|---------|
| **Tên dự án** | ETECHS Integration Platform (Ingestion Middleware) |
| **Mục đích** | Nền tảng tích hợp dữ liệu doanh nghiệp, đóng vai trò trung gian giữa các hệ thống nguồn và hệ thống đích (Data Lake, DWH) |
| **Phạm vi** | Toàn bộ vòng đời dữ liệu: thu thập → ánh xạ → đồng bộ → kiểm tra → đối soát |
| **Giai đoạn** | Early Development (Pha 2: Xây dựng Backend & Frontend) |

### Nguồn lực dự án (theo `docs/works.xlsx`)

| Vai trò | Nhân sự |
|---------|---------|
| **Phân tích nghiệp vụ (DA)** | Võ Lê Huỳnh Anh, Võ Bách Kim Thanh, Huỳnh Phúc An Khang |
| **Backend (BE)** | Trần Văn Khánh, Lý Trần Việt, Văn Trung Tịnh |
| **Frontend (DP)** | Nguyễn Minh Thu, Dương Ngọc Hân, Bùi Lê Ngọc Như |
| **Cơ sở dữ liệu (DB)** | Lê Tuấn Đạt, Nguyễn Hữu Trung Đức, Nguyễn Minh Trà |
| **DevOps** | Hà Nhật Nam |
| **Data Engineer (DE)** | Phan Trọng Nguyên |

### Kế hoạch dự án

- **Pha 1: Khảo sát & Phân tích** — Đã hoàn thành (Tasks P01-001 đến P01-037)
  - Khảo sát chức năng các hệ thống nguồn (EOS, ETS, HLS)
  - Phân tích domain: Organization, User, Project, Task, Work Result, Performance
  - Xây dựng chuẩn dữ liệu, Data Dictionary, Mapping Matrix
- **Pha 2: Xây dựng** — Đang thực hiện
  - Tasks P02-001 trở đi: Khởi tạo Middleware, Migration, CRUD APIs
- **Pha 3+**: Pipeline đồng bộ, Reconciliation, Monitoring

---

## 2. Kiến trúc tổng quan

Dự án áp dụng mô hình kiến trúc phân rã với **Hexagonal Architecture (Ports & Adapters)** kết hợp **Domain-Driven Design (DDD)**.

```
[ CRM / ERP / HRM / POS ]  ← Các hệ thống nguồn
           │
           ▼ (API / Webhook / Batch)
  ┌──────────────────────────────────────────────────┐
  │              Ingestion Middleware                │
  │  ┌──────────────────────────────────────────┐   │
  │  │          API Gateway (Gateway)           │   │
  │  ├──────────────────────────────────────────┤   │
  │  │  Bounded Contexts (DDD):                 │   │
  │  │  - IAM (Identity & Access Management)    │   │
  │  │  - API Catalog / Source Systems          │   │
  │  │  - Sync Objects / Mapping Rules          │   │
  │  │  - Pipeline / Reconciliation             │   │
  │  │  - Audit / Ingestion Logs / Alerts       │   │
  │  ├──────────────────────────────────────────┤   │
  │  │  Shared Kernel (common/):                │   │
  │  │  Exception Handler, Pagination, RBAC,    │   │
  │  │  Response Envelope, Domain Events        │   │
  │  └──────────────────────────────────────────┘   │
  └──────────────┬────────────────────────┬──────────┘
                 │                        │
                 ▼ (Dữ liệu đã xử lý)      ▼ (Quản trị)
  ┌─────────────────────┐    ┌──────────────────────────┐
  │  Data Lake & DWH    │    │   Console Dashboard      │
  │  - Raw Payload      │    │   (Frontend React 19)    │
  │  - Normalized Data  │    │   - Giám sát đồng bộ     │
  │  - Entity Mapping   │    │   - Cấu hình hệ thống    │
  └─────────────────────┘    └──────────────────────────┘
```

### Nguyên tắc kiến trúc

1. **Dependency Rule**: Phụ thuộc từ ngoài vào trong
   - `Infrastructure (Django/DRF)` → `Application (Use Cases, Ports)` → `Domain (Entities, Events)`
   - Domain layer **KHÔNG được import** Django, DRF hay bất kỳ framework nào
2. **CQRS pattern**: Command (ghi) và Query (đọc) được tách riêng trong từng Use Case
3. **Ports & Adapters**: Domain định nghĩa interface (Port), Infrastructure implement (Adapter)
4. **Event-Driven**: Sử dụng Domain Events cho truyền thông giữa các Bounded Context

---

## 3. Cấu trúc thư mục

```
test_gitlab/
├── middleware-be/           # Backend - Python/Django + Hexagonal + DDD
│   ├── config/              # Django project settings, urls, wsgi/asgi
│   ├── common/              # Shared Kernel (exception, pagination, RBAC...)
│   ├── infrastructure/      # Global infrastructure (Redis, Kafka, Storage...)
│   ├── apps/                # 15 Bounded Contexts (bounded contexts)
│   │   ├── iam/             # [✅ HOÀN CHỈNH] Identity & Access Management
│   │   ├── lov/             # [⚠️ MỘT PHẦN] LOV Dictionary (demo view)
│   │   ├── api_catalog/     # [⏳ SHELL] API Inbound Catalog
│   │   ├── source_systems/  # [⏳ SHELL] Source Systems
│   │   ├── sync_objects/    # [⏳ SHELL] Sync Objects
│   │   ├── mapping_rules/   # [⏳ SHELL] Mapping & Transform Rules
│   │   ├── pipeline/        # [⏳ SHELL] Pipeline Engine
│   │   ├── reconciliation/  # [⏳ SHELL] Reconciliation
│   │   ├── gateway/         # [⏳ SHELL] Gateway Guard
│   │   ├── schemas/         # [⏳ SHELL] Canonical Schema
│   │   ├── system_config/   # [⏳ SHELL] System Configuration
│   │   ├── ingestion_logs/  # [⏳ SHELL] Ingestion Logs
│   │   ├── audit/           # [⏳ SHELL] Audit Trail
│   │   ├── dashboard/       # [⏳ SHELL] Dashboard & Statistics
│   │   └── alerts/          # [⏳ SHELL] Alerts & Notifications
│   ├── Dockerfile
│   ├── docker-compose.yml
│   └── requirements.txt
│
├── middleware-fe/           # Frontend - React 19 + Vite + TypeScript + TailwindCSS
│   ├── src/
│   │   ├── components/
│   │   │   ├── common/     # 7 UI components dùng chung (Button, Card, DataTable...)
│   │   │   └── layout/     # AppLayout (sidebar + header + main)
│   │   ├── features/       # 11 pages
│   │   │   ├── DashboardPage.tsx
│   │   │   ├── SystemsPage.tsx
│   │   │   ├── ApiCatalogPage.tsx
│   │   │   ├── SyncObjectsPage.tsx
│   │   │   ├── MappingsPage.tsx
│   │   │   ├── DataDictionaryPage.tsx
│   │   │   ├── UserRecordsPage.tsx
│   │   │   ├── SyncMonitorPage.tsx
│   │   │   ├── ReconciliationPage.tsx
│   │   │   ├── LogsPage.tsx
│   │   │   └── SettingsPage.tsx
│   │   ├── types/          # TypeScript interfaces
│   │   ├── mock/           # Dữ liệu giả lập
│   │   └── utils/          # format, maskSensitive
│   └── package.json
│
├── devops/                 # [⏳ SKELETON] Cấu hình triển khai
│   ├── docker/             # docker-compose.dev.yml (Postgres + Redis)
│   ├── ci-cd/              # [⏳ TRỐNG] CI/CD scripts
│   ├── k8s/                # [⏳ TRỐNG] Kubernetes manifests
│   ├── terraform/          # [⏳ TRỐNG] Infrastructure as Code
│   ├── monitoring/         # [⏳ TRỐNG] Prometheus + Grafana
│   └── security/           # [⏳ TRỐNG] Vault config
│
├── dw/                     # Tài liệu Data Warehouse
│   └── Identifier_Standard.md  # Chuẩn định danh dữ liệu
│
└── docs/
    └── works.xlsx          # Kế hoạch & theo dõi công việc
```

---

## 4. Backend Middleware

### 4.1 Công nghệ

| Công nghệ | Phiên bản | Mục đích |
|-----------|-----------|----------|
| Python | 3.11 | Ngôn ngữ lập trình |
| Django | >=4.2, <5.0 | Web framework |
| Django REST Framework | >=3.14 | REST API framework |
| SimpleJWT | >=5.3 | JWT authentication |
| drf-spectacular | >=0.26 | OpenAPI 3.0 documentation |
| structlog | >=23.2 | Structured logging |
| pytest | >=7.4 | Testing |
| PostgreSQL | 15 | Database |

### 4.2 Shared Kernel (`common/`)

Đã triển khai:
- ✅ `exceptions.py` — Base `DomainException` và ánh xạ HTTP status code
- ✅ `exception_handler.py` — Xử lý lỗi chuẩn RFC 7807 (Problem JSON)
- ✅ `pagination.py` — `EnterprisePageNumberPagination`
- ✅ `renderers.py` — `EnvelopeJSONRenderer` (tự động bọc response trong envelope)
- ✅ `permissions.py` — `HasPermission` (Dynamic RBAC theo AIP-211)

Chưa triển khai:
- ⏳ `base_entity.py`, `base_repository.py`, `domain_events.py`, `correlation.py`, `utils.py`

### 4.3 Global Infrastructure (`infrastructure/`)

Tất cả đều rỗng (chỉ có scaffolding):
- ⏳ `redis_client.py` — Redis connection pool
- ⏳ `kafka_producer.py` — Kafka/Redis Stream producer
- ⏳ `storage.py` — S3/MinIO file storage
- ⏳ `notifications.py` — Telegram/Slack webhook
- ⏳ `metrics.py` — Prometheus exporter

### 4.4 Bounded Contexts — Mức độ hoàn thiện

| App | Kiến trúc | Trạng thái | Chi tiết |
|-----|-----------|-----------|----------|
| **iam** | Hexagonal (3 lớp) | ✅ **100%** | User đăng ký, đăng nhập, refresh token, profile + RBAC + unit test |
| **lov** | Driving adapter | ⚠️ **~10%** | 1 view test demo response envelope + error format |
| **13 apps còn lại** | Hexagonal (3 lớp) | ⏳ **0%** | Chỉ có scaffolding (domain, application, infrastructure đều rỗng) |

#### IAM (Identity & Access Management) — App duy nhất có code

**Domain Layer**:
- `User` entity (id, email, full_name, password_hash, is_active, roles)
- `Role` / `Permission` entities
- UserAlreadyExistsError, InvalidCredentialsError, InvalidTokenError, UserInactiveError

**Application Layer**:
- `RegisterUseCase` / `LoginUseCase` / `RefreshTokenUseCase` / `GetMeQuery`
- Ports: `UserRepositoryPort`, `PasswordHasherPort`, `TokenServicePort`

**Infrastructure Layer**:
- Django ORM: `UserModel`, `RoleModel`, `PermissionModel`
- `DjangoUserRepository` (ORM ↔ Entity mapping)
- `DjangoPasswordHasher`, `SimpleJwtService`
- Views: `LoginView`, `RegisterView`, `RefreshTokenView`, `MeView`
- Serializers: input validation + authentication
- **API endpoints**: `/api/v1/iam/auth/login/`, `/api/v1/iam/auth/register/`, `/api/v1/iam/auth/refresh/`, `/api/v1/iam/users/me/`

**Tests**: 4 unit tests (test_register_success, test_register_duplicate_email, test_login_success, test_login_wrong_password) sử dụng Fake Repositories.

### 4.5 API Response Standards

Tất cả API response đều được bọc trong envelope format:
```json
{
  "data": { ... },
  "meta": {
    "timestamp": "2026-07-01T12:00:00Z",
    "request_id": "uuid"
  }
}
```
Lỗi trả về theo chuẩn RFC 7807:
```json
{
  "type": "about:blank",
  "title": "Conflict",
  "status": 409,
  "detail": "User with email xxx@example.com already exists"
}
```

---

## 5. Frontend Console

### 5.1 Công nghệ

| Công nghệ | Phiên bản |
|-----------|-----------|
| React | 19.2.6 |
| Vite | 8.0.12 |
| TypeScript | ~6.0.2 |
| TailwindCSS | 4.3.1 |
| lucide-react | 1.18.0 |

### 5.2 Trang đã triển khai (11 pages)

| Trang | File | Trạng thái | Tính năng |
|-------|------|-----------|-----------|
| Dashboard | `DashboardPage.tsx` | ✅ Hoàn chỉnh | 6 metric cards, 3 bảng (trạng thái hệ thống, đồng bộ gần đây, lỗi) |
| Hệ thống kết nối | `SystemsPage.tsx` | ✅ Hoàn chỉnh | DataTable + Drawer chi tiết + JsonViewer |
| Danh mục API | `ApiCatalogPage.tsx` | ✅ Hoàn chỉnh | Filter bar + DataTable + Drawer với Tabs |
| Đối tượng đồng bộ | `SyncObjectsPage.tsx` | ⚠️ Đơn giản | DataTable 8 cột |
| Mapping dữ liệu | `MappingsPage.tsx` | ✅ Hoàn chỉnh | Filter bar + action bar + DataTable |
| Từ điển dữ liệu | `DataDictionaryPage.tsx` | ⚠️ Đơn giản | DataTable 7 cột |
| Người dùng đồng bộ | `UserRecordsPage.tsx` | ✅ Hoàn chỉnh | Banner + DataTable + Drawer 6 tabs |
| Đồng bộ dữ liệu | `SyncMonitorPage.tsx` | ✅ Hoàn chỉnh | Filter 5 controls + DataTable 12 cột + Drawer 6 tabs |
| Đối soát dữ liệu | `ReconciliationPage.tsx` | ✅ Hoàn chỉnh | Filter + 6 metric cards + DataTable |
| Nhật ký hệ thống | `LogsPage.tsx` | ✅ Hoàn chỉnh | 5 tab logs riêng biệt |
| Cấu hình | `SettingsPage.tsx` | ⚠️ Đơn giản | Grid card hiển thị 6 cấu hình |

### 5.3 Component dùng chung (7 components)

| Component | Chức năng |
|-----------|-----------|
| `Button` | 4 variants (primary, secondary, ghost, danger) |
| `Card` / `MetricCard` | Card container + metric display |
| `StatusBadge` | Badge tự động màu cho 20+ trạng thái + 5 HTTP methods |
| `DataTable<T>` | Bảng dữ liệu generic với scroll ngang, empty state |
| `Drawer` | Side panel phải với animation |
| `Tabs` | Tab navigation component |
| `JsonViewer` | JSON viewer tích hợp `maskSensitiveData` |
| `TruncateText` | Text truncation + tooltip |

### 5.4 Bảo mật UI

- `maskSensitiveData()` — Đệ quy duyệt object, che password/token/secret/api_key bằng `'********'`
- Được tích hợp trong `JsonViewer` và preview data

### 5.5 Hạn chế hiện tại

- ⏳ Chưa có React Router (routing thủ công bằng `useState`)
- ⏳ Chưa kết nối API thực tế (toàn bộ dữ liệu mock)
- ⏳ Chưa có state management (Redux/Zustand)
- ⏳ Chưa có authentication/authorization UI
- ⏳ Chưa có unit test / component test
- ⏳ Chưa có loading/skeleton/error states

---

## 6. DevOps & Hạ tầng

### 6.1 Mức độ hoàn thiện

| Thành phần | Mức độ | Chi tiết |
|------------|--------|----------|
| **Docker** | 40% | ✅ Dockerfile (python:3.11-slim), docker-compose.yml (Postgres + Django), docker-compose.dev.yml (Postgres + Redis). ⏳ Thiếu Dockerfile prod, Nginx, Gunicorn |
| **CI/CD** | 0% | ⏳ Chưa có `.gitlab-ci.yml`, GitHub Actions, Jenkinsfile |
| **Kubernetes** | 0% | ⏳ Chỉ có thư mục rỗng `k8s/base/`, `k8s/overlays/` |
| **Terraform** | 0% | ⏳ Chỉ có thư mục rỗng `terraform/modules/`, `terraform/environments/` |
| **Monitoring** | 0% | ⏳ Chỉ có thư mục rỗng `monitoring/prometheus/`, `monitoring/grafana/` |
| **Security** | 0% | ⏳ Chỉ có thư mục rỗng `security/vault/`. ⚠️ Security policy đã được quy định trong README (Data Masking, Soft Delete) |

### 6.2 Docker hiện tại

```yaml
# docker-compose.yml (middleware-be)
services:
  db:
    image: postgres:15-alpine
    healthcheck: pg_isready
  web:
    build: .
    depends_on: [db]
    ports: ["8000:8000"]
```

---

## 7. Kho dữ liệu (DWH)

### Tài liệu đã hoàn thành

| File | Trạng thái | Nội dung |
|------|-----------|----------|
| `dw/Identifier_Standard.md` | ✅ Draft (80%) | Chuẩn định danh: source_system, source_object, source_record_id, datahub_id, master_record_id |

### 5 trường định danh cốt lõi

| Trường | Kiểu | Mô tả |
|--------|------|-------|
| `source_system` | String | Hệ thống nguồn (CRM, ERP, HRM, POS_RETAIL) |
| `source_object` | String | Loại thực thể (user, customer, product, order) |
| `source_record_id` | String | ID khóa chính tại hệ thống nguồn |
| `datahub_id` | UUID | ID duy nhất sinh bởi Middleware |
| `master_record_id` | UUID | ID thực thể sau hợp nhất (Giai đoạn 1: NULL) |

### Tài liệu cần bổ sung (theo task P01)

- ⏳ `Naming_Convention.md` (P01-012)
- ⏳ `Data_Type_Standard.md` (P01-013)
- ⏳ `LOV_Catalog.md` (P01-015)

---

## 8. Đánh giá mức độ hoàn thiện

### Thống kê tổng thể

| Thành phần | Hoàn thiện | Ghi chú |
|------------|-----------|---------|
| **Kiến trúc & Thiết kế** | 80% | Hexagonal + DDD rất bài bản, tài liệu OKF (`docs.md` ~2000 dòng) |
| **Backend (middleware-be)** | **~10%** | Chỉ IAM là hoàn chỉnh, 13/15 app chỉ có scaffolding |
| **Frontend (middleware-fe)** | **~40%** | UI đầy đủ 11 pages, nhưng chưa có API, auth, routing thực thụ |
| **DevOps** | **~5%** | Docker cơ bản, CI/CD/K8s/Terraform/Monitoring đều rỗng |
| **DWH Documentation** | 60% | Identifier_Standard hoàn chỉnh draft |
| **Project Management** | 90% | works.xlsx đầy đủ task + assignee |

### Chi tiết từng module

```
Backend Bounded Contexts:
╔═════════════════════╤═════════════╗
║ IAM                 │ ████████████ 100% ║
║ LOV                 │ ██░░░░░░░░░░  10% ║
║ 13 apps khác        │ ░░░░░░░░░░░░   0% ║
╚═════════════════════╧═════════════╝

Backend Infrastructure:
╔═════════════════════╤═════════════╗
║ Shared Kernel       │ ██████░░░░░░  60% ║
║ Global Infra        │ ░░░░░░░░░░░░   0% ║
╚═════════════════════╧═════════════╝

Frontend:
╔═════════════════════╤═════════════╗
║ UI Components       │ ████████████ 100% ║
║ Pages (11/11)       │ ████████████ 100% ║
║ API Integration     │ ░░░░░░░░░░░░   0% ║
║ Routing             │ ██░░░░░░░░░░  20% ║
║ Testing             │ ░░░░░░░░░░░░   0% ║
╚═════════════════════╧═════════════╝
```

---

## 9. Lộ trình phát triển

### Ngắn hạn (Ưu tiên cao)

| Task | Module | Mô tả |
|------|--------|-------|
| CI/CD Pipeline | DevOps | Thiết lập GitHub Actions / GitLab CI chạy lint + pytest |
| Production Docker | DevOps | Dockerfile multi-stage + Gunicorn + Nginx + docker-compose.prod.yml |
| React Router | Frontend | Thay thế routing thủ công bằng react-router-dom |
| API Integration | Frontend | Kết nối API thực tế (axios/fetch + React Query) |
| Source Systems CRUD | Backend | Triển khai app `source_systems` |
| API Catalog CRUD | Backend | Triển khai app `api_catalog` |
| Authentication UI | Frontend | Login/Register page + token management |

### Trung hạn

| Task | Module | Mô tả |
|------|--------|-------|
| Sync Objects + Mapping | Backend | Triển khai 2 app chính về đồng bộ và ánh xạ |
| Reconciliation Engine | Backend | Động cơ đối soát dữ liệu |
| Pipeline Engine | Backend | Trình điều phối đồng bộ dữ liệu |
| Audit & Logging | Backend | Ghi nhận toàn bộ vết tương tác |
| RBAC UI | Frontend | Giao diện quản lý người dùng, vai trò, quyền |
| K8s Manifests | DevOps | Deployment, Service, ConfigMap, Ingress cơ bản |

### Dài hạn

| Task | Module | Mô tả |
|------|--------|-------|
| Terraform IaC | DevOps | Modules cho RDS, ElastiCache, EKS |
| Monitoring | DevOps | Prometheus metrics + Grafana dashboard |
| Security | DevOps | Vault / Sealed Secrets cho secret management |
| Real-time Sync | Backend | Kafka/Redis Stream cho đồng bộ thời gian thực |
| Global State | Frontend | Zustand hoặc Redux Toolkit |

---

## 10. Hướng dẫn khởi chạy

### Yêu cầu hệ thống

- Docker & Docker Compose
- Node.js 18+
- Python 3.10+ (nếu chạy không qua Docker)

### Khởi động Backend

```bash
cd middleware-be
docker-compose up --build
```

Backend API tại: `http://localhost:8000`

API docs (Swagger): `http://localhost:8000/api/docs/`

### Khởi động Frontend

```bash
cd middleware-fe
npm install
npm run dev
```

Frontend tại: `http://localhost:5173`

### Môi trường Development (chỉ database)

```bash
cd devops/docker
docker-compose -f docker-compose.dev.yml up -d
```

Chạy PostgreSQL 15 (port 5432) và Redis 7 (port 6379).

---

## 11. Tiêu chuẩn làm việc

### Git Branch Convention

```
type/app_name/mô_tả
```

| Type | Ý nghĩa |
|------|---------|
| `feat` | Tính năng mới |
| `fix` | Sửa lỗi |
| `refactor` | Tái cấu trúc |
| `docs` | Tài liệu |

Ví dụ: `feat/iam/login-api`, `fix/pipeline/timeout`

### Commit Message

```
type(layer): mô tả chi tiết
```

Ví dụ: `feat(domain): add UserEntity`, `fix(infra): update postgres driver`

### Code Standards

- **Backend**: Python 3.11, Black formatter, isort, pytest
- **Frontend**: TypeScript strict mode, ESLint, Prettier
- **Bảo mật**: Token/Key/Secret luôn được mask khi log hoặc render UI
- **Dữ liệu**: Soft delete cho mọi cấu hình quan trọng

---

*Dự án được quản lý bởi ETECHS — Cập nhật lần cuối: 01/07/2026*
