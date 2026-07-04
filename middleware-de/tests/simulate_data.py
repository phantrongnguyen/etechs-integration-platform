import os
import json

def generate_mock_data():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    raw_dir = os.path.join(base_dir, "data", "raw")
    os.makedirs(raw_dir, exist_ok=True)
    
    # 1. Mock Users
    users = [
        {
            "employee_id": "USR_1001",
            "full_name": "Phan Trọng Nguyên",
            "email": "  TrongNguyen.Phan@Etechs.Com.vn  ",
            "phone": "+84 901-234-567",
            "created_at": "2026-07-01T10:30:00Z"
        },
        {
            "employee_id": "USR_1002",
            "full_name": "Lê Tuấn Đạt",
            "email": "DAT.LT@Etechs.vn",
            "phone": "0987.654.321",
            "created_at": "2026/06/15 08:12:34"
        },
        {
            "employee_id": "USR_1003",
            "full_name": "Dương Ngọc Hân",
            "email": "  Han.Duong@etechs.vn",
            "phone": "0912-345-678 ",
            "created_at": "2026-05-20"
        }
    ]
    
    # 2. Mock Projects
    projects = [
        {
            "id": "PRJ_5001",
            "name": "Hệ thống Tích hợp ETECHS Middleware",
            "status": "active",
            "created_date": "2026-01-10T00:00:00Z"
        },
        {
            "id": "PRJ_5002",
            "name": "Nâng cấp Cổng dịch vụ EOS",
            "status": "planning",
            "created_date": "2026-03-15T09:00:00Z"
        }
    ]
    
    # 3. Mock Tasks
    tasks = [
        {
            "task_id": "TSK_9001",
            "project_id": "PRJ_5001",
            "title": "Xây dựng Ingestion Engine với Celery & Pandas",
            "priority": "HIGH",
            "due_date": "2026-07-10T17:00:00Z"
        },
        {
            "task_id": "TSK_9002",
            "project_id": "PRJ_5001",
            "title": "Thiết kế chuẩn 5 trường định danh MDM",
            "priority": "CRITICAL",
            "due_date": "2026-07-05T12:00:00Z"
        },
        {
            "task_id": "TSK_9003",
            "project_id": "PRJ_5002",
            "title": "Khảo sát API Endpoint hệ thống EOS",
            "priority": "MEDIUM",
            "due_date": "2026-08-01T18:00:00Z"
        }
    ]
    
    # Write to files
    user_file = os.path.join(raw_dir, "users.json")
    project_file = os.path.join(raw_dir, "projects.json")
    task_file = os.path.join(raw_dir, "tasks.json")
    
    with open(user_file, "w", encoding="utf-8") as f:
        json.dump(users, f, indent=2, ensure_ascii=False)
    with open(project_file, "w", encoding="utf-8") as f:
        json.dump(projects, f, indent=2, ensure_ascii=False)
    with open(task_file, "w", encoding="utf-8") as f:
        json.dump(tasks, f, indent=2, ensure_ascii=False)
        
    print(f"Generated mock data at:")
    print(f" - {user_file}")
    print(f" - {project_file}")
    print(f" - {task_file}")

if __name__ == "__main__":
    generate_mock_data()
