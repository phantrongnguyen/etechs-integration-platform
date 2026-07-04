import os
import json

def generate_mock_data():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    raw_dir = os.path.join(base_dir, "data", "raw")
    os.makedirs(raw_dir, exist_ok=True)
    
    # 1. Mock Users
    users = [
        {
            "id": "USR_1001",
            "name": "Nguyen Van A",
            "email": " NGUYENvANA@Gmail.com ",
            "phone": "+84 (909) 123-456",
            "created_at": "2026/07/01 08:30:00"
        },
        {
            "id": "USR_1002",
            "name": "Tran Thi B",
            "email": "tranthib@Yahoo.com",
            "phone": "0987654321",
            "created_at": "02/07/2026 14:15:30"
        }
    ]
    
    # 2. Mock Projects
    projects = [
        {
            "project_id": "PRJ_5001",
            "name": "Xay dung he thong MDM",
            "owner_email": "owner@company.com",
            "start_date": "2026-07-04"
        }
    ]
    
    # 3. Mock Tasks
    tasks = [
        {
            "task_id": "TSK_9001",
            "project_id": "PRJ_5001",
            "title": "Khoi tao middleware-de",
            "status": "Done",
            "created_date": "2026-07-04T15:00:00Z"
        },
        {
            "task_id": "TSK_9002",
            "project_id": "PRJ_5001",
            "title": "Thiet ke chuan 5 truong dinh danh MDM",
            "status": "In Progress",
            "created_date": "04-07-2026"
        }
    ]
    
    # Write to files
    with open(os.path.join(raw_dir, "users.json"), "w", encoding="utf-8") as f:
        json.dump(users, f, indent=2, ensure_ascii=False)
        
    with open(os.path.join(raw_dir, "projects.json"), "w", encoding="utf-8") as f:
        json.dump(projects, f, indent=2, ensure_ascii=False)
        
    with open(os.path.join(raw_dir, "tasks.json"), "w", encoding="utf-8") as f:
        json.dump(tasks, f, indent=2, ensure_ascii=False)
        
    print(f"Dữ liệu giả lập đã được tạo thành công tại: {raw_dir}")

if __name__ == "__main__":
    generate_mock_data()
