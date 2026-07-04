import os
import sys
import json
import shutil
import sqlite3
import pandas as pd

# Thêm đường dẫn hiện tại vào sys.path để Python giải quyết được các module import
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Định dạng utf-8 cho đầu ra trên Windows console
if sys.platform.startswith("win"):
    import io
    sys.stdout.reconfigure(encoding='utf-8')

base_dir = os.path.dirname(os.path.abspath(__file__))

# 0. Dọn dẹp dữ liệu cũ để chạy phiên mới sạch sẽ
raw_dir = os.path.join(base_dir, "data", "raw")
if os.path.exists(raw_dir):
    shutil.rmtree(raw_dir)

standardized_dir = os.path.join(base_dir, "data", "standardized")
if os.path.exists(standardized_dir):
    shutil.rmtree(standardized_dir)

db_path = os.path.join(base_dir, "data", "sync_jobs.db")
if os.path.exists(db_path):
    try:
        os.remove(db_path)
    except PermissionError:
        print("Cảnh báo: Không thể xóa file DB cũ do đang bị giữ bởi tiến trình khác.")

# 1. Sinh dữ liệu giả lập
from tests.simulate_data import generate_mock_data
generate_mock_data()

# 2. Cấu hình Celery chạy ở chế độ Eager Mode (Không cần chạy Redis cục bộ)
from src.core.celery_app import app
app.conf.update(
    task_always_eager=True,
    task_eager_propagates=True
)

from src.application.tasks import process_ingestion_task
from src.core.sync import SyncEngine

def run_demo():
    raw_dir = os.path.join(base_dir, "data", "raw")
    
    user_file = os.path.join(raw_dir, "users.json")
    project_file = os.path.join(raw_dir, "projects.json")
    task_file = os.path.join(raw_dir, "tasks.json")
    
    print("\n" + "="*80)
    print("BẮT ĐẦU CHẠY GIẢ LẬP LUỒNG XỬ LÝ (CELERY + PANDAS)")
    print("="*80)
    
    # Chạy tác vụ xử lý Users
    print("\n--- Tiến trình xử lý Users ---")
    u_res = process_ingestion_task.delay(user_file, "user").get()
    print(f"Hoàn tất task. Kết quả: {u_res}")
    
    # Chạy tác vụ xử lý Projects
    print("\n--- Tiến trình xử lý Projects ---")
    p_res = process_ingestion_task.delay(project_file, "project").get()
    print(f"Hoàn tất task. Kết quả: {p_res}")
    
    # Chạy tác vụ xử lý Tasks
    print("\n--- Tiến trình xử lý Tasks ---")
    t_res = process_ingestion_task.delay(task_file, "task").get()
    print(f"Hoàn tất task. Kết quả: {t_res}")
    
    # 3. Truy vấn cơ sở dữ liệu SQLite để xác minh logs trạng thái
    print("\n" + "="*80)
    print("KIỂM TRA CƠ SỞ DỮ LIỆU ĐỐI SOÁT TRẠNG THÁI (SQLite sync_jobs.db)")
    print("="*80)
    
    engine = SyncEngine()
    with sqlite3.connect(engine.db_path) as conn:
        print("\n--- Bảng sync_jobs (Theo dõi Job) ---")
        jobs_df = pd.read_sql_query("SELECT id, source_system, object_type, status, start_time, duration FROM sync_jobs", conn)
        print(jobs_df.to_string(index=False))
        
        print("\n--- Bảng sync_records (Lưu vết 5 trường định danh MDM) ---")
        records_df = pd.read_sql_query(
            "SELECT id, job_id, source_system, source_object, source_record_id, datahub_id, master_record_id FROM sync_records", 
            conn
        )
        print(records_df.to_string(index=False))

    # 4. Hiển thị thông điệp đầu ra đã được chuẩn hóa định dạng Canonical Schema
    print("\n" + "="*80)
    print("KIỂM TRA KẾT QUẢ ĐẦU RA JSON CHUẨN HÓA MDM")
    print("="*80)
    
    standardized_dir = os.path.join(base_dir, "data", "standardized")
    for file_name in os.listdir(standardized_dir):
        if file_name.endswith(".json"):
            file_path = os.path.join(standardized_dir, file_name)
            print(f"\nTệp kết quả: {file_name}")
            with open(file_path, "r", encoding="utf-8") as f:
                records = json.load(f)
                
            # Đưa vào DataFrame để hiển thị dạng bảng trực quan
            table_data = []
            for rec in records:
                table_data.append({
                    "source_system": rec["source_system"],
                    "source_object": rec["source_object"],
                    "source_record_id": rec["source_record_id"],
                    "datahub_id": rec["datahub_id"],
                    "master_record_id": rec["master_record_id"],
                    "payload_preview": str(rec["payload"])
                })
            df_view = pd.DataFrame(table_data)
            pd.set_option('display.max_colwidth', 50)
            print(df_view.to_string(index=False))

if __name__ == "__main__":
    run_demo()
