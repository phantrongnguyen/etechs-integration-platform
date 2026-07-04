import os
import sys
import json
import pandas as pd
import sqlite3

# Ensure current directory is in python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
if sys.platform.startswith("win"):
    import io
    sys.stdout.reconfigure(encoding='utf-8')


# 1. Generate Mock Data
from tests.simulate_data import generate_mock_data
generate_mock_data()

# 2. Configure Celery for Eager Execution (no Redis needed for the local demo run)
from src.core.queue.celery_app import app
app.conf.update(
    task_always_eager=True,
    task_eager_propagates=True
)

from src.core.queue.tasks import process_ingestion_task
from src.core.sync.engine import SyncEngine

def run_demo():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    raw_dir = os.path.join(base_dir, "data", "raw")
    
    user_file = os.path.join(raw_dir, "users.json")
    project_file = os.path.join(raw_dir, "projects.json")
    task_file = os.path.join(raw_dir, "tasks.json")
    
    print("\n" + "="*80)
    print("STARTING PIPELINE SIMULATION (CELERY + PANDAS)")
    print("="*80)
    
    # Run user ingestion task
    print("\n--- Ingesting Users ---")
    u_res = process_ingestion_task.delay(user_file, "user").get()
    print(f"Task completed. Result: {u_res}")
    
    # Run project ingestion task
    print("\n--- Ingesting Projects ---")
    p_res = process_ingestion_task.delay(project_file, "project").get()
    print(f"Task completed. Result: {p_res}")
    
    # Run task ingestion task
    print("\n--- Ingesting Tasks ---")
    t_res = process_ingestion_task.delay(task_file, "task").get()
    print(f"Task completed. Result: {t_res}")
    
    # 3. Read SQLite Database to Verify Job Logs
    print("\n" + "="*80)
    print("VERIFYING STATE DATABASE (SQLite sync_jobs.db)")
    print("="*80)
    
    engine = SyncEngine()
    with sqlite3.connect(engine.db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        print("\n--- Sync Jobs Table ---")
        jobs_df = pd.read_sql_query("SELECT * FROM sync_jobs", conn)
        print(jobs_df.to_string(index=False))
        
        print("\n--- Sync Records Table (Audit trail - 5 MDM Fields) ---")
        records_df = pd.read_sql_query("SELECT id, job_id, source_system, source_object, source_record_id, datahub_id, master_record_id FROM sync_records", conn)
        print(records_df.to_string(index=False))

    # 4. Display Standardized JSON Payloads
    print("\n" + "="*80)
    print("VERIFYING STANDARDIZED OUTPUT PAYLOADS WITH 5 MDM FIELDS")
    print("="*80)
    
    standardized_dir = os.path.join(base_dir, "data", "standardized")
    for file_name in os.listdir(standardized_dir):
        if file_name.endswith(".json"):
            file_path = os.path.join(standardized_dir, file_name)
            print(f"\nFile: {file_name}")
            with open(file_path, "r", encoding="utf-8") as f:
                records = json.load(f)
                
            # Convert to Pandas DataFrame for a clean tabular view of MDM fields and payloads
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
