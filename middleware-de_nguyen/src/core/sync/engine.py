# [TASK 5] Triển khai Sync Framework
# State machine cho sync job/record: pending, validating, mapping, pushing_datahub, completed, failed, retrying.
# Lưu start_time, end_time, duration, error_detail.

import os
import sqlite3
from datetime import datetime

class SyncState:
    PENDING = "pending"
    VALIDATING = "validating"
    MAPPING = "mapping"
    PUSHING_DATAHUB = "pushing_datahub"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"

class SyncEngine:
    def __init__(self, db_path: str = None):
        if db_path is None:
            # Default database location inside the project's data directory
            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
            data_dir = os.path.join(base_dir, "data")
            os.makedirs(data_dir, exist_ok=True)
            db_path = os.path.join(data_dir, "sync_jobs.db")
        
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Create jobs table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sync_jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_system TEXT NOT NULL,
                    object_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    start_time TEXT NOT NULL,
                    end_time TEXT,
                    duration REAL,
                    error_detail TEXT
                )
            """)
            
            # Create records mapping table (MDM compliance)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sync_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id INTEGER NOT NULL,
                    source_system TEXT NOT NULL,
                    source_object TEXT NOT NULL,
                    source_record_id TEXT NOT NULL,
                    datahub_id TEXT NOT NULL,
                    master_record_id TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(job_id) REFERENCES sync_jobs(id)
                )
            """)
            conn.commit()

    def create_job(self, source_system: str, object_type: str) -> int:
        start_time = datetime.now().isoformat()
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO sync_jobs (source_system, object_type, status, start_time) VALUES (?, ?, ?, ?)",
                (source_system, object_type, SyncState.PENDING, start_time)
            )
            conn.commit()
            return cursor.lastrowid

    def update_job_status(self, job_id: int, status: str, error_detail: str = None):
        end_time = None
        duration = None
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            if status in [SyncState.COMPLETED, SyncState.FAILED]:
                end_time = datetime.now().isoformat()
                # Calculate duration
                cursor.execute("SELECT start_time FROM sync_jobs WHERE id = ?", (job_id,))
                row = cursor.fetchone()
                if row:
                    start_time = datetime.fromisoformat(row[0])
                    end_dt = datetime.fromisoformat(end_time)
                    duration = (end_dt - start_time).total_seconds()
            
            if end_time:
                cursor.execute(
                    "UPDATE sync_jobs SET status = ?, end_time = ?, duration = ?, error_detail = ? WHERE id = ?",
                    (status, end_time, duration, error_detail, job_id)
                )
            else:
                cursor.execute(
                    "UPDATE sync_jobs SET status = ?, error_detail = ? WHERE id = ?",
                    (status, error_detail, job_id)
                )
            conn.commit()

    def log_record_sync(self, job_id: int, source_system: str, source_object: str, source_record_id: str, datahub_id: str, master_record_id: str = None):
        created_at = datetime.now().isoformat()
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """INSERT INTO sync_records 
                   (job_id, source_system, source_object, source_record_id, datahub_id, master_record_id, created_at) 
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (job_id, source_system, source_object, str(source_record_id), datahub_id, master_record_id, created_at)
            )
            conn.commit()

    def get_job(self, job_id: int) -> dict:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM sync_jobs WHERE id = ?", (job_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_records_for_job(self, job_id: int) -> list:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM sync_records WHERE job_id = ?", (job_id,))
            return [dict(row) for row in cursor.fetchall()]

