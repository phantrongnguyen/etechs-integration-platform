# [TASK 6] Triển khai Queue/Retry cho sync và xử lý bằng Celery + Pandas
import os
import sys
import uuid
import json
import traceback
from datetime import datetime
import pandas as pd

# Add source directory to Python path to ensure module resolution
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.core.queue.celery_app import app
from config import settings
from src.core.sync.engine import SyncEngine, SyncState
from src.core.standardization.rules import normalize_email, normalize_phone, normalize_iso_date

@app.task(name="tasks.process_ingestion_task")
def process_ingestion_task(file_path: str, object_type: str) -> dict:
    """
    Celery task to ingest a simulated JSON file, process it using Pandas,
    apply MDM identifier mapping, and track status.
    """
    engine = SyncEngine()
    job_id = engine.create_job("EOS", object_type)
    
    try:
        # Step 1: Validation phase
        engine.update_job_status(job_id, SyncState.VALIDATING)
        
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Source JSON file not found at {file_path}")
            
        with open(file_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)
            
        # Load into Pandas DataFrame
        df = pd.DataFrame(raw_data)
        if df.empty:
            raise ValueError(f"JSON file {file_path} is empty or has invalid structure")
            
        # Standardize standard columns if they exist
        # 1. Email standardization
        email_cols = [col for col in df.columns if "email" in col.lower()]
        for col in email_cols:
            df[col] = df[col].astype(str).apply(normalize_email)
            
        # 2. Phone standardization
        phone_cols = [col for col in df.columns if "phone" in col.lower() or "sdt" in col.lower()]
        for col in phone_cols:
            df[col] = df[col].astype(str).apply(normalize_phone)
            
        # 3. Date standardization
        date_cols = [col for col in df.columns if "date" in col.lower() or "created_at" in col.lower() or "updated_at" in col.lower()]
        for col in date_cols:
            df[col] = df[col].astype(str).apply(normalize_iso_date)
            
        # Step 2: Mapping phase (5 MDM Core Identifier Fields)
        engine.update_job_status(job_id, SyncState.MAPPING)
        
        # Determine source_record_id column
        # Look for typical ID columns
        id_candidates = ["id", f"{object_type}_id", "code", "employee_id"]
        id_col = None
        for cand in id_candidates:
            if cand in df.columns:
                id_col = cand
                break
        if not id_col:
            # fallback to the first column if no typical ID candidate found
            id_col = df.columns[0]
            
        # Apply the 5 MDM fields
        df["source_system"] = "EOS"
        df["source_object"] = object_type
        df["source_record_id"] = df[id_col].astype(str)
        
        # Generate UUIDs for datahub_id and master_record_id (Phase 1: same as datahub_id)
        df["datahub_id"] = [str(uuid.uuid4()) for _ in range(len(df))]
        df["master_record_id"] = df["datahub_id"]
        
        # Step 3: Pushing/Output data
        engine.update_job_status(job_id, SyncState.PUSHING_DATAHUB)
        
        # Save standardized data to local files (simulating DataHub submission)
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        output_dir = os.path.join(base_dir, "data", "standardized")
        os.makedirs(output_dir, exist_ok=True)
        
        output_file = os.path.join(output_dir, f"{object_type}_standardized_job_{job_id}.json")
        
        # Format payload structure as standard DataHub JSON (preserving the original columns in a 'payload' sub-object)
        standardized_records = []
        for _, row in df.iterrows():
            # Extract standard fields
            mdm_fields = {
                "source_system": row["source_system"],
                "source_object": row["source_object"],
                "source_record_id": row["source_record_id"],
                "datahub_id": row["datahub_id"],
                "master_record_id": row["master_record_id"],
            }
            
            # Exclude MDM fields from payload itself to keep it clean
            payload = row.drop(["source_system", "source_object", "source_record_id", "datahub_id", "master_record_id"]).to_dict()
            
            record = {
                **mdm_fields,
                "payload": payload,
                "metadata": {
                    "job_id": job_id,
                    "ingested_at": normalize_iso_date(datetime.now().isoformat())
                }
            }
            standardized_records.append(record)
            
            # Log sync record to SQLite database for auditing
            engine.log_record_sync(
                job_id=job_id,
                source_system=mdm_fields["source_system"],
                source_object=mdm_fields["source_object"],
                source_record_id=mdm_fields["source_record_id"],
                datahub_id=mdm_fields["datahub_id"],
                master_record_id=mdm_fields["master_record_id"]
            )
            
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(standardized_records, f, indent=2, ensure_ascii=False)
            
        # Update job to completed
        engine.update_job_status(job_id, SyncState.COMPLETED)
        
        return {
            "job_id": job_id,
            "status": "success",
            "records_processed": len(df),
            "output_file": output_file
        }
        
    except Exception as e:
        error_detail = traceback.format_exc()
        engine.update_job_status(job_id, SyncState.FAILED, error_detail=error_detail)
        return {
            "job_id": job_id,
            "status": "failed",
            "error": str(e),
            "error_detail": error_detail
        }
