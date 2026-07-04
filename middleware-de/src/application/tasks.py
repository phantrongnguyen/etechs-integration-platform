import os
import json
import logging
import traceback
from datetime import datetime
from src.core.celery_app import app
from src.core.sync import SyncEngine, SyncState
from src.application.pipeline import DataPipeline

logger = logging.getLogger(__name__)

@app.task(name="tasks.process_ingestion_task")
def process_ingestion_task(file_path: str, object_type: str) -> dict:
    """
    Tác vụ Celery để xử lý nạp tệp JSON giả lập bất đồng bộ qua Pandas và chuẩn hóa MDM.
    """
    engine = SyncEngine()
    job_id = engine.create_job(source_system="EOS", object_type=object_type)
    
    try:
        # Bước 1: Đọc và xác thực dữ liệu nguồn
        engine.update_job_status(job_id, SyncState.VALIDATING)
        
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Không tìm thấy tệp JSON nguồn tại: {file_path}")
            
        with open(file_path, "r", encoding="utf-8") as f:
            raw_records = json.load(f)
            
        if not isinstance(raw_records, list):
            # Nếu JSON gốc không phải là list, bọc lại thành list
            if isinstance(raw_records, dict):
                raw_records = [raw_records]
            else:
                raise ValueError("Định dạng dữ liệu JSON nguồn phải là một list hoặc dict")
                
        if not raw_records:
            raise ValueError("Tệp JSON nguồn không chứa dữ liệu")
            
        # Bước 2: Thực hiện chuẩn hóa & Ánh xạ MDM qua pipeline
        engine.update_job_status(job_id, SyncState.MAPPING)
        
        pipeline = DataPipeline()
        standardized_records = pipeline.process(
            records=raw_records, 
            object_type=object_type, 
            job_id=job_id, 
            sync_engine=engine
        )
        
        # Bước 3: Lưu trữ dữ liệu đầu ra đã chuẩn hóa (Giả lập đẩy lên DataHub)
        engine.update_job_status(job_id, SyncState.PUSHING_DATAHUB)
        
        # Thư mục lưu trữ kết quả đầu ra
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        output_dir = os.path.join(base_dir, "data", "standardized")
        os.makedirs(output_dir, exist_ok=True)
        
        output_file = os.path.join(output_dir, f"{object_type}_standardized_job_{job_id}.json")
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(standardized_records, f, indent=2, ensure_ascii=False)
            
        # Cập nhật công việc hoàn tất thành công
        engine.update_job_status(job_id, SyncState.COMPLETED)
        logger.info(f"Hoàn thành job {job_id} xử lý đối tượng {object_type}. Kết quả ghi nhận tại {output_file}")
        
        return {
            "job_id": job_id,
            "status": "success",
            "records_processed": len(standardized_records),
            "output_file": output_file
        }
        
    except Exception as e:
        error_detail = traceback.format_exc()
        logger.error(f"Lỗi khi xử lý job {job_id}: {error_detail}")
        engine.update_job_status(job_id, SyncState.FAILED, error_detail=error_detail)
        return {
            "job_id": job_id,
            "status": "failed",
            "error": str(e),
            "error_detail": error_detail
        }
