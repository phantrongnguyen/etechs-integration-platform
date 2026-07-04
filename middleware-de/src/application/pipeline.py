import uuid
import logging
import pandas as pd
from datetime import datetime
from src.infrastructure.redis.rule_client import RuleClient
from src.core.standardization.rules import normalize_email, normalize_phone, normalize_iso_date
from src.domain.entities import CanonicalSchema

logger = logging.getLogger(__name__)

class DataPipeline:
    """
    Data Pipeline xử lý làm sạch, chuẩn hóa và ánh xạ dữ liệu (MDM) bằng Pandas.
    """
    def __init__(self):
        # Khởi tạo RuleClient nhưng bắt lỗi để không làm nghẽn luồng nếu Redis chưa chạy
        try:
            self.rule_client = RuleClient()
        except Exception as e:
            logger.warning(f"Không thể kết nối Redis trong DataPipeline: {e}")
            self.rule_client = None

    def process(self, records: list, object_type: str, job_id: int = None, sync_engine = None) -> list:
        """
        Xử lý danh sách các bản ghi thô đầu vào bằng Pandas, áp dụng chuẩn hóa
        và gán 5 trường định danh MDM.
        """
        if not records:
            return []
            
        logger.info(f"Bắt đầu xử lý {len(records)} bản ghi của đối tượng {object_type}")
        
        # 1. Load vào Pandas DataFrame để xử lý hàng loạt
        df = pd.DataFrame(records)
        
        # 2. Làm sạch dữ liệu theo các cột đặc trưng nếu có
        # Email
        email_cols = [col for col in df.columns if "email" in col.lower()]
        for col in email_cols:
            df[col] = df[col].astype(str).apply(normalize_email)
            
        # Số điện thoại
        phone_cols = [col for col in df.columns if "phone" in col.lower() or "sdt" in col.lower()]
        for col in phone_cols:
            df[col] = df[col].astype(str).apply(normalize_phone)
            
        # Ngày tháng
        date_cols = [col for col in df.columns if "date" in col.lower() or "created_at" in col.lower() or "updated_at" in col.lower()]
        for col in date_cols:
            df[col] = df[col].astype(str).apply(normalize_iso_date)
            
        # 3. Xác định trường ID nguồn (source_record_id)
        id_candidates = ["id", f"{object_type}_id", "code", "employee_id"]
        id_col = None
        for cand in id_candidates:
            if cand in df.columns:
                id_col = cand
                break
        if not id_col:
            id_col = df.columns[0]  # mặc định lấy cột đầu tiên
            
        # 4. Ánh xạ chuẩn 5 trường định danh MDM
        df["source_system"] = "EOS"
        df["source_object"] = object_type
        df["source_record_id"] = df[id_col].astype(str)
        df["datahub_id"] = [str(uuid.uuid4()) for _ in range(len(df))]
        df["master_record_id"] = df["datahub_id"]  # Giai đoạn 1: master_record_id = datahub_id
        
        # 5. Định dạng dữ liệu đầu ra theo Canonical Schema
        standardized_records = []
        for _, row in df.iterrows():
            mdm_fields = {
                "source_system": row["source_system"],
                "source_object": row["source_object"],
                "source_record_id": row["source_record_id"],
                "datahub_id": row["datahub_id"],
                "master_record_id": row["master_record_id"],
            }
            
            # Tách payload gốc (loại trừ các trường MDM vừa thêm vào)
            original_columns = [col for col in df.columns if col not in ["source_system", "source_object", "source_record_id", "datahub_id", "master_record_id"]]
            payload = row[original_columns].to_dict()
            
            # Ghi log lịch sử ánh xạ ID vào SQLite nếu có sync_engine
            if sync_engine and job_id:
                sync_engine.log_record_sync(
                    job_id=job_id,
                    source_system=mdm_fields["source_system"],
                    source_object=mdm_fields["source_object"],
                    source_record_id=mdm_fields["source_record_id"],
                    datahub_id=mdm_fields["datahub_id"],
                    master_record_id=mdm_fields["master_record_id"]
                )
                
            # Đóng gói theo CanonicalSchema
            canonical_rec = CanonicalSchema(
                source_system=mdm_fields["source_system"],
                source_object=mdm_fields["source_object"],
                source_record_id=mdm_fields["source_record_id"],
                datahub_id=mdm_fields["datahub_id"],
                master_record_id=mdm_fields["master_record_id"],
                payload=payload,
                metadata={
                    "job_id": job_id,
                    "processed_at": datetime.now().isoformat()
                }
            )
            standardized_records.append(canonical_rec.model_dump())
            
        logger.info(f"Đã xử lý xong {len(standardized_records)} bản ghi.")
        return standardized_records
