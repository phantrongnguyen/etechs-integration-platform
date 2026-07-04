from pydantic import BaseModel
from typing import Optional, Dict, Any

class CanonicalSchema(BaseModel):
    """
    Mô hình dữ liệu tiêu chuẩn (Canonical Data Model - CDM) chuẩn hóa MDM.
    Tất cả dữ liệu đầu ra sau khi xử lý sẽ được transform về định dạng này.
    """
    source_system: str
    source_object: str
    source_record_id: str
    datahub_id: str
    master_record_id: Optional[str] = None
    payload: Dict[str, Any]
    metadata: Optional[Dict[str, Any]] = None

