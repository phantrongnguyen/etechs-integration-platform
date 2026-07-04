import re
from datetime import datetime

def normalize_email(email: str) -> str:
    return email.strip().lower() if email else ""

def normalize_phone(phone: str) -> str:
    if not phone:
        return ""
    # Chỉ giữ các chữ số
    digits = re.sub(r'\D', '', phone)
    return digits

def normalize_iso_date(date_str: str) -> str:
    if not date_str:
        return ""
    date_str = date_str.strip()
    try:
        # Thay thế ký tự 'Z' bằng '+00:00' để python fromisoformat đọc được
        dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        return dt.isoformat()
    except ValueError:
        pass
    
    # Một số định dạng ngày thông dụng khác để parse
    formats = [
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%dT%H:%M:%S",
        "%Y/%m/%d",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y",
        "%Y-%m-%d",
        "%d-%m-%Y %H:%M:%S",
        "%d-%m-%Y",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.isoformat()
        except ValueError:
            continue
    return date_str
