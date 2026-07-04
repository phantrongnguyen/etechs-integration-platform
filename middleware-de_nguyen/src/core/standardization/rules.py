# [TASK 3] Rule chuẩn hóa dữ liệu
# Lập rule chuẩn hóa email lowercase, phone format, ngày tháng ISO, status/priority về LOV chuẩn...
# Assignee: Dương Ngọc Hân (DP) + Phan Trọng Nguyên (DE)

import re
from datetime import datetime

def normalize_email(email: str) -> str:
    return email.strip().lower() if email else ""

def normalize_phone(phone: str) -> str:
    # Remove non-numeric characters, format to standard if necessary
    if not phone:
        return ""
    digits = re.sub(r'\D', '', phone)
    return digits

def normalize_iso_date(date_str: str) -> str:
    # Normalize date/datetime to ISO format
    if not date_str:
        return ""
    try:
        dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        return dt.isoformat()
    except ValueError:
        return date_str
