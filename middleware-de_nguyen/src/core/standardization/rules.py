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
    if not date_str:
        return ""
    date_str = date_str.strip()
    try:
        dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        return dt.isoformat()
    except ValueError:
        pass
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
