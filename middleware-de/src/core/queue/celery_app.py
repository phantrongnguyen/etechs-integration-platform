# [TASK 6] Triển khai Queue/Retry cho sync
# Cấu hình Redis 7 và Celery optional để xử lý sync nền
from celery import Celery
from config import settings

app = Celery("middleware_de", broker=settings.REDIS_URL, backend=settings.REDIS_URL)

app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="Asia/Ho_Chi_Minh",
    enable_utc=True,
)
