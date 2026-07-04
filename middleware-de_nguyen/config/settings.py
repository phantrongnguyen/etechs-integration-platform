# [DE Config] Settings for Database, Redis, Celery, and System Environments
import os

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", 5432))
DB_NAME = os.getenv("DB_NAME", "middleware_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# External endpoints
EOS_API_URL = os.getenv("EOS_API_URL", "http://localhost:8001")
DATAHUB_API_URL = os.getenv("DATAHUB_API_URL", "http://localhost:8002")
