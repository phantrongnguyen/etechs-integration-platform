import os

class Settings:
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "middleware-de-group")
    KAFKA_INGESTION_TOPIC = os.getenv("KAFKA_INGESTION_TOPIC", "raw-ingestion-events")
    KAFKA_DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "dead-letter")
    REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    DB_PATH = os.getenv("DB_PATH", "")  # Sẽ tự động gán trong SyncEngine nếu trống

settings = Settings()

