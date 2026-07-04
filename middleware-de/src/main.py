import logging
from src.infrastructure.kafka.consumer import IngestionConsumer
from src.core.config import settings

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting middleware-de (Data Plane) Kafka Consumer...")
    consumer = IngestionConsumer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id=settings.KAFKA_CONSUMER_GROUP,
        topic=settings.KAFKA_INGESTION_TOPIC
    )
    
    try:
        consumer.start()
    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
    finally:
        consumer.stop()

if __name__ == "__main__":
    main()
