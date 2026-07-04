import json
import logging
from confluent_kafka import Consumer, KafkaError
from src.application.pipeline import DataPipeline

logger = logging.getLogger(__name__)

class IngestionConsumer:
    def __init__(self, bootstrap_servers: str, group_id: str, topic: str):
        self.topic = topic
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest'
        })
        self.pipeline = DataPipeline()
        self.running = False

    def start(self):
        self.consumer.subscribe([self.topic])
        self.running = True
        logger.info(f"Subscribed to topic: {self.topic}")
        
        while self.running:
            msg = self.consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Consumer error: {msg.error()}")
                    continue

            try:
                payload = json.loads(msg.value().decode('utf-8'))
                self.pipeline.process(payload)
            except Exception as e:
                logger.error(f"Failed to process message: {e}")
                # TODO: route to DLQ topic here
                # self.dlq_producer.produce(payload, error=str(e))

    def stop(self):
        self.running = False
        self.consumer.close()
        logger.info("Consumer connection closed.")
