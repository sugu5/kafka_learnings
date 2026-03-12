import traceback
from kafka import KafkaProducer
from .logger import get_logger
from metrics.metrics import events_sent, events_failed

logger = get_logger("producer")

class AdKafkaProducer:

    def __init__(self, bootstrap_servers):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks="all",
                retries=10,
                linger_ms=20,
                batch_size=32768,
                compression_type="gzip",
                max_in_flight_requests_per_connection=5
            )
            logger.info("Kafka producer initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            logger.error(traceback.format_exc())
            raise

    def send_event(self, topic, key, value):
        """Sends an already-serialized event to Kafka."""
        try:
            # The key_serializer will handle encoding the key.
            future = self.producer.send(
                topic=topic,
                key=key,
                value=value
            )
            
            result = future.get(timeout=10)
            
            logger.info(
                f"Event with key '{key}' delivered to {result.topic} partition {result.partition} offset {result.offset}"
            )
            events_sent.inc()
            
        except Exception as e:
            events_failed.inc()
            logger.error(f"Failed to send event with key '{key}' to topic {topic}: {e}")
            logger.error(traceback.format_exc())

    def close(self):
        logger.info("Closing Kafka producer")
        try:
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka producer closed successfully")
        except Exception as e:
            logger.error(f"Error closing Kafka producer: {e}")