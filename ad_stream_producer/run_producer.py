import os
import sys
import logging
import signal
import time
from pathlib import Path
from datetime import datetime

# Add project root to path BEFORE importing local modules
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from ad_stream_producer import ProducerService, metrics
from ad_stream_producer import Config
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
SCHEMA_PATH = PROJECT_ROOT / "schema" / Config.SCHEMA_PATH
# Configure structured logging for production
from logger import get_logger

logger = get_logger("run_producer")


# Global flag for graceful shutdown
shutdown_requested = False

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    global shutdown_requested
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
    shutdown_requested = True

def validate_configuration():
    """Validate all required configuration before starting"""
    logger.info("Validating configuration...")

    # Check schema file exists
    if not SCHEMA_PATH.exists():
        raise FileNotFoundError(f"Schema file not found: {SCHEMA_PATH}")

    # Check Kafka brokers are configured
    if not Config.KAFKA_BOOTSTRAP_SERVERS:
        raise ValueError("KAFKA_BOOTSTRAP_SERVERS not configured")

    # Check topic is configured
    if not Config.TOPIC:
        raise ValueError("TOPIC not configured")

    # Check Schema Registry URL
    if not Config.SCHEMA_REGISTRY_URL:
        raise ValueError("SCHEMA_REGISTRY_URL not configured")

    logger.info("✓ Configuration validation passed")

def get_schema_registry_client():
    """Initialize and return Avro serializer with proper error handling"""
    try:
        logger.info("Initializing Schema Registry client...")
        schema_registry_conf = {"url": Config.SCHEMA_REGISTRY_URL}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)

        logger.info(f"Loading Avro schema from: {SCHEMA_PATH}")
        with open(SCHEMA_PATH, 'r') as f:
            schema_str = f.read()

        logger.info("Creating Avro serializer...")
        avro_serializer = AvroSerializer(schema_registry_client, schema_str)

        logger.info("✓ Schema Registry client initialized successfully")
        return avro_serializer

    except Exception as e:
        logger.error(f"Failed to initialize Schema Registry client: {str(e)}")
        raise

def main():
    """Main application entry point"""
    global shutdown_requested

    start_time = datetime.now()
    logger.info("="*80)
    logger.info("Kafka Producer Service Starting")
    logger.info(f"Start time: {start_time}")
    logger.info("="*80)

    try:
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Validate configuration
        validate_configuration()

        # Initialize components
        logger.info("Initializing producer components...")
        avro_serializer = get_schema_registry_client()

        logger.info("Starting metrics server...")
        metrics.start_metrics_server()

        # Get configuration
        topic = Config.TOPIC
        bootstrap_servers = Config.KAFKA_BOOTSTRAP_SERVERS

        logger.info("Configuration:")
        logger.info(f"  Topic: {topic}")
        logger.info(f"  Bootstrap servers: {', '.join(bootstrap_servers)}")
        logger.info(f"  Rate: 10 events/sec")

        # Create and start producer service
        logger.info("Creating producer service...")
        service = ProducerService(topic, bootstrap_servers, avro_serializer)

        logger.info("✓✓✓ Producer service initialized successfully")
        logger.info("="*80)
        logger.info("Starting event production...")
        logger.info("="*80)

        # Run the producer service
        service.run(rate_per_sec=10)

    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
    except Exception as e:
        logger.error(f"Critical error in producer service: {str(e)}")
        logger.error("Full traceback:", exc_info=True)
        sys.exit(1)
    finally:
        # Cleanup and final logging
        end_time = datetime.now()
        runtime = end_time - start_time

        logger.info("="*80)
        logger.info("Producer Service Shutdown")
        logger.info(f"End time: {end_time}")
        logger.info(f"Total runtime: {runtime}")
        logger.info("="*80)

        # Ensure clean exit
        sys.exit(0)

if __name__ == "__main__":
    main()
