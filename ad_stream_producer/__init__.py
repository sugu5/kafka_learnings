"""
This file makes the 'ad_stream_producer' directory a Python package
and promotes key classes to the package level for easier imports.
"""
from .producer_service import ProducerService
from .schema import AdEvent
from .kafka_producer import AdKafkaProducer
from metrics import metrics
from .config import Config
__all__ = [
    "ProducerService",
    "AdEvent",
    "metrics",
    "Config",
    "AdKafkaProducer"
]