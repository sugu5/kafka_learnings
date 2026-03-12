from prometheus_client import Counter, Histogram, Gauge, start_http_server

# counters
events_sent = Counter("events_sent_total", "Total events produced")
events_failed = Counter("events_failed_total", "Total failed events")

# rate
events_per_second = Gauge("events_per_second", "Events produced per second")

# latency
producer_latency = Histogram(
    "producer_latency_seconds",
    "Kafka producer delivery latency"
)

# lag simulation
producer_lag = Gauge(
    "producer_queue_size",
    "Kafka producer queue backlog"
)

def start_metrics_server():
    start_http_server(8001)