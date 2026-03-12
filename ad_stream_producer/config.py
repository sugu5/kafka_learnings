class Config:
    TOPIC = "ads_events"
    SCHEMA_PATH = "ad_event_update.avsc"
    KAFKA_BOOTSTRAP_SERVERS = ["localhost:9095", "localhost:9093", "localhost:9094"]
    POSTGRES_URL = "jdbc:postgresql://localhost:5432/postgres"
    POSTGRES_PROPERTIES = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }
    POSTGRES_TABLE = "event_schema.ad_events"
    SCHEMA_REGISTRY_URL = "http://localhost:8081"
