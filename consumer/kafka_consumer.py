import os
import sys
import json
import shutil
import logging
from pathlib import Path
from datetime import datetime
from ad_stream_producer import Config
# Add project root to path BEFORE importing local modules
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
    handlers=[
        logging.FileHandler(str(PROJECT_ROOT / "output" / "consumer.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

logger.info("="*80)
logger.info("Kafka Consumer Starting")
logger.info("="*80)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Paths & Config
CHECKPOINT_PATH = str(PROJECT_ROOT / "output" / "checkpoints_final_robust")
CORRUPTED_RECORDS_PATH = str(PROJECT_ROOT / "output" / "corrupted_records")
OUTPUT_PARQUET_PATH = str(PROJECT_ROOT / "output" / "ads_events_parquet")
SCHEMA_REGISTRY_URL = "http://localhost:8081"

# PostgreSQL Config - Update these with your local PostgreSQL credentials
POSTGRES_URL = "jdbc:postgresql://localhost:5432/postgres"
POSTGRES_PROPERTIES = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}
POSTGRES_TABLE = "event_schema.ad_events"  # Updated table name based on your schema

logger.info(f"PostgreSQL Configuration:")
logger.info(f"  URL: {POSTGRES_URL}")
logger.info(f"  Table: {POSTGRES_TABLE}")
logger.info(f"  User: {POSTGRES_PROPERTIES.get('user')}")
logger.info(f"  Driver: {POSTGRES_PROPERTIES.get('driver')}")

# Clear stale checkpoints and output
for path in [CHECKPOINT_PATH, CORRUPTED_RECORDS_PATH, OUTPUT_PARQUET_PATH]:
    if Path(path).exists():
        shutil.rmtree(path)
        print(f"Cleared: {path}")

# Define the final schema for the parsed JSON data.
# This matches the ad_event_update.avsc schema
output_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("campaign_id", IntegerType(), True),
    StructField("ad_id", IntegerType(), True),
    StructField("device", StringType(), True),
    StructField("country", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("ad_creative_id", StringType(), True)
])

# --- Spark Session ---
logger.info("Creating Spark session...")
spark = (SparkSession.builder
         .appName("KafkaSparkStreamingRobust")
         .config("spark.jars.packages",
                 "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                 "org.postgresql:postgresql:42.6.0")
         .config("spark.driver.host", "127.0.0.1")
         # Suppress KAFKA-1894 warning and improve Kafka consumer stability
         .config("spark.streaming.kafka.consumer.cache.enabled", "false")
         .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
         .getOrCreate())

# Set log level to reduce noise
spark.sparkContext.setLogLevel("ERROR")

logger.info("✓ Spark session created successfully")
logger.info(f"  Spark version: {spark.version}")
logger.info(f"  App Name: {spark.sparkContext.appName}")

# Broadcast the schema registry URL for use in UDF
schema_registry_url_broadcast = spark.sparkContext.broadcast(SCHEMA_REGISTRY_URL)

# Pre-fetch the schema from Schema Registry and broadcast it
def get_avro_schema_from_registry(schema_registry_url, subject):
    """Fetch the latest schema from Schema Registry"""
    import requests
    response = requests.get(f"{schema_registry_url}/subjects/{subject}/versions/latest")
    if response.status_code == 200:
        schema_json = response.json()
        return schema_json.get("schema")
    return None

# Get the schema for ads_events-value subject
avro_schema_str = get_avro_schema_from_registry(SCHEMA_REGISTRY_URL, f"{Config.TOPIC}-value")
if avro_schema_str:
    avro_schema = json.loads(avro_schema_str)
    logger.info(f"✓ Fetched schema from registry: {avro_schema.get('name', 'Unknown')}")
    logger.debug(f"Schema fields: {[f['name'] for f in avro_schema.get('fields', [])]}")
else:
    # Fallback to local schema
    schema_path = PROJECT_ROOT / "schema" / "ad_event_update.avsc"
    logger.warning("Schema Registry unavailable, using local schema file")
    with open(schema_path) as f:
        avro_schema = json.load(f)
    logger.info(f"✓ Using local schema file: {schema_path}")

# Broadcast the parsed schema for UDF use
schema_broadcast = spark.sparkContext.broadcast(avro_schema)


def deserialize_avro_message(data):
    """
    Deserializes a single message using the Confluent Avro wire format.

    Confluent Avro wire format:
    - Byte 0: Magic byte (0)
    - Bytes 1-4: Schema ID (big-endian int)
    - Bytes 5+: Avro binary data
    """
    if data is None:
        return None
    try:
        # Import inside UDF for Spark serialization
        import fastavro
        from io import BytesIO
        import json

        # Skip the first 5 bytes (magic byte + schema ID)
        if len(data) < 5:
            return None

        # Get the schema from broadcast
        schema = schema_broadcast.value

        # Read Avro data from bytes (skip magic byte and schema id)
        avro_bytes = BytesIO(data[5:])
        record = fastavro.schemaless_reader(avro_bytes, schema)

        # Handle nullable fields - fastavro returns None for null union values
        # Convert the record to ensure JSON compatibility
        return json.dumps(record, default=str)
    except Exception as e:
        print(f"Deserialization error: {e}")
        return None


deserialize_udf = udf(deserialize_avro_message, StringType())

# --- Data Processing Pipeline ---
logger.info("Connecting to Kafka topic: ads_events")
logger.info(f"  Bootstrap servers: localhost:9093,localhost:9094,localhost:9095")
logger.info(f"  Starting from: earliest")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9093,localhost:9094,localhost:9095") \
    .option("subscribe", "ads_events") \
    .option("startingOffsets", "earliest") \
    .option("kafka.session.timeout.ms", "30000") \
    .option("kafka.request.timeout.ms", "40000") \
    .option("kafka.max.poll.interval.ms", "300000") \
    .option("failOnDataLoss", "false") \
    .load()

logger.info("✓ Kafka stream connected successfully")


# --- Sink Logic with Error Handling ---
def process_batch(batch_df, epoch_id):
    """
    Processes a micro-batch: deserializes Avro, splits good/bad records,
    and writes them to different destinations.
    - Good records -> PostgreSQL database
    - Bad records -> Local output folder
    """
    batch_start_time = datetime.now()
    logger.info(f"{'='*80}")
    logger.info(f"[BATCH {epoch_id}] Starting batch processing at {batch_start_time}")
    logger.info(f"{'='*80}")

    if batch_df.isEmpty():
        logger.warning(f"[BATCH {epoch_id}] No records in batch")
        return

    total_records = batch_df.count()
    logger.info(f"[BATCH {epoch_id}] Total records in batch: {total_records}")

    # 1. Use the robust UDF to deserialize Avro data
    logger.info(f"[BATCH {epoch_id}] Step 1: Deserializing Avro messages...")
    deserialized_df = batch_df.select(
        deserialize_udf(col("value")).alias("json_or_corrupted")
    )
    deserialized_df.cache()
    logger.info(f"[BATCH {epoch_id}] ✓ Deserialization complete")

    # 2. Attempt to parse the JSON. Corrupted records will result in null.
    logger.info(f"[BATCH {epoch_id}] Step 2: Parsing JSON and validating schema...")
    parsed_df = deserialized_df.withColumn("data", from_json(col("json_or_corrupted"), output_schema))
    parsed_df.cache()

    good_records = parsed_df.where("data is not null").select("data.*")
    bad_records = parsed_df.where("data is null AND json_or_corrupted is not null").select(
        col("json_or_corrupted").alias("corrupted_payload")
    )

    good_count = good_records.count()
    bad_count = bad_records.count()
    logger.info(f"[BATCH {epoch_id}] ✓ Parse complete: {good_count} good records, {bad_count} bad records")

    # Convert data types to match PostgreSQL schema exactly
    logger.info(f"[BATCH {epoch_id}] Step 3: Converting data types for PostgreSQL...")
    if not good_records.isEmpty():
        logger.debug(f"[BATCH {epoch_id}] Before conversion - Sample record:")
        good_records.show(1, truncate=False)

        converted_records = good_records \
            .withColumn("event_id", col("event_id").cast("string")) \
            .withColumn("event_time", to_timestamp(col("event_time"))) \
            .withColumn("user_id", col("user_id").cast("string")) \
            .withColumn("campaign_id", col("campaign_id").cast(IntegerType())) \
            .withColumn("ad_id", col("ad_id").cast(IntegerType())) \
            .withColumn("device", col("device").cast("string")) \
            .withColumn("country", col("country").cast("string")) \
            .withColumn("event_type", col("event_type").cast("string")) \
            .withColumn("ad_creative_id", col("ad_creative_id").cast("string"))

        converted_records.cache()

        logger.debug(f"[BATCH {epoch_id}] After conversion - Sample record:")
        converted_records.show(1, truncate=False)

        logger.info(f"[BATCH {epoch_id}] ✓ Data type conversion complete")
        logger.info(f"[BATCH {epoch_id}] Converted record schema:")
        for field in converted_records.schema.fields:
            logger.info(f"[BATCH {epoch_id}]   - {field.name}: {field.dataType}")
    else:
        converted_records = good_records
        logger.warning(f"[BATCH {epoch_id}] No good records to convert")

    # 3. Write good records to PostgreSQL database
    if not good_records.isEmpty():
        write_start = datetime.now()
        logger.info(f"[BATCH {epoch_id}] Step 4: Writing {good_count} records to PostgreSQL...")
        logger.info(f"[BATCH {epoch_id}] Connection Details:")
        logger.info(f"[BATCH {epoch_id}]   URL: {POSTGRES_URL}")
        logger.info(f"[BATCH {epoch_id}]   Table: {POSTGRES_TABLE}")
        logger.info(f"[BATCH {epoch_id}]   Mode: append")

        try:
            logger.info(f"[BATCH {epoch_id}] Initiating JDBC write...")
            converted_records.write \
                .jdbc(url=POSTGRES_URL,
                      table=POSTGRES_TABLE,
                      mode="append",
                      properties=POSTGRES_PROPERTIES)

            write_duration = (datetime.now() - write_start).total_seconds()
            logger.info(f"[BATCH {epoch_id}] ✓✓✓ SUCCESSFULLY wrote {good_count} records to PostgreSQL in {write_duration:.2f}s")
            logger.info(f"[BATCH {epoch_id}] Sample records written:")
            converted_records.show(3, truncate=False)

        except Exception as e:
            logger.error(f"[BATCH {epoch_id}] ✗✗✗ ERROR writing to PostgreSQL: {str(e)}")
            logger.error(f"[BATCH {epoch_id}] Exception type: {type(e).__name__}")
            import traceback
            logger.error(f"[BATCH {epoch_id}] Full traceback:")
            for line in traceback.format_exc().split('\n'):
                logger.error(f"[BATCH {epoch_id}]   {line}")

            # Fallback: write to parquet if PostgreSQL fails
            logger.info(f"[BATCH {epoch_id}] Attempting fallback: Writing to Parquet...")
            try:
                good_records.write.mode("append").parquet(OUTPUT_PARQUET_PATH)
                logger.info(f"[BATCH {epoch_id}] ✓ Fallback successful: Parquet write completed")
            except Exception as parquet_error:
                logger.error(f"[BATCH {epoch_id}] ✗ Fallback failed: {str(parquet_error)}")

    # 4. Write bad/corrupted records to local output folder
    if not bad_records.isEmpty():
        logger.info(f"[BATCH {epoch_id}] Step 5: Writing {bad_count} corrupted records to {CORRUPTED_RECORDS_PATH}...")
        try:
            bad_records.repartition(1).write.mode("append").format("json").save(CORRUPTED_RECORDS_PATH)
            logger.info(f"[BATCH {epoch_id}] ✓ Wrote {bad_count} corrupted records successfully")
        except Exception as e:
            logger.error(f"[BATCH {epoch_id}] ✗ Error writing corrupted records: {str(e)}")

    batch_duration = (datetime.now() - batch_start_time).total_seconds()
    logger.info(f"[BATCH {epoch_id}] Batch processing complete in {batch_duration:.2f}s")
    logger.info(f"{'='*80}")


# --- Stream Execution ---
query = (df.writeStream
         .foreachBatch(process_batch)
         .outputMode("append")
         .option("checkpointLocation", CHECKPOINT_PATH)
         .start())

logger.info("✓✓✓ Streaming query started successfully!")
logger.info(f"Query ID: {query.id}")
logger.info(f"Run ID: {query.runId}")
logger.info(f"Active: {query.isActive}")
logger.info("="*80)
logger.info("Awaiting stream termination (Processing batches in real-time)...")
logger.info("="*80)

query.awaitTermination()