# Databricks notebook source
"""
Kafka Integration Pipeline for Spark Declarative Pipelines (SDP)

This example demonstrates:
- Reading from Kafka topics
- Processing Kafka messages with Avro/JSON
- Multiple Kafka sources
- Watermarking and late data handling
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType


# COMMAND ----------
# Bronze Layer - Raw Kafka ingestion
# COMMAND ----------

dp.create_streaming_table(
    name="bronze_kafka_events",
    comment="Raw events from Kafka topic"
)

@dp.append_flow(
    target="bronze_kafka_events",
    name="ingest_kafka_events"
)
def ingest_kafka_events():
    """
    Read raw data from Kafka topic.
    Kafka messages have: key, value, topic, partition, offset, timestamp
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "{{kafka_brokers}}")  # Set via config
        .option("subscribe", "events")  # Topic name
        .option("startingOffsets", "earliest")  # Or "latest" for prod
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", "{{kafka_sasl_config}}")
        .load()
        .select(
            F.col("key").cast("string").alias("event_key"),
            F.col("value").cast("string").alias("event_value"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_timestamp")
        )
    )


# COMMAND ----------
# Silver Layer - Parsed JSON events
# COMMAND ----------

# Define expected schema for JSON payload
event_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("user_id", StringType(), True),
    StructField("event_timestamp", TimestampType(), False),
    StructField("properties", StringType(), True)  # Nested JSON
])

dp.create_streaming_table(
    name="silver_parsed_events",
    comment="Parsed and validated Kafka events",
    expect_all_or_drop={
        "valid_json": "parsed_value IS NOT NULL",
        "valid_event_id": "event_id IS NOT NULL",
        "valid_event_type": "event_type IS NOT NULL"
    }
)

@dp.append_flow(
    target="silver_parsed_events",
    name="parse_kafka_json"
)
def parse_kafka_json():
    """
    Parse JSON from Kafka value field and extract fields.
    Drop records that don't match expected schema.
    """
    return (
        spark.readStream.table("LIVE.bronze_kafka_events")
        .withColumn("parsed_value", F.from_json(F.col("event_value"), event_schema))
        .select(
            "event_key",
            "kafka_timestamp",
            "topic",
            "partition",
            "offset",
            F.col("parsed_value.event_id").alias("event_id"),
            F.col("parsed_value.event_type").alias("event_type"),
            F.col("parsed_value.user_id").alias("user_id"),
            F.col("parsed_value.event_timestamp").alias("event_timestamp"),
            F.col("parsed_value.properties").alias("properties")
        )
        .withColumn("processing_time", F.current_timestamp())
    )


# COMMAND ----------
# Example: Avro-formatted Kafka messages
# COMMAND ----------

dp.create_streaming_table(
    name="bronze_kafka_avro",
    comment="Raw Avro-encoded messages from Kafka"
)

@dp.append_flow(
    target="bronze_kafka_avro",
    name="ingest_kafka_avro"
)
def ingest_kafka_avro():
    """
    Read Avro-encoded messages from Kafka with Schema Registry.
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "{{kafka_brokers}}")
        .option("subscribe", "avro_events")
        .option("startingOffsets", "latest")
        .load()
        .select(
            F.col("key").cast("string"),
            F.expr("""
                from_avro(
                    value,
                    '{{avro_schema}}',
                    map('mode', 'PERMISSIVE')
                )
            """).alias("avro_data"),
            F.col("timestamp").alias("kafka_timestamp")
        )
        .select(
            "key",
            "kafka_timestamp",
            "avro_data.*"
        )
    )


# COMMAND ----------
# Gold Layer - Windowed aggregations with watermarking
# COMMAND ----------

@dp.table(
    name="gold_event_metrics_hourly",
    comment="Hourly event metrics with 1-hour watermark for late data"
)
def gold_event_metrics_hourly():
    """
    Aggregate events by type in 1-hour windows.
    Watermark handles late-arriving data within 1 hour.
    """
    return (
        spark.readStream.table("LIVE.silver_parsed_events")
        .withWatermark("event_timestamp", "1 hour")
        .groupBy(
            F.window("event_timestamp", "1 hour"),
            "event_type"
        )
        .agg(
            F.count("*").alias("event_count"),
            F.countDistinct("user_id").alias("unique_users"),
            F.min("event_timestamp").alias("window_start_actual"),
            F.max("event_timestamp").alias("window_end_actual")
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "event_type",
            "event_count",
            "unique_users",
            "window_start_actual",
            "window_end_actual"
        )
    )


# COMMAND ----------
# Example: Multiple Kafka topics with pattern matching
# COMMAND ----------

dp.create_streaming_table(
    name="bronze_all_app_events",
    comment="Events from multiple app-* topics"
)

@dp.append_flow(
    target="bronze_all_app_events",
    name="ingest_app_topics"
)
def ingest_app_topics():
    """
    Subscribe to multiple topics using pattern matching.
    Useful for consuming from topic families (app-prod-*, app-dev-*, etc.)
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "{{kafka_brokers}}")
        .option("subscribePattern", "app-.*")  # Pattern instead of subscribe
        .option("startingOffsets", "latest")
        .load()
        .select(
            F.col("key").cast("string"),
            F.col("value").cast("string"),
            F.col("topic"),
            F.col("timestamp").alias("kafka_timestamp")
        )
    )


# COMMAND ----------
# Example: Stateful processing with deduplication
# COMMAND ----------

@dp.table(
    name="gold_unique_events",
    comment="Deduplicated events using dropDuplicates with watermark"
)
def gold_unique_events():
    """
    Remove duplicate events within the watermark window.
    Maintains state for deduplication based on event_id.
    """
    return (
        spark.readStream.table("LIVE.silver_parsed_events")
        .withWatermark("event_timestamp", "24 hours")
        .dropDuplicates(["event_id"])
        .select(
            "event_id",
            "event_type",
            "user_id",
            "event_timestamp",
            "properties"
        )
    )
