# Databricks notebook source
"""
Advanced Spark Declarative Pipeline (SDP) with Flows and CDC

This example demonstrates:
- Append flows for streaming ingestion
- Auto CDC flows for change data capture
- SCD Type 1 and Type 2 patterns
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


# COMMAND ----------
# Bronze Layer - Streaming ingestion with append flow
# COMMAND ----------

dp.create_streaming_table(
    name="bronze_customers_raw",
    comment="Raw customer data ingested via Auto Loader"
)

@dp.append_flow(
    target="bronze_customers_raw",
    name="ingest_customer_data"
)
def ingest_customer_data():
    """
    Ingest customer data using Auto Loader with cloudFiles.
    This creates an append flow that continuously ingests new files.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/mnt/schemas/customers")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/mnt/raw/customers")
    )


# COMMAND ----------
# Silver Layer - Cleaned CDC data
# COMMAND ----------

dp.create_streaming_table(
    name="silver_customers_cdc_clean",
    comment="Cleaned CDC data with quality checks",
    expect_all_or_drop={
        "valid_id": "customer_id IS NOT NULL",
        "valid_operation": "operation IN ('INSERT', 'UPDATE', 'DELETE')",
        "valid_timestamp": "operation_timestamp IS NOT NULL",
        "no_rescued_data": "_rescued_data IS NULL"
    }
)

@dp.append_flow(
    target="silver_customers_cdc_clean",
    name="clean_cdc_data"
)
def clean_cdc_data():
    """
    Clean and validate CDC records before applying changes.
    Drops invalid records based on expectations.
    """
    return (
        spark.readStream.table("LIVE.bronze_customers_raw")
        .withColumn("processed_at", F.current_timestamp())
        .dropDuplicates(["customer_id", "operation_timestamp"])
    )


# COMMAND ----------
# Gold Layer - SCD Type 2 (track history)
# COMMAND ----------

dp.create_auto_cdc_flow(
    target="gold_customers_scd2",
    source="silver_customers_cdc_clean",
    keys=["customer_id"],
    sequence_by=F.col("operation_timestamp"),
    apply_as_deletes=F.expr("operation = 'DELETE'"),
    stored_as_scd_type="2",
    track_history_column_list=["name", "email", "address", "phone"]
)

# The above creates a table that:
# - Maintains full history of changes
# - Adds __START_AT, __END_AT, and __CURRENT columns
# - Soft deletes records when operation = 'DELETE'


# COMMAND ----------
# Gold Layer - SCD Type 1 (current state only)
# COMMAND ----------

dp.create_auto_cdc_flow(
    target="gold_customers_current",
    source="silver_customers_cdc_clean",
    keys=["customer_id"],
    sequence_by=F.col("operation_timestamp"),
    apply_as_deletes=F.expr("operation = 'DELETE'"),
    stored_as_scd_type="1",
    except_column_list=["operation", "operation_timestamp", "processed_at"]
)

# The above creates a table that:
# - Only maintains current state (no history)
# - Updates existing records in place
# - Physically deletes records when operation = 'DELETE'
# - Excludes CDC metadata columns from final table


# COMMAND ----------
# Example: Multi-key CDC flow
# COMMAND ----------

dp.create_streaming_table(
    name="bronze_orders_raw",
    comment="Raw order line items with CDC"
)

@dp.append_flow(
    target="bronze_orders_raw",
    name="ingest_order_data"
)
def ingest_order_data():
    """Ingest order data with multiple key columns."""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("/mnt/raw/orders")
    )


dp.create_auto_cdc_flow(
    target="gold_order_items",
    source="bronze_orders_raw",
    keys=["order_id", "line_item_id"],  # Composite key
    sequence_by=F.col("updated_at"),
    apply_as_deletes=F.expr("is_deleted = true"),
    stored_as_scd_type="1"
)


# COMMAND ----------
# Example: Custom flow logic with transformations
# COMMAND ----------

dp.create_streaming_table(
    name="gold_customer_metrics",
    comment="Aggregated customer metrics updated via flow"
)

@dp.append_flow(
    target="gold_customer_metrics",
    name="compute_customer_metrics"
)
def compute_customer_metrics():
    """
    Custom flow with business logic transformations.
    Use flows when you need complex logic before appending.
    """
    orders = spark.readStream.table("LIVE.gold_order_items")

    return (
        orders
        .filter("order_status = 'COMPLETED'")
        .groupBy("customer_id")
        .agg(
            F.count("*").alias("total_orders"),
            F.sum("order_amount").alias("lifetime_value"),
            F.max("order_date").alias("last_order_date")
        )
        .withColumn("customer_tier",
            F.when(F.col("lifetime_value") > 10000, "Premium")
             .when(F.col("lifetime_value") > 1000, "Gold")
             .otherwise("Standard")
        )
    )
