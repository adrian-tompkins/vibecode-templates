# Databricks notebook source
"""
Example Spark Declarative Pipeline (SDP)

This template demonstrates the medallion architecture using basic @table decorators.
Replace with your actual pipeline logic.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


# COMMAND ----------
# Bronze Layer - Raw data ingestion
# COMMAND ----------

@dp.table(
    name="bronze_raw_data",
    comment="Raw data ingested from source"
)
def bronze_raw_data():
    """
    Ingest raw data from source.
    Replace path and format with your actual data source.
    """
    return (
        spark.read
        .format("json")
        .load("/path/to/raw/data")
    )


# COMMAND ----------
# Silver Layer - Cleaned and validated data
# COMMAND ----------

@dp.table(
    name="silver_cleaned_data",
    comment="Cleaned and validated data"
)
def silver_cleaned_data():
    """
    Clean and validate bronze data.
    Apply transformations and data quality rules.
    """
    return (
        spark.read.table("LIVE.bronze_raw_data")
        .filter("id IS NOT NULL")
        .filter("event_timestamp IS NOT NULL")
        .withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn("processed_at", F.current_timestamp())
        .dropDuplicates(["id", "event_timestamp"])
    )


# COMMAND ----------
# Gold Layer - Business-level aggregates
# COMMAND ----------

@dp.table(
    name="gold_daily_metrics",
    comment="Daily aggregated metrics for reporting"
)
def gold_daily_metrics():
    """
    Create business-level aggregations.
    This is read-optimized data for analytics and reporting.
    """
    return (
        spark.read.table("LIVE.silver_cleaned_data")
        .groupBy("event_date")
        .agg(
            F.count("*").alias("total_events"),
            F.countDistinct("id").alias("unique_ids"),
            F.min("event_timestamp").alias("first_event"),
            F.max("event_timestamp").alias("last_event")
        )
    )
