# Databricks notebook source
# =====================================================================
# DLT Streaming Pipeline — Domain 3: Data Processing & Pipeline Dev
# =====================================================================

import dlt
from pyspark.sql import functions as F

source_path = "abfss://raw-data@ecommdatasy2026.dfs.core.windows.net/"
checkpoint_path = "abfss://checkpoints@ecommdatasy2026.dfs.core.windows.net/"
schema_location = f"{checkpoint_path}_schema"


@dlt.table(
    name="events_bronze_streaming",
    comment="Raw streaming ingestion via Autoloader with schema evolution enabled",
    table_properties={"quality": "bronze"},
)
def events_bronze_streaming():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .load(source_path)
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_file", F.col("_metadata.file_path"))
    )


@dlt.table(
    name="events_silver_validated",
    comment="Cleaned events passing all data quality expectations",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_user_id", "user_id IS NOT NULL")
@dlt.expect_or_drop("valid_event_type", "event_type IS NOT NULL")
@dlt.expect_or_drop("valid_price", "price IS NULL OR price > 0")
@dlt.expect("reasonable_timestamp", "event_time IS NOT NULL")
def events_silver_validated():
    return (
        dlt.read_stream("events_bronze_streaming")
        .withColumn("event_type", F.lower(F.trim(F.col("event_type"))))
    )


@dlt.table(
    name="events_quarantine",
    comment="Records failing data quality expectations — dead-letter table",
    table_properties={"quality": "quarantine"},
)
def events_quarantine():
    return (
        dlt.read_stream("events_bronze_streaming")
        .withColumn("event_type", F.lower(F.trim(F.col("event_type"))))
        .filter(
            F.col("user_id").isNull()
            | F.col("event_type").isNull()
            | (F.col("price").isNotNull() & (F.col("price") <= 0))
        )
        .withColumn("quarantine_reason",
            F.when(F.col("user_id").isNull(), "missing_user_id")
             .when(F.col("event_type").isNull(), "missing_event_type")
             .otherwise("invalid_price")
        )
    )
