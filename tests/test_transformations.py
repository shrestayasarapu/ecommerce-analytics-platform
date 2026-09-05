"""
Unit tests for Silver and Gold layer transformation logic.
Uses a local PySpark session — no live Databricks cluster required.
Run with: pytest tests/test_transformations.py -v
"""
import sys
import os
import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "notebooks"))


@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("unit-tests")
        .getOrCreate()
    )


def clean_and_deduplicate(df):
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    required_cols = ["user_id", "event_type", "event_time"]
    df = df.dropna(subset=required_cols)
    df = df.withColumn("event_type", F.lower(F.trim(F.col("event_type"))))

    if "price" in df.columns:
        df = df.filter(F.col("price") > 0)

    dedup_window = Window.partitionBy(
        "user_id", "product_id", "event_time", "event_type"
    ).orderBy(F.col("ingestion_timestamp").desc())

    df = (
        df.withColumn("_row_num", F.row_number().over(dedup_window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )
    return df


def test_silver_removes_null_user_id(spark):
    data = [
        (1, "VIEW", "p1", 10.0, "2026-01-01", "2026-01-01"),
        (None, "view", "p2", 5.0, "2026-01-01", "2026-01-01"),
    ]
    cols = ["user_id", "event_type", "product_id", "price", "event_time", "ingestion_timestamp"]
    df = spark.createDataFrame(data, cols)

    result = clean_and_deduplicate(df)
    assert result.count() == 1
    assert result.filter("user_id IS NULL").count() == 0


def test_silver_standardizes_event_type_case(spark):
    data = [(1, "VIEW", "p1", 10.0, "2026-01-01", "2026-01-01")]
    cols = ["user_id", "event_type", "product_id", "price", "event_time", "ingestion_timestamp"]
    df = spark.createDataFrame(data, cols)

    result = clean_and_deduplicate(df)
    assert result.collect()[0]["event_type"] == "view"


def test_silver_deduplicates_on_key_columns(spark):
    data = [
        (1, "view", "p1", 10.0, "2026-01-01", "2026-01-01T10:00:00"),
        (1, "view", "p1", 10.0, "2026-01-01", "2026-01-01T11:00:00"),  # duplicate, later ingestion
    ]
    cols = ["user_id", "event_type", "product_id", "price", "event_time", "ingestion_timestamp"]
    df = spark.createDataFrame(data, cols)

    result = clean_and_deduplicate(df)
    assert result.count() == 1


def test_silver_filters_non_positive_price(spark):
    data = [
        (1, "purchase", "p1", 10.0, "2026-01-01", "2026-01-01"),
        (2, "purchase", "p2", -5.0, "2026-01-01", "2026-01-01"),
        (3, "purchase", "p3", 0.0, "2026-01-01", "2026-01-01"),
    ]
    cols = ["user_id", "event_type", "product_id", "price", "event_time", "ingestion_timestamp"]
    df = spark.createDataFrame(data, cols)

    result = clean_and_deduplicate(df)
    assert result.count() == 1


def test_gold_customer_metrics_revenue_calculation(spark):
    from pyspark.sql import functions as F

    data = [
        (1, "purchase", "p1", 10.0),
        (1, "purchase", "p2", 20.0),
        (1, "view", "p3", None),
    ]
    cols = ["user_id", "event_type", "product_id", "price"]
    df = spark.createDataFrame(data, cols)

    result = (
        df.groupBy("user_id")
        .agg(
            F.sum(F.when(F.col("event_type") == "purchase", F.col("price")).otherwise(0.0)).alias("total_revenue")
        )
    )
    row = result.collect()[0]
    assert row["total_revenue"] == 30.0
