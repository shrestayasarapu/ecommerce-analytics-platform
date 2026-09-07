"""
tests/test_data_quality.py

Data-quality test suite for the E-Commerce Analytics Platform medallion
pipeline (Bronze -> Silver -> Gold).

Run locally:
    pip install pytest pyspark databricks-connect --break-system-packages
    pytest tests/test_data_quality.py -v
"""

import sys
import os

try:
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
except NameError:
    sys.path.insert(0, os.path.join(os.getcwd(), ".."))

import pytest
from config import Config, get_config

cfg = get_config(env=os.environ.get("BUNDLE_TARGET", "dev"))


@pytest.fixture(scope="module")
def spark():
    try:
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()
    except Exception as e:
        pytest.skip(f"No Spark session available: {e}")


# ---------------------------------------------------------------------
# Table existence
# ---------------------------------------------------------------------

@pytest.mark.parametrize("table", cfg.GOLD_TABLES)
def test_gold_table_exists(spark, table):
    assert spark.catalog.tableExists(cfg.gold(table)), (
        f"Expected gold table {cfg.gold(table)} to exist"
    )


def test_bronze_table_exists(spark):
    assert spark.catalog.tableExists(cfg.bronze("events"))


def test_silver_table_exists(spark):
    assert spark.catalog.tableExists(cfg.silver("events_clean"))


# ---------------------------------------------------------------------
# Row-count sanity across the medallion layers
# ---------------------------------------------------------------------

def test_bronze_has_rows(spark):
    count = spark.table(cfg.bronze("events")).count()
    assert count > 0, "Bronze table is empty"


def test_silver_row_count_not_greater_than_bronze(spark):
    bronze_count = spark.table(cfg.bronze("events")).count()
    silver_count = spark.table(cfg.silver("events_clean")).count()
    assert silver_count <= bronze_count, (
        f"Silver ({silver_count}) has more rows than Bronze ({bronze_count}); "
        "dedup/quarantine logic may be broken"
    )


def test_silver_dedup_removed_expected_ratio(spark):
    bronze_count = spark.table(cfg.bronze("events")).count()
    silver_count = spark.table(cfg.silver("events_clean")).count()
    if bronze_count == 0:
        pytest.skip("Bronze table empty, skipping ratio check")
    dedup_ratio = 1 - (silver_count / bronze_count)
    assert 0 <= dedup_ratio < 0.05, (
        f"Unexpected dedup ratio {dedup_ratio:.4%}; expected < 5% rows removed"
    )


# ---------------------------------------------------------------------
# Null / required-field checks
# ---------------------------------------------------------------------

@pytest.mark.parametrize("column", cfg.REQUIRED_EVENT_COLUMNS)
def test_silver_no_nulls_in_required_columns(spark, column):
    df = spark.table(cfg.silver("events_clean"))
    null_count = df.filter(df[column].isNull()).count()
    assert null_count == 0, (
        f"Found {null_count} null values in required column '{column}' "
        "in silver layer — quarantine logic should have caught these"
    )


def test_silver_prices_are_positive(spark):
    df = spark.table(cfg.silver("events_clean"))
    invalid = df.filter(df["price"] <= 0).count()
    assert invalid == 0, f"Found {invalid} rows with non-positive price in silver"


def test_silver_event_type_is_valid(spark):
    df = spark.table(cfg.silver("events_clean"))
    invalid = df.filter(~df["event_type"].isin(*cfg.VALID_EVENT_TYPES)).count()
    assert invalid == 0, (
        f"Found {invalid} rows with an event_type outside {cfg.VALID_EVENT_TYPES}"
    )


# ---------------------------------------------------------------------
# Uniqueness / referential sanity on gold aggregates
# ---------------------------------------------------------------------

def test_customer_metrics_unique_customers(spark):
    df = spark.table(cfg.gold("customer_metrics"))
    total = df.count()
    distinct = df.select("user_id").distinct().count()
    assert total == distinct, (
        f"customer_metrics has {total - distinct} duplicate user_id rows"
    )


def test_product_performance_unique_products(spark):
    df = spark.table(cfg.gold("product_performance"))
    total = df.count()
    distinct = df.select("product_id").distinct().count()
    assert total == distinct, (
        f"product_performance has {total - distinct} duplicate product_id rows"
    )


# ---------------------------------------------------------------------
# Dashboard-facing gold tables
# ---------------------------------------------------------------------

@pytest.mark.parametrize("column", Config.KPI_SUMMARY_COLUMNS)
def test_kpi_summary_has_expected_column(spark, column):
    df = spark.table(cfg.gold("kpi_summary"))
    assert column in df.columns, f"kpi_summary missing expected column '{column}'"


def test_kpi_summary_not_empty(spark):
    df = spark.table(cfg.gold("kpi_summary"))
    assert df.count() > 0, "kpi_summary should have at least one row"


def test_kpi_summary_metrics_non_negative(spark):
    df = spark.table(cfg.gold("kpi_summary"))
    for column in Config.KPI_SUMMARY_COLUMNS:
        negative_count = df.filter(df[column] < 0).count()
        assert negative_count == 0, f"kpi_summary.{column} has negative values"


def test_kpi_summary_funnel_is_monotonic(spark):
    row = spark.table(cfg.gold("kpi_summary")).select(
        "total_views", "total_cart_adds", "total_purchases"
    ).first()
    assert row["total_views"] >= row["total_cart_adds"] >= row["total_purchases"], (
        f"Funnel out of order: views={row['total_views']}, "
        f"cart_adds={row['total_cart_adds']}, purchases={row['total_purchases']}"
    )


@pytest.mark.parametrize("column", Config.TOP_PRODUCTS_COLUMNS)
def test_top_products_has_expected_column(spark, column):
    df = spark.table(cfg.gold("top_products"))
    assert column in df.columns, f"top_products missing expected column '{column}'"


def test_top_products_revenue_non_negative(spark):
    df = spark.table(cfg.gold("top_products"))
    assert df.filter(df["total_revenue"] < 0).count() == 0


def test_top_products_unique_product_ids(spark):
    df = spark.table(cfg.gold("top_products"))
    total = df.count()
    distinct = df.select("product_id").distinct().count()
    assert total == distinct, (
        f"top_products has {total - distinct} duplicate product_id rows"
    )


@pytest.mark.parametrize("column", Config.REVENUE_BY_CATEGORY_COLUMNS)
def test_revenue_by_category_has_expected_column(spark, column):
    df = spark.table(cfg.gold("revenue_by_category"))
    assert column in df.columns, (
        f"revenue_by_category missing expected column '{column}'"
    )


def test_revenue_by_category_no_negative_revenue(spark):
    df = spark.table(cfg.gold("revenue_by_category"))
    assert df.filter(df["category_revenue"] < 0).count() == 0


def test_revenue_by_category_unique_categories(spark):
    df = spark.table(cfg.gold("revenue_by_category"))
    total = df.count()
    distinct = df.select("category_code").distinct().count()
    assert total == distinct, (
        f"revenue_by_category has {total - distinct} duplicate category_code rows"
    )


# ---------------------------------------------------------------------
# Freshness
# ---------------------------------------------------------------------

def test_gold_tables_recently_updated(spark):
    from datetime import datetime, timedelta
    from delta.tables import DeltaTable

    max_age_days = 7
    for table in cfg.GOLD_TABLES:
        full_name = cfg.gold(table)
        try:
            dt = DeltaTable.forName(spark, full_name)
        except Exception:
            pytest.skip(f"{full_name} is not a Delta table or not accessible")
            continue
        last_ts = dt.history(1).select("timestamp").collect()[0]["timestamp"]
        age = datetime.now() - last_ts.replace(tzinfo=None)
        assert age < timedelta(days=max_age_days), (
            f"{full_name} last updated {age} ago, exceeds {max_age_days}-day freshness SLA"
        )