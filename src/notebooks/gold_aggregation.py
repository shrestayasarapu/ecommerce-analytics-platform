# Databricks notebook source
# =====================================================================
# Gold Layer Aggregation
# Domain 4: Delta Lake & Medallion Architecture
# =====================================================================

# COMMAND ----------
dbutils.widgets.text("catalog", "ecommerce_analytics_platform_dev")
dbutils.widgets.text("environment", "dev")

catalog = dbutils.widgets.get("catalog")
silver_table = f"{catalog}.silver_layer.events_cleaned"
customer_metrics_table = f"{catalog}.gold_layer.customer_metrics"
product_performance_table = f"{catalog}.gold_layer.product_performance"

# COMMAND ----------
from pyspark.sql import DataFrame, functions as F


def build_customer_metrics(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("user_id")
        .agg(
            F.count("*").alias("total_events"),
            F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("total_views"),
            F.sum(F.when(F.col("event_type") == "cart", 1).otherwise(0)).alias("total_cart_adds"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("total_purchases"),
            F.sum(F.when(F.col("event_type") == "purchase", F.col("price")).otherwise(0.0)).alias("total_revenue"),
        )
        .withColumn(
            "conversion_rate",
            F.when(F.col("total_views") > 0, F.col("total_purchases") / F.col("total_views")).otherwise(0.0),
        )
    )


def build_product_performance(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("product_id", "category_code")
        .agg(
            F.count("*").alias("total_interactions"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("units_sold"),
            F.sum(F.when(F.col("event_type") == "purchase", F.col("price")).otherwise(0.0)).alias("total_revenue"),
        )
        .withColumn(
            "conversion_rate",
            F.when(F.col("total_interactions") > 0, F.col("units_sold") / F.col("total_interactions")).otherwise(0.0),
        )
    )


# COMMAND ----------
silver_df = spark.table(silver_table)
customer_metrics_df = build_customer_metrics(silver_df)
product_performance_df = build_product_performance(silver_df)

# COMMAND ----------
(customer_metrics_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(customer_metrics_table))
(product_performance_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(product_performance_table))

# COMMAND ----------
cm_count = spark.table(customer_metrics_table).count()
pp_count = spark.table(product_performance_table).count()
print(f"customer_metrics rows: {cm_count}")
print(f"product_performance rows: {pp_count}")
assert cm_count > 0 and pp_count > 0, "Gold tables must not be empty"
print("Gold aggregation validation passed.")

display(spark.table(customer_metrics_table).limit(10))
