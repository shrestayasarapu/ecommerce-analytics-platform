# Databricks notebook source
# =====================================================================
# Silver Layer Transformation
# Domain 4: Delta Lake & Medallion Architecture
# =====================================================================

# COMMAND ----------
dbutils.widgets.text("catalog", "ecommerce_analytics_platform_dev")
dbutils.widgets.text("environment", "dev")

catalog = dbutils.widgets.get("catalog")
bronze_table = f"{catalog}.bronze_layer.events_raw"
silver_table = f"{catalog}.silver_layer.events_cleaned"

# COMMAND ----------
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window


def clean_and_deduplicate(df: DataFrame) -> DataFrame:
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


# COMMAND ----------
bronze_df = spark.table(bronze_table)
bronze_count = bronze_df.count()

silver_df = clean_and_deduplicate(bronze_df)
silver_count = silver_df.count()

print(f"Bronze row count: {bronze_count}")
print(f"Silver row count: {silver_count}")

# COMMAND ----------
(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_table)
)

# COMMAND ----------
assert silver_count <= bronze_count, "Silver should not exceed Bronze count"
assert silver_count > 0, "Silver transformation produced zero rows"
print("Silver transformation validation passed.")

display(spark.table(silver_table).limit(10))
