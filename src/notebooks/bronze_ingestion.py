# Databricks notebook source
# =====================================================================
# Bronze Layer Ingestion
# Domain 4: Delta Lake & Medallion Architecture
# =====================================================================

# COMMAND ----------
dbutils.widgets.text("catalog", "ecommerce_analytics_platform_dev")
dbutils.widgets.text("environment", "dev")

catalog = dbutils.widgets.get("catalog")
environment = dbutils.widgets.get("environment")
source_path = "abfss://raw-data@ecommdatasy2026.dfs.core.windows.net/"
bronze_table = f"{catalog}.bronze_layer.events_raw"

print(f"Catalog: {catalog}")
print(f"Source path: {source_path}")
print(f"Target table: {bronze_table}")

# COMMAND ----------
from pyspark.sql import functions as F

raw_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(source_path)
)

source_row_count = raw_df.count()
print(f"Source CSV row count: {source_row_count}")

# COMMAND ----------
bronze_df = (
    raw_df
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("source_file", F.col("_metadata.file_path"))
    .withColumn("created_at", F.current_timestamp())
)

# COMMAND ----------
(
    bronze_df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(bronze_table)
)

# COMMAND ----------
target_row_count = spark.table(bronze_table).count()
print(f"Bronze table total row count: {target_row_count}")

assert source_row_count > 0, "Source read returned zero rows"
print("Bronze ingestion validation passed.")

# COMMAND ----------
spark.sql(f"""
    ALTER TABLE {bronze_table}
    SET TBLPROPERTIES ('quality' = 'bronze')
""")

display(spark.table(bronze_table).limit(10))
