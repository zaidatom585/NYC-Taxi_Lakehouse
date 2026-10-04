# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Bronze: raw data
# MAGIC Copies the source data unchanged into a Delta table, adding columns that record
# MAGIC when and where each row was loaded from. Nothing is cleaned or removed here.

# COMMAND ----------


from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "nyctaxi"
SOURCE = "samples.nyctaxi.trips"
TARGET = f"{CATALOG}.{SCHEMA}.bronze_trips"

# COMMAND ----------

raw = spark.read.table(SOURCE)

bronze = (
    raw
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source", F.lit(SOURCE))
)

(
    bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET)
)

print(f"{TARGET}: {spark.table(TARGET).count():,} rows")