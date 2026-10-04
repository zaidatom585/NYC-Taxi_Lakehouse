# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Silver: cleaned and enriched data
# MAGIC Removes invalid trips using explicit, named rules, records how many rows each rule
# MAGIC caught, and adds the columns used throughout the analysis.

# COMMAND ----------

from functools import reduce
from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "nyctaxi"
BRONZE = f"{CATALOG}.{SCHEMA}.bronze_trips"
SILVER = f"{CATALOG}.{SCHEMA}.silver_trips"
DQ_LOG = f"{CATALOG}.{SCHEMA}.silver_dq_log"

# COMMAND ----------

bronze = spark.read.table(BRONZE)

trips = bronze.withColumn(
    "trip_duration_min",
    (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60,
)

# Each rule describes a BAD row. A trip is removed if any rule is true.
rules = {
    "missing_values": (
        F.col("tpep_pickup_datetime").isNull() | F.col("tpep_dropoff_datetime").isNull()
        | F.col("trip_distance").isNull() | F.col("fare_amount").isNull()
        | F.col("pickup_zip").isNull() | F.col("dropoff_zip").isNull()
    ),
    "non_positive_distance": F.col("trip_distance") <= 0,
    "non_positive_fare": F.col("fare_amount") <= 0,
    "dropoff_not_after_pickup": F.col("trip_duration_min") <= 0,
    "duration_over_3_hours": F.col("trip_duration_min") > 180,
    "distance_over_100_miles": F.col("trip_distance") > 100,
}
# Comparisons with NULL return NULL, so treat NULL as "rule not triggered".
rules = {name: F.coalesce(cond, F.lit(False)) for name, cond in rules.items()}
is_bad = reduce(lambda a, b: a | b, rules.values())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data quality log
# MAGIC Rules can overlap, so the rule counts can add up to more than the rows removed.

# COMMAND ----------

dq = trips.agg(
    F.count("*").alias("input_rows"),
    *[F.sum(F.when(cond, 1).otherwise(0)).alias(name) for name, cond in rules.items()],
    F.sum(F.when(is_bad, 1).otherwise(0)).alias("rows_removed"),
).withColumn("run_at", F.current_timestamp())
dq.write.format("delta").mode("append").saveAsTable(DQ_LOG)
display(dq)

# COMMAND ----------

distance_bucket = (
    F.when(F.col("trip_distance") < 1, "1. under 1 mile")
    .when(F.col("trip_distance") < 3, "2. 1-3 miles")
    .when(F.col("trip_distance") < 5, "3. 3-5 miles")
    .when(F.col("trip_distance") < 10, "4. 5-10 miles")
    .otherwise("5. 10+ miles")
)

silver = (
    trips.filter(~is_bad)
    .select(
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "trip_distance",
        "fare_amount",
        "pickup_zip",
        "dropoff_zip",
        F.round("trip_duration_min", 2).alias("trip_duration_min"),
        F.to_date("tpep_pickup_datetime").alias("pickup_date"),
        F.hour("tpep_pickup_datetime").alias("pickup_hour"),
        F.dayofweek("tpep_pickup_datetime").alias("pickup_dow"),
        F.date_format("tpep_pickup_datetime", "E").alias("pickup_day_name"),
        F.dayofweek("tpep_pickup_datetime").isin(1, 7).alias("is_weekend"),
        F.round(F.col("fare_amount") / F.col("trip_distance"), 2).alias("fare_per_mile"),
        F.round(F.col("trip_distance") / (F.col("trip_duration_min") / 60), 2).alias("avg_speed_mph"),
        distance_bucket.alias("distance_bucket"),
    )
)

(
    silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER)
)

print(f"{SILVER}: {spark.table(SILVER).count():,} rows")