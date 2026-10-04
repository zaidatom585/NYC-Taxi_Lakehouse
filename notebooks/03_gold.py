# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Gold: business-ready tables
# MAGIC Aggregates the cleaned trips into the tables that power the dashboard.

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "nyctaxi"
SILVER = f"{CATALOG}.{SCHEMA}.silver_trips"

silver = spark.read.table(SILVER)


def save(df, name):
    table = f"{CATALOG}.{SCHEMA}.{name}"
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table)
    )
    print(f"{table}: {spark.table(table).count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demand by hour (weekday vs. weekend)

# COMMAND ----------

hourly = silver.groupBy("pickup_hour", "is_weekend").agg(
    F.count("*").alias("trips"),
    F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
    F.round(F.avg("trip_duration_min"), 1).alias("avg_duration_min"),
    F.round(F.avg("avg_speed_mph"), 1).alias("avg_speed_mph"),
)
save(hourly, "gold_hourly_demand")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Daily trend with a 7-day rolling average

# COMMAND ----------

by_date = Window.orderBy("pickup_date")

daily = (
    silver.groupBy("pickup_date")
    .agg(
        F.count("*").alias("trips"),
        F.round(F.sum("fare_amount"), 2).alias("total_fares"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
    )
    .withColumn("trips_7day_avg", F.round(F.avg("trips").over(by_date.rowsBetween(-6, 0)), 1))
    .withColumn("trips_change_vs_prev_day", F.col("trips") - F.lag("trips").over(by_date))
)
save(daily, "gold_daily_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pickup ZIP codes (minimum 50 trips)

# COMMAND ----------

zips = (
    silver.groupBy("pickup_zip")
    .agg(
        F.count("*").alias("trips"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
    )
    .filter(F.col("trips") >= 50)
    .withColumn("rank_by_trips", F.dense_rank().over(Window.orderBy(F.desc("trips"))))
)
save(zips, "gold_zip_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Most common routes (minimum 20 trips)

# COMMAND ----------

routes = (
    silver.groupBy("pickup_zip", "dropoff_zip")
    .agg(
        F.count("*").alias("trips"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("trip_duration_min"), 1).alias("avg_duration_min"),
    )
    .filter(F.col("trips") >= 20)
    .withColumn("rank_by_trips", F.dense_rank().over(Window.orderBy(F.desc("trips"))))
)
save(routes, "gold_top_routes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fare per mile by distance

# COMMAND ----------

buckets = silver.groupBy("distance_bucket").agg(
    F.count("*").alias("trips"),
    F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
    F.round(F.avg("fare_per_mile"), 2).alias("avg_fare_per_mile"),
    F.round(F.avg("avg_speed_mph"), 1).alias("avg_speed_mph"),
)
save(buckets, "gold_distance_buckets")