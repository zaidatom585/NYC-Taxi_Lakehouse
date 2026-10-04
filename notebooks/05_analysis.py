# Databricks notebook source
# MAGIC %md
# MAGIC # 05 Analysis
# MAGIC Answers the project's questions from the gold tables. Run it cell by cell and
# MAGIC write your findings into the README.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. How much data was removed in cleaning, and why?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.nyctaxi.silver_dq_log ORDER BY run_at DESC LIMIT 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. When is demand highest, and does it differ on weekends?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT pickup_hour,
# MAGIC        SUM(CASE WHEN NOT is_weekend THEN trips END) AS weekday_trips,
# MAGIC        SUM(CASE WHEN is_weekend THEN trips END)     AS weekend_trips
# MAGIC FROM workspace.nyctaxi.gold_hourly_demand
# MAGIC GROUP BY pickup_hour
# MAGIC ORDER BY pickup_hour

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. When is traffic slowest?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT pickup_hour, ROUND(AVG(avg_speed_mph), 1) AS avg_speed_mph
# MAGIC FROM workspace.nyctaxi.gold_hourly_demand
# MAGIC GROUP BY pickup_hour
# MAGIC ORDER BY avg_speed_mph
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Which areas generate the most trips?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT rank_by_trips, pickup_zip, trips, avg_fare, avg_distance
# MAGIC FROM workspace.nyctaxi.gold_zip_summary
# MAGIC WHERE rank_by_trips <= 10
# MAGIC ORDER BY rank_by_trips

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Why do short trips cost more per mile?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.nyctaxi.gold_distance_buckets ORDER BY distance_bucket

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. How does demand change day to day?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT pickup_date, trips, trips_7day_avg, trips_change_vs_prev_day
# MAGIC FROM workspace.nyctaxi.gold_daily_summary
# MAGIC ORDER BY pickup_date