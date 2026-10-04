# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Quality checks
# MAGIC Verifies the pipeline's output. If any check fails, this notebook raises an error,
# MAGIC which makes the scheduled job fail and alerts you instead of silently publishing bad data.

# COMMAND ----------




CATALOG = "workspace"
SCHEMA = "nyctaxi"


def table(name):
    return spark.table(f"{CATALOG}.{SCHEMA}.{name}")


bronze = table("bronze_trips")
silver = table("silver_trips")
silver_rows = silver.count()

results = []


def check(name, passed):
    results.append((name, bool(passed)))

# COMMAND ----------

check("silver table is not empty", silver_rows > 0)
check("silver has no more rows than bronze", silver_rows <= bronze.count())
check("no zero or negative fares", silver.filter("fare_amount <= 0").count() == 0)
check("no zero or negative distances", silver.filter("trip_distance <= 0").count() == 0)
check("durations between 0 and 180 minutes",
      silver.filter("trip_duration_min <= 0 OR trip_duration_min > 180").count() == 0)
check("pickup hours between 0 and 23", silver.filter("pickup_hour NOT BETWEEN 0 AND 23").count() == 0)
check("hourly table accounts for every trip",
      table("gold_hourly_demand").agg({"trips": "sum"}).first()[0] == silver_rows)
check("daily table accounts for every trip",
      table("gold_daily_summary").agg({"trips": "sum"}).first()[0] == silver_rows)
check("distance buckets account for every trip",
      table("gold_distance_buckets").agg({"trips": "sum"}).first()[0] == silver_rows)

# COMMAND ----------

for name, passed in results:
    print(f"{'PASS' if passed else 'FAIL'}  {name}")

failed = [name for name, passed in results if not passed]
if failed:
    raise Exception(f"{len(failed)} quality check(s) failed: {failed}")
print(f"\nAll {len(results)} checks passed.")