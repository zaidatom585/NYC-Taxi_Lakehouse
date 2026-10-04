-- =====================================================================
-- Results snapshot: every number needed for results/run_log.md and the README.
-- Run ONE query at a time in SQLTools (highlight it, then Ctrl+E, Ctrl+E),
-- and export each result as CSV into the results/ folder.
-- =====================================================================

-- Q1. Row counts for every table  ->  results/table_counts.csv
SELECT 'bronze_trips'          AS table_name, COUNT(*) AS row_count FROM workspace.nyctaxi.bronze_trips
UNION ALL SELECT 'silver_trips',          COUNT(*) FROM workspace.nyctaxi.silver_trips
UNION ALL SELECT 'gold_hourly_demand',    COUNT(*) FROM workspace.nyctaxi.gold_hourly_demand
UNION ALL SELECT 'gold_daily_summary',    COUNT(*) FROM workspace.nyctaxi.gold_daily_summary
UNION ALL SELECT 'gold_zip_summary',      COUNT(*) FROM workspace.nyctaxi.gold_zip_summary
UNION ALL SELECT 'gold_top_routes',       COUNT(*) FROM workspace.nyctaxi.gold_top_routes
UNION ALL SELECT 'gold_distance_buckets', COUNT(*) FROM workspace.nyctaxi.gold_distance_buckets;


-- Q2. Data cleaning summary from the latest run  ->  results/data_quality.csv
SELECT input_rows,
       rows_removed,
       ROUND(100.0 * rows_removed / input_rows, 2) AS pct_removed,
       missing_values,
       non_positive_distance,
       non_positive_fare,
       dropoff_not_after_pickup,
       duration_over_3_hours,
       distance_over_100_miles
FROM workspace.nyctaxi.silver_dq_log
ORDER BY run_at DESC
LIMIT 1;


-- Q3. Date range and daily volume  ->  results/daily_overview.csv
SELECT MIN(pickup_date)              AS first_date,
       MAX(pickup_date)              AS last_date,
       COUNT(*)                      AS days,
       ROUND(AVG(trips), 1)          AS avg_trips_per_day,
       MAX_BY(pickup_date, trips)    AS busiest_date,
       MAX(trips)                    AS busiest_date_trips,
       MIN_BY(pickup_date, trips)    AS quietest_date,
       MIN(trips)                    AS quietest_date_trips
FROM workspace.nyctaxi.gold_daily_summary;


-- Q4. Top 3 pickup hours, weekday vs. weekend  ->  results/peak_hours.csv
-- Weekdays outnumber weekend days, so compare WHICH hours peak, not the raw counts.
SELECT day_type, pickup_hour, trips
FROM (
  SELECT CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
         pickup_hour,
         trips,
         ROW_NUMBER() OVER (PARTITION BY is_weekend ORDER BY trips DESC) AS rn
  FROM workspace.nyctaxi.gold_hourly_demand
)
WHERE rn <= 3
ORDER BY day_type, rn;


-- Q5. Slowest hours (trip-weighted average speed)  ->  results/slowest_hours.csv
SELECT pickup_hour,
       ROUND(SUM(avg_speed_mph * trips) / SUM(trips), 1) AS avg_speed_mph,
       SUM(trips)                                        AS trips
FROM workspace.nyctaxi.gold_hourly_demand
GROUP BY pickup_hour
ORDER BY avg_speed_mph
LIMIT 5;


-- Q6. Top 5 pickup ZIP codes  ->  results/top_zips.csv
SELECT rank_by_trips, pickup_zip, trips, avg_fare, avg_distance
FROM workspace.nyctaxi.gold_zip_summary
WHERE rank_by_trips <= 5
ORDER BY rank_by_trips;


-- Q7. Fare per mile by trip distance  ->  results/fare_per_mile.csv
SELECT distance_bucket, trips, avg_fare, avg_fare_per_mile, avg_speed_mph
FROM workspace.nyctaxi.gold_distance_buckets
ORDER BY distance_bucket;