-- Datasets for the Databricks dashboard. Add each query as a separate dataset.

-- Dataset 1: headline numbers (counter widgets)
SELECT COUNT(*)                        AS trips,
       ROUND(SUM(fare_amount), 0)      AS total_fares,
       ROUND(AVG(fare_amount), 2)      AS avg_fare,
       ROUND(AVG(trip_duration_min), 1) AS avg_duration_min
FROM workspace.nyctaxi.silver_trips;

-- Dataset 2: hourly demand, weekday vs. weekend (line or bar chart)
SELECT pickup_hour,
       CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
       trips, avg_fare, avg_speed_mph
FROM workspace.nyctaxi.gold_hourly_demand;

-- Dataset 3: daily trend (line chart: trips and trips_7day_avg)
SELECT pickup_date, trips, trips_7day_avg
FROM workspace.nyctaxi.gold_daily_summary;

-- Dataset 4: top 10 pickup ZIP codes (bar chart)
SELECT CAST(pickup_zip AS STRING) AS pickup_zip, trips, avg_fare
FROM workspace.nyctaxi.gold_zip_summary
WHERE rank_by_trips <= 10;

-- Dataset 5: fare per mile by distance (bar chart)
SELECT distance_bucket, trips, avg_fare, avg_fare_per_mile
FROM workspace.nyctaxi.gold_distance_buckets;