# Databricks notebook source
import pyspark.sql.functions as F
from dateutil.relativedelta import relativedelta
from datetime import date

# COMMAND ----------

# Get the first day of the month two months ago
two_months_ago_start = date.today().replace(day=1) - relativedelta(months=2)

# COMMAND ----------

# Load the enriched table
# and filter to only include trips with a pickup datetime later than the start date from two months ago
df = spark.read.table("nyctaxi.02_silver.yellow_trips_enriched").filter(f"tpep_pickup_datetime > '{two_months_ago_start}' ")

# COMMAND ----------

# Aggregate trip data by pickup date with key metrics

df = (
    df
    # group records by calendar date
    .groupBy(F.to_date(F.col("tpep_pickup_datetime")).alias("pickup_date"))
    .agg(
        F.count("*").alias("total_trips"),                                              # Total number of trips per day
        F.round(F.avg("passenger_count"), 1).alias("average_passengers"),               # average passengers per day
        F.round(F.avg("trip_distance"), 1).alias("average_distance"),                   # average distance per day
        F.round(F.avg("fare_amount"), 1).alias("average_fare_per_trip"),                # average fair per trip ($)
        F.max("fare_amount").alias("max_fare"),                                         # highest single trip fair
        F.min("fare_amount").alias("min_fare"),                                         # lowest single trip fair
        F.round(F.sum("total_amount"), 2).alias("total_revenue")                          # total revenue per day ($)
    )
)

# COMMAND ----------

# Write the daily summary to a Unity Catalog managed Delta table in the gold schema
df.write.mode("append").saveAsTable("nyctaxi.03_gold.daily_trip_summary")