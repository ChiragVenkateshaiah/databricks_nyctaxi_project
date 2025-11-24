# Databricks notebook source
from pyspark.sql import functions as F
from dateutil.relativedelta import relativedelta
from datetime import date, datetime, timezone

# COMMAND ----------

"""
date.today() → today's date, e.g., '2025-11-21'
relativedelta(months=2) → subtracts exactly 2 calendar months
It also handles month-end correctly:
    - If today = March 31 → subtract 1 month → Feb 28/29 (valid day)
"""
two_months_ago = date.today() - relativedelta(months=2)

# Convert to format "YYYY-MM"
formatted_date = two_months_ago.strftime("%Y-%m")


# COMMAND ----------

# Read all the parquet files for the specified month from the landing directory into a DataFrame

df = spark.read.format("parquet").load(f"/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/{formatted_date}")

# COMMAND ----------

# Add a column to capture when the data was processed
df = df.withColumn("processed_timestamp", F.current_timestamp())

# COMMAND ----------

# Wrtie the DataFrame to a Unity Catalog managed Delta table in the bronze schema, appending the new data
df.write.mode("append").saveAsTable("nyctaxi.01_bronze.yellow_trips_raw")