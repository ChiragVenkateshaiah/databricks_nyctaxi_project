# Databricks notebook source
import pyspark.sql.functions as F
import pyspark.sql.types as T

# COMMAND ----------

# Read the taxi zone lookup CSV (with header) into a DataFrame

df = spark.read.format("csv").option("header", "true").load("/Volumes/nyctaxi/00_landing/data_sources/lookup/taxi_zone_lookup.csv")

# COMMAND ----------

# Select and rename fields, casting types, and add audit columns

df = (
    df.select(
        F.col("LocationID").cast(T.IntegerType()).alias("location_id"),
        F.col("Borough").alias("borough"),
        F.col("Zone").alias("zone"),
        F.col("service_zone"),
        F.current_timestamp().alias("effective_date"),
        F.lit(None).cast(T.TimestampType()).alias("end_date")
    )
)

# COMMAND ----------

# Write to a Unity Catalog managed Delta table in the silver schema, overwriting any existing records

df.write.mode("overwrite").saveAsTable("nyctaxi.02_silver.taxi_zone_lookup")