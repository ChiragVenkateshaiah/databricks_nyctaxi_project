# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# Which vendor makes the most orevenue?

df = spark.read.table("nyctaxi.02_silver.yellow_trips_enriched")

df.\
    groupBy("vendor").\
    agg(
        round(sum("total_amount"), 2).alias("total_revenue")
        ).\
    orderBy("total_revenue", ascending=False).\
    display()

# COMMAND ----------

# What is the most popular pickup borough?

df.\
    groupBy("pu_borough").\
    agg(
        count("*").alias("number_of_trips")
        ).\
        orderBy("number_of_trips", ascending=False).\
        display()

# COMMAND ----------

# What is the most common journey (borough to borough)?

df.\
    groupBy(concat("pu_borough", lit(" -> "), "do_borough").alias("journey")).\
    agg(
        count("*").alias("number_of_trips")
        ).\
    orderBy("number_of_trips", ascending=False).\
    display()

# COMMAND ----------

# Create a time series chart showing the number of trips and total revenue per day

df2 = spark.read.table("nyctaxi.03_gold.daily_trip_summary")
df2.display()