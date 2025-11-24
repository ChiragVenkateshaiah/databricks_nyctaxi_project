# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

spark.read.table("nyctaxi.`02_silver`.yellow_trips_cleansed").\
    agg(F.max("tpep_pickup_datetime"), F.min("tpep_pickup_datetime")).\
        display()

# COMMAND ----------

