# Databricks notebook source
# MAGIC %md
# MAGIC ## Bronze: Autoloader ingestion of the simulated live order feed

# COMMAND ----------

import sys
sys.path.append("..")

from src.ingestion.bronze_stream_orders import main

dbutils.widgets.text("catalog_name", "retail_lakehouse")
dbutils.widgets.text("landing_path", "/Volumes/retail_lakehouse/bronze/order_stream_landing/")

sys.argv = ["bronze_stream_orders.py", dbutils.widgets.get("catalog_name"), dbutils.widgets.get("landing_path")]
main()
