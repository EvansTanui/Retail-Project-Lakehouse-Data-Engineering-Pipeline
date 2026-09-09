# Databricks notebook source
# MAGIC %md
# MAGIC ## Bronze: batch ingest from `samples.tpcds_sf1`
# MAGIC Thin wrapper -- all real logic lives in `src/ingestion/bronze_ingest.py`
# MAGIC so it stays testable and reusable outside a notebook context.

# COMMAND ----------

import sys
sys.path.append("..")

from src.ingestion.bronze_ingest import main

dbutils.widgets.text("catalog_name", "retail_lakehouse")
catalog_name = dbutils.widgets.get("catalog_name")

sys.argv = ["bronze_ingest.py", catalog_name]
main()
