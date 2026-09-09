# Databricks notebook source
# MAGIC %md
# MAGIC ## Silver: clean, dedupe, and join bronze into `silver.sales_enriched`

# COMMAND ----------

import sys
sys.path.append("..")

from src.transform.silver_transform import main

dbutils.widgets.text("catalog_name", "retail_lakehouse")
sys.argv = ["silver_transform.py", dbutils.widgets.get("catalog_name")]
main()
