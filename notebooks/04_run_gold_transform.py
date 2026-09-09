# Databricks notebook source
# MAGIC %md
# MAGIC ## Gold: business aggregates and customer RFM segments

# COMMAND ----------

import sys
sys.path.append("..")

from src.transform.gold_transform import main

dbutils.widgets.text("catalog_name", "retail_lakehouse")
sys.argv = ["gold_transform.py", dbutils.widgets.get("catalog_name")]
main()
