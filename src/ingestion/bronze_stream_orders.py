"""
Bronze layer: incremental ingestion of the simulated live order feed via
Databricks Autoloader.

Runs as a Databricks Job task using `trigger(availableNow=True)` so it
behaves like a scheduled incremental batch job (processes whatever new
files have landed since last run, then stops) rather than a
never-ending streaming job -- this fits Free Edition's serverless job
model cleanly and avoids paying for an always-on cluster.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name


def main() -> None:
    spark = SparkSession.builder.getOrCreate()

    target_catalog = sys.argv[1] if len(sys.argv) > 1 else "retail_lakehouse"
    landing_path = (
        sys.argv[2]
        if len(sys.argv) > 2
        else f"/Volumes/{target_catalog}/bronze/order_stream_landing/"
    )
    checkpoint_path = f"/Volumes/{target_catalog}/bronze/_checkpoints/order_stream/"
    target_table = f"{target_catalog}.bronze.web_orders_stream"

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.bronze")

    stream_df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", checkpoint_path + "_schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(landing_path)
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )

    query = (
        stream_df.writeStream.format("delta")
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(target_table)
    )

    query.awaitTermination()

    row_count = spark.table(target_table).count()
    print(f"Autoloader run complete. {target_table} now has {row_count:,} total rows.")


if __name__ == "__main__":
    main()
