"""
Bronze layer: batch ingestion from the Databricks-provided samples catalog.

Reads a fixed set of TPC-DS tables from `samples.tpcds_sf1` (already Delta,
already in Unity Catalog, no download/upload required) and writes them as
managed Delta tables into our own `<catalog>.bronze` schema. This mirrors
a real "land the source data as-is" bronze pattern, just pointed at a
Databricks-hosted source instead of an external file/API.

Run this as a Databricks Job task (spark_python_task). The target catalog
is passed in as a job parameter (sys.argv[1]) so the same code works
across dev/prod catalogs.
"""

import sys

from pyspark.sql import SparkSession

# Tables we need for the retail sales story: 3 sales-channel fact tables
# plus the dimensions they join to.
SOURCE_SCHEMA = "samples.tpcds_sf1"
TABLES_TO_INGEST = [
    "store_sales",
    "catalog_sales",
    "web_sales",
    "customer",
    "customer_address",
    "item",
    "promotion",
    "date_dim",
    "store",
]


def ingest_table(spark: SparkSession, table_name: str, target_catalog: str) -> int:
    source_table = f"{SOURCE_SCHEMA}.{table_name}"
    target_table = f"{target_catalog}.bronze.{table_name}"

    df = spark.table(source_table)
    row_count = df.count()

    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )

    print(f"Ingested {source_table} -> {target_table} ({row_count:,} rows)")
    return row_count


def main() -> None:
    spark = SparkSession.builder.getOrCreate()
    target_catalog = sys.argv[1] if len(sys.argv) > 1 else "retail_lakehouse"

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.bronze")

    total_rows = 0
    for table_name in TABLES_TO_INGEST:
        total_rows += ingest_table(spark, table_name, target_catalog)

    print(f"Bronze ingestion complete: {total_rows:,} total rows across {len(TABLES_TO_INGEST)} tables")


if __name__ == "__main__":
    main()
