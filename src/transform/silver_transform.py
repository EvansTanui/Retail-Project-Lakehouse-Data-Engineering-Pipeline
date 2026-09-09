"""
Silver layer: clean, dedupe, type-cast, and join bronze tables into one
enriched sales fact table.

Joins the three TPC-DS sales channels (store, catalog, web) to their
shared dimensions (customer, item, date_dim) into a single unioned,
denormalized `silver.sales_enriched` table, and separately cleans the
simulated streaming web orders into `silver.web_orders_clean`.

Data quality checks run at the end and raise DataQualityError (failing
the Databricks task) if anything looks wrong.
"""

import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.quality.expectations import (
    check_min_row_count,
    check_no_negative_values,
    check_null_ratio,
    check_row_count_within_tolerance,
    run_checks,
)


def build_sales_channel(
    spark: SparkSession,
    catalog: str,
    sales_table: str,
    channel_name: str,
    quantity_col: str,
    price_col: str,
    customer_key_col: str,
    item_key_col: str,
    date_key_col: str,
) -> DataFrame:
    """Read one TPC-DS sales fact table and normalize its columns to a
    common shape so store/catalog/web sales can be unioned."""
    sales = spark.table(f"{catalog}.bronze.{sales_table}")
    customer = spark.table(f"{catalog}.bronze.customer")
    item = spark.table(f"{catalog}.bronze.item")
    date_dim = spark.table(f"{catalog}.bronze.date_dim")

    joined = (
        sales.join(customer, sales[customer_key_col] == customer["c_customer_sk"], "left")
        .join(item, sales[item_key_col] == item["i_item_sk"], "left")
        .join(date_dim, sales[date_key_col] == date_dim["d_date_sk"], "left")
        .select(
            F.col("c_customer_sk").alias("customer_sk"),
            F.col("c_first_name").alias("customer_first_name"),
            F.col("c_last_name").alias("customer_last_name"),
            F.col("i_item_sk").alias("item_sk"),
            F.col("i_category").alias("item_category"),
            F.col("i_current_price").alias("item_price"),
            F.col("d_date").alias("sale_date"),
            F.col(quantity_col).cast("int").alias("quantity"),
            F.col(price_col).cast("decimal(10,2)").alias("sale_amount"),
            F.lit(channel_name).alias("channel"),
        )
        .filter(F.col("sale_amount").isNotNull())
    )
    return joined


def build_silver_sales(spark: SparkSession, catalog: str) -> DataFrame:
    store = build_sales_channel(
        spark, catalog, "store_sales", "store",
        "ss_quantity", "ss_net_paid", "ss_customer_sk", "ss_item_sk", "ss_sold_date_sk",
    )
    catalog_ch = build_sales_channel(
        spark, catalog, "catalog_sales", "catalog",
        "cs_quantity", "cs_net_paid", "cs_bill_customer_sk", "cs_item_sk", "cs_sold_date_sk",
    )
    web = build_sales_channel(
        spark, catalog, "web_sales", "web",
        "ws_quantity", "ws_net_paid", "ws_bill_customer_sk", "ws_item_sk", "ws_sold_date_sk",
    )
    return store.unionByName(catalog_ch).unionByName(web)


def build_silver_streaming_orders(spark: SparkSession, catalog: str) -> DataFrame:
    raw = spark.table(f"{catalog}.bronze.web_orders_stream")
    return (
        raw.dropDuplicates(["order_id"])
        .withColumn("sale_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
        .select(
            "order_id",
            "order_ts",
            "customer_name",
            "customer_email",
            "customer_city",
            "customer_state",
            "item_category",
            "item_name",
            "quantity",
            "unit_price",
            "sale_amount",
            "channel",
        )
        .filter(F.col("sale_amount") >= 0)
    )


def main() -> None:
    spark = SparkSession.builder.getOrCreate()
    catalog = sys.argv[1] if len(sys.argv) > 1 else "retail_lakehouse"

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver")

    upstream_count = sum(
        spark.table(f"{catalog}.bronze.{t}").count()
        for t in ("store_sales", "catalog_sales", "web_sales")
    )

    silver_sales = build_silver_sales(spark, catalog)
    silver_sales_count = silver_sales.count()

    (
        silver_sales.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{catalog}.silver.sales_enriched")
    )

    silver_orders = build_silver_streaming_orders(spark, catalog)
    (
        silver_orders.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{catalog}.silver.web_orders_clean")
    )

    # Data quality gate
    null_customer = silver_sales.filter(F.col("customer_sk").isNull()).count()
    min_sale_amount = silver_sales.agg(F.min("sale_amount")).collect()[0][0] or 0

    run_checks(
        [
            check_min_row_count(silver_sales_count, minimum=1000, table_name="silver.sales_enriched"),
            check_null_ratio(null_customer, silver_sales_count, max_ratio=0.05, column_name="customer_sk"),
            check_no_negative_values(float(min_sale_amount), column_name="sale_amount"),
            check_row_count_within_tolerance(
                upstream_count, silver_sales_count, max_drop_ratio=0.10, stage_name="silver_sales_join"
            ),
        ],
        stage_name="silver",
    )

    print(f"Silver transform complete: {silver_sales_count:,} rows in sales_enriched, "
          f"{silver_orders.count():,} rows in web_orders_clean")


if __name__ == "__main__":
    main()
