"""
Gold layer: business-facing aggregate tables built on top of silver.

Produces:
  - gold.revenue_by_category_channel : revenue and units by item category and channel
  - gold.daily_sales_kpi             : daily revenue/order-count trend for the dashboard
  - gold.customer_rfm_segments       : per-customer RFM score and segment label

RFM scoring uses the pure-Python rules in src/transform/rfm_logic.py via
a pandas UDF, so the scoring logic itself stays unit-testable outside Spark.
"""

import sys

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, IntegerType

from src.quality.expectations import check_min_row_count, run_checks
from src.transform.rfm_logic import score_customer

RFM_SCHEMA = StructType(
    [
        StructField("customer_sk", IntegerType(), True),
        StructField("recency_score", IntegerType(), True),
        StructField("frequency_score", IntegerType(), True),
        StructField("monetary_score", IntegerType(), True),
        StructField("segment", StringType(), True),
    ]
)


def build_revenue_by_category_channel(spark: SparkSession, catalog: str):
    sales = spark.table(f"{catalog}.silver.sales_enriched")
    return (
        sales.groupBy("item_category", "channel")
        .agg(
            F.sum("sale_amount").alias("total_revenue"),
            F.sum("quantity").alias("total_units"),
            F.count("*").alias("num_transactions"),
        )
        .orderBy(F.desc("total_revenue"))
    )


def build_daily_sales_kpi(spark: SparkSession, catalog: str):
    sales = spark.table(f"{catalog}.silver.sales_enriched")
    return (
        sales.groupBy("sale_date")
        .agg(
            F.sum("sale_amount").alias("daily_revenue"),
            F.count("*").alias("daily_order_count"),
            F.countDistinct("customer_sk").alias("daily_unique_customers"),
        )
        .orderBy("sale_date")
    )


def build_customer_rfm_segments(spark: SparkSession, catalog: str):
    sales = spark.table(f"{catalog}.silver.sales_enriched")

    customer_agg = (
        sales.groupBy("customer_sk")
        .agg(
            F.datediff(F.current_date(), F.max("sale_date")).alias("recency_days"),
            F.count("*").alias("frequency"),
            F.sum("sale_amount").alias("monetary"),
        )
        .filter(F.col("customer_sk").isNotNull())
    )

    def score_partition(pdf: pd.DataFrame) -> pd.DataFrame:
        rows = []
        for _, row in pdf.iterrows():
            result = score_customer(
                recency_days=float(row["recency_days"] or 9999),
                frequency=int(row["frequency"]),
                monetary=float(row["monetary"] or 0),
            )
            rows.append(
                {
                    "customer_sk": row["customer_sk"],
                    "recency_score": result.recency_score,
                    "frequency_score": result.frequency_score,
                    "monetary_score": result.monetary_score,
                    "segment": result.segment,
                }
            )
        return pd.DataFrame(rows)

    return customer_agg.groupBy("customer_sk").applyInPandas(score_partition, schema=RFM_SCHEMA)


def main() -> None:
    spark = SparkSession.builder.getOrCreate()
    catalog = sys.argv[1] if len(sys.argv) > 1 else "retail_lakehouse"

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.gold")

    revenue_df = build_revenue_by_category_channel(spark, catalog)
    revenue_df.write.format("delta").mode("overwrite").saveAsTable(
        f"{catalog}.gold.revenue_by_category_channel"
    )

    daily_kpi_df = build_daily_sales_kpi(spark, catalog)
    daily_kpi_df.write.format("delta").mode("overwrite").saveAsTable(
        f"{catalog}.gold.daily_sales_kpi"
    )

    rfm_df = build_customer_rfm_segments(spark, catalog)
    rfm_row_count = rfm_df.count()
    rfm_df.write.format("delta").mode("overwrite").saveAsTable(
        f"{catalog}.gold.customer_rfm_segments"
    )

    run_checks(
        [
            check_min_row_count(revenue_df.count(), minimum=1, table_name="gold.revenue_by_category_channel"),
            check_min_row_count(daily_kpi_df.count(), minimum=1, table_name="gold.daily_sales_kpi"),
            check_min_row_count(rfm_row_count, minimum=1, table_name="gold.customer_rfm_segments"),
        ],
        stage_name="gold",
    )

    print(f"Gold transform complete: {rfm_row_count:,} customers scored")


if __name__ == "__main__":
    main()
