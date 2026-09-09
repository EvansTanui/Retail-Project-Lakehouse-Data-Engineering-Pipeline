# Retail Lakehouse Data Engineering Pipeline

An end-to-end data engineering pipeline built on **Databricks (Free Edition)**, using the
**medallion architecture** (bronze -> silver -> gold), orchestrated with **Databricks Workflows**,
version-controlled and deployed via **GitHub + GitHub Actions + Databricks Asset Bundles**.

## Data sources

- **`samples.tpcds_sf1`** -- a TPC-DS benchmark dataset (scale factor 1, ~1GB) already available
  in every Databricks workspace's Unity Catalog `samples` catalog. It models a retail company's
  data warehouse: sales across store/catalog/web channels, customers, items, promotions, dates.
  No download or upload needed -- we read it directly.
- **Simulated live order stream** -- a small Python/Faker generator (`src/ingestion/stream_simulator.py`)
  that writes new synthetic web-order JSON files to a Unity Catalog Volume over time, so the
  pipeline also demonstrates incremental/streaming ingestion via Databricks Autoloader, not just
  a one-off batch load.

## Architecture

```
samples.tpcds_sf1  ----+
                        +--> BRONZE --> SILVER --> GOLD --> Databricks SQL dashboard
simulated live feed ---+     (raw)     (cleaned,   (business
                                         joined)     aggregates)
```

| Layer  | What happens | Code |
|---|---|---|
| Bronze | Snapshot key TPC-DS tables into our own catalog; Autoloader ingests simulated live orders | `src/ingestion/` |
| Silver | Clean, dedupe, cast types, join facts to dimensions into one enriched sales table | `src/transform/silver_transform.py` |
| Gold   | Revenue by channel/category/region, customer RFM segments, daily KPIs | `src/transform/gold_transform.py` |
| Quality | Row-count and null-ratio checks between layers | `src/quality/expectations.py` |

## Repo layout

```
retail-lakehouse-pipeline/
├── databricks.yml                   # Databricks Asset Bundle entry point
├── resources/
│   └── jobs.yml                     # Workflow/Job definition (bronze -> silver -> gold)
├── src/
│   ├── ingestion/
│   │   ├── bronze_ingest.py         # Batch snapshot of samples.tpcds_sf1 -> bronze
│   │   ├── stream_simulator.py      # Faker-based live order generator
│   │   └── bronze_stream_orders.py  # Autoloader ingestion of the simulated feed
│   ├── transform/
│   │   ├── rfm_logic.py             # Pure-Python RFM segmentation logic (unit tested)
│   │   ├── silver_transform.py      # PySpark: clean + join into silver
│   │   └── gold_transform.py        # PySpark: business aggregates
│   └── quality/
│       └── expectations.py          # Pure-Python data quality checks (unit tested)
├── tests/
│   ├── test_expectations.py
│   └── test_rfm_logic.py
├── notebooks/                       # Thin Databricks notebooks that call into src/
├── .github/workflows/ci.yml         # Lint + test on every push/PR, deploy bundle on main
└── docs/setup.md                    # Step-by-step setup guide
```

Business logic that doesn't need Spark (RFM segmentation rules, data quality thresholds) is kept
in plain Python modules with no Spark dependency, so it can be unit tested in GitHub Actions
without needing a cluster. The PySpark scripts are thin orchestration layers that call these
functions and handle the actual I/O against Delta tables.

## Setup

See [`docs/setup.md`](docs/setup.md) for the full step-by-step guide: creating the Unity Catalog
catalog/schemas, connecting this repo to Databricks Repos, configuring GitHub Actions secrets,
and deploying the bundle.

## License

MIT
