# Setup guide

Step-by-step from an empty GitHub repo + Databricks Free Edition workspace to a running,
scheduled pipeline.

## 1. Clone this repo and push the generated files

```bash
git clone https://github.com/EvansTanui/Retail-Project-Lakehouse-Data-Engineering-Pipeline.git
cd Retail-Project-Lakehouse-Data-Engineering-Pipeline
# copy in the generated project files, then:
git add .
git commit -m "Scaffold retail lakehouse pipeline"
git push
```

## 2. Create the Unity Catalog catalog and a Volume for the stream landing zone

In a Databricks SQL editor or notebook, run once:

```sql
CREATE CATALOG IF NOT EXISTS retail_lakehouse;
CREATE SCHEMA IF NOT EXISTS retail_lakehouse.bronze;
CREATE VOLUME IF NOT EXISTS retail_lakehouse.bronze.order_stream_landing;
```

## 3. Generate a Databricks personal access token

Workspace -> User Settings -> Developer -> Access tokens -> Generate new token.
Copy it now; you won't be able to see it again.

## 4. Add GitHub Actions secrets

In your GitHub repo: Settings -> Secrets and variables -> Actions -> New repository secret.

| Secret name | Value |
|---|---|
| `DATABRICKS_HOST` | Your workspace URL, e.g. `https://dbc-xxxxxxx.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | The personal access token from step 3 |

## 5. Connect Databricks Repos to this GitHub repo (optional but recommended)

Workspace sidebar -> Repos -> Add Repo -> paste the GitHub URL. This lets you edit notebooks
directly in the Databricks UI and pull/push against the same GitHub history as your local clone.
You'll need to first link your GitHub account under User Settings -> Linked accounts.

## 6. Install the Databricks CLI locally (optional, for manual deploys)

```bash
pip install databricks-cli
databricks configure --token   # paste your workspace URL and token when prompted
```

## 7. Deploy the bundle

```bash
databricks bundle validate -t dev   # sanity-checks the yaml before deploying
databricks bundle deploy -t dev
```

This creates the `retail-lakehouse-pipeline` Job in your workspace (paused by default).

## 8. Run it once manually to validate

```bash
databricks bundle run retail_lakehouse_pipeline -t dev
```

Or trigger it from the Databricks Jobs UI. Watch the task graph run:
`simulate_live_orders` -> `bronze_batch_ingest` + `bronze_stream_orders` -> `silver_transform` -> `gold_transform`.

## 9. Unpause the schedule

Once a manual run succeeds, edit `resources/jobs.yml`, set `pause_status: UNPAUSED`, commit, push --
GitHub Actions will redeploy the bundle with the schedule active.

## 10. Build a dashboard on the gold tables

In Databricks SQL, create a dashboard against:
- `retail_lakehouse.gold.revenue_by_category_channel`
- `retail_lakehouse.gold.daily_sales_kpi`
- `retail_lakehouse.gold.customer_rfm_segments`

## Troubleshooting

- **"Catalog not found"** -- make sure you ran step 2, and that your Free Edition account has
  permission to create catalogs (it does by default).
- **CI passes but deploy fails** -- double check the `DATABRICKS_HOST`/`DATABRICKS_TOKEN` secrets
  are set correctly and the token hasn't expired.
- **Job task fails on a data quality check** -- that's the pipeline working as intended; read the
  `DataQualityError` message in the task's stdout logs, it names exactly which check failed and why.
