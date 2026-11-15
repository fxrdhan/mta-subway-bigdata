# Batch ELT pipeline (MTA Subway Hourly Ridership)

Implements the batch path of the week 9 design: SODA API → MinIO (`bronze` → `silver` → `gold`, Parquet)
→ PostgreSQL, with DuckDB as the transformation engine.

| Step | Module | What it does |
|---|---|---|
| Extract and load | `ingest.py` | Compares each day's server fingerprint (`count(*)`, `sum(ridership)`, `sum(transfers)`, `max(:updated_at)`) with `ingest_day` and copies changed days as they are to `bronze/ridership/YYYY-MM/YYYY-MM-DD.parquet`. |
| Transform | `transform.py`, `quality.py` | Checks the 12 quality rules per month (a blocking violation stops the run before anything is written), repairs station mode and negative transfers, flags DST hours, and writes `silver` per month and the `gold` marts. |
| Load | `load.py` | Replaces each changed month in the `fact_*` tables in one transaction (safe to rerun) and reloads `dim_station`, `dim_date`, and `mart_station_hour_profile`. |
| Bookkeeping | `db.py` | `pipeline_run` (one row per run), `ingest_day`, `silver_month`, `quality_month`. |
| Check | `verify.py` | Daily ridership in PostgreSQL must equal the server totals stored in `ingest_day`. |
| Model | `train.py` | Weekly (CronJob `mta-train`): trains a HistGradientBoosting (Poisson) model of hourly ridership per station from the last 365 days of silver and stores it in MinIO (`models/ridership/`) and in `ml_model`. |
| App | `app/streamlit_app.py` | Streamlit dashboard and prediction (Deployment `mta-app`), reading PostgreSQL with the read-only account in the Secret `mta-app-db`. |

## Run

In the cluster it runs as the CronJob `mta-batch-elt` (01:00 UTC daily) in the `mta-pipeline` namespace:

```bash
sh k8s/deploy.sh                                   # publish the code (ConfigMaps), the CronJobs, and the app
kubectl -n mta-pipeline create job --from=cronjob/mta-batch-elt mta-batch-elt-manual
kubectl -n mta-pipeline logs -f job/mta-batch-elt-manual
```

Options: `--limit-days N` fetches only the N most recent changed days (tests); `--rebuild` rebuilds every month
from bronze, for example after a rule change.

Locally, against a local directory instead of MinIO:

```bash
LAKE=/tmp/lake PGHOST=127.0.0.1 PGUSER=mta PGDATABASE=mta PGPASSWORD=... uv run python -m mta_pipeline --limit-days 3
```

Credentials come from the Kubernetes Secrets `mta-minio` and `mta-postgres`; they are never stored in this repository.
