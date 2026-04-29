# RecoMart – End-to-End Data Pipeline

This project contains the deliverables for the DMML Group Assignment 1.

| Task | Folder / File | Notes |
| ---- | ------------- | ----- |
| 2  Ingestion | `scripts/` | CSV + REST API ingestion |
| 3  Raw storage | `data/raw/` | Hive-style partitioned layout |
| 4  Validation | `pipeline/tasks/validate.py` | Schema/null/range checks |
| 5  Preparation | `GROUP29_DMML_ASSIGNMENT1.ipynb`, `pipeline/tasks/prepare.py` | |
| 6  Transformation | notebook, `pipeline/tasks/transform.py` | |
| 7  Feature store | `feature_store/`, `pipeline/tasks/feature_store.py` | versioned |
| 9  Model training | `pipeline/tasks/train.py` | TruncatedSVD recommender |
| **10  Orchestration** | **`pipeline/`** | **Prefect flow + DAG; see `pipeline/README.md` and `pipeline/TASK10.md`** |

Quick start for the orchestrated end-to-end run:

```powershell
py -3 -m pip install -r requirements.txt
py -3 -m pipeline.flow --once
```

A console log of a successful run is preserved at
`sample_logs/prefect_flow_run.log` and a structured run summary at
`logs/pipeline_summary_<run_id>.json`.

---

## Ingestion scripts (Task 2)

This folder contains ingestion scripts for **two data types**:
1. **User interactions** from CSV files (clickstream exports)
2. **Product catalog** from a REST API (with retry + audit logging)

## Folder / Bucket structure (Raw)

```
<project_or_bucket_root>/
  data/
    raw/
      clickstream/
        events/
          ingest_date=YYYY-MM-DD/
            ingest_hour=HH/
              clickstream_*.csv
      catalog/
        products/
          ingest_date=YYYY-MM-DD/
            ingest_hour=HH/
              products_page=1_YYYYMMDDTHHMMSSZ.json
  logs/
  sample_logs/
  .checkpoints/
```

## How to run

### 1) Clickstream CSV ingestion

```bash
python scripts/ingest_clickstream_csv.py \
  --input-dir sample_input \
  --pattern "clickstream_*.csv" \
  --raw-root . \
  --log-file sample_logs/clickstream_ingestion.log \
  --once
```

### 2) Products API ingestion

**API mode**:
```bash
python scripts/ingest_products_api.py \
  --endpoint "https://api.recomart.com/v1/products" \
  --raw-root . \
  --log-file sample_logs/products_ingestion.log \
  --page-size 200 \
  --once
```

**Offline mock mode** (for testing without network):
```bash
python scripts/ingest_products_api.py \
  --mock-file sample_input/products_mock.json \
  --raw-root . \
  --log-file sample_logs/products_ingestion.log \
  --once
```

## Scheduling

- In production, schedule scripts via **cron**, **Airflow**, **Dagster**, **K8s CronJobs**, etc.
- Both scripts support periodic execution using `--interval-sec` (omit `--once`).

## Logging

Logs are emitted as JSON lines to stdout and `--log-file` for monitoring/audit.

## Notes

- Clickstream ingestion is idempotent using a checkpoint file under `.checkpoints/`.
- Products API ingestion has retries with exponential backoff for transient failures.
