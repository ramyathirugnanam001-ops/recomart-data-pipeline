# RecoMart – Data Collection & Ingestion (Task 2)

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
