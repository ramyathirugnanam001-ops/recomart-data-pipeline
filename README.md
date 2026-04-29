# RecoMart – End-to-End Data Pipeline

This project contains the deliverables for the DMML Group Assignment 1.

| Task | Folder / File | Notes                                                      |
| ---- | ------------- |------------------------------------------------------------|
| 2  Ingestion | `scripts/` | CSV + REST API ingestion                                   |
| 3  Raw storage | `data/raw/` | Hive-style partitioned layout                              |
| 4  Validation | `pipeline/tasks/validate.py` | Schema/null/range checks                                   |
| 5  Preparation | `GROUP29_DMML_ASSIGNMENT1.ipynb`, `pipeline/tasks/prepare.py` |                                                            |
| 6  Transformation | notebook, `pipeline/tasks/transform.py` |                                                            |
| 7  Feature store | `feature_store/`, `pipeline/tasks/feature_store.py` | versioned                                                  |
| 9  Model training | `pipeline/tasks/train.py` | TruncatedSVD recommender                                   |
| **10  Orchestration** | **`pipeline/`** | **Prefect flow + DAG; For reference `pipeline/TASK10.md`** |

Quick start for the orchestrated end-to-end run:

```powershell
py -3 -m pip install -r requirements.txt
py -3 -m pipeline.flow --once
```

A console log of a successful run is preserved at
`sample_logs/prefect_flow_run.log` and a structured run summary at
`logs/pipeline_summary_<run_id>.json`.

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

---

## Pipeline Orchestration (Task 10)

The end-to-end data pipeline is automated through a Prefect-based
orchestration layer located in the `pipeline/` package. The orchestration
layer coordinates ingestion, validation, preparation, transformation,
feature publication, and model training into a single reproducible workflow.
Detailed documentation is provided in `pipeline/TASK10.md`; a high-level
summary is included below.

### DAG

```
ingest_clickstream ──┐
                    ├──► validate ──► prepare ──► transform ──► feature_store_publish ──► train_model
ingest_products    ──┘
```

The two ingestion tasks are submitted concurrently. The validation stage is
gated on the successful completion of both ingestion tasks via Prefect's
`wait_for` mechanism. The remaining stages execute sequentially, with each
task consuming the return value of the previous one.

### Module Layout

```
pipeline/
├── __init__.py
├── flow.py                   # Prefect @flow + @task DAG wiring
├── README.md                 # operational guide
├── TASK10.md                 # formal Task 10 documentation
└── tasks/
    ├── ingest.py             # wraps Task 2 ingestion scripts
    ├── validate.py           # Task 4 — schema, null, range checks
    ├── prepare.py            # Task 5 — clean, merge, encode, normalize
    ├── transform.py          # Task 6 — feature engineering
    ├── feature_store.py      # Task 7 — versioned feature publication
    └── train.py              # Task 9 — TruncatedSVD recommender
```

### Execution

A single command executes the full DAG end-to-end:

```powershell
py -3 -m pip install -r requirements.txt
py -3 -m pipeline.flow --once
```

The command line interface accepts the following parameters for input
overrides: `--clickstream-input-dir`, `--clickstream-pattern`,
`--products-mock`, `--products-endpoint`, and `--products-page-size`.

### Optional Execution with the Prefect User Interface

For graphical monitoring and screenshot evidence, a local Prefect server may
be started in a dedicated terminal:

```powershell
prefect server start
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
py -3 -m pipeline.flow --once
```

The DAG view, task-level logs, and per-task state transitions are then
accessible at `http://127.0.0.1:4200`.

### Output Artefacts (per run)

| Stage | Output |
| ----- | ------ |
| Ingestion | `data/raw/clickstream/...`, `data/raw/catalog/...` |
| Stage logs | `logs/clickstream_ingestion_<run_id>.log`, `logs/products_ingestion_<run_id>.log` |
| Validation | `logs/data_quality_<run_id>.json` |
| Preparation | `data/prepared/prepared_<run_id>.csv` |
| Transformation | `data/features/final_features_<run_id>.csv` |
| Feature Store | `feature_store/<version>/{final_features.csv, feature_metadata.json}` |
| Model | `models/<version>/{model.pkl, run_metadata.json}` |
| Run Summary | `logs/pipeline_summary_<run_id>.json` |

### Logging and Monitoring

Three complementary mechanisms are employed:

- Prefect native task-level logs, surfaced in the console and in the optional
  web user interface, including state transitions, durations, retry counts,
  and exception traces.
- Structured JSON-line logs produced through `common.logger` for each
  ingestion task, enabling offline parsing of `stage_start`,
  `ingest_success`, `skip_already_processed`, and `stage_complete` events.
- A consolidated `pipeline_summary_<run_id>.json` document, written at the
  end of every successful run, aggregating the return values of every stage.

### Retry and Failure Handling

- The two ingestion tasks are decorated with `retries=2` and `retries=3`
  respectively, paired with `retry_delay_seconds=exponential_backoff(...)`,
  so transient network or filesystem errors trigger automatic retries with
  increasing delays.
- The validation task raises `ValueError` on hard data-quality failures.
  Prefect propagates the exception and prevents downstream tasks from being
  scheduled, ensuring fail-fast behaviour that protects feature store and
  model artefacts from polluted data.
- All retries and failures are visible in both console output and the
  Prefect user interface.

### Versioning Strategy

- **Run identifier (`run_id`)**: a UTC timestamp generated at flow start,
  threaded through every per-run output path so that artefacts from
  concurrent runs cannot collide.
- **Feature store version**: a UTC timestamp used as the directory name
  under `feature_store/`. Each version directory contains the data file
  and its accompanying `feature_metadata.json`.
- **Model version**: reuses the feature store version, ensuring that each
  trained model is deterministically associated with the exact feature
  snapshot used for its training. Each model directory under
  `models/<version>/` records a UUID run identifier, parameters, and
  evaluation metrics in `run_metadata.json`.

### Tool Selection

Prefect was selected over Apache Airflow and Dagster on the basis of native
Windows compatibility, ephemeral execution mode (no scheduler, broker, or
relational database required), a Python-native `@flow`/`@task` programming
model that allows ordinary functions to be promoted to orchestratable units,
and a built-in web user interface that satisfies the assignment's
requirement for screenshot or log evidence with minimal setup overhead.
A fuller justification is provided in Section 10 of `pipeline/TASK10.md`.

---
