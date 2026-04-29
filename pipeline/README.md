# Task 10 — Pipeline Orchestration (Prefect)

This package automates the end-to-end RecoMart data pipeline using
[Prefect 2](https://docs.prefect.io). It defines a single **flow**
(`recomart_end_to_end_pipeline`) wiring six **tasks** in the required DAG:

```
            ┌────────────────────┐
            │ ingest_clickstream │─┐
            └────────────────────┘ │
                                   ├──► validate ──► prepare ──► transform ──► feature_store ──► train_model
            ┌────────────────────┐ │
            │  ingest_products   │─┘
            └────────────────────┘
```

Why Prefect (and not Airflow)? Prefect runs natively on Windows / Python 3.13
(the user's environment). Airflow's scheduler is *not* officially supported on
Windows, which would force WSL/Docker. Prefect provides the same DAG model,
retries, scheduling and a UI at `http://127.0.0.1:4200`.

## Layout

```
pipeline/
├── __init__.py
├── flow.py                 # Prefect flow + DAG wiring
├── README.md               # this file
└── tasks/
    ├── __init__.py
    ├── ingest.py           # wraps Task 2 ingestion scripts
    ├── validate.py         # Task 4 — schema / completeness / range checks
    ├── prepare.py          # Task 5 — clean, merge, encode, normalize
    ├── transform.py        # Task 6 — feature engineering
    ├── feature_store.py    # Task 7 — versioned feature store publish
    └── train.py            # Task 9 — TruncatedSVD recommender + metrics
```

Outputs produced by each run (timestamped with `run_id`):

| Stage           | Output                                                |
|-----------------|-------------------------------------------------------|
| ingest          | `data/raw/clickstream/...`, `data/raw/catalog/...`    |
| validate        | `logs/data_quality_<run_id>.json`                     |
| prepare         | `data/prepared/prepared_<run_id>.csv`                 |
| transform       | `data/features/final_features_<run_id>.csv`           |
| feature_store   | `feature_store/<run_id>/{final_features.csv,feature_metadata.json}` |
| train_model     | `models/<run_id>/{model.pkl,run_metadata.json}`       |
| flow summary    | `logs/pipeline_summary_<run_id>.json`                 |

Per-stage logs are written to `logs/clickstream_ingestion_<run_id>.log` and
`logs/products_ingestion_<run_id>.log` in JSON format (re-using the
`common.logger` JSON formatter), in addition to Prefect's own task logs.

## Setup

```powershell
# from the project root: E:\BITS ASSIGNMENT\DMML
py -3 -m pip install -r requirements.txt
```

## Run the flow once (no UI)

```powershell
py -3 -m pipeline.flow --once
```

This is enough to satisfy the deliverable for "logs from the orchestration tool
showing successful execution"; Prefect prints state transitions for every task
to the console:

```
13:00:01 | INFO | Flow run 'kind-frog' - Beginning flow run 'kind-frog'
13:00:01 | INFO | Task run 'ingest_clickstream-0' - Created task run...
13:00:02 | INFO | Task run 'ingest_clickstream-0' - Finished in state Completed()
13:00:02 | INFO | Task run 'ingest_products-0'   - Finished in state Completed()
13:00:02 | INFO | Task run 'validate-0'          - Finished in state Completed()
13:00:03 | INFO | Task run 'prepare-0'           - Finished in state Completed()
13:00:03 | INFO | Task run 'transform-0'         - Finished in state Completed()
13:00:03 | INFO | Task run 'feature_store_publish-0' - Finished in state Completed()
13:00:04 | INFO | Task run 'train_model-0'       - Finished in state Completed()
13:00:04 | INFO | Flow run 'kind-frog' - Finished in state Completed()
```

## Run via the Prefect UI (recommended for screenshots)

1. **Start the Prefect Orion / server** (one-time, in a dedicated terminal):

   ```powershell
   prefect server start
   ```

   This boots the API + UI at `http://127.0.0.1:4200`.

2. **Point the CLI at the local server** (only needed once per shell):

   ```powershell
   prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
   ```

3. **Trigger the flow** in another terminal:

   ```powershell
   py -3 -m pipeline.flow --once
   ```

4. Open the UI → **Flow Runs** → click the latest run → take screenshots of:
   - The DAG / task-graph view (shows the 7 nodes and their edges)
   - The **Logs** tab (shows the JSON `stage_start`/`stage_complete` events)
   - The **Task Runs** table with all states `Completed`

   Save them under `docs/screenshots/`.

## Scheduling (optional, for production)

Deploy the flow on a cron schedule (every 6 hours):

```powershell
prefect deploy pipeline/flow.py:recomart_pipeline `
    --name recomart-hourly `
    --cron "0 */6 * * *" `
    --pool default-agent-pool

prefect worker start --pool default-agent-pool
```

The flow is also fully usable in **Airflow** or **Dagster** by importing the
same `pipeline.tasks.*` functions inside the corresponding DAG / job.

## Failure handling & monitoring

- Each ingestion task uses Prefect `retries=2`/`3` with **exponential backoff**.
- `validate_task` raises `ValueError` on hard schema/range failures, which
  Prefect surfaces as a failed task run (downstream tasks are not started).
- Every stage writes structured JSON logs via `common.logger` and a
  per-run `pipeline_summary_<run_id>.json` for offline auditing.
- The Prefect UI provides task-level retry counts, durations, exceptions and
  log search out of the box.
