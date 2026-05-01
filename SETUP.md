# RecoMart End-to-End Pipeline — Setup Guide

This document walks a fresh machine (Windows / PowerShell) from `git clone`
to a successful end-to-end run of the orchestrated pipeline. It also covers
the optional Prefect UI and Git LFS configuration.

## 1. Prerequisites

Install the following before cloning:

| Tool | Version | Notes |
|---|---|---|
| Python | 3.10+ (3.13 verified) | The `py -3` launcher must be available. Check with `py -3 --version`. |
| Git | 2.30+ | For cloning. |
| Git LFS *(optional)* | 3.x | Only needed if large data artefacts must be pulled / pushed via LFS. |
| PowerShell | 5.1+ or 7+ | Default on Windows. |

If `py` is missing, install Python from `https://www.python.org/downloads/`
and ensure the Python launcher is enabled.

## 2. Clone the Repository

```powershell
git clone <REPO_URL> recomart-data-pipeline
cd recomart-data-pipeline
```

Replace `<REPO_URL>` with the project's Git URL.

### 2a. (Optional) Pull Git LFS-tracked data
If the remote stores large artefacts through Git LFS:

```powershell
git lfs install
git lfs pull
```

The repository's `.gitattributes` already declares the LFS-tracked paths
(`data/raw/**`, `data/prepared/**`, `data/features/**`,
`feature_store/**/*.csv`, `models/**`, `logs/**`, `sample_logs/**`).

## 3. Install Python Dependencies

A pinned `requirements.txt` is provided at the project root.

```powershell
py -3 -m pip install --upgrade pip
py -3 -m pip install -r requirements.txt
```

Installed packages: `pandas`, `numpy`, `scikit-learn`, `requests`, `prefect`.

## 4. One-Time Prefect Hygiene

The pipeline runs Prefect in *ephemeral* mode (no server required). Make
sure the Prefect CLI is **not** pointing at a non-existent server:

```powershell
prefect config unset PREFECT_API_URL
```

If a previous Prefect installation left a corrupted local state DB, reset
it with:

```powershell
$prefHome = Join-Path $HOME ".prefect"
Remove-Item -Force "$prefHome\prefect.db", "$prefHome\prefect.db-shm", "$prefHome\prefect.db-wal" -ErrorAction SilentlyContinue
```

## 5. Verify Sample Inputs

The repository ships with two static sample inputs that the pipeline reads
when no other source is specified. They must be present:

```
sample_input/
├── clickstream_2026-02-15.csv
└── products_mock.json
```

Sanity check:

```powershell
Test-Path sample_input\clickstream_2026-02-15.csv
Test-Path sample_input\products_mock.json
```

Both should print `True`. If either is missing, restore it from the
repository (it is part of the committed source tree).

## 6. Reset the Idempotency Checkpoint *(only when re-ingesting the same file)*

The clickstream ingester is idempotent: a CSV that has already been processed
(by SHA-256) will be skipped on subsequent runs. To force a fresh ingestion
of the bundled sample CSV, clear the checkpoint:

```powershell
Remove-Item .checkpoints\clickstream_files.txt -ErrorAction SilentlyContinue
```

This step is optional. Skipping it is safe and is the expected behaviour
for repeated runs.

## 7. Run the Full Pipeline

```powershell
py -3 -m pipeline.flow --once
```

The flow executes the following DAG:

```
ingest_clickstream ──┐
                     ├──► validate ──► prepare ──► transform ──► feature_store_publish ──► train_model ──► lineage_manifest
ingest_products    ──┘
```

A successful run prints `Finished in state Completed()` for every task and
exits with code `0`.

To save the console output for evidence:

```powershell
py -3 -m pipeline.flow --once *>&1 | Tee-Object -FilePath sample_logs\prefect_flow_run.log
```

## 8. Verify Outputs

Each run is identified by a UTC timestamp `run_id`. Artefacts produced:

| Stage | Output |
|---|---|
| Raw landing | `data/raw/clickstream/...`, `data/raw/catalog/...` |
| Stage logs | `logs/clickstream_ingestion_<run_id>.log`, `logs/products_ingestion_<run_id>.log` |
| Validation | `logs/data_quality_<run_id>.json` |
| Preparation | `data/prepared/prepared_<run_id>.csv` |
| Transformation | `data/features/final_features_<run_id>.csv` |
| Feature store | `feature_store/<version>/{final_features.csv, feature_metadata.json}` |
| Model | `models/<version>/{model.pkl, run_metadata.json}` |
| Lineage manifest | `data/metadata/dataset_lineage.json` |
| Run summary | `logs/pipeline_summary_<run_id>.json` |

Quick verification:

```powershell
Get-ChildItem logs\pipeline_summary_*.json | Sort-Object LastWriteTime -Descending | Select-Object -First 1 | Get-Content
Get-ChildItem models, feature_store -Directory | Sort-Object LastWriteTime -Descending | Select-Object -First 4 Name, LastWriteTime
Get-Content data\metadata\dataset_lineage.json -TotalCount 30
```

## 9. (Optional) Run with the Prefect UI

For graphical monitoring or screenshots, start the Prefect server in a
dedicated terminal:

```powershell
prefect server start
```

Wait until `Check out the dashboard at http://127.0.0.1:4200` is shown.

In a second terminal, point the CLI at the local server and trigger a run:

```powershell
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
py -3 -m pipeline.flow --once
```

Open `http://127.0.0.1:4200` in a browser, navigate to **Flow Runs**, and
inspect the latest run for the DAG view, task-level logs, and state
transitions.

To return to ephemeral mode afterwards:

```powershell
prefect config unset PREFECT_API_URL
```

## 10. Optional CLI Overrides

The flow exposes the following parameters:

```
--clickstream-input-dir <DIR>      # default: sample_input
--clickstream-pattern   <GLOB>     # default: clickstream_*.csv
--products-mock         <FILE>     # default: sample_input/products_mock.json
--products-endpoint     <URL>      # use the live API instead of the mock
--products-page-size    <N>        # default: 200
```

Examples:

```powershell
# Use a different input directory
py -3 -m pipeline.flow --once --clickstream-input-dir custom_inputs

# Use the live products API instead of the bundled mock
py -3 -m pipeline.flow --once --products-endpoint "https://api.recomart.com/v1/products" --products-mock ""
```

## 11. Optional Manual Lineage Manifest Refresh

The lineage manifest at `data/metadata/dataset_lineage.json` is regenerated
automatically as the final stage of every flow run. To regenerate it
manually (e.g. after a manual ingestion outside the orchestrator):

```powershell
py -3 scripts\generate_lineage_manifest.py
```

## 12. Troubleshooting

| Symptom | Cause / Fix |
|---|---|
| `Cannot create flow run. Failed to reach API at http://127.0.0.1:4200/api/` | Stale `PREFECT_API_URL`. Run `prefect config unset PREFECT_API_URL` or start `prefect server start` in another terminal. |
| `alembic.util.exc.CommandError: Can't locate revision identified by '...'` | Corrupted Prefect ephemeral DB. Delete `~/.prefect/prefect.db` (and `prefect.db-shm`, `prefect.db-wal`) and re-run. |
| Clickstream stage reports all files `skipped` | Expected on a re-run. Clear `.checkpoints\clickstream_files.txt` to force re-ingestion. |
| `ModuleNotFoundError: No module named 'prefect'` (or `pandas`/`sklearn`) | Run `py -3 -m pip install -r requirements.txt` again. |
| `FileNotFoundError: No clickstream CSVs found under ...` | Sample input is missing. Ensure `sample_input/clickstream_*.csv` exists. |
| Validation task fails with `ValueError: Validation failed: [...]` | Hard data-quality issue (missing columns, negative price, etc.). Inspect `logs/data_quality_<run_id>.json`. |

## 13. Sanity Checklist for the Demo

Before presenting:

```powershell
# 1. Dependencies present
py -3 -m pip show prefect pandas scikit-learn

# 2. Prefect not pointing at a dead server
prefect config view

# 3. Sample inputs present
Test-Path sample_input\clickstream_2026-02-15.csv
Test-Path sample_input\products_mock.json

# 4. Pipeline runs cleanly
Remove-Item .checkpoints\clickstream_files.txt -ErrorAction SilentlyContinue
py -3 -m pipeline.flow --once
$LASTEXITCODE   # must be 0

# 5. Latest run summary contains all stages incl. lineage
Get-ChildItem logs\pipeline_summary_*.json | Sort-Object LastWriteTime -Descending | Select-Object -First 1 | Get-Content
```

If every step succeeds, the project is demo-ready.

## 14. Reference Documentation

- `README.md` — top-level project index and quick-start.
- `pipeline/README.md` — operational guide for the orchestrator.
- `pipeline/TASK10.md` — formal Task 10 documentation (architecture, DAG,
  logging, retries, versioning, tool justification).
- `GROUP29_DMML_ASSIGNMENT1.ipynb` — Tasks 5/6/7 narrative with EDA.
- `data/metadata/dataset_lineage.json` — Task 8 lineage manifest.
