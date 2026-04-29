# Pipeline Orchestration (Task 10)

## 1. Overview
The RecoMart data management pipeline is automated end-to-end through a single
orchestration layer implemented with Prefect. The orchestration layer is
responsible for coordinating the execution of every preceding stage of the
assignment — ingestion, validation, preparation, transformation, feature
publication, and model training — into a single reproducible workflow.

The orchestration code is contained in the `pipeline/` Python package and is
designed so that each stage is implemented as an independent, importable
function. The Prefect flow imports those functions and wires them into a
directed acyclic graph (DAG) that enforces correct execution order, propagates
artifacts between stages, captures structured logs, and applies retry and
failure-handling policies.

## 2. Directed Acyclic Graph (DAG)
The pipeline is modelled as the following DAG:

```
ingest_clickstream ──┐
                     ├──► validate ──► prepare ──► transform ──► feature_store_publish ──► train_model
ingest_products    ──┘
```

Both ingestion tasks are independent and are submitted concurrently. The
validation stage is gated on the successful completion of both ingestion
tasks. The remaining stages execute sequentially because each consumes the
output of the previous stage. Edges in the DAG are derived implicitly from the
data dependencies between Prefect tasks; explicit synchronisation is added at
the validation stage through Prefect's `wait_for` mechanism.

## 3. Stage Descriptions

### 3.1 Ingestion (`pipeline/tasks/ingest.py`)
Two independent ingestion tasks are exposed:

- `run_clickstream_ingestion(...)` reuses `scripts/ingest_clickstream_csv.py`
  to copy clickstream CSV files into the partitioned raw landing zone
  `data/raw/clickstream/events/ingest_date=YYYY-MM-DD/ingest_hour=HH/`.
  Idempotency is preserved through a checkpoint file
  (`.checkpoints/clickstream_files.txt`) keyed by filename and SHA-256.
- `run_products_ingestion(...)` reuses `scripts/ingest_products_api.py` to
  retrieve product catalogue data either from a REST API or from a local mock
  file. Pages are persisted under
  `data/raw/catalog/products/ingest_date=YYYY-MM-DD/ingest_hour=HH/`.

Two helper functions, `discover_latest_clickstream(raw_root)` and
`discover_latest_products(raw_root)`, locate the most recent file in the
partitioned raw zone so that downstream stages always operate on the latest
ingested artefact.

### 3.2 Validation (`pipeline/tasks/validate.py`)
The validation stage performs the data-quality checks required by Task 4 on
the freshly ingested files:

- Schema check: presence of required columns in the clickstream dataset
  (`user_id`, `session_id`, `event_type`, `item_id`, `event_timestamp`) and
  required fields in the product dataset (`item_id`, `title`, `category`,
  `brand`, `price`).
- Completeness: per-column null counts and total row counts.
- Duplicates: row-level duplicate counts on both datasets.
- Domain checks: comparison of `event_type` values against an approved set
  (`view`, `add_to_cart`, `purchase`, `click`, `remove_from_cart`) and a
  non-negative range check on `price`.

A JSON data-quality report is written to `logs/data_quality_<run_id>.json`.
Hard violations cause the stage to raise `ValueError`, which Prefect surfaces
as a failed task, automatically preventing all downstream stages from
executing.

### 3.3 Preparation (`pipeline/tasks/prepare.py`)
The preparation stage implements Task 5. The following operations are applied:

- Removal of duplicate rows from both source datasets.
- Imputation of missing identifiers with the literal `unknown`.
- Mean-imputation of missing `price` values.
- Conversion of `event_timestamp` to UTC datetime.
- Left-join of the cleaned clickstream onto the product catalogue using
  `item_id` as the join key.
- Label encoding of `category`, `brand`, and `platform`.
- Z-score normalisation of `price`.

The prepared dataset is persisted to `data/prepared/prepared_<run_id>.csv`.

### 3.4 Transformation (`pipeline/tasks/transform.py`)
The transformation stage implements Task 6 and produces the model-ready
feature set:

- `recency` — seconds elapsed between each interaction and the maximum
  observed `event_timestamp`.
- `activity_count` — number of interactions per `user_id`.
- `avg_price` — mean product price per `user_id`.
- `popularity` — number of interactions per `item_id`.

The final columns retained are
`[user_id, item_id, activity_count, avg_price, popularity, recency]` and the
result is persisted to `data/features/final_features_<run_id>.csv`.

### 3.5 Feature Store Publication (`pipeline/tasks/feature_store.py`)
The feature publication stage implements Task 7. A new versioned directory of
the form `feature_store/<version>/` is created, where `<version>` is a UTC
timestamp. The engineered features are copied into the version directory as
`final_features.csv`, and a `feature_metadata.json` document is written
containing the feature store name, version, creation timestamp, entity keys,
source dataset, row count, and per-feature metadata (name, dtype, source,
transformation rule).

### 3.6 Model Training (`pipeline/tasks/train.py`)
The training stage implements Task 9. A user-item interaction matrix is
constructed from `activity_count` and a Truncated Singular Value
Decomposition model from scikit-learn is fitted. The number of components is
clamped to the valid range derived from the matrix dimensions to support
small datasets. Three offline metrics are computed:

- Root Mean Squared Error (RMSE) of the reconstructed matrix.
- Cumulative explained variance ratio.
- Precision@K, where K is bounded by the number of available items.

The trained model is serialised to `models/<version>/model.pkl`, and a
`run_metadata.json` document containing a UUID run identifier, model name,
parameters, metrics, artefact path, and entity counts is written to the same
directory. The metadata document fulfils the experiment-tracking requirement
in lieu of a dedicated tracking server.

## 4. Inter-Stage Data Flow
Data is propagated between stages exclusively through the local filesystem,
with each stage producing artefacts under deterministic, run-versioned paths.
The flow function generates a single `run_id` (a UTC timestamp) at the start
of each invocation and threads it through every output path so that artefacts
from concurrent runs cannot collide.

The following table summarises the artefacts produced per run:

- Raw landing: `data/raw/clickstream/...`, `data/raw/catalog/...`
- Stage logs: `logs/clickstream_ingestion_<run_id>.log`,
  `logs/products_ingestion_<run_id>.log`
- Data quality report: `logs/data_quality_<run_id>.json`
- Prepared dataset: `data/prepared/prepared_<run_id>.csv`
- Engineered features: `data/features/final_features_<run_id>.csv`
- Feature store version: `feature_store/<version>/{final_features.csv, feature_metadata.json}`
- Model artefacts: `models/<version>/{model.pkl, run_metadata.json}`
- Run summary: `logs/pipeline_summary_<run_id>.json`

The run summary aggregates the return values of every Prefect task into a
single JSON document for offline auditing.

## 5. Prefect Flow and Task Structure
The orchestration module `pipeline/flow.py` defines:

- One Prefect `@flow`, `recomart_end_to_end_pipeline`, which acts as the
  entry-point for the DAG.
- Seven Prefect `@task`s: `ingest_clickstream`, `ingest_products`, `validate`,
  `prepare`, `transform`, `feature_store_publish`, and `train_model`.

Each Prefect task is a thin wrapper around the corresponding function in
`pipeline/tasks/*.py`. The wrappers are responsible solely for acquiring a
Prefect run logger, emitting structured stage-level log lines, and invoking
the underlying business logic. The DAG edges are encoded by passing the
return values of upstream tasks as arguments to downstream tasks; explicit
ordering is added between the parallel ingestion tasks and the validation
stage through the `wait_for=[...]` argument.

The flow exposes a command-line interface that supports the following
parameters: `--clickstream-input-dir`, `--clickstream-pattern`,
`--products-mock`, `--products-endpoint`, and `--products-page-size`. The
flag `--once` causes the flow to be executed exactly once and to terminate
upon completion.

## 6. Logging and Monitoring
Three complementary logging mechanisms are employed:

- Prefect's native task-level logs, which are emitted to standard output and
  also persisted by the Prefect backend. Log records include timestamps,
  task run identifiers, state transitions, and exception traces.
- Structured JSON-line logs produced through `common.logger`. Each ingestion
  task writes a per-run log file under `logs/`, allowing offline ingestion
  events such as `stage_start`, `ingest_success`, `skip_already_processed`,
  and `stage_complete` to be parsed independently of Prefect.
- A consolidated run summary document, `logs/pipeline_summary_<run_id>.json`,
  written at the end of each successful flow run, containing the return
  values of every stage.

When the Prefect server is running, all logs and state transitions become
available in the web user interface at `http://127.0.0.1:4200`, including
task durations, retry counts, and full stack traces for failed runs.

## 7. Retry and Failure Handling
The orchestration layer applies the following resilience strategy:

- The two ingestion tasks are decorated with `retries=2` and `retries=3`
  respectively, paired with `retry_delay_seconds=exponential_backoff(...)`.
  Transient network or filesystem errors therefore cause automatic retries
  with increasing delays before a task is considered failed.
- The validation task is configured to raise `ValueError` on hard failures
  (missing required columns, missing required fields, or negative prices).
  Prefect propagates the exception, marks the task as failed, and prevents
  all downstream tasks from being scheduled, ensuring a fail-fast behaviour
  that protects the feature store and model artefacts from being polluted by
  invalid data.
- Failures and retries are visible both in the console output and in the
  Prefect user interface, supporting post-mortem analysis without ad-hoc
  instrumentation.

## 8. Versioning Strategy
Three independent but related versioning concepts are applied:

- **Run identifier (`run_id`)**: a UTC timestamp generated at flow start. The
  identifier is propagated to every output path of the run, including the
  per-stage logs, prepared dataset, engineered features, data quality report,
  and run summary. This guarantees that artefacts produced by different runs
  cannot overwrite one another.
- **Feature store version**: a UTC timestamp generated by the feature
  publication stage and used as the directory name under `feature_store/`.
  Each version directory contains both the data file and its accompanying
  metadata, enabling reproducible retrieval of historical feature sets.
- **Model version**: the same timestamp value used by the feature store stage
  is reused as the model version, so that a trained model is deterministically
  associated with the exact feature snapshot it was trained on. Each model
  directory under `models/<version>/` additionally records a UUID run
  identifier, model parameters, and evaluation metrics in
  `run_metadata.json`.

## 9. Execution
The pipeline is executed end-to-end with a single command:

```
py -3 -m pipeline.flow --once
```

The command produces all artefacts described in Section 4 and prints state
transitions for every task to standard output.

### 9.1 Optional: Execution with the Prefect User Interface
For graphical monitoring and screenshot evidence, a local Prefect server may
be started in a separate terminal:

```
prefect server start
```

The Prefect command-line interface is then pointed at the local server:

```
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
```

The flow is subsequently triggered with the same command shown above. The
resulting run becomes visible at `http://127.0.0.1:4200`, where the DAG view,
task-level logs, and per-task state transitions can be inspected.

## 10. Tool Selection Justification
Three orchestration frameworks were considered for Task 10: Apache Airflow,
Dagster, and Prefect. Prefect was selected for the following reasons.

### 10.1 Native Compatibility with Windows
Prefect 2 is implemented in pure Python and runs natively on Windows. Apache
Airflow's scheduler, in contrast, is not officially supported on Windows and
typically requires the Windows Subsystem for Linux or Docker for development.
Selecting Prefect therefore eliminated an additional virtualisation layer in
an environment where the assignment was developed and demonstrated.

### 10.2 Lightweight Setup
Prefect can be executed in an ephemeral mode that does not require a
long-running scheduler, message broker, or relational database. Local state
is persisted in a SQLite database that is created on demand. Airflow, by
comparison, requires a scheduler process, a metadata database, and (in
production deployments) a message broker. Dagster requires a daemon process
to run schedules and sensors. The reduced operational footprint of Prefect
is well aligned with an academic, single-machine deployment.

### 10.3 Python-Native Flow and Task Model
Prefect's `@flow` and `@task` decorators allow ordinary Python functions to
be promoted to orchestratable units without altering their signatures. This
property made it possible to implement each pipeline stage as a standalone,
importable function and to reuse those functions unmodified inside the
orchestration layer. The same property would simplify a future migration to
Airflow's `PythonOperator` or Dagster's `@op`, should that be required.

### 10.4 Built-in User Interface for Monitoring
Prefect ships with a web interface that displays flow runs, task runs, state
transitions, durations, retries, and exception traces, all without additional
configuration. This satisfies the assignment's requirement for "screenshots
or logs from the orchestration tool showing successful execution" with
minimal setup overhead.

### 10.5 Suitability for Local and Academic Environments
The combination of native Windows support, an ephemeral execution mode, a
Python-native programming model, and a zero-configuration user interface
makes Prefect an appropriate choice for a local academic deliverable in
which simplicity, reproducibility, and ease of demonstration are
prioritised over horizontal scalability and multi-tenant isolation.
