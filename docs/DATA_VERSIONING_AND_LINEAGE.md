# Task 8 - Data Versioning and Lineage

## Objective

Task 8 requires the project to version raw and transformed datasets and to
track metadata such as source, ingestion date, and transformations. This
repository uses Git LFS for large data artifacts and a JSON lineage manifest
for auditable dataset metadata.

## Repository Structure

```text
recomart-data-pipeline/
  .gitattributes                         # Git LFS tracking rules
  data/
    raw/
      clickstream/events/
        ingest_date=YYYY-MM-DD/
          ingest_hour=HH/
            clickstream_*.csv            # raw clickstream versions
      catalog/products/
        ingest_date=YYYY-MM-DD/
          ingest_hour=HH/
            products_*.json              # raw catalog versions
    metadata/
      dataset_lineage.json               # generated lineage manifest
  feature_store/
    YYYYMMDD_HHMMSS/
      final_features.csv                 # transformed feature version
      feature_metadata.json              # feature-level metadata
  scripts/
    generate_lineage_manifest.py         # regenerates dataset lineage
```

The raw zone is partitioned by `ingest_date` and `ingest_hour`, so each
ingestion creates a new immutable dataset version. The feature store uses a
timestamp directory, for example `feature_store/20260428_160452/`, so every
transformed dataset can be traced back to a specific processing run.

## Versioning Tool

Git LFS is used because the assignment repository stores CSV, JSON, model, and
log artifacts directly in the Git project. The tracking rules are defined in
`.gitattributes`:

```text
data/raw/** filter=lfs diff=lfs merge=lfs -text
data/prepared/** filter=lfs diff=lfs merge=lfs -text
data/features/** filter=lfs diff=lfs merge=lfs -text
feature_store/**/*.csv filter=lfs diff=lfs merge=lfs -text
models/** filter=lfs diff=lfs merge=lfs -text
logs/** filter=lfs diff=lfs merge=lfs -text
sample_logs/** filter=lfs diff=lfs merge=lfs -text
```

For a fresh machine, initialize Git LFS once:

```powershell
git lfs install
git lfs track "data/raw/**" "data/prepared/**" "data/features/**" "feature_store/**/*.csv" "models/**" "logs/**" "sample_logs/**"
git add .gitattributes
```

Then add dataset versions normally:

```powershell
git add data/raw feature_store data/metadata/dataset_lineage.json
git commit -m "Add versioned data artifacts and lineage manifest"
git push
```

## Metadata and Lineage

The lineage manifest is stored at `data/metadata/dataset_lineage.json`.
Each dataset version records:

- `dataset_id`: logical dataset name, such as `raw_clickstream_events`
- `version`: partition timestamp or feature-store timestamp
- `layer`: `raw` or `transformed`
- `path`: repository-relative artifact path
- `source`: original file/API or upstream dataset
- `ingestion`: `ingest_date` and `ingest_hour` for raw datasets
- `transformations`: applied processing steps
- `sha256`: content hash for reproducibility
- `bytes`: artifact size

Regenerate the manifest after each ingestion or feature-store publish:

```powershell
py -3 scripts/generate_lineage_manifest.py
```

## Lineage Flow

```text
sample_input/clickstream_2026-02-15.csv
  -> data/raw/clickstream/events/ingest_date=YYYY-MM-DD/ingest_hour=HH/*.csv
  -> data/prepared/prepared_<run_id>.csv
  -> data/features/final_features_<run_id>.csv
  -> feature_store/<version>/final_features.csv

sample_input/products_mock.json or products REST API
  -> data/raw/catalog/products/ingest_date=YYYY-MM-DD/ingest_hour=HH/*.json
  -> data/prepared/prepared_<run_id>.csv
  -> data/features/final_features_<run_id>.csv
  -> feature_store/<version>/final_features.csv
```

The transformation lineage is implemented by the pipeline stages:

- `pipeline/tasks/prepare.py`: cleaning, imputation, timestamp parsing, join,
  categorical encoding, and price normalization
- `pipeline/tasks/transform.py`: feature engineering for `activity_count`,
  `avg_price`, `popularity`, and `recency`
- `pipeline/tasks/feature_store.py`: publication into a timestamped
  `feature_store/<version>/` directory with `feature_metadata.json`

## Assignment Deliverables

The Task 8 deliverables are satisfied by:

- Repository structure showing dataset versions:
  `data/raw/.../ingest_date=.../ingest_hour=.../` and
  `feature_store/<version>/`
- Versioning workflow documentation: this file
- Git LFS tracking configuration: `.gitattributes`
- Lineage metadata: `data/metadata/dataset_lineage.json`
- Reproducible metadata generation:
  `scripts/generate_lineage_manifest.py`
