"""Prefect flow that orchestrates the RecoMart end-to-end data pipeline.

DAG (executed sequentially with explicit data dependencies):

    ingest_clickstream  ┐
                        ├── validate ── prepare ── transform ── feature_store ── train
    ingest_products     ┘

Run locally with:

    py -3 -m pipeline.flow --once

Or, via the Prefect server UI:

    prefect server start          # in one shell
    py -3 -m pipeline.flow --once # in another shell

The Prefect UI at http://127.0.0.1:4200 will then show task-level run state,
logs, retries, and failure details for monitoring.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

# Ensure the project root is on sys.path so `pipeline` and `common` resolve when
# this module is run as a script.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from prefect import flow, get_run_logger, task  # noqa: E402
from prefect.tasks import exponential_backoff  # noqa: E402

from pipeline.tasks import (  # noqa: E402
    feature_store as fs_stage,
    ingest as ingest_stage,
    lineage as lineage_stage,
    prepare as prepare_stage,
    train as train_stage,
    transform as transform_stage,
    validate as validate_stage,
)


# ---------------------------------------------------------------------------
# Prefect tasks
# ---------------------------------------------------------------------------

@task(
    name="ingest_clickstream",
    retries=2,
    retry_delay_seconds=exponential_backoff(backoff_factor=5),
    log_prints=True,
)
def ingest_clickstream_task(
    input_dir: str,
    pattern: str,
    raw_root: str,
    checkpoint: str,
    log_file: str,
) -> dict:
    logger = get_run_logger()
    logger.info("Starting clickstream ingestion from %s", input_dir)
    stats = ingest_stage.run_clickstream_ingestion(
        input_dir=Path(input_dir),
        pattern=pattern,
        raw_root=Path(raw_root),
        checkpoint=Path(checkpoint),
        log_file=Path(log_file),
    )
    logger.info("Clickstream ingestion stats: %s", stats)
    return stats


@task(
    name="ingest_products",
    retries=3,
    retry_delay_seconds=exponential_backoff(backoff_factor=5),
    log_prints=True,
)
def ingest_products_task(
    raw_root: str,
    log_file: str,
    mock_file: str | None,
    endpoint: str | None,
    page_size: int,
) -> dict:
    logger = get_run_logger()
    logger.info(
        "Starting products ingestion (mode=%s)",
        "mock" if mock_file else "api",
    )
    stats = ingest_stage.run_products_ingestion(
        raw_root=Path(raw_root),
        log_file=Path(log_file),
        mock_file=Path(mock_file) if mock_file else None,
        endpoint=endpoint,
        page_size=page_size,
    )
    logger.info("Products ingestion stats: %s", stats)
    return stats


@task(name="validate", log_prints=True)
def validate_task(
    raw_root: str,
    products_mock_path: str | None,
    report_path: str,
) -> dict:
    logger = get_run_logger()
    cs_path = ingest_stage.discover_latest_clickstream(Path(raw_root))
    try:
        prod_path = ingest_stage.discover_latest_products(Path(raw_root))
    except FileNotFoundError:
        # Fall back to the bundled mock if catalog ingestion produced no file
        if not products_mock_path:
            raise
        prod_path = Path(products_mock_path)
    logger.info("Validating clickstream=%s products=%s", cs_path, prod_path)
    summary = validate_stage.validate_datasets(cs_path, prod_path, Path(report_path))
    logger.info("Validation passed: %s", summary["status"])
    return {"clickstream": str(cs_path), "products": str(prod_path), "report": report_path}


@task(name="prepare", log_prints=True)
def prepare_task(validate_out: dict, output_path: str) -> dict:
    logger = get_run_logger()
    stats = prepare_stage.prepare_dataset(
        clickstream_path=Path(validate_out["clickstream"]),
        products_path=Path(validate_out["products"]),
        output_path=Path(output_path),
    )
    logger.info("Prepared dataset stats: %s", stats)
    return stats


@task(name="transform", log_prints=True)
def transform_task(prepare_out: dict, output_path: str) -> dict:
    logger = get_run_logger()
    stats = transform_stage.transform_features(
        prepared_path=Path(prepare_out["output"]),
        output_path=Path(output_path),
    )
    logger.info("Feature transform stats: %s", stats)
    return stats


@task(name="feature_store_publish", log_prints=True)
def feature_store_task(transform_out: dict, feature_store_root: str) -> dict:
    logger = get_run_logger()
    info = fs_stage.publish_to_feature_store(
        features_path=Path(transform_out["output"]),
        feature_store_root=Path(feature_store_root),
    )
    logger.info("Feature store version published: %s", info)
    return info


@task(name="train_model", log_prints=True)
def train_task(fs_out: dict, models_root: str) -> dict:
    logger = get_run_logger()
    metadata = train_stage.train_recommender(
        features_path=Path(fs_out["path"]) / "final_features.csv",
        models_root=Path(models_root),
        version=fs_out["version"],
    )
    logger.info(
        "Model trained: run_id=%s metrics=%s",
        metadata["run_id"],
        metadata["metrics"],
    )
    return metadata


@task(name="lineage_manifest", log_prints=True)
def lineage_task(project_root: str, output_path: str) -> dict:
    logger = get_run_logger()
    info = lineage_stage.regenerate_manifest(
        project_root=Path(project_root),
        output_path=Path(output_path),
    )
    logger.info("Lineage manifest regenerated: %s", info)
    return info


# ---------------------------------------------------------------------------
# Prefect flow (DAG)
# ---------------------------------------------------------------------------

@flow(name="recomart_end_to_end_pipeline", log_prints=True)
def recomart_pipeline(
    project_root: str = str(PROJECT_ROOT),
    clickstream_input_dir: str = "sample_input",
    clickstream_pattern: str = "clickstream_*.csv",
    products_mock_path: str | None = "sample_input/products_mock.json",
    products_api_endpoint: str | None = None,
    products_page_size: int = 200,
) -> dict:
    """End-to-end DAG: ingestion → validation → preparation → transformation
    → feature store → model training.
    """
    logger = get_run_logger()
    root = Path(project_root)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    logger.info("Pipeline run_id=%s root=%s", run_id, root)

    cs_log = root / "logs" / f"clickstream_ingestion_{run_id}.log"
    pr_log = root / "logs" / f"products_ingestion_{run_id}.log"
    cs_log.parent.mkdir(parents=True, exist_ok=True)

    cs_stats = ingest_clickstream_task.submit(
        input_dir=str(root / clickstream_input_dir),
        pattern=clickstream_pattern,
        raw_root=str(root),
        checkpoint=str(root / ".checkpoints" / "clickstream_files.txt"),
        log_file=str(cs_log),
    )
    pr_stats = ingest_products_task.submit(
        raw_root=str(root),
        log_file=str(pr_log),
        mock_file=str(root / products_mock_path) if products_mock_path else None,
        endpoint=products_api_endpoint,
        page_size=products_page_size,
    )

    validate_out = validate_task.submit(
        raw_root=str(root),
        products_mock_path=str(root / products_mock_path) if products_mock_path else None,
        report_path=str(root / "logs" / f"data_quality_{run_id}.json"),
        wait_for=[cs_stats, pr_stats],
    )

    prepared_path = root / "data" / "prepared" / f"prepared_{run_id}.csv"
    prepare_out = prepare_task.submit(validate_out, str(prepared_path))

    features_path = root / "data" / "features" / f"final_features_{run_id}.csv"
    transform_out = transform_task.submit(prepare_out, str(features_path))

    fs_out = feature_store_task.submit(
        transform_out, str(root / "feature_store")
    )

    train_out = train_task.submit(fs_out, str(root / "models"))

    lineage_out = lineage_task.submit(
        str(root),
        str(root / "data" / "metadata" / "dataset_lineage.json"),
        wait_for=[train_out],
    )

    summary = {
        "run_id": run_id,
        "ingest_clickstream": cs_stats.result(),
        "ingest_products": pr_stats.result(),
        "validate": validate_out.result(),
        "prepare": prepare_out.result(),
        "transform": transform_out.result(),
        "feature_store": fs_out.result(),
        "train": train_out.result(),
        "lineage": lineage_out.result(),
    }

    summary_path = root / "logs" / f"pipeline_summary_{run_id}.json"
    summary_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    logger.info("Pipeline complete. Summary written to %s", summary_path)
    return summary


def main():
    ap = argparse.ArgumentParser(description="Run the RecoMart end-to-end Prefect pipeline.")
    ap.add_argument("--once", action="store_true", help="Execute the flow a single time and exit.")
    ap.add_argument("--clickstream-input-dir", default="sample_input")
    ap.add_argument("--clickstream-pattern", default="clickstream_*.csv")
    ap.add_argument("--products-mock", default="sample_input/products_mock.json")
    ap.add_argument("--products-endpoint", default=None)
    ap.add_argument("--products-page-size", type=int, default=200)
    args = ap.parse_args()

    summary = recomart_pipeline(
        project_root=str(PROJECT_ROOT),
        clickstream_input_dir=args.clickstream_input_dir,
        clickstream_pattern=args.clickstream_pattern,
        products_mock_path=args.products_mock,
        products_api_endpoint=args.products_endpoint,
        products_page_size=args.products_page_size,
    )
    print(json.dumps({k: (v if not isinstance(v, dict) else "...") for k, v in summary.items()}, indent=2))


if __name__ == "__main__":
    main()
