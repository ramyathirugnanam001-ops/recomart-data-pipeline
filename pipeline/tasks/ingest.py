"""Ingestion stage.

Re-uses the Task 2 ingestion logic (`scripts/ingest_clickstream_csv.py` and
`scripts/ingest_products_api.py`) by importing their functions directly. This
keeps the orchestration layer decoupled from the script entry-points while
re-using validated, tested code.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

# Make sibling packages importable when this module is executed from anywhere
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common.fs import raw_partition_path, utc_now  # noqa: E402
from common.logger import get_logger  # noqa: E402
from scripts.ingest_clickstream_csv import ingest_once as ingest_clickstream_once  # noqa: E402
from scripts.ingest_products_api import ingest_once as ingest_products_once  # noqa: E402


def run_clickstream_ingestion(
    input_dir: Path,
    pattern: str,
    raw_root: Path,
    checkpoint: Path,
    log_file: Path,
) -> dict:
    """Run clickstream CSV ingestion once and return summary stats."""
    logger = get_logger("pipeline.ingest_clickstream", str(log_file))
    logger.info(
        "stage_start",
        extra={"extra": {"event": "stage_start", "stage": "ingest_clickstream"}},
    )
    stats = ingest_clickstream_once(
        input_dir=Path(input_dir),
        pattern=pattern,
        raw_root=Path(raw_root),
        checkpoint=Path(checkpoint),
        logger=logger,
    )
    logger.info(
        "stage_complete",
        extra={"extra": {"event": "stage_complete", "stage": "ingest_clickstream", **stats}},
    )
    return stats


def run_products_ingestion(
    raw_root: Path,
    log_file: Path,
    mock_file: Path | None = None,
    endpoint: str | None = None,
    page_size: int = 200,
    timeout_sec: int = 10,
    headers: dict | None = None,
) -> dict:
    """Run product catalog ingestion once and return summary stats."""
    logger = get_logger("pipeline.ingest_products", str(log_file))
    logger.info(
        "stage_start",
        extra={"extra": {"event": "stage_start", "stage": "ingest_products"}},
    )
    stats = ingest_products_once(
        endpoint=endpoint,
        raw_root=Path(raw_root),
        logger=logger,
        page_size=page_size,
        timeout=timeout_sec,
        headers=headers or {},
        mock_file=Path(mock_file) if mock_file else None,
    )
    logger.info(
        "stage_complete",
        extra={"extra": {"event": "stage_complete", "stage": "ingest_products", **stats}},
    )
    return stats


def discover_latest_clickstream(raw_root: Path) -> Path:
    """Return the most recent clickstream CSV under data/raw/clickstream/."""
    base = Path(raw_root) / "data" / "raw" / "clickstream" / "events"
    candidates = sorted(base.rglob("*.csv"))
    if not candidates:
        raise FileNotFoundError(f"No clickstream CSVs found under {base}")
    return candidates[-1]


def discover_latest_products(raw_root: Path) -> Path:
    """Return the most recent products JSON under data/raw/catalog/products/."""
    base = Path(raw_root) / "data" / "raw" / "catalog" / "products"
    candidates = sorted(base.rglob("*.json"))
    if not candidates:
        raise FileNotFoundError(f"No product JSON pages found under {base}")
    return candidates[-1]
