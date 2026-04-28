"""Filesystem utilities for partitioned raw landing."""
from __future__ import annotations
from pathlib import Path
from datetime import datetime, timezone


def utc_now():
    return datetime.now(timezone.utc)


def raw_partition_path(root: Path, source: str, dataset: str, ts: datetime) -> Path:
    # Example: data/raw/clickstream/events/ingest_date=YYYY-MM-DD/ingest_hour=HH/
    d = ts.strftime('%Y-%m-%d')
    h = ts.strftime('%H')
    return root / 'data' / 'raw' / source / dataset / f"ingest_date={d}" / f"ingest_hour={h}"
