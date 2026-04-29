"""Feature store stage (Task 7).

Versions the engineered features under `feature_store/<timestamp>/` and writes
metadata so the registry can be inspected and queried later.
"""

from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

FEATURE_METADATA = [
    {
        "name": "activity_count",
        "dtype": "int",
        "source": "clickstream",
        "transformation": "count of interactions grouped by user_id",
    },
    {
        "name": "avg_price",
        "dtype": "float",
        "source": "products + clickstream",
        "transformation": "average price grouped by user_id",
    },
    {
        "name": "popularity",
        "dtype": "int",
        "source": "clickstream",
        "transformation": "count of interactions grouped by item_id",
    },
    {
        "name": "recency",
        "dtype": "float",
        "source": "clickstream",
        "transformation": "time difference in seconds from latest event_timestamp",
    },
]


def publish_to_feature_store(
    features_path: Path,
    feature_store_root: Path,
    version: str | None = None,
) -> dict:
    """Copy features into a versioned feature store folder with metadata."""
    version = version or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    version_dir = Path(feature_store_root) / version
    version_dir.mkdir(parents=True, exist_ok=True)

    target = version_dir / "final_features.csv"
    shutil.copy2(features_path, target)

    df = pd.read_csv(target)
    metadata = {
        "feature_store_name": "recomart_feature_store",
        "version": version,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "entity_keys": ["user_id", "item_id"],
        "source_dataset": "final_features",
        "row_count": int(len(df)),
        "features": FEATURE_METADATA,
    }
    (version_dir / "feature_metadata.json").write_text(
        json.dumps(metadata, indent=4), encoding="utf-8"
    )

    return {
        "version": version,
        "path": str(version_dir),
        "rows": int(len(df)),
    }
