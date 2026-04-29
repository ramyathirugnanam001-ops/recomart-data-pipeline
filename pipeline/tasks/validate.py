"""Validation stage (Task 4).

Performs schema, completeness and range checks on the freshly ingested raw
files and produces a JSON data-quality report. Raises ValueError if a hard
quality rule fails so that orchestration can fail-fast.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

CLICKSTREAM_REQUIRED_COLS = {
    "user_id",
    "session_id",
    "event_type",
    "item_id",
    "event_timestamp",
}
PRODUCT_REQUIRED_FIELDS = {"item_id", "title", "category", "brand", "price"}
ALLOWED_EVENT_TYPES = {"view", "add_to_cart", "purchase", "click", "remove_from_cart"}


def _load_products(products_path: Path) -> pd.DataFrame:
    payload: Any = json.loads(Path(products_path).read_text(encoding="utf-8"))
    if isinstance(payload, dict) and "items" in payload:
        items = payload["items"]
    elif isinstance(payload, list):
        items = payload
    else:
        raise ValueError("Unsupported products payload structure")
    return pd.DataFrame(items)


def validate_datasets(
    clickstream_path: Path,
    products_path: Path,
    report_path: Path,
) -> dict:
    """Validate the input datasets and write a JSON quality report.

    Returns the summary dict. Raises ValueError on hard failures so the
    orchestration tool will mark the run as failed.
    """
    issues: list[str] = []
    summary: dict[str, Any] = {
        "clickstream": {"path": str(clickstream_path)},
        "products": {"path": str(products_path)},
    }

    # --- Clickstream ---
    cs = pd.read_csv(clickstream_path)
    cs_cols = set(cs.columns)
    cs_missing = CLICKSTREAM_REQUIRED_COLS - cs_cols
    if cs_missing:
        issues.append(f"clickstream_missing_columns:{sorted(cs_missing)}")

    summary["clickstream"].update(
        {
            "rows": int(len(cs)),
            "duplicates": int(cs.duplicated().sum()),
            "null_counts": {c: int(cs[c].isnull().sum()) for c in cs.columns},
        }
    )

    if "event_type" in cs.columns:
        bad_events = sorted(set(cs["event_type"].dropna().unique()) - ALLOWED_EVENT_TYPES)
        summary["clickstream"]["unknown_event_types"] = bad_events
        # Soft issue, not raising

    # --- Products ---
    products_df = _load_products(products_path)
    p_missing = PRODUCT_REQUIRED_FIELDS - set(products_df.columns)
    if p_missing:
        issues.append(f"products_missing_fields:{sorted(p_missing)}")

    summary["products"].update(
        {
            "rows": int(len(products_df)),
            "duplicates": int(products_df.duplicated().sum()),
            "null_counts": {c: int(products_df[c].isnull().sum()) for c in products_df.columns},
        }
    )

    if "price" in products_df.columns:
        prices = pd.to_numeric(products_df["price"], errors="coerce")
        summary["products"]["price_min"] = float(prices.min()) if len(prices) else None
        summary["products"]["price_max"] = float(prices.max()) if len(prices) else None
        if (prices < 0).any():
            issues.append("products_negative_prices")

    summary["issues"] = issues
    summary["status"] = "failed" if issues else "passed"

    Path(report_path).parent.mkdir(parents=True, exist_ok=True)
    Path(report_path).write_text(json.dumps(summary, indent=2), encoding="utf-8")

    if issues:
        raise ValueError(f"Validation failed: {issues}")

    return summary
