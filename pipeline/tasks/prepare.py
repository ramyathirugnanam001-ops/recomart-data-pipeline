"""Data preparation stage (Task 5).

Loads raw clickstream + products, performs cleaning/encoding/normalization
and saves a merged "prepared" parquet/csv ready for transformation.

This mirrors the logic in `GROUP29_DMML_ASSIGNMENT1.ipynb` cells for Task 5
but exposes it as a callable function for the orchestrator.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


def _load_products(products_path: Path) -> pd.DataFrame:
    payload = json.loads(Path(products_path).read_text(encoding="utf-8"))
    if isinstance(payload, dict) and "items" in payload:
        items = payload["items"]
    else:
        items = payload
    return pd.json_normalize(items)


def prepare_dataset(
    clickstream_path: Path,
    products_path: Path,
    output_path: Path,
) -> dict:
    """Clean, merge, encode and normalize the raw datasets.

    Returns a stats dict and writes the prepared dataset to output_path (CSV).
    """
    clickstream = pd.read_csv(clickstream_path)
    products_df = _load_products(products_path)

    # ---- Cleaning (Task 5) ----
    clickstream = clickstream.drop_duplicates()
    products_df = products_df.drop_duplicates()

    clickstream = clickstream.fillna({"user_id": "unknown", "item_id": "unknown"})
    if "price" in products_df.columns:
        products_df["price"] = products_df["price"].fillna(products_df["price"].mean())

    clickstream["event_timestamp"] = pd.to_datetime(
        clickstream["event_timestamp"], errors="coerce", utc=True
    )

    # ---- Merge ----
    df = clickstream.merge(products_df, on="item_id", how="left")

    # ---- Encoding ----
    for col in ("category", "brand", "platform"):
        if col in df.columns:
            df[f"{col}_encoded"] = df[col].astype("category").cat.codes

    # ---- Normalization ----
    if "price" in df.columns and df["price"].std(skipna=True):
        df["price_normalized"] = (df["price"] - df["price"].mean()) / df["price"].std()
    else:
        df["price_normalized"] = 0.0

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)

    return {
        "rows": int(len(df)),
        "columns": int(df.shape[1]),
        "output": str(output_path),
        "users": int(df["user_id"].nunique()) if "user_id" in df.columns else 0,
        "items": int(df["item_id"].nunique()) if "item_id" in df.columns else 0,
    }
