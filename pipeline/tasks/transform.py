"""Feature engineering / transformation stage (Task 6).

Builds the user/item behavioural features used by the recommendation model
(activity_count, avg_price, popularity, recency).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


FINAL_COLS = ["user_id", "item_id", "activity_count", "avg_price", "popularity", "recency"]


def transform_features(prepared_path: Path, output_path: Path) -> dict:
    """Compute derived features and persist the modelling-ready dataset."""
    df = pd.read_csv(prepared_path)
    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], errors="coerce", utc=True)

    df["recency"] = (df["event_timestamp"].max() - df["event_timestamp"]).dt.total_seconds()

    user_activity = df.groupby("user_id").size().reset_index(name="activity_count")
    avg_price_user = (
        df.groupby("user_id")["price"].mean().reset_index(name="avg_price")
        if "price" in df.columns
        else pd.DataFrame({"user_id": df["user_id"].unique(), "avg_price": 0.0})
    )
    item_popularity = df.groupby("item_id").size().reset_index(name="popularity")

    features = (
        df.merge(user_activity, on="user_id", how="left")
        .merge(avg_price_user, on="user_id", how="left")
        .merge(item_popularity, on="item_id", how="left")
    )

    final_features = features[FINAL_COLS].copy()
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    final_features.to_csv(output_path, index=False)

    return {
        "rows": int(len(final_features)),
        "users": int(final_features["user_id"].nunique()),
        "items": int(final_features["item_id"].nunique()),
        "output": str(output_path),
    }
