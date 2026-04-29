"""Model training stage (Task 9).

Trains a lightweight collaborative-filtering style model on the engineered
features. We use Truncated SVD on a user-item interaction matrix to keep the
implementation dependency-light (only `pandas` + `numpy`/`scikit-learn`).

Outputs:
- `models/<version>/model.pkl`          - serialised TruncatedSVD model
- `models/<version>/run_metadata.json`  - run id, params, metrics
"""

from __future__ import annotations

import json
import pickle
import uuid
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd


def _build_user_item_matrix(features: pd.DataFrame) -> tuple[pd.DataFrame, list, list]:
    """Build a user-item interaction matrix from the feature dataframe."""
    interactions = (
        features.groupby(["user_id", "item_id"])["activity_count"].max().reset_index()
    )
    matrix = interactions.pivot(
        index="user_id", columns="item_id", values="activity_count"
    ).fillna(0.0)
    return matrix, list(matrix.index), list(matrix.columns)


def train_recommender(
    features_path: Path,
    models_root: Path,
    version: str | None = None,
    n_components: int = 2,
) -> dict:
    """Train a TruncatedSVD recommender and persist artifacts + metrics."""
    from sklearn.decomposition import TruncatedSVD

    features = pd.read_csv(features_path)
    matrix, users, items = _build_user_item_matrix(features)

    n_components = max(1, min(n_components, min(matrix.shape) - 1 or 1))

    svd = TruncatedSVD(n_components=n_components, random_state=42)
    user_factors = svd.fit_transform(matrix.values)
    item_factors = svd.components_.T
    reconstructed = user_factors @ item_factors.T

    # Simple offline metrics
    rmse = float(np.sqrt(((matrix.values - reconstructed) ** 2).mean()))
    explained_variance = float(svd.explained_variance_ratio_.sum())

    # Precision@K on the same matrix (toy, but demonstrates the metric path).
    k = min(2, len(items))
    precision_at_k_values = []
    for u_idx in range(matrix.shape[0]):
        truth = set(np.where(matrix.values[u_idx] > 0)[0])
        if not truth:
            continue
        top_k = np.argsort(-reconstructed[u_idx])[:k]
        hits = len(truth.intersection(top_k))
        precision_at_k_values.append(hits / k)
    precision_at_k = float(np.mean(precision_at_k_values)) if precision_at_k_values else 0.0

    version = version or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    version_dir = Path(models_root) / version
    version_dir.mkdir(parents=True, exist_ok=True)

    with (version_dir / "model.pkl").open("wb") as f:
        pickle.dump(
            {
                "svd": svd,
                "users": users,
                "items": items,
                "user_factors": user_factors,
                "item_factors": item_factors,
            },
            f,
        )

    metadata = {
        "run_id": str(uuid.uuid4()),
        "model": "TruncatedSVD",
        "version": version,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "params": {"n_components": n_components, "random_state": 42},
        "metrics": {
            "rmse": rmse,
            "explained_variance": explained_variance,
            f"precision_at_{k}": precision_at_k,
        },
        "artifact": str(version_dir / "model.pkl"),
        "n_users": len(users),
        "n_items": len(items),
    }
    (version_dir / "run_metadata.json").write_text(
        json.dumps(metadata, indent=2), encoding="utf-8"
    )

    return metadata
