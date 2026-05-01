"""Lineage manifest stage (Task 8 integration).

A thin wrapper around ``scripts/generate_lineage_manifest.py``. After every
successful pipeline run the manifest at ``data/metadata/dataset_lineage.json``
is regenerated so that newly-created raw partitions and feature-store
versions are reflected in the lineage record.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.generate_lineage_manifest import build_manifest  # noqa: E402


def regenerate_manifest(project_root: Path, output_path: Path) -> dict:
    """Rebuild the dataset lineage manifest and persist it to disk.

    Returns a small summary dict suitable for inclusion in the run summary.
    """
    project_root = Path(project_root).resolve()
    output_path = Path(output_path)
    if not output_path.is_absolute():
        output_path = project_root / output_path

    manifest = build_manifest(project_root)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    datasets = manifest.get("datasets", [])
    raw = sum(1 for d in datasets if d.get("layer") == "raw")
    transformed = sum(1 for d in datasets if d.get("layer") == "transformed")

    return {
        "output": str(output_path),
        "total_datasets": len(datasets),
        "raw": raw,
        "transformed": transformed,
    }
