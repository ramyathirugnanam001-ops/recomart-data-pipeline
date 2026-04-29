"""Generate a dataset version and lineage manifest for RecoMart.

The manifest records the source, ingestion partition, file hash, and applied
transformations for raw and transformed datasets. It is intentionally small so
it can be regenerated whenever new data versions are added.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path


PARTITION_RE = re.compile(r"ingest_date=(?P<date>[^\\/]+)[\\/]ingest_hour=(?P<hour>[^\\/]+)")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize(path: Path, root: Path) -> str:
    return path.relative_to(root).as_posix()


def partition_metadata(path: Path) -> dict[str, str | None]:
    match = PARTITION_RE.search(path.as_posix())
    if not match:
        return {"ingest_date": None, "ingest_hour": None}
    return {"ingest_date": match.group("date"), "ingest_hour": match.group("hour")}


def raw_dataset_record(path: Path, root: Path) -> dict:
    rel_path = normalize(path, root)
    partition = partition_metadata(path)
    if "clickstream" in rel_path:
        dataset_id = "raw_clickstream_events"
        source = "sample_input/clickstream_2026-02-15.csv"
        transformations = ["landed unchanged in partitioned raw zone"]
    else:
        dataset_id = "raw_product_catalog"
        source = "sample_input/products_mock.json or products REST API"
        transformations = ["landed unchanged in partitioned raw zone"]

    version = f"{partition['ingest_date']}_{partition['ingest_hour']}" if partition["ingest_date"] else path.stem
    return {
        "dataset_id": dataset_id,
        "version": version,
        "layer": "raw",
        "path": rel_path,
        "format": path.suffix.lstrip("."),
        "source": source,
        "ingestion": partition,
        "transformations": transformations,
        "bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def feature_store_record(version_dir: Path, root: Path) -> dict | None:
    features = version_dir / "final_features.csv"
    metadata = version_dir / "feature_metadata.json"
    if not features.exists():
        return None

    record = {
        "dataset_id": "feature_store_final_features",
        "version": version_dir.name,
        "layer": "transformed",
        "path": normalize(features, root),
        "format": "csv",
        "source": [
            "raw_clickstream_events",
            "raw_product_catalog",
        ],
        "upstream_layers": ["raw", "prepared"],
        "transformations": [
            "drop duplicate clickstream and product rows",
            "impute missing identifiers and product prices",
            "parse event_timestamp as UTC",
            "join clickstream events to product catalog on item_id",
            "encode category, brand, and platform",
            "normalize price",
            "derive activity_count, avg_price, popularity, and recency",
        ],
        "bytes": features.stat().st_size,
        "sha256": sha256_file(features),
    }

    if metadata.exists():
        feature_metadata = json.loads(metadata.read_text(encoding="utf-8"))
        record["metadata_path"] = normalize(metadata, root)
        record["created_at"] = feature_metadata.get("created_at")
        record["entity_keys"] = feature_metadata.get("entity_keys")
        record["features"] = feature_metadata.get("features")
    return record


def build_manifest(root: Path) -> dict:
    raw_files = sorted(
        [
            *root.glob("data/raw/**/*.csv"),
            *root.glob("data/raw/**/*.json"),
        ]
    )
    feature_versions = sorted(
        (
            record
            for version_dir in root.glob("feature_store/*")
            if version_dir.is_dir()
            for record in [feature_store_record(version_dir, root)]
            if record is not None
        ),
        key=lambda record: record["version"],
    )

    datasets = [raw_dataset_record(path, root) for path in raw_files] + feature_versions

    return {
        "project": "recomart-data-pipeline",
        "generated_at": iso_now(),
        "versioning_tool": "Git LFS",
        "versioning_rules": {
            "tracked_patterns": [
                "data/raw/**",
                "data/prepared/**",
                "data/features/**",
                "feature_store/**/*.csv",
                "models/**",
                "logs/**",
                "sample_logs/**",
            ],
            "rules_file": ".gitattributes",
            "workflow": "data files are stored through Git LFS pointers while lineage metadata is committed to Git",
        },
        "datasets": datasets,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate data lineage manifest.")
    parser.add_argument("--root", default=".", help="Repository root")
    parser.add_argument(
        "--output",
        default="data/metadata/dataset_lineage.json",
        help="Manifest output path relative to root",
    )
    args = parser.parse_args()

    root = Path(args.root).resolve()
    output = root / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(build_manifest(root), indent=2), encoding="utf-8")
    print(f"Wrote {output}")


if __name__ == "__main__":
    main()
