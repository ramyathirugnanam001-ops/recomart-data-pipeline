#!/usr/bin/env python3
"""Ingest user interaction clickstream CSV files.

Features:
- Periodic or one-shot execution
- Idempotent processing via checkpoint (processed file list)
- Schema validation (required columns)
- Writes raw copies into partitioned raw folder
- Structured JSON logs for monitoring/audit

Example:
  python scripts/ingest_clickstream_csv.py \
    --input-dir sample_input \
    --pattern "clickstream_*.csv" \
    --raw-root . \
    --log-file sample_logs/clickstream_ingestion.log \
    --once
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import os
import time
from pathlib import Path
from datetime import datetime, timezone

import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from common.logger import get_logger
from common.fs import raw_partition_path, utc_now

REQUIRED_COLS = {"user_id", "session_id", "event_type", "item_id", "event_timestamp"}


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def validate_csv(path: Path) -> tuple[bool, str]:
    with path.open('r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            return False, "missing_header"
        cols = set(reader.fieldnames)
        missing = REQUIRED_COLS - cols
        if missing:
            return False, f"missing_required_columns:{sorted(missing)}"
    return True, "ok"


def load_checkpoint(cp_path: Path) -> set[str]:
    if not cp_path.exists():
        return set()
    return set(x.strip() for x in cp_path.read_text(encoding='utf-8').splitlines() if x.strip())


def append_checkpoint(cp_path: Path, entry: str) -> None:
    cp_path.parent.mkdir(parents=True, exist_ok=True)
    with cp_path.open('a', encoding='utf-8') as f:
        f.write(entry + "\n")


def ingest_once(input_dir: Path, pattern: str, raw_root: Path, checkpoint: Path, logger) -> dict:
    processed = load_checkpoint(checkpoint)
    files = sorted(input_dir.glob(pattern))

    stats = {"found": len(files), "ingested": 0, "skipped": 0, "failed": 0}

    for fp in files:
        sha = file_sha256(fp)
        key = f"{fp.name}:{sha}"

        if key in processed:
            stats["skipped"] += 1
            logger.info("skip_already_processed", extra={"extra": {"event": "skip", "file": fp.name, "sha256": sha}})
            continue

        ok, reason = validate_csv(fp)
        if not ok:
            stats["failed"] += 1
            logger.error("validation_failed", extra={"extra": {"event": "validation_failed", "file": fp.name, "reason": reason}})
            continue

        ts = utc_now()
        out_dir = raw_partition_path(raw_root, source="clickstream", dataset="events", ts=ts)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / fp.name

        try:
            out_file.write_bytes(fp.read_bytes())
            append_checkpoint(checkpoint, key)
            stats["ingested"] += 1
            logger.info("ingest_success", extra={"extra": {"event": "ingest_success", "file": fp.name, "sha256": sha, "output": str(out_file)}})
        except Exception as e:  # noqa
            stats["failed"] += 1
            logger.exception("ingest_failed", extra={"extra": {"event": "ingest_failed", "file": fp.name, "error": str(e)}})

    return stats


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--input-dir', required=True)
    ap.add_argument('--pattern', default='*.csv')
    ap.add_argument('--raw-root', required=True, help='Project root or bucket mount')
    ap.add_argument('--checkpoint', default='.checkpoints/clickstream_files.txt')
    ap.add_argument('--log-file', default='logs/clickstream_ingestion.log')

    ap.add_argument('--interval-sec', type=int, default=3600, help='Run every N seconds (ignored with --once)')
    ap.add_argument('--once', action='store_true')
    ap.add_argument('--log-level', default='INFO')

    args = ap.parse_args()

    logger = get_logger('ingest_clickstream_csv', args.log_file, args.log_level)
    input_dir = Path(args.input_dir)
    raw_root = Path(args.raw_root)
    checkpoint = Path(args.checkpoint)

    logger.info('job_start', extra={"extra": {"event": "job_start", "input_dir": str(input_dir), "pattern": args.pattern}})

    def run_cycle():
        start = time.time()
        try:
            stats = ingest_once(input_dir, args.pattern, raw_root, checkpoint, logger)
            logger.info('cycle_complete', extra={"extra": {"event": "cycle_complete", **stats, "duration_sec": round(time.time()-start, 3)}})
        except Exception as e:  # noqa
            logger.exception('cycle_error', extra={"extra": {"event": "cycle_error", "error": str(e)}})

    run_cycle()
    if args.once:
        logger.info('job_end', extra={"extra": {"event": "job_end"}})
        return

    while True:
        time.sleep(args.interval_sec)
        run_cycle()


if __name__ == '__main__':
    main()
