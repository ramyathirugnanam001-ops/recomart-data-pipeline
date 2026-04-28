#!/usr/bin/env python3
"""Ingest product catalog from a REST API.

Features:
- Periodic or one-shot execution
- Retry with exponential backoff (network/5xx/429)
- Logging for audit trails
- Stores raw JSON pages into partitioned raw folder
- Optional mock mode for offline testing

Assumptions:
- API supports pagination via `page` and `page_size` query params
- Response schema: {"items": [...], "next_page": <int|null>} or a list

Example:
  python scripts/ingest_products_api.py \
    --endpoint "https://api.recomart.com/v1/products" \
    --raw-root . \
    --log-file sample_logs/products_ingestion.log \
    --page-size 200 \
    --once

Offline test:
  python scripts/ingest_products_api.py --mock-file sample_input/products_mock.json --raw-root . --once
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from datetime import datetime, timezone

import requests

from common.logger import get_logger
from common.retry import retry, RetryConfig
from common.fs import raw_partition_path, utc_now


def is_retryable_http(e: Exception) -> bool:
    if isinstance(e, requests.exceptions.RequestException):
        return True
    return False


def fetch_page(session: requests.Session, endpoint: str, page: int, page_size: int, headers: dict, timeout: int):
    resp = session.get(endpoint, params={"page": page, "page_size": page_size}, headers=headers, timeout=timeout)
    if resp.status_code in (429, 500, 502, 503, 504):
        raise requests.exceptions.HTTPError(f"retryable_status:{resp.status_code}")
    resp.raise_for_status()
    return resp.json()


def normalize_payload(payload):
    # supports list or dict with items
    if isinstance(payload, list):
        return payload, None
    if isinstance(payload, dict) and "items" in payload:
        nxt = payload.get("next_page")
        return payload["items"], nxt
    # fallback: treat as single page
    return payload, None


def ingest_once(endpoint: str | None, raw_root: Path, logger, page_size: int, timeout: int, headers: dict, mock_file: Path | None):
    ts = utc_now()
    out_dir = raw_partition_path(raw_root, source="catalog", dataset="products", ts=ts)
    out_dir.mkdir(parents=True, exist_ok=True)

    pages_written = 0
    items_written = 0

    if mock_file is not None:
        payload = json.loads(mock_file.read_text(encoding='utf-8'))
        items, _ = normalize_payload(payload)
        out_file = out_dir / f"products_mock_{ts.strftime('%Y%m%dT%H%M%SZ')}.json"
        out_file.write_text(json.dumps(payload, ensure_ascii=False), encoding='utf-8')
        pages_written = 1
        try:
            items_written = len(items)
        except Exception:
            items_written = 0
        logger.info('ingest_success', extra={"extra": {"event": "ingest_success", "mode": "mock", "pages": pages_written, "items": items_written, "output": str(out_file)}})
        return {"pages": pages_written, "items": items_written}

    if not endpoint:
        raise ValueError('endpoint is required when not using --mock-file')

    session = requests.Session()

    def on_retry(attempt, delay, err):
        logger.warning('retry', extra={"extra": {"event": "retry", "attempt": attempt, "delay_sec": round(delay, 2), "error": str(err)}})

    page = 1
    while True:
        def call():
            return fetch_page(session, endpoint, page, page_size, headers, timeout)

        payload = retry(call, is_retryable=is_retryable_http, on_retry=on_retry, cfg=RetryConfig(max_attempts=5, base_delay_sec=1.0, max_delay_sec=12.0, jitter=0.25))

        out_file = out_dir / f"products_page={page}_{ts.strftime('%Y%m%dT%H%M%SZ')}.json"
        out_file.write_text(json.dumps(payload, ensure_ascii=False), encoding='utf-8')
        pages_written += 1

        items, next_page = normalize_payload(payload)
        try:
            items_written += len(items)
        except Exception:
            pass

        logger.info('page_written', extra={"extra": {"event": "page_written", "page": page, "output": str(out_file)}})

        if next_page is None:
            break
        page = next_page

    logger.info('ingest_success', extra={"extra": {"event": "ingest_success", "mode": "api", "pages": pages_written, "items": items_written, "endpoint": endpoint, "output_dir": str(out_dir)}})
    return {"pages": pages_written, "items": items_written}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--endpoint', default=None)
    ap.add_argument('--raw-root', required=True)
    ap.add_argument('--log-file', default='logs/products_ingestion.log')
    ap.add_argument('--log-level', default='INFO')
    ap.add_argument('--page-size', type=int, default=200)
    ap.add_argument('--timeout-sec', type=int, default=10)
    ap.add_argument('--header', action='append', default=[], help='Custom headers key=value (repeatable)')
    ap.add_argument('--mock-file', default=None, help='Path to JSON file used instead of calling the API')

    ap.add_argument('--interval-sec', type=int, default=21600)
    ap.add_argument('--once', action='store_true')

    args = ap.parse_args()
    logger = get_logger('ingest_products_api', args.log_file, args.log_level)

    headers = {}
    for kv in args.header:
        if '=' in kv:
            k, v = kv.split('=', 1)
            headers[k.strip()] = v.strip()

    raw_root = Path(args.raw_root)
    mock_file = Path(args.mock_file) if args.mock_file else None

    logger.info('job_start', extra={"extra": {"event": "job_start", "endpoint": args.endpoint, "page_size": args.page_size, "mode": "mock" if mock_file else "api"}})

    def run_cycle():
        start = time.time()
        try:
            stats = ingest_once(args.endpoint, raw_root, logger, args.page_size, args.timeout_sec, headers, mock_file)
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
