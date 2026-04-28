"""Retry helpers with exponential backoff."""
from __future__ import annotations
import random
import time
from dataclasses import dataclass
from typing import Callable, Type

@dataclass
class RetryConfig:
    max_attempts: int = 5
    base_delay_sec: float = 1.0
    max_delay_sec: float = 30.0
    jitter: float = 0.2


def retry(call: Callable[[], object], *, is_retryable: Callable[[Exception], bool], on_retry: Callable[[int, float, Exception], None], cfg: RetryConfig) -> object:
    attempt = 0
    while True:
        try:
            return call()
        except Exception as e:  # noqa
            attempt += 1
            if attempt >= cfg.max_attempts or not is_retryable(e):
                raise
            delay = min(cfg.max_delay_sec, cfg.base_delay_sec * (2 ** (attempt - 1)))
            delay = delay * (1.0 + random.uniform(-cfg.jitter, cfg.jitter))
            on_retry(attempt, delay, e)
            time.sleep(delay)
