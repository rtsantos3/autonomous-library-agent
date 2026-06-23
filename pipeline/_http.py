import threading
import time
import os
from typing import Optional

import requests


class _RateLimiter:
    def __init__(self, min_interval: float):
        self._min = min_interval
        self._next = 0.0
        self._lock = threading.Lock()

    def wait(self):
        with self._lock:
            now = time.monotonic()
            if now < self._next:
                time.sleep(self._next - now)
            self._next = max(now, self._next) + self._min


# Avoid shared API rate-limit bursts while preserving resilience to transient
# upstream failures that are common in literature metadata services.
NCBI_LIMITER = _RateLimiter(0.11 if os.getenv("NCBI_API_KEY") else 0.34)
S2_LIMITER = _RateLimiter(1.05)
CROSSREF_LIMITER = _RateLimiter(0.2)


def http_get(
    url,
    *,
    params=None,
    headers=None,
    limiter: _RateLimiter,
    timeout: int = 30,
    retries: int = 4,
) -> requests.Response:
    last_attempt = max(retries, 1) - 1
    for attempt in range(max(retries, 1)):
        limiter.wait()
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=timeout)
            if resp.status_code == 429 or 500 <= resp.status_code < 600:
                if attempt == last_attempt:
                    resp.raise_for_status()
                retry_after = _retry_after_seconds(resp) if resp.status_code == 429 else None
                time.sleep(retry_after if retry_after is not None else _backoff_delay(attempt))
                continue
            resp.raise_for_status()
            return resp
        except requests.RequestException:
            if attempt == last_attempt:
                raise
            time.sleep(_backoff_delay(attempt))
            continue

    raise requests.RequestException(f"GET failed after {retries} attempts: {url}")


def _retry_after_seconds(resp: requests.Response) -> Optional[int]:
    try:
        return int(resp.headers.get("Retry-After", ""))
    except (TypeError, ValueError):
        return None


def _backoff_delay(attempt: int) -> float:
    return min(0.5 * (2**attempt), 8.0)
