"""Production HTTP client for connector-fabric.

Replaces the bare `requests` calls scattered through http_server.py with a
single async client that gives us:

- Explicit per-tier timeouts (connect/read/write/pool) rather than httpx
  defaults (5s on everything, which is dangerous for B2B APIs)
- Exponential backoff + jitter retries on 429/503/504 + network errors
- `Retry-After` header honored (Power BI uses this aggressively when capacity
  is throttled)
- Per-host circuit breaker — when api.powerbi.com is degraded, fail fast
  rather than dragging down callers and burning quota
- Structured duration logging — slow calls surface in logs

Design notes
------------
- One module-level `httpx.AsyncClient` shared across all requests (re-uses
  connections, respects pool limits). Initialised lazily on first request,
  closed on shutdown via `close_client()`.
- Circuit breaker is in-process per host. State is lost on restart, which is
  fine — we'd rather rediscover health than persist stale state.
- We don't bring in stamina/tenacity/purgatory — the retry + breaker logic
  fits in ~150 lines and adding deps to a deployed connector has real cost
  (build time, attack surface, version skew).

Public API
----------
`request(method, url, ...)` -> httpx.Response — drop-in for `requests.request`.
`get(url, ...)` / `post(url, ...)` — convenience.
`close_client()` — call on shutdown to drain connections.
`get_breaker_state()` — diagnostics for /health.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass
from typing import Any

import httpx

logger = logging.getLogger("connector-fabric.http")

# --- Timeouts ---
# httpx default is 5s on all four tiers — dangerous for any real B2B API.
# DEFAULT covers normal REST calls (workspace/dataset discovery, refresh
# history, etc.). DAX is much longer because executeQueries can legitimately
# take a minute or more on a big model.
DEFAULT_TIMEOUT = httpx.Timeout(connect=10.0, read=60.0, write=30.0, pool=10.0)
DAX_TIMEOUT = httpx.Timeout(connect=10.0, read=180.0, write=30.0, pool=10.0)
TOKEN_TIMEOUT = httpx.Timeout(connect=5.0, read=30.0, write=10.0, pool=5.0)

# --- Connection pool ---
# Cap connections per process. 100 is generous for our scale (~15 agents) and
# leaves headroom; keepalive 20 keeps the hot path fast without holding
# idle sockets forever.
LIMITS = httpx.Limits(max_connections=100, max_keepalive_connections=20)

# --- Retry policy ---
RETRYABLE_STATUSES = frozenset({429, 503, 504})
MAX_RETRIES = 5
INITIAL_BACKOFF_S = 1.0
MAX_BACKOFF_S = 60.0
JITTER_S = 2.0

# --- Circuit breaker policy ---
BREAKER_FAILURE_THRESHOLD = 5  # consecutive failures before opening
BREAKER_COOL_DOWN_S = 60.0  # how long we stay open before half-open
BREAKER_WINDOW_S = 30.0  # failures older than this don't count


class CircuitOpenError(Exception):
    """Raised when the circuit breaker for a host is open.

    Callers should treat this as a clear "downstream is degraded, retry
    later" signal rather than retrying immediately.
    """


@dataclass
class _BreakerState:
    failures: int = 0
    first_failure_at: float = 0.0  # monotonic timestamp of first failure in window
    opened_at: float = 0.0  # monotonic timestamp when circuit opened (0 = closed)

    def record_success(self) -> None:
        self.failures = 0
        self.first_failure_at = 0.0
        self.opened_at = 0.0

    def record_failure(self) -> None:
        now = time.monotonic()
        # If our first failure is too old, reset the window
        if self.first_failure_at and now - self.first_failure_at > BREAKER_WINDOW_S:
            self.failures = 0
            self.first_failure_at = now
        if not self.first_failure_at:
            self.first_failure_at = now
        self.failures += 1
        if self.failures >= BREAKER_FAILURE_THRESHOLD:
            self.opened_at = now

    def is_open(self) -> bool:
        if not self.opened_at:
            return False
        now = time.monotonic()
        if now - self.opened_at > BREAKER_COOL_DOWN_S:
            # Half-open — allow a probe through. We don't reset opened_at
            # here; a failed probe keeps us open, a success in record_success
            # will fully reset.
            return False
        return True

    def as_dict(self) -> dict[str, Any]:
        now = time.monotonic()
        return {
            "state": "open" if self.is_open() else "closed",
            "failures": self.failures,
            "opened_for_s": (now - self.opened_at) if self.opened_at else 0,
        }


_breakers: dict[str, _BreakerState] = {}


def _breaker_for(host: str) -> _BreakerState:
    breaker = _breakers.get(host)
    if breaker is None:
        breaker = _BreakerState()
        _breakers[host] = breaker
    return breaker


def get_breaker_state() -> dict[str, dict[str, Any]]:
    """Diagnostic snapshot of circuit breaker state for /health."""
    return {host: breaker.as_dict() for host, breaker in _breakers.items()}


# --- Shared client (lazy) ---
_client: httpx.AsyncClient | None = None
_client_lock = asyncio.Lock()


async def _get_client() -> httpx.AsyncClient:
    global _client
    if _client is None:
        async with _client_lock:
            if _client is None:
                _client = httpx.AsyncClient(
                    timeout=DEFAULT_TIMEOUT,
                    limits=LIMITS,
                    # HTTP/2 needs the `h2` extra. We default to HTTP/1.1 to
                    # avoid the extra dep; flip to True after adding h2 if we
                    # see pool contention against api.powerbi.com.
                    http2=False,
                )
    return _client


async def close_client() -> None:
    """Drain and close the shared client. Call on app shutdown."""
    global _client
    if _client is not None:
        try:
            await _client.aclose()
        finally:
            _client = None


# --- Retry helpers ---
def _parse_retry_after(value: str | None) -> float | None:
    """Parse Retry-After header. Returns seconds, or None if absent/invalid.

    Spec allows either seconds (int) or HTTP-date. We only handle seconds —
    HTTP-date is rare for these APIs.
    """
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return None


def _compute_backoff(attempt: int, retry_after_s: float | None) -> float:
    """Wait time before next retry.

    If the server gave us Retry-After, honor it (plus a small jitter so we
    don't synchronise with other clients on recovery). Otherwise exponential
    backoff with jitter.
    """
    if retry_after_s is not None:
        return min(MAX_BACKOFF_S, retry_after_s + random.uniform(0.0, JITTER_S))
    base = INITIAL_BACKOFF_S * (2 ** (attempt - 1))
    return min(MAX_BACKOFF_S, base + random.uniform(0.0, JITTER_S))


def _path_for_log(url: str) -> str:
    """Strip querystring for log lines (often contains tokens or large filters)."""
    return url.split("?", 1)[0]


# --- Core request method ---
async def request(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    json: Any = None,
    data: Any = None,
    timeout: httpx.Timeout | None = None,
    max_retries: int = MAX_RETRIES,
) -> httpx.Response:
    """Send an HTTP request with retries, Retry-After, circuit breaker, and timing.

    Returns the final `httpx.Response` (which may be a non-success status if
    we exhausted retries — caller decides what to do with 4xx etc.).

    Raises:
        CircuitOpenError: when the breaker for the target host is open.
        httpx.TimeoutException / httpx.TransportError: when all retries are
            exhausted on connection/timeout errors.
    """
    client = await _get_client()
    host = httpx.URL(url).host
    breaker = _breaker_for(host)

    if breaker.is_open():
        # Fail fast — don't add load to a downstream that's already in trouble.
        logger.warning("Circuit open for %s — failing fast", host)
        raise CircuitOpenError(
            f"Circuit breaker open for {host}: downstream is degraded; "
            f"retry in ~{BREAKER_COOL_DOWN_S:.0f}s"
        )

    effective_timeout = timeout if timeout is not None else DEFAULT_TIMEOUT
    started = time.monotonic()
    last_exc: BaseException | None = None
    response: httpx.Response | None = None

    for attempt in range(1, max_retries + 1):
        try:
            response = await client.request(
                method,
                url,
                headers=headers,
                json=json,
                data=data,
                timeout=effective_timeout,
            )

            # Retryable status code
            if response.status_code in RETRYABLE_STATUSES and attempt < max_retries:
                retry_after = _parse_retry_after(response.headers.get("retry-after"))
                wait = _compute_backoff(attempt, retry_after)
                logger.warning(
                    "%s %s -> %d (retry %d/%d in %.1fs%s)",
                    method,
                    _path_for_log(url),
                    response.status_code,
                    attempt,
                    max_retries,
                    wait,
                    f", Retry-After={retry_after}" if retry_after is not None else "",
                )
                # Failures during retry phase count toward breaker only after
                # we give up — transient retryables shouldn't open the circuit.
                await asyncio.sleep(wait)
                continue

            # Done — either success or non-retryable error
            duration_ms = (time.monotonic() - started) * 1000
            if response.is_success:
                breaker.record_success()
                # Surface slow calls so we can find them later
                if duration_ms > 5000:
                    logger.info(
                        "SLOW %s %s -> %d in %.0fms (attempt %d)",
                        method,
                        _path_for_log(url),
                        response.status_code,
                        duration_ms,
                        attempt,
                    )
            else:
                # Non-success, non-retryable (4xx etc.) — count as a failure
                # for breaker purposes only if it's a server-side issue.
                if response.status_code >= 500:
                    breaker.record_failure()
                logger.warning(
                    "%s %s -> %d in %.0fms",
                    method,
                    _path_for_log(url),
                    response.status_code,
                    duration_ms,
                )
            return response

        except (httpx.TimeoutException, httpx.TransportError) as exc:
            last_exc = exc
            if attempt < max_retries:
                wait = _compute_backoff(attempt, retry_after_s=None)
                logger.warning(
                    "%s %s (%s) — retry %d/%d in %.1fs",
                    method,
                    _path_for_log(url),
                    type(exc).__name__,
                    attempt,
                    max_retries,
                    wait,
                )
                await asyncio.sleep(wait)
                continue
            # Out of retries — count toward breaker and raise
            breaker.record_failure()
            duration_ms = (time.monotonic() - started) * 1000
            logger.error(
                "%s %s FAILED after %d attempts in %.0fms: %s",
                method,
                _path_for_log(url),
                attempt,
                duration_ms,
                exc,
            )
            raise

    # Loop exited via exhausting retries on retryable status. Return the last
    # response so the caller can see what happened.
    if response is not None:
        breaker.record_failure()
        return response
    # Should not be reachable, but for type safety:
    assert last_exc is not None
    raise last_exc


# --- Convenience methods ---
async def get(url: str, **kwargs: Any) -> httpx.Response:
    return await request("GET", url, **kwargs)


async def post(url: str, **kwargs: Any) -> httpx.Response:
    return await request("POST", url, **kwargs)
