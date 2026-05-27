"""Long-running operation registry for connector-fabric.

Power BI's refresh and Fabric's pipeline-run APIs are LROs upstream:
  POST .../refreshes returns HTTP 202 + a request id (in Location header)
  GET  .../refreshes/{id} returns status; final state is Completed/Failed

The old `fabric_trigger_refresh` tool returned `{status: "triggered"}` and
walked away — agents had no way to know whether the refresh actually
finished, succeeded, or failed. This module flips that on its head: trigger
tools return a `job_id`, a background task polls the upstream API to
completion, and callers fetch the final state via `fabric_check_job`.

Architecture
------------
- In-process dict of jobs (`Job` dataclass). UUIDv4 keys.
- Each job runs as an `asyncio.Task`. The poller calls `update()` as it learns
  more; final state is set on completion or exception.
- Jobs auto-expire after `JOB_TTL_S` so the dict doesn't grow forever.
- This is single-replica state. With max-replicas > 1 the job could be created
  on replica A and queried on replica B, which would 404. The Container App is
  currently min=1/max=3; document this and consider pinning to 1 if LRO
  becomes load-bearing. A Redis-backed registry is a clean future swap-in —
  the public API (submit/check) doesn't need to change.

Public API
----------
- `JobStatus` — enum-like literals
- `submit(name, coro_factory) -> job_id` — schedule a coroutine, get a handle
- `get(job_id) -> Job | None` — read current state
- `gc()` — drop expired jobs (called from /health or a periodic task)
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Literal

logger = logging.getLogger("connector-fabric.lro")

JobStatus = Literal["pending", "running", "completed", "failed", "cancelled"]

JOB_TTL_S = 24 * 60 * 60  # keep finished jobs for 24h so users can come back


@dataclass
class Job:
    id: str
    name: str
    status: JobStatus = "pending"
    progress: dict[str, Any] = field(default_factory=dict)  # {step, total, message}
    result: Any | None = None
    error: str | None = None
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    _task: asyncio.Task[Any] | None = None  # internal — not serialised

    def to_dict(self) -> dict[str, Any]:
        """JSON-serialisable snapshot."""
        return {
            "job_id": self.id,
            "name": self.name,
            "status": self.status,
            "progress": self.progress,
            "result": self.result,
            "error": self.error,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "elapsed_s": round(self.updated_at - self.created_at, 1),
        }

    def update(
        self,
        *,
        status: JobStatus | None = None,
        progress: dict[str, Any] | None = None,
        result: Any | None = None,
        error: str | None = None,
    ) -> None:
        if status is not None:
            self.status = status
        if progress is not None:
            self.progress = progress
        if result is not None:
            self.result = result
        if error is not None:
            self.error = error
        self.updated_at = time.time()


_jobs: dict[str, Job] = {}


def submit(
    name: str,
    coro_factory: Callable[[Job], Awaitable[Any]],
) -> Job:
    """Schedule a long-running coroutine.

    `coro_factory` is given the Job object so it can call `job.update(...)`
    as it makes progress. Its return value becomes `job.result`. Any uncaught
    exception is recorded as `job.error` and `status` flips to `failed`.

    Returns the Job synchronously (status `pending`); the task runs in the
    background on the asyncio loop.
    """
    job = Job(id=str(uuid.uuid4()), name=name)
    _jobs[job.id] = job

    async def runner() -> None:
        job.update(status="running")
        try:
            result = await coro_factory(job)
            # Only flip to completed if the coroutine didn't already set a
            # terminal state itself.
            if job.status not in ("completed", "failed", "cancelled"):
                job.update(status="completed", result=result)
        except asyncio.CancelledError:
            job.update(status="cancelled", error="job cancelled")
            raise
        except Exception as exc:  # noqa: BLE001 — we want to surface all errors
            logger.exception("LRO %s (%s) failed", job.id, name)
            job.update(status="failed", error=f"{type(exc).__name__}: {exc}")

    # NB: we capture the task on the job so cancellation can target it,
    # but we do NOT await it here — submit() is intentionally non-blocking.
    job._task = asyncio.create_task(runner(), name=f"lro:{name}:{job.id[:8]}")
    return job


def get(job_id: str) -> Job | None:
    return _jobs.get(job_id)


def cancel(job_id: str) -> bool:
    """Cancel a running job. Returns True if a task was cancelled."""
    job = _jobs.get(job_id)
    if job is None or job._task is None or job._task.done():
        return False
    job._task.cancel()
    return True


def gc() -> int:
    """Drop terminal jobs older than JOB_TTL_S. Returns count purged."""
    now = time.time()
    expired = [
        jid
        for jid, job in _jobs.items()
        if job.status in ("completed", "failed", "cancelled")
        and (now - job.updated_at) > JOB_TTL_S
    ]
    for jid in expired:
        _jobs.pop(jid, None)
    if expired:
        logger.info("LRO gc purged %d expired jobs", len(expired))
    return len(expired)


def snapshot() -> list[dict[str, Any]]:
    """All jobs, sorted newest first — for diagnostics."""
    return sorted(
        (job.to_dict() for job in _jobs.values()),
        key=lambda d: d["created_at"],
        reverse=True,
    )
