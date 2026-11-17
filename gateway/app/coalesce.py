"""In-flight coalescing for identical upstream requests.

Identical concurrent calls are grouped by ``(route version, target, path,
delay_ms)``. Every caller still gets its own ``inflight`` row and its own full
response; only the upstream execution is shared.

The upstream fetch runs in a task that is independent of any single HTTP
caller. Callers wait on it through :func:`asyncio.shield`, so a browser that
disconnects only cancels its own waiter: the shared execution keeps running
and the other callers still receive the complete result. The disconnected
caller's in-flight row is released together with everyone else's when the
shared execution ends, so an old target never looks idle while work is still
running for it.

A group leaves the registry as soon as its execution finishes, whether it
succeeded or failed. A request that arrives later therefore executes again
instead of joining a finished (possibly failed) execution.

Module state is process-local: only executions inside one gateway process are
coalesced. The persisted ``inflight`` rows remain one per caller.
"""
from __future__ import annotations

import asyncio
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from . import db

RECENT_LIMIT = 20

FetchFn = Callable[[], Awaitable[dict[str, Any]]]


@dataclass
class _Group:
    key: tuple
    execution_id: str
    target: str
    route_version: int
    path: str
    delay_ms: int | None
    created_at: float
    creator_id: str
    callers: int = 0
    inflight_ids: set[str] = field(default_factory=set)
    final_callers: int = 0
    task: asyncio.Task | None = None

    def age_ms(self) -> int:
        return int((time.monotonic() - self.created_at) * 1000)


_lock = threading.Lock()
_registry: dict[tuple, _Group] = {}
_recent: list[dict[str, Any]] = []
_stats: dict[str, Any] = {
    "client_requests": 0,
    "upstream_executions": 0,
    "coalesced_joins": 0,
    "failed_executions": 0,
    "per_target": {},
}


def _target_stats(target: str) -> dict[str, int]:
    return _stats["per_target"].setdefault(
        target,
        {
            "client_requests": 0,
            "upstream_executions": 0,
            "coalesced_joins": 0,
            "failed_executions": 0,
        },
    )


def join(
    key: tuple,
    *,
    target: str,
    route_version: int,
    path: str,
    delay_ms: int | None,
    request_id: str,
    fetch: FetchFn,
) -> tuple[_Group, bool]:
    """Join the active execution for *key* or create a new one.

    There is no ``await`` between the lookup and the registration, so two
    concurrent callers cannot both dispatch. Returns ``(group, created)``;
    ``created`` is true for the caller that dispatched the upstream call.
    """
    with _lock:
        _stats["client_requests"] += 1
        tstats = _target_stats(target)
        tstats["client_requests"] += 1

        group = _registry.get(key)
        # A finished-but-not-yet-cleaned execution must never be joined: its
        # outcome is already decided (possibly a failure that has to be
        # retried by the next request).
        if group is not None and group.task is not None and group.task.done():
            group = None
        created = False
        if group is None:
            group = _Group(
                key=key,
                execution_id=uuid.uuid4().hex,
                target=target,
                route_version=route_version,
                path=path,
                delay_ms=delay_ms,
                created_at=time.monotonic(),
                creator_id=request_id,
            )
            task = asyncio.create_task(fetch())
            group.task = task
            task.add_done_callback(lambda t, g=group, k=key: _finish(k, g, t))
            _registry[key] = group
            _stats["upstream_executions"] += 1
            tstats["upstream_executions"] += 1
            created = True
        else:
            _stats["coalesced_joins"] += 1
            tstats["coalesced_joins"] += 1

        group.callers += 1
        group.inflight_ids.add(request_id)
        return group, created


def _finish(key: tuple, group: _Group, task: asyncio.Task) -> None:
    """Executed exactly once per shared execution, on every outcome. The group
    is unregistered first so later requests start a fresh execution."""
    with _lock:
        if _registry.get(key) is group:
            _registry.pop(key, None)
        # All callers that were still registered when the execution ended.
        # Their rows are removed together, including any caller that went
        # away mid-flight; nobody can see the target as idle before this.
        caller_ids = set(group.inflight_ids)
        final_callers = group.callers
        group.final_callers = final_callers

    ok = False
    error: str | None = None
    result: Any = None
    try:
        result = task.result()
    except asyncio.CancelledError:
        error = "execution_cancelled"
    except Exception as exc:  # noqa: BLE001 - reported to each caller
        error = f"{type(exc).__name__}: {exc}"[:200]
    else:
        if isinstance(result, dict):
            ok = bool(result.get("ok"))
            if not ok:
                error = f"upstream_status_{result.get('status')}"
        else:
            ok = True

    with _lock:
        if not ok:
            _stats["failed_executions"] += 1
            _target_stats(group.target)["failed_executions"] += 1
        _recent.append(
            {
                "execution_id": group.execution_id,
                "target": group.target,
                "route_version": group.route_version,
                "path": group.path,
                "delay_ms": group.delay_ms,
                "callers": final_callers,
                "ok": ok,
                "status": result.get("status") if isinstance(result, dict) else None,
                "error": error,
                "duration_ms": group.age_ms(),
                "finished_at": time.time(),
            }
        )
        del _recent[:-RECENT_LIMIT]

    # Every caller's row is released here, including callers that disconnected
    # while the execution was running.
    try:
        db.finish_many(caller_ids)
    except Exception:  # noqa: BLE001 - cleanup must not break other callers
        pass


def inflight_annotations() -> dict[str, dict[str, Any]]:
    """Map caller request id -> execution info for the in-flight detail list."""
    out: dict[str, dict[str, Any]] = {}
    with _lock:
        for group in list(_registry.values()):
            for rid in list(group.inflight_ids):
                out[rid] = {
                    "execution_id": group.execution_id,
                    "execution_callers": group.callers,
                    "upstream_executed": rid == group.creator_id,
                    "shared_execution": rid != group.creator_id,
                }
    return out


def snapshot() -> dict[str, Any]:
    """Coalescing stats + active groups for the state API / web console."""
    with _lock:
        groups = sorted(_registry.values(), key=lambda g: g.created_at)
        return {
            "stats": {
                "client_requests": _stats["client_requests"],
                "upstream_executions": _stats["upstream_executions"],
                "coalesced_joins": _stats["coalesced_joins"],
                "failed_executions": _stats["failed_executions"],
                "per_target": {
                    name: dict(values) for name, values in _stats["per_target"].items()
                },
            },
            "active_groups": [
                {
                    "execution_id": g.execution_id,
                    "target": g.target,
                    "route_version": g.route_version,
                    "path": g.path,
                    "delay_ms": g.delay_ms,
                    "callers": g.callers,
                    "age_ms": g.age_ms(),
                }
                for g in groups
            ],
            "recent_groups": [dict(row) for row in _recent],
        }


def caller_info(group: _Group, request_id: str) -> dict[str, Any]:
    """Per-caller view of a (possibly shared) execution, for the response."""
    with _lock:
        callers = group.final_callers or group.callers
        upstream_executed = request_id == group.creator_id
    return {
        "execution_id": group.execution_id,
        "coalesced": not upstream_executed,
        "execution_callers": callers,
        "upstream_executed": upstream_executed,
    }
