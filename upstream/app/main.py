"""Demo upstream with configurable response delay, a health endpoint used by
the gateway's pre-switch probe, runtime failure injection and execution
counters so the demo can show how often the upstream actually ran /work."""
from __future__ import annotations

import asyncio
import os
import time

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

NAME = os.environ.get("UPSTREAM_NAME", "upstream")
DEFAULT_DELAY_MS = int(os.environ.get("DEFAULT_DELAY_MS", "0"))
MAX_DELAY_MS = 60_000

app = FastAPI(title=f"upstream-{NAME}")
_state = {
    "health_fail": os.environ.get("HEALTH_FAIL", "0") == "1",
    "work_fail": os.environ.get("WORK_FAIL", "0") == "1",
    # How many times this upstream actually executed /work. Probe traffic is
    # counted separately so the demo can compare real executions with the
    # gateway's client-request / shared-execution counters.
    "work_executions": 0,
    "failed_executions": 0,
    "probe_executions": 0,
}


@app.get("/health")
def health():
    if _state["health_fail"]:
        return JSONResponse(
            status_code=503, content={"status": "failing", "upstream": NAME}
        )
    return {"status": "ok", "upstream": NAME}


def _stats() -> dict:
    return {
        "upstream": NAME,
        "reachable": True,
        "work_executions": _state["work_executions"],
        "failed_executions": _state["failed_executions"],
        "probe_executions": _state["probe_executions"],
        "default_delay_ms": DEFAULT_DELAY_MS,
        "work_fail": _state["work_fail"],
    }


@app.get("/stats")
def stats():
    """How many /work executions this upstream really performed."""
    return _stats()


class HealthMode(BaseModel):
    fail: bool


@app.post("/admin/health_mode")
def set_health_mode(mode: HealthMode):
    """Demo hook: make /health fail or recover at runtime."""
    _state["health_fail"] = mode.fail
    return _stats()


class WorkMode(BaseModel):
    fail: bool


@app.post("/admin/work_mode")
def set_work_mode(mode: WorkMode):
    """Demo hook: make /work return 500 at runtime (used to show that a failed
    shared execution is dropped so the next request executes again)."""
    _state["work_fail"] = mode.fail
    return _stats()


@app.api_route("/work", methods=["GET", "POST"])
async def work(request: Request, delay_ms: int | None = Query(default=None)):
    delay = DEFAULT_DELAY_MS if delay_ms is None else max(0, min(delay_ms, MAX_DELAY_MS))
    if request.query_params.get("probe") == "1":
        _state["probe_executions"] += 1
        return {"upstream": NAME, "delay_ms": delay, "probe": True}
    _state["work_executions"] += 1
    try:
        await asyncio.sleep(delay / 1000)
        if _state["work_fail"]:
            _state["failed_executions"] += 1
            return JSONResponse(
                status_code=500,
                content={"upstream": NAME, "delay_ms": delay, "error": "injected_failure"},
            )
        return {"upstream": NAME, "delay_ms": delay, "ts": time.time()}
    except asyncio.CancelledError:
        # The gateway hung up before the response was delivered.
        raise


@app.get("/")
def root():
    return {"upstream": NAME, "default_delay_ms": DEFAULT_DELAY_MS}
