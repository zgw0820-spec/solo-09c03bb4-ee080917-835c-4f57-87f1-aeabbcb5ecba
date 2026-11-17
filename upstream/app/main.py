"""Demo upstream with configurable response delay and a health endpoint
used by the gateway's pre-switch probe."""
from __future__ import annotations

import asyncio
import os
import time

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

NAME = os.environ.get("UPSTREAM_NAME", "upstream")
DEFAULT_DELAY_MS = int(os.environ.get("DEFAULT_DELAY_MS", "0"))
MAX_DELAY_MS = 60_000

app = FastAPI(title=f"upstream-{NAME}")
_state = {"health_fail": os.environ.get("HEALTH_FAIL", "0") == "1"}


@app.get("/health")
def health():
    if _state["health_fail"]:
        return JSONResponse(
            status_code=503, content={"status": "failing", "upstream": NAME}
        )
    return {"status": "ok", "upstream": NAME}


class HealthMode(BaseModel):
    fail: bool


@app.post("/admin/health_mode")
def set_health_mode(mode: HealthMode):
    """Demo hook: make /health fail or recover at runtime."""
    _state["health_fail"] = mode.fail
    return {"upstream": NAME, "health_fail": _state["health_fail"]}


@app.api_route("/work", methods=["GET", "POST"])
async def work(delay_ms: int | None = Query(default=None)):
    delay = DEFAULT_DELAY_MS if delay_ms is None else max(0, min(delay_ms, MAX_DELAY_MS))
    await asyncio.sleep(delay / 1000)
    return {"upstream": NAME, "delay_ms": delay, "ts": time.time()}


@app.get("/")
def root():
    return {"upstream": NAME, "default_delay_ms": DEFAULT_DELAY_MS}
