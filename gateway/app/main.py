"""Edge gateway: proxies requests to the currently routed upstream and
exposes the operations API + web console for on-duty target switching."""
from __future__ import annotations

import asyncio
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

from . import coalesce, db

STATIC_DIR = Path(__file__).parent / "static"


def parse_upstreams() -> dict[str, str]:
    raw = os.environ.get(
        "UPSTREAMS", "alpha=http://127.0.0.1:9001;beta=http://127.0.0.1:9002"
    )
    out: dict[str, str] = {}
    for part in raw.split(";"):
        part = part.strip()
        if not part:
            continue
        name, _, url = part.partition("=")
        out[name.strip()] = url.strip().rstrip("/")
    return out


UPSTREAMS = parse_upstreams()
INITIAL_TARGET = os.environ.get("INITIAL_TARGET") or next(iter(UPSTREAMS))


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.boot_info = db.init_db(UPSTREAMS, INITIAL_TARGET)
    app.state.http = httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=3.0))
    yield
    await app.state.http.aclose()


app = FastAPI(title="edge-gateway", lifespan=lifespan)


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/health")
def health():
    return {"ok": True}


@app.get("/api/state")
async def state():
    s = db.get_state()
    s["boot"] = app.state.boot_info
    # Annotate each in-flight row with its shared execution (if any) so the
    # console can show per-request sharing, and expose coalescing stats.
    annotations = coalesce.inflight_annotations()
    for row in s["inflight"]:
        row.update(annotations.get(row["id"], {}))
    s["coalescing"] = coalesce.snapshot()
    return s


@app.get("/api/upstream_stats")
async def upstream_stats():
    """How many /work executions each upstream really performed. Kept separate
    from /api/state so polling state never waits on an unreachable upstream."""
    names = [u["name"] for u in db.get_state()["upstreams"]]

    async def one(name: str):
        url = db.upstream_url(name)
        try:
            resp = await app.state.http.get(f"{url}/stats", timeout=0.8)
            return name, resp.json()
        except (httpx.HTTPError, ValueError):
            return name, {"reachable": False}

    results = await asyncio.gather(*(one(name) for name in names))
    return {"upstreams": {name: data for name, data in results}}


class CallRequest(BaseModel):
    delay_ms: int | None = Field(default=None, ge=0, le=60_000)
    path: str = "/work"


@app.post("/api/call")
async def call(req: CallRequest):
    """Admit a request and proxy it to the upstream that is current *at
    admission time*. Identical concurrent requests (same route version,
    target, path and delay_ms) share one upstream execution while each caller
    keeps its own admission, in-flight row and full response."""
    path = req.path if req.path.startswith("/") else "/work"
    if "://" in path or ".." in path:
        return JSONResponse(status_code=400, content={"ok": False, "error": "bad_path"})
    admission = db.admit(path)
    target_url = db.upstream_url(admission["target"])
    delay_ms = req.delay_ms

    async def fetch() -> dict:
        params = {} if delay_ms is None else {"delay_ms": delay_ms}
        resp = await app.state.http.get(f"{target_url}{path}", params=params)
        try:
            body = resp.json()
        except ValueError:
            body = {"text": resp.text[:500]}
        return {"status": resp.status_code, "body": body}

    group, _created = coalesce.join(
        (admission["version"], admission["target"], path, delay_ms),
        target=admission["target"],
        route_version=admission["version"],
        path=path,
        delay_ms=delay_ms,
        request_id=admission["id"],
        fetch=fetch,
    )
    started = time.monotonic()
    try:
        # Shield the shared execution: if this caller goes away, the upstream
        # call keeps running for every other caller of the same execution
        # (and its own in-flight row is released when that execution ends).
        result = await asyncio.shield(group.task)
    except asyncio.CancelledError:
        # This browser disconnected. Shield kept the shared execution alive for
        # the other callers; this caller's in-flight row is released when that
        # execution ends.
        raise
    except Exception as exc:  # noqa: BLE001 - shared task failure
        return JSONResponse(
            status_code=502,
            content={
                "ok": False,
                "error": f"upstream_error: {exc}",
                "request_id": admission["id"],
                "target": admission["target"],
                "route_version": admission["version"],
                "coalescing": coalesce.caller_info(group, admission["id"]),
            },
        )
    duration_ms = int((time.monotonic() - started) * 1000)
    body = result["body"]
    return {
        "ok": result["status"] == 200,
        "status": result["status"],
        "request_id": admission["id"],
        "target": admission["target"],
        "route_version": admission["version"],
        "duration_ms": duration_ms,
        "upstream": body,
        "coalescing": coalesce.caller_info(group, admission["id"]),
    }


class ProbeRequest(BaseModel):
    target: str
    base_version: int


class WorkModeRequest(BaseModel):
    fail: bool


@app.post("/api/upstreams/{name}/work_mode")
async def upstream_work_mode(name: str, req: WorkModeRequest):
    """Demo hook: inject (or clear) a /work failure on one upstream so the
    console can show that a failed shared execution is dropped and the next
    request executes again."""
    url = db.upstream_url(name)
    if url is None:
        return JSONResponse(
            status_code=404, content={"ok": False, "error": "unknown_target"}
        )
    try:
        resp = await app.state.http.post(
            f"{url}/admin/work_mode", json={"fail": req.fail}, timeout=3.0
        )
    except httpx.HTTPError as exc:
        return JSONResponse(
            status_code=502, content={"ok": False, "error": f"upstream_error: {exc}"}
        )
    return resp.json()


@app.post("/api/switch/probe")
async def probe(req: ProbeRequest):
    """Probe a candidate target. Never mutates the route: while probing, new
    requests keep flowing to the old target, and a failed probe leaves the
    original route untouched."""
    route = db.get_route()
    url = db.upstream_url(req.target)
    if url is None:
        return JSONResponse(
            status_code=404, content={"ok": False, "error": "unknown_target"}
        )
    if req.target == route["current_target"]:
        return {"ok": False, "error": "target_is_current", "current_version": route["version"]}
    ok, detail = False, {}
    try:
        health = await app.state.http.get(f"{url}/health", timeout=2.5)
        work = await app.state.http.get(
            f"{url}/work", params={"delay_ms": 0, "probe": "1"}, timeout=5.0
        )
        ok = health.status_code == 200 and work.status_code == 200
        detail = {"health_status": health.status_code, "work_status": work.status_code}
    except httpx.HTTPError as exc:
        detail = {"error": str(exc)}
    rec = db.record_probe(req.target, req.base_version, ok, detail)
    return {
        "ok": ok,
        "probe_id": rec["id"],
        "target": req.target,
        "base_version": req.base_version,
        "current_version": route["version"],
        "version_matches": route["version"] == req.base_version,
        "detail": detail,
    }


class CommitRequest(BaseModel):
    target: str
    base_version: int
    probe_id: str


@app.post("/api/switch/commit")
def commit(req: CommitRequest):
    """Confirm a switch. Version-guarded: once this commits, newly admitted
    requests immediately go to the new target without waiting for old
    in-flight requests; a stale commit is rejected with 409."""
    res = db.commit_switch(req.target, req.base_version, req.probe_id)
    if not res["ok"]:
        code = 409 if res["error"] == "version_conflict" else 400
        return JSONResponse(status_code=code, content=res)
    return res
