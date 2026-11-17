"""SQLite persistence for the edge gateway.

Tables:
  upstreams  - registered upstream targets (seeded from env, upserted on boot)
  routes     - single-row table holding the effective route and its version
  inflight   - one row per admitted proxied request; wiped on startup so a
               restarted gateway never reports interrupted requests as in-flight
  probes     - audit log of candidate probes; a commit requires a fresh,
               successful probe recorded against the same route version
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid
from pathlib import Path

DB_PATH = Path(os.environ.get("GATEWAY_DB", "/data/gateway.db"))
PROBE_TTL_SECONDS = float(os.environ.get("PROBE_TTL_SECONDS", "60"))

_lock = threading.RLock()
_conn: sqlite3.Connection | None = None


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def init_db(upstreams: dict[str, str], initial_target: str) -> dict:
    """Create tables, upsert the upstream registry, seed the route once,
    and drop in-flight rows left behind by a previous (killed) process."""
    global _conn
    if not upstreams:
        raise RuntimeError("no upstreams configured")
    if initial_target not in upstreams:
        raise RuntimeError(f"initial target {initial_target!r} not in upstreams")
    _conn = _connect()
    with _lock, _conn:
        _conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS upstreams (
                name TEXT PRIMARY KEY,
                url  TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS routes (
                id             INTEGER PRIMARY KEY CHECK (id = 1),
                current_target TEXT NOT NULL,
                version        INTEGER NOT NULL,
                updated_at     REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS inflight (
                id         TEXT PRIMARY KEY,
                target     TEXT NOT NULL,
                path       TEXT NOT NULL,
                started_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS probes (
                id           TEXT PRIMARY KEY,
                target       TEXT NOT NULL,
                base_version INTEGER NOT NULL,
                ok           INTEGER NOT NULL,
                detail       TEXT,
                created_at   REAL NOT NULL
            );
            """
        )
        for name, url in upstreams.items():
            _conn.execute(
                "INSERT INTO upstreams (name, url) VALUES (?, ?) "
                "ON CONFLICT(name) DO UPDATE SET url = excluded.url",
                (name, url),
            )
        seeded = False
        if _conn.execute("SELECT COUNT(*) AS c FROM routes").fetchone()["c"] == 0:
            _conn.execute(
                "INSERT INTO routes (id, current_target, version, updated_at) "
                "VALUES (1, ?, 0, ?)",
                (initial_target, time.time()),
            )
            seeded = True
        # Interrupted requests of a previous process must not linger as in-flight.
        cleared = _conn.execute("DELETE FROM inflight").rowcount
    return {"seeded_route": seeded, "cleared_inflight": cleared}


def _db() -> sqlite3.Connection:
    assert _conn is not None, "db not initialised"
    return _conn


def get_route() -> dict:
    with _lock:
        row = _db().execute(
            "SELECT current_target, version, updated_at FROM routes WHERE id = 1"
        ).fetchone()
    return dict(row)


def upstream_url(name: str) -> str | None:
    with _lock:
        row = _db().execute(
            "SELECT url FROM upstreams WHERE name = ?", (name,)
        ).fetchone()
    return row["url"] if row else None


def admit(path: str) -> dict:
    """Atomically snapshot the effective route and register the request as
    in-flight. The request keeps this target even if a switch commits while
    it is running, so it always completes against the original upstream."""
    rid = uuid.uuid4().hex
    with _lock, _db() as conn:
        route = conn.execute(
            "SELECT current_target, version FROM routes WHERE id = 1"
        ).fetchone()
        conn.execute(
            "INSERT INTO inflight (id, target, path, started_at) VALUES (?, ?, ?, ?)",
            (rid, route["current_target"], path, time.time()),
        )
    return {"id": rid, "target": route["current_target"], "version": route["version"]}


def finish(request_id: str) -> None:
    with _lock, _db() as conn:
        conn.execute("DELETE FROM inflight WHERE id = ?", (request_id,))


def record_probe(target: str, base_version: int, ok: bool, detail: dict) -> dict:
    """A probe never mutates the route; it only records an auditable result."""
    pid = uuid.uuid4().hex
    with _lock, _db() as conn:
        conn.execute(
            "INSERT INTO probes (id, target, base_version, ok, detail, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (pid, target, base_version, 1 if ok else 0, json.dumps(detail), time.time()),
        )
    return {"id": pid}


def commit_switch(target: str, base_version: int, probe_id: str) -> dict:
    """Compare-and-swap the route on its version. A commit carrying a stale
    version (e.g. a late probe from an interleaved switch attempt) loses and
    cannot overwrite the already-effective route."""
    with _lock, _db() as conn:
        route = conn.execute(
            "SELECT current_target, version FROM routes WHERE id = 1"
        ).fetchone()
        if route["version"] != base_version:
            return {
                "ok": False,
                "error": "version_conflict",
                "current_version": route["version"],
                "current_target": route["current_target"],
            }
        if route["current_target"] == target:
            return {
                "ok": False,
                "error": "target_is_current",
                "current_version": route["version"],
                "current_target": route["current_target"],
            }
        if upstream_url(target) is None:
            return {"ok": False, "error": "unknown_target"}
        probe = conn.execute(
            "SELECT * FROM probes WHERE id = ?", (probe_id,)
        ).fetchone()
        if (
            probe is None
            or not probe["ok"]
            or probe["target"] != target
            or probe["base_version"] != base_version
        ):
            return {"ok": False, "error": "probe_invalid"}
        if time.time() - probe["created_at"] > PROBE_TTL_SECONDS:
            return {"ok": False, "error": "probe_stale"}
        cur = conn.execute(
            "UPDATE routes SET current_target = ?, version = version + 1, updated_at = ? "
            "WHERE id = 1 AND version = ?",
            (target, time.time(), base_version),
        )
        if cur.rowcount != 1:  # guarded by _lock; belt and braces
            return {"ok": False, "error": "version_conflict"}
        return {"ok": True, "current_target": target, "version": base_version + 1}


def get_state() -> dict:
    with _lock:
        route = dict(_db().execute("SELECT * FROM routes WHERE id = 1").fetchone())
        upstreams = [
            dict(r) for r in _db().execute("SELECT name, url FROM upstreams ORDER BY name")
        ]
        inflight = [
            dict(r) for r in _db().execute("SELECT * FROM inflight ORDER BY started_at")
        ]
        probes = [
            dict(r)
            for r in _db().execute(
                "SELECT * FROM probes ORDER BY created_at DESC LIMIT 10"
            )
        ]
    counts: dict[str, int] = {}
    for row in inflight:
        counts[row["target"]] = counts.get(row["target"], 0) + 1
    now = time.time()
    for u in upstreams:
        u["inflight"] = counts.get(u["name"], 0)
        u["is_current"] = u["name"] == route["current_target"]
        # A non-current target may only be taken offline once its in-flight
        # requests have fully drained.
        u["can_offline"] = (not u["is_current"]) and u["inflight"] == 0
    for row in inflight:
        row["age_ms"] = int((now - row["started_at"]) * 1000)
    for p in probes:
        p["detail"] = json.loads(p["detail"]) if p["detail"] else {}
    return {
        "now": now,
        "route": route,
        "upstreams": upstreams,
        "inflight": inflight,
        "recent_probes": probes,
    }
