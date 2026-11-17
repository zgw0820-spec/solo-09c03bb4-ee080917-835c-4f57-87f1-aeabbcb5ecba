#!/usr/bin/env python3
"""End-to-end smoke test for the edge gateway.

Usage:
  smoke_test.py BASE_URL SAVE_FILE phase1   # switch semantics, arms an interrupted request
  smoke_test.py BASE_URL SAVE_FILE phase2   # post-restart recovery checks
"""
import json
import sys
import threading
import time
import urllib.error
import urllib.request

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"
SAVE = sys.argv[2] if len(sys.argv) > 2 else "/tmp/edge-gw-phase1.json"
PHASE = sys.argv[3] if len(sys.argv) > 3 else "phase1"

FAILURES: list[str] = []


def api(method, path, body=None, timeout=70):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE + path, method=method, data=data,
        headers={"content-type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def check(name, cond, extra=""):
    mark = "PASS" if cond else "FAIL"
    print(f"[{mark}] {name}" + (f"  ({extra})" if extra and not cond else ""))
    if not cond:
        FAILURES.append(name)


def inflight_of(state, name):
    for u in state["upstreams"]:
        if u["name"] == name:
            return u["inflight"]
    return 0


def upstream_of(state, name):
    return next(u for u in state["upstreams"] if u["name"] == name)


def wait_state(pred, what, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        _, st = api("GET", "/api/state")
        if pred(st):
            return st
        time.sleep(0.1)
    check(f"等待状态: {what}", False)
    return api("GET", "/api/state")[1]


def phase1():
    _, st = api("GET", "/api/state")
    check("初始当前目标为 alpha", st["route"]["current_target"] == "alpha")
    check("初始版号为 0", st["route"]["version"] == 0)
    check("初始无在途请求", len(st["inflight"]) == 0)

    # 1) slow request admitted to alpha, keeps running through the switch
    slow = {}

    def slow_call():
        try:
            s, b = api("POST", "/api/call", {"delay_ms": 4000})
            slow["status"], slow["body"] = s, b
        except Exception as e:  # noqa: BLE001
            slow["error"] = str(e)

    t = threading.Thread(target=slow_call, daemon=True)
    t.start()
    wait_state(lambda s: inflight_of(s, "alpha") == 1, "alpha 在途变为 1")

    # 2) before any switch, a fast call still goes to alpha
    s, r = api("POST", "/api/call", {"delay_ms": 50})
    check("切换前快请求走 alpha", r["target"] == "alpha" and r["upstream"]["upstream"] == "alpha")

    # 3) probe beta; while/after probing (before commit) traffic stays on alpha
    s, p = api("POST", "/api/switch/probe", {"target": "beta", "base_version": 0})
    check("探测 beta 成功", p["ok"] is True and p["version_matches"] is True)
    s, r = api("POST", "/api/call", {"delay_ms": 50})
    check("探测后未确认前请求仍走 alpha", r["target"] == "alpha")
    _, st = api("GET", "/api/state")
    check("探测不改变路由", st["route"]["current_target"] == "alpha" and st["route"]["version"] == 0)

    # 4) commit the switch
    s, c = api("POST", "/api/switch/commit",
               {"target": "beta", "base_version": 0, "probe_id": p["probe_id"]})
    check("确认切换成功，版号升为 1", c.get("ok") is True and c.get("version") == 1)

    # 5) new requests immediately go to beta, no waiting for the old one
    s, r = api("POST", "/api/call", {"delay_ms": 50})
    check("切换后新请求立即走 beta", r["target"] == "beta" and r["upstream"]["upstream"] == "beta")
    _, st = api("GET", "/api/state")
    check("旧目标 alpha 仍有 1 条在途（未等待其结束）", inflight_of(st, "alpha") == 1)
    check("alpha 在途未清零，不可下线", upstream_of(st, "alpha")["can_offline"] is False)

    # 6) stale commit (page version outdated) must not override the effective route
    s, p_stale = api("POST", "/api/switch/probe", {"target": "alpha", "base_version": 0})
    check("过期版号的探测被标记 version_matches=false",
          p_stale["ok"] is True and p_stale["version_matches"] is False)
    s, c_stale = api("POST", "/api/switch/commit",
                     {"target": "alpha", "base_version": 0, "probe_id": p_stale["probe_id"]})
    check("过期版号的确认被拒绝(409 version_conflict)",
          s == 409 and c_stale["error"] == "version_conflict")
    _, st = api("GET", "/api/state")
    check("路由未被迟到提交覆盖，仍为 beta@v1",
          st["route"]["current_target"] == "beta" and st["route"]["version"] == 1)

    # 7) failed probe keeps the original route
    s, pd = api("POST", "/api/switch/probe", {"target": "dead", "base_version": 1})
    check("探测不可达候选失败", pd["ok"] is False)
    _, st = api("GET", "/api/state")
    check("探测失败后维持原路由 beta@v1",
          st["route"]["current_target"] == "beta" and st["route"]["version"] == 1)
    s, cd = api("POST", "/api/switch/commit",
                {"target": "dead", "base_version": 1, "probe_id": pd["probe_id"]})
    check("失败探测对应的确认被拒绝", s == 400 and cd["error"] == "probe_invalid")

    # 8) two interleaved switches on the same base version: exactly one wins
    s, px = api("POST", "/api/switch/probe", {"target": "alpha", "base_version": 1})
    s, py = api("POST", "/api/switch/probe", {"target": "alpha", "base_version": 1})
    s, cx = api("POST", "/api/switch/commit",
                {"target": "alpha", "base_version": 1, "probe_id": px["probe_id"]})
    check("交错切换：先到的确认生效(v2)", cx.get("ok") is True and cx.get("version") == 2)
    s, cy = api("POST", "/api/switch/commit",
                {"target": "alpha", "base_version": 1, "probe_id": py["probe_id"]})
    check("交错切换：迟到的确认被版号拒绝", s == 409 and cy["error"] == "version_conflict")

    # 9) late probe must not overwrite an already-effective route
    s, pz = api("POST", "/api/switch/probe", {"target": "beta", "base_version": 2})
    check("探测 beta@v2 成功", pz["ok"] is True)
    s, pz2 = api("POST", "/api/switch/probe", {"target": "beta", "base_version": 2})
    s, cz2 = api("POST", "/api/switch/commit",
                 {"target": "beta", "base_version": 2, "probe_id": pz2["probe_id"]})
    check("另一值班员先完成切换 beta@v3", cz2.get("ok") is True and cz2.get("version") == 3)
    s, cz = api("POST", "/api/switch/commit",
                {"target": "beta", "base_version": 2, "probe_id": pz["probe_id"]})
    check("迟到探测的确认不得覆盖已生效路由", s == 409 and cz["error"] == "version_conflict")
    _, st = api("GET", "/api/state")
    check("路由保持 beta@v3", st["route"]["current_target"] == "beta" and st["route"]["version"] == 3)

    # 10) the slow pre-switch request completes with the full response from alpha
    t.join(timeout=15)
    check("慢请求已结束", "body" in slow, slow.get("error", ""))
    body = slow.get("body", {})
    check("慢请求从原上游 alpha 取回完整响应",
          body.get("ok") is True and body.get("upstream", {}).get("upstream") == "alpha"
          and body.get("upstream", {}).get("delay_ms") == 4000)

    # 11) alpha drains to zero -> only then may it be taken offline
    wait_state(lambda s: inflight_of(s, "alpha") == 0, "alpha 在途清零")
    _, st = api("GET", "/api/state")
    check("alpha 在途清零后提示可下线", upstream_of(st, "alpha")["can_offline"] is True)
    check("当前目标 beta 不标记可下线", upstream_of(st, "beta")["can_offline"] is False)

    # 12) arm one in-flight request that the harness will interrupt with kill -9
    def interrupted():
        try:
            api("POST", "/api/call", {"delay_ms": 15000})
        except Exception:  # noqa: BLE001 - connection reset when gateway dies
            pass

    threading.Thread(target=interrupted, daemon=True).start()
    wait_state(lambda s: inflight_of(s, "beta") == 1, "重启前 beta 在途为 1")
    with open(SAVE, "w") as f:
        json.dump({"target": "beta", "version": 3}, f)
    print("PHASE1 READY: 已在 beta 上挂起 1 条在途请求，等待网关被杀")

    # keep the client connection open until the gateway is killed
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            api("GET", "/api/state", timeout=2)
            time.sleep(0.2)
        except Exception:  # noqa: BLE001
            print("PHASE1 OK: 网关已停止")
            return
    check("等待网关被杀", False)


def phase2():
    with open(SAVE) as f:
        saved = json.load(f)
    _, st = api("GET", "/api/state")
    check("重启后按已登记路由接流量(目标不变)", st["route"]["current_target"] == saved["target"])
    check("重启后路由版号保持", st["route"]["version"] == saved["version"])
    check("中断请求未留作在途", len(st["inflight"]) == 0)
    check("启动时清理了中断在途记录", st["boot"]["cleared_inflight"] >= 1)
    s, r = api("POST", "/api/call", {"delay_ms": 50})
    check("重启后新请求正常发往已登记目标",
          r["target"] == saved["target"] and r["ok"] is True)


if PHASE == "phase1":
    phase1()
else:
    phase2()

if FAILURES:
    print(f"\n{len(FAILURES)} 项检查失败:")
    for f in FAILURES:
        print(" -", f)
    sys.exit(1)
print(f"\n全部检查通过 ({PHASE})")
