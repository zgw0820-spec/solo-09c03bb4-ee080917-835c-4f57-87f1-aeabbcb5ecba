#!/usr/bin/env python3
"""End-to-end checks for in-flight request coalescing.

Run against a live gateway that has two reachable upstreams (alpha, beta).
Covers:
  * identical concurrent requests execute upstream once, every caller gets the
    full result and keeps its own in-flight row;
  * different delay_ms are not coalesced;
  * a request admitted after a switch never joins an execution of the old
    route version, even when the target name becomes the same again;
  * an old target is not reported offlinable while its shared execution runs;
  * one disconnecting caller does not affect the others;
  * a failed shared execution is dropped so the next request executes again.

Usage: coalesce_test.py BASE_URL
"""
import json
import socket
import sys
import threading
import time
import urllib.error
import urllib.request

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"
FAILURES: list[str] = []


def api(method, path, body=None, timeout=40):
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
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f"  ({extra})" if extra and not cond else ""))
    if not cond:
        FAILURES.append(name)


def state():
    return api("GET", "/api/state")[1]


def upstream_of(st, name):
    return next(u for u in st["upstreams"] if u["name"] == name)


def inflight_of(st, name):
    return upstream_of(st, name)["inflight"]


def upstream_exec_count(name):
    _, body = api("GET", "/api/upstream_stats")
    info = body["upstreams"].get(name, {})
    return info.get("work_executions") if info.get("reachable") else None


def wait_state(pred, what, timeout=12):
    deadline = time.time() + timeout
    while time.time() < deadline:
        st = state()
        if pred(st):
            return st
        time.sleep(0.05)
    check(f"等待状态: {what}", False)
    return state()


def call_async(body, out, key):
    def run():
        try:
            out[key] = api("POST", "/api/call", body, timeout=40)
        except Exception as exc:  # noqa: BLE001
            out[key] = ("error", {"error": str(exc)})
    t = threading.Thread(target=run, daemon=True)
    t.start()
    return t


def wait_group(st, delay_ms, target, callers, version=None, timeout=8):
    def pred(s):
        for g in s["coalescing"]["active_groups"]:
            if (g["delay_ms"] == delay_ms and g["target"] == target
                    and g["callers"] == callers
                    and (version is None or g["route_version"] == version)):
                return True
        return False
    return wait_state(lambda s: pred(s), f"{target} 合并组 callers={callers}", timeout)


def main():
    st = state()
    base_target = st["route"]["current_target"]
    other = next(u["name"] for u in st["upstreams"] if u["name"] != base_target)
    current_version = st["route"]["version"]
    print(f"起始路由 {base_target}@v{current_version}，另一目标 {other}")

    # ---------- A. identical concurrent requests share one execution ------
    c0 = state()["coalescing"]["stats"]
    up0 = upstream_exec_count(base_target)
    assert up0 is not None, "upstream stats unreachable"
    out: dict = {}
    threads = [call_async({"delay_ms": 1500}, out, i) for i in range(5)]
    st5 = wait_state(
        lambda s: inflight_of(s, base_target) == 5
        and any(g["callers"] == 5 for g in s["coalescing"]["active_groups"]),
        "5 个调用者共享 1 次执行", timeout=6,
    )
    group = next((g for g in st5["coalescing"]["active_groups"]
                  if g["callers"] == 5 and g["delay_ms"] == 1500), None)
    check("合并组存在且目标/版号一致",
          group is not None and group["target"] == base_target
          and group["route_version"] == current_version)
    check("5 个调用者各占 1 条在途记录",
          inflight_of(st5, base_target) == 5 and len(st5["inflight"]) == 5, str(st5["inflight"]))
    check("在途明细标注 1 次上游执行 + 4 个共享",
          sum(1 for r in st5["inflight"] if r.get("upstream_executed")) == 1
          and sum(1 for r in st5["inflight"] if r.get("shared_execution")) == 4)
    check("共享调用者显示的该执行调用者数为 5",
          all(r.get("execution_callers") == 5 for r in st5["inflight"]))
    for t in threads:
        t.join(timeout=20)
    results = [out[i][1] for i in range(5)]
    check("5 个调用者全部成功", all(r.get("ok") for r in results),
          str([r.get("error") for r in results]))
    check("5 个调用者都收到完整响应",
          all(r.get("upstream", {}).get("delay_ms") == 1500 for r in results))
    exec_ids = {r.get("coalescing", {}).get("execution_id") for r in results}
    check("5 个调用者对应同一执行号", len(exec_ids) == 1 and None not in exec_ids)
    check("恰好 1 个调用者执行了上游",
          sum(1 for r in results if r["coalescing"]["upstream_executed"]) == 1)
    check("4 个调用者标记为合并(coalesced)",
          sum(1 for r in results if r["coalescing"]["coalesced"]) == 4)
    c1 = state()["coalescing"]["stats"]
    check("网关计数: 5 客户端请求 / 1 上游执行 / 4 合并",
          c1["client_requests"] - c0["client_requests"] == 5
          and c1["upstream_executions"] - c0["upstream_executions"] == 1
          and c1["coalesced_joins"] - c0["coalesced_joins"] == 4,
          json.dumps(c1))
    up1 = upstream_exec_count(base_target)
    check("上游实际执行次数只 +1", up1 == up0 + 1, f"{up0} -> {up1}")
    st_after = wait_state(lambda s: inflight_of(s, base_target) == 0, "合并执行结束清空在途")
    check("执行结束后合并组被移除",
          not any(g["delay_ms"] == 1500 for g in st_after["coalescing"]["active_groups"]))
    recent = [g for g in st_after["coalescing"]["recent_groups"]
              if g["delay_ms"] == 1500 and g["finished_at"] > time.time() - 30]
    check("最近执行记录显示调用者 5 个", bool(recent) and recent[-1]["callers"] == 5)

    # ---------- B. different delay_ms -> separate executions ---------------
    up_b = upstream_exec_count(base_target)
    out_b: dict = {}
    t1 = call_async({"delay_ms": 900}, out_b, 1)
    t2 = call_async({"delay_ms": 1000}, out_b, 2)
    wait_state(lambda s: inflight_of(s, base_target) == 2, "两个不同延迟的调用者在途")
    t1.join(20); t2.join(20)
    check("不同 delay_ms 的请求都成功",
          out_b[1][1].get("ok") is True and out_b[2][1].get("ok") is True)
    check("不同 delay_ms 不合并（执行号不同）",
          out_b[1][1]["coalescing"]["execution_id"]
          != out_b[2][1]["coalescing"]["execution_id"])
    check("不同 delay_ms 各自执行上游", upstream_exec_count(base_target) == up_b + 2)

    # ---------- C. version isolation, even when target name repeats --------
    stable_target = base_target
    up_c = upstream_exec_count(stable_target)
    # slow request on the *current* target/version
    out_c: dict = {}
    slow = call_async({"delay_ms": 7000}, out_c, "old")
    st = wait_state(
        lambda s: inflight_of(s, stable_target) == 1
        and any(g["delay_ms"] == 7000 for g in s["coalescing"]["active_groups"]),
        "旧版慢请求进入共享执行", timeout=6,
    )
    old_group = next(g for g in st["coalescing"]["active_groups"] if g["delay_ms"] == 7000)
    old_exec = old_group["execution_id"]
    old_version = st["route"]["version"]

    # switch away while it runs
    _, p = api("POST", "/api/switch/probe", {"target": other, "base_version": old_version})
    s, c = api("POST", "/api/switch/commit",
               {"target": other, "base_version": old_version, "probe_id": p["probe_id"]})
    check("切换离开旧目标成功", c.get("ok") is True, json.dumps(c))
    mid = state()
    check("旧目标仍有 1 条共计在途", inflight_of(mid, stable_target) == 1)
    check("旧目标共享执行未结束前不可下线",
          upstream_of(mid, stable_target)["can_offline"] is False)
    check("慢请求不因切换被中断", slow.is_alive())

    # switch back: same target *name* as the still-running old execution
    v2 = state()["route"]["version"]
    _, p2 = api("POST", "/api/switch/probe", {"target": stable_target, "base_version": v2})
    s, c2 = api("POST", "/api/switch/commit",
                {"target": stable_target, "base_version": v2, "probe_id": p2["probe_id"]})
    check("切回同名目标成功（新版号）", c2.get("ok") is True)
    new_version = state()["route"]["version"]
    check("切回后版号已增加", new_version > old_version)

    out_d: dict = {}
    fresh = call_async({"delay_ms": 7000}, out_d, "new")
    stn = wait_state(
        lambda s: any(g["delay_ms"] == 7000 and g["route_version"] == new_version
                      for g in s["coalescing"]["active_groups"]),
        "新版号的新请求建立新执行", timeout=6,
    )
    new_group = next(g for g in stn["coalescing"]["active_groups"]
                     if g["delay_ms"] == 7000 and g["route_version"] == new_version)
    check("新版号请求的执行号与旧版不同", new_group["execution_id"] != old_exec)
    check("同名目标的新旧执行并存为两个执行",
          sum(1 for g in stn["coalescing"]["active_groups"]
              if g["target"] == stable_target and g["delay_ms"] == 7000) == 2)
    check("新版号请求不加入旧版执行（在途分属两个执行）",
          len({r.get("execution_id") for r in stn["inflight"] if r["target"] == stable_target}) == 2)

    slow.join(20); fresh.join(20)
    check("旧版慢请求仍从原目标取回完整响应",
          out_c["old"][1].get("ok") is True
          and out_c["old"][1]["coalescing"]["execution_id"] == old_exec)
    check("新版请求从同名目标取回完整响应",
          out_d["new"][1].get("ok") is True
          and out_d["new"][1]["coalescing"]["execution_id"] != old_exec)
    check("两次执行是两个独立上游执行", upstream_exec_count(stable_target) == up_c + 2,
          f"{up_c} -> {upstream_exec_count(stable_target)}")

    # ---------- D. a disconnected caller must not affect the others --------
    up_d = upstream_exec_count(stable_target)
    c0 = state()["coalescing"]["stats"]
    out_e: dict = {}
    keep = call_async({"delay_ms": 2500}, out_e, "keep")
    wait_state(lambda s: inflight_of(s, stable_target) == 1, "正常调用者已接纳")
    # second caller: open a raw socket, send the request, then drop the connection
    host, port = BASE.replace("http://", "").split("/")[0].split(":")
    body = json.dumps({"delay_ms": 2500}).encode()
    raw = socket.create_connection((host, int(port)), timeout=5)
    raw.sendall(
        b"POST /api/call HTTP/1.1\r\nHost: " + host.encode() + b"\r\n"
        b"Content-Type: application/json\r\n"
        b"Content-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body
    )
    st2 = wait_state(lambda s: inflight_of(s, stable_target) == 2,
                     "两个调用者共享同一执行", timeout=6)
    multi = [g for g in st2["coalescing"]["active_groups"]
             if g["target"] == stable_target and g["delay_ms"] == 2500]
    check("断开的调用者加入同一执行", len(multi) == 1 and multi[0]["callers"] == 2)
    time.sleep(0.2)
    raw.close()
    keep.join(20)
    res = out_e["keep"][1]
    check("其余调用者不受断开影响，成功取回完整结果",
          res.get("ok") is True and res["upstream"].get("delay_ms") == 2500
          and res["coalescing"]["execution_callers"] == 2)
    check("断开后仍只执行上游一次",
          upstream_exec_count(stable_target) == up_d + 1,
          f"{up_d} -> {upstream_exec_count(stable_target)}")
    c1 = state()["coalescing"]["stats"]
    check("两个调用者计入客户端请求，仅一次上游执行",
          c1["client_requests"] - c0["client_requests"] == 2
          and c1["upstream_executions"] - c0["upstream_executions"] == 1)
    wait_state(lambda s: inflight_of(s, stable_target) == 0, "共享执行结束后在途清零")
    check("断开调用者的在途记录随共享执行结束一起释放",
          inflight_of(state(), stable_target) == 0)

    # ---------- E. failed execution is dropped, next request re-executes ---
    target = stable_target
    api("POST", f"/api/upstreams/{target}/work_mode", {"fail": True})
    try:
        up_e = upstream_exec_count(target)
        c0 = state()["coalescing"]["stats"]
        out_f: dict = {}
        fts = [call_async({"delay_ms": 500}, out_f, i) for i in range(2)]
        wait_state(lambda s: inflight_of(s, target) == 2, "失败执行的两个调用者")
        for t in fts:
            t.join(20)
        fres = [out_f[i][1] for i in range(2)]
        check("注入失败后两个调用者都拿到失败结果",
              all(r.get("ok") is False for r in fres), json.dumps(fres))
        check("失败请求共享同一次执行",
              len({r["coalescing"]["execution_id"] for r in fres}) == 1)
        check("上游只执行一次（失败）", upstream_exec_count(target) == up_e + 1)
        c1 = state()["coalescing"]["stats"]
        check("网关记录一次失败执行",
              c1["failed_executions"] - c0["failed_executions"] == 1)
        stf = wait_state(lambda s: inflight_of(s, target) == 0, "失败执行清理在途")
        check("失败后合并组已清除",
              not any(g["target"] == target and g["delay_ms"] == 500
                      for g in stf["coalescing"]["active_groups"]))
    finally:
        api("POST", f"/api/upstreams/{target}/work_mode", {"fail": False})

    s, ok_res = api("POST", "/api/call", {"delay_ms": 300})
    check("失败清除后同一参数的后续请求可重新执行成功",
          ok_res.get("ok") is True and ok_res["upstream"].get("delay_ms") == 300,
          json.dumps(ok_res))
    check("后续请求是新执行（不是失败执行）",
          ok_res["coalescing"]["execution_id"]
          not in {r["coalescing"]["execution_id"] for r in fres}
          and ok_res["coalescing"]["execution_callers"] == 1)
    check("重新执行确实又到达上游", upstream_exec_count(target) == up_e + 2)

    # ---------- F. console/state surface -----------------------------------
    st = state()
    check("状态接口包含合并统计", "client_requests" in st["coalescing"]["stats"])
    with urllib.request.urlopen(BASE + "/", timeout=5) as r:
        html = r.read().decode()
    check("网页展示共享标记", "共享同一执行" in html and "上游实际执行" in html)
    check("网页保留切换操作", "确认切换" in html and "探测候选" in html)
    check("网页可演示注入上游失败", "注入失败" in html)
    _, us = api("GET", "/api/upstream_stats")
    check("上游统计接口可达", us["upstreams"].get(base_target, {}).get("reachable") is True)

    return 0


if __name__ == "__main__":
    try:
        code = main()
    except Exception as exc:  # noqa: BLE001 - report and fail loudly
        import traceback
        traceback.print_exc()
        FAILURES.append(f"unexpected: {exc}")
        code = 1
    if FAILURES:
        print(f"\n{len(FAILURES)} 项检查失败:")
        for f in FAILURES:
            print(" -", f)
        sys.exit(1)
    print("\n全部合并检查通过 (coalescing)")
    sys.exit(0)
