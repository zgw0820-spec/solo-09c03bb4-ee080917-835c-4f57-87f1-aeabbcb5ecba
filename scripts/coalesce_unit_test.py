#!/usr/bin/env python3
"""Unit-level checks for gateway/app/coalesce.py.

Exercises the parts that are hard to provoke over HTTP:
  * a cancelled waiter does not cancel the shared upstream execution, and the
    remaining waiter still receives the full result;
  * a failed execution (exception) is unregistered, so the next caller starts
    a fresh execution instead of joining a finished one;
  * every caller has its own in-flight row, all released together.

Run: python3 scripts/coalesce_unit_test.py
"""
import asyncio
import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "gateway"))

os.environ["GATEWAY_DB"] = os.path.join(tempfile.mkdtemp(prefix="coalesce-unit-"), "gw.db")

from app import coalesce, db  # noqa: E402

FAILURES = []


def check(name, cond, extra=""):
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f"  ({extra})" if extra and not cond else ""))
    if not cond:
        FAILURES.append(name)


async def scenario_cancel():
    db.init_db({"alpha": "http://alpha"}, "alpha")
    calls = {"n": 0}
    gate = asyncio.Event()
    results = {}

    async def fetch():
        calls["n"] += 1
        await gate.wait()
        return {"status": 200, "body": {"upstream": "alpha"}}

    key = (0, "alpha", "/work", 1000)
    a = db.admit("/work")
    g1, created1 = coalesce.join(key, target="alpha", route_version=0, path="/work",
                                 delay_ms=1000, request_id=a["id"], fetch=fetch)
    b = db.admit("/work")
    g2, created2 = coalesce.join(key, target="alpha", route_version=0, path="/work",
                                 delay_ms=1000, request_id=b["id"], fetch=fetch)

    check("第一个调用者创建执行", created1 is True)
    check("第二个调用者加入同一执行", created2 is False and g2 is g1)
    check("两个调用者各占 1 条在途", len(db.get_state()["inflight"]) == 2)
    await asyncio.sleep(0)  # let the single scheduled task start
    check("上游只被调度一次", calls["n"] == 1)

    async def waiter(admission, group, tag):
        try:
            results[tag] = await asyncio.shield(group.task)
        except asyncio.CancelledError:
            results[tag] = "cancelled"

    wa = asyncio.create_task(waiter(a, g1, "a"))
    wb = asyncio.create_task(waiter(b, g1, "b"))
    await asyncio.sleep(0.05)
    wa.cancel()                      # simulate one browser going away
    await asyncio.sleep(0.05)
    gate.set()
    await wb

    check("被取消的调用者不影响共享执行", results.get("b", {}).get("status") == 200,
          str(results))
    check("共享执行没有被取消", calls["n"] == 1 and g1.task.done()
          and not g1.task.cancelled())
    await asyncio.sleep(0.05)
    st = db.get_state()
    check("执行结束后所有在途记录都被释放", st["inflight"] == [], str(st["inflight"]))
    check("执行结束后合并组被移除",
          all(g["execution_id"] != g1.execution_id
              for g in coalesce.snapshot()["active_groups"]))


async def scenario_failure():
    db.init_db({"alpha": "http://alpha"}, "alpha")
    calls = {"n": 0}

    async def failing_fetch():
        calls["n"] += 1
        await asyncio.sleep(0.02)
        raise RuntimeError("boom")

    key = (0, "alpha", "/work", 2000)
    ids = []
    tasks = []

    async def waiter(i):
        adm = db.admit("/work")
        ids.append(adm["id"])
        group, _ = coalesce.join(key, target="alpha", route_version=0, path="/work",
                                 delay_ms=2000, request_id=adm["id"], fetch=failing_fetch)
        try:
            await asyncio.shield(group.task)
        except RuntimeError:
            return "failed"
        return "unexpected"

    tasks = [asyncio.create_task(waiter(i)) for i in range(3)]
    outcomes = await asyncio.gather(*tasks)
    check("失败执行的所有调用者都收到错误", outcomes == ["failed"] * 3, str(outcomes))
    check("失败执行只调度上游一次", calls["n"] == 1, str(calls))
    check("失败后合并组立即清除",
          coalesce.snapshot()["active_groups"] == [],
          str(coalesce.snapshot()["active_groups"]))
    recent = coalesce.snapshot()["recent_groups"]
    check("最近执行记录了失败", recent and recent[-1]["ok"] is False
          and recent[-1]["callers"] == 3, str(recent[-1:]))

    # a later caller with the same identity executes again
    adm = db.admit("/work")
    ran = {"n": 0}

    async def ok_fetch():
        ran["n"] += 1
        return {"status": 200, "body": {"upstream": "alpha"}}

    group, created = coalesce.join(key, target="alpha", route_version=0, path="/work",
                                   delay_ms=2000, request_id=adm["id"], fetch=ok_fetch)
    await asyncio.shield(group.task)
    await asyncio.sleep(0.02)
    check("失败清除后同键请求重新执行", created is True and ran["n"] == 1)
    check("所有调用者的失败在途记录都已释放", db.get_state()["inflight"] == [])


async def main():
    await scenario_cancel()
    await scenario_failure()


if __name__ == "__main__":
    asyncio.run(main())
    if FAILURES:
        print(f"\n{len(FAILURES)} 项检查失败:")
        for f in FAILURES:
            print(" -", f)
        sys.exit(1)
    print("\n全部合并单元检查通过")
