"""notree/worker 뼈대: 결과별 처리, lease 상실, 종료 처리, 동시 실행, permit."""
import asyncio
import dataclasses
import os
import sqlite3
import sys
import time
from contextlib import AsyncExitStack
from pathlib import Path

import fakeredis
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from notree.common import settings as S  # noqa: E402
from notree.scheduler.allocator import run_allocator  # noqa: E402
from notree.worker.loop import Worker  # noqa: E402
from notree.worker.mcp_client import McpClient  # noqa: E402

RT = dataclasses.replace(S.RT, lease_sec=1.0, heartbeat_sec=0.3, claim_idle_ms=1200, job_timeout_sec=5,
                         max_attempts=2, concurrency=3)


class Env:
    def __init__(self, tmp_path, allocator: bool = True):
        self.with_allocator = allocator
        self.db = tmp_path / "skel.db"
        self.env = {**os.environ, "NOTREE_DB": str(self.db), "NOTREE_REQUIRE_PERMIT": "1"}
        self.r = fakeredis.FakeAsyncRedis(server=fakeredis.FakeServer(), decode_responses=True)  # 테스트마다 새 Redis
        self.stop = asyncio.Event()

    async def __aenter__(self):
        self.stack = AsyncExitStack()               # MCP 연결은 연 역순으로 닫아야 한다
        self.api = await self.stack.enter_async_context(McpClient(env=self.env))
        self.m_s = await self.stack.enter_async_context(McpClient(env=self.env))
        self.m_w = await self.stack.enter_async_context(McpClient(env=self.env))
        self.tasks = ([asyncio.create_task(run_allocator(self.r, self.m_s, self.stop, interval=0.1))]
                      if self.with_allocator else [])
        return self

    def start_worker(self, cfg=RT, **kw):
        w = Worker("t", cfg, self.r, self.m_w, block_ms=0, **kw)
        self.tasks.append(asyncio.create_task(w.run(self.stop)))
        return w

    async def job(self, payload, kind="echo"):
        return (await self.api.tool("job_create", kind=kind, payload=payload, user_id="u-test",
                                    stream=RT.stream))["job_id"]

    async def row(self, job_id):
        return await self.api.tool("job_get", job_id=job_id)

    async def wait_state(self, job_id, states=("done", "failed"), timeout=15):
        end = time.time() + timeout
        while time.time() < end:
            if (await self.row(job_id))["state"] in states:
                return True
            await asyncio.sleep(0.1)
        return False

    async def pending(self):
        return (await self.r.xpending(RT.stream, S.GROUP))["pending"]

    async def __aexit__(self, *exc):
        self.stop.set()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        await self.stack.aclose()


def run(coro):
    return asyncio.run(coro)


def test_outcomes(tmp_path):
    async def t():
        async with Env(tmp_path) as e:
            e.start_worker()
            ok = await e.job({"text": "hi"})
            fatal = await e.job({"fail": "fatal"})
            retry = await e.job({"fail": "retry:1"})
            always = await e.job({"fail": "always"})
            missing = await e.job({}, kind="no_such_kind")
            for j in (ok, fatal, retry, always, missing):
                assert await e.wait_state(j)
            assert (await e.row(ok))["state"] == "done"
            assert (await e.row(fatal))["state"] == "failed"
            r = await e.row(retry)
            assert r["state"] == "done" and r["attempts"] == 2
            a = await e.row(always)
            assert a["state"] == "failed" and a["attempts"] == RT.max_attempts and "한도" in a["fail_reason"]
            assert "핸들러 없음" in (await e.row(missing))["fail_reason"]
            await asyncio.sleep(0.2)
            assert await e.pending() == 0
    run(t())


def test_lease_lost_cancels_handler_without_ack(tmp_path, capsys):
    async def t():
        async with Env(tmp_path) as e:
            e.start_worker()
            j = await e.job({"text": "should not print", "sleep": 3})
            assert await e.wait_state(j, ("running",))
            # 다른 worker가 lease를 가져간 상황을 DB에서 직접 만든다
            con = sqlite3.connect(e.db)
            con.execute("UPDATE jobs SET lease_owner='intruder', lease_until=? WHERE job_id=?", (time.time() + 60, j))
            con.commit(); con.close()
            await asyncio.sleep(1.0)
            row = await e.row(j)
            assert row["state"] == "running" and row["lease_owner"] == "intruder"
            assert await e.pending() == 1                      # ACK 안 함
    run(t())
    out = capsys.readouterr().out
    assert "lease를 잃음" in out and "should not print" not in out


def test_graceful_shutdown_waits_for_running_job(tmp_path):
    async def t():
        async with Env(tmp_path) as e:
            e.start_worker()
            j = await e.job({"sleep": 1.0})
            assert await e.wait_state(j, ("running",))
            e.stop.set()
            await asyncio.gather(*e.tasks)
            assert (await e.row(j))["state"] == "done"
            assert await e.pending() == 0
    run(t())


def test_shutdown_grace_exceeded_releases_lease(tmp_path):
    async def t():
        async with Env(tmp_path) as e:
            e.start_worker(shutdown_grace_sec=0.2)
            j = await e.job({"sleep": 5})
            assert await e.wait_state(j, ("running",))
            e.stop.set()
            await asyncio.gather(*e.tasks)
            row = await e.row(j)
            assert row["state"] == "sent" and row["lease_until"] is None   # 반납 → 다른 worker가 곧 잡을 수 있음
            assert await e.pending() == 1
    run(t())


def test_concurrency_runs_jobs_in_parallel(tmp_path):
    async def t():
        async with Env(tmp_path) as e:
            e.start_worker()                                   # 동시 3개
            jobs = [await e.job({"sleep": 1.0}) for _ in range(3)]
            t0 = time.time()
            for j in jobs:
                assert await e.wait_state(j)
            assert time.time() - t0 < 2.5                      # 순서대로였다면 3초 이상
    run(t())


def test_forged_permit_is_rejected(tmp_path):
    async def t():
        async with Env(tmp_path) as e:
            j = await e.job({})
            await e.api.tool("job_reserve_pending")
            st = await e.api.tool("job_start", job_id=j, worker="w", lease_sec=5)
            assert st["status"] == "started"
            bad = st["permit"][:-4] + "0000"
            forged = await e.api.call("job_finish", job_id=j, worker="w", permit=bad)
            assert forged == {"ok": False, "code": "permit_invalid", "message": "permit 서명 불일치",
                              "retryable": False}
            missing = await e.api.call("job_finish", job_id=j, worker="w")
            assert missing["code"] == "permit_invalid" and missing["message"] == "permit 필요"
            assert (await e.api.tool("job_finish", job_id=j, worker="w", permit=st["permit"]))["finished"] is True
    run(t())
