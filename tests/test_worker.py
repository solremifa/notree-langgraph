"""worker 루프 + Redis Streams + lease 시나리오 (fakeredis, MCP는 stdio 서버 하나에 역할별 연결)."""
import asyncio
import os
import sys
from pathlib import Path

import fakeredis
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import llm  # noqa: E402
import settings as S  # noqa: E402
from api import request_course_update  # noqa: E402
from mcp_saver import McpClient  # noqa: E402
from scheduler import run_allocator  # noqa: E402
from worker import run_worker  # noqa: E402


@pytest.fixture(autouse=True)
def fast_timing(monkeypatch):
    monkeypatch.setattr(S, "LEASE_SEC", 1.0)
    monkeypatch.setattr(S, "HEARTBEAT_SEC", 0.3)
    monkeypatch.setattr(S, "CLAIM_IDLE_MS", 1500)
    llm.CALLS.clear()


async def until(pred, timeout=20):
    end = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < end:
        if await pred():
            return True
        await asyncio.sleep(0.1)
    return False


async def scenario(tmp_path, die_at=None, slow_a=False, monkeypatch=None):
    env = {**os.environ, "NOTREE_DB": str(tmp_path / "w.db")}
    r = fakeredis.FakeAsyncRedis(decode_responses=True)
    stop = asyncio.Event()
    async with McpClient(env=env) as m_api, McpClient(env=env) as m_s, McpClient(env=env) as m_a, \
            McpClient(env=env) as m_b:
        sched = asyncio.create_task(run_allocator(r, m_s, stop, interval=0.1))
        wa = asyncio.create_task(run_worker("A", r, m_a, die_at=die_at, stop=stop, block_ms=0))
        await asyncio.sleep(0.3)
        if slow_a:
            monkeypatch.setattr(llm, "FAKE_DELAY", 1.2)      # extract·merge 하나가 재배송 기준보다 오래 걸림
        job_id = await request_course_update(m_api, ["hello", "big", "world"])
        await until(lambda: _started(m_api, job_id))
        wb = asyncio.create_task(run_worker("B", r, m_b, stop=stop, block_ms=0))
        ok = await until(lambda: _done(m_api, job_id))
        stop.set()
        res = await asyncio.gather(sched, wa, wb, return_exceptions=True)
        job = await m_api.tool("job_get", job_id=job_id)
        note = await m_api.tool("note_get", idem_key=job_id)
        pending = (await r.xpending(S.BATCH_STREAM, S.GROUP))["pending"]
        return ok, job, note, pending, res


async def _started(m, job_id):
    return (await m.tool("job_get", job_id=job_id)).get("state") in ("running", "done")


async def _done(m, job_id):
    return (await m.tool("job_get", job_id=job_id)).get("state") == "done"


def test_dead_worker_job_is_taken_over_and_resumed(tmp_path):
    ok, job, note, pending, res = asyncio.run(scenario(tmp_path, die_at="extract:world"))
    assert ok and res[1] == "died"
    assert job["attempts"] == 2 and job["lease_owner"] == "worker-B"
    assert note["note"] == "HELLO BIG WORLD!"
    assert pending == 0
    # A가 끝낸 hello·big은 B가 다시 부르지 않음
    assert sorted(llm.CALLS) == ["extract:big", "extract:hello", "extract:world", "merge#1"]


def test_publish_crash_is_finished_without_double_publish(tmp_path):
    ok, job, note, pending, res = asyncio.run(scenario(tmp_path, die_at="publish"))
    assert ok and job["attempts"] == 2 and pending == 0
    assert note["note"] == "HELLO BIG WORLD!"
    assert llm.CALLS.count("merge#1") == 1


def test_slow_but_alive_worker_is_not_robbed(tmp_path, monkeypatch):
    ok, job, note, pending, res = asyncio.run(scenario(tmp_path, slow_a=True, monkeypatch=monkeypatch))
    assert ok and pending == 0
    assert job["attempts"] == 1 and job["lease_owner"] == "worker-A"   # B는 재배송을 받아도 lease 때문에 손대지 않음
    assert llm.CALLS.count("extract:hello") == 1
