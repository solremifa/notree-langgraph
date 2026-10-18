"""배정·회수·Redis 유실 복구 (리뷰 2차, 2026-10-18). 실제 redis-server가 필요한 시험은 없으면 건너뛴다.

  1. 배정기가 둘 이상이어도 같은 작업을 두 번 허가하지 않고, 끝난 작업을 되돌리지 않는다
  4. XAUTOCLAIM이 돌려준 다음 탐색 위치를 이어서 쓴다 (앞쪽 PEL 10개 이상이 살아 있어도 뒤의 죽은 메시지를 회수)
  5. Redis가 통째로 사라져도: worker가 consumer group을 다시 만들고, scheduler가 고립된 작업을 다시 보낸다
"""
import asyncio
import dataclasses
import shutil
import socket
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import pytest
import redis.asyncio as aioredis

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from notree.common import settings as S  # noqa: E402
from notree.worker.loop import Worker  # noqa: E402
from notree.worker.mcp_client import McpClient  # noqa: E402
from test_skeleton import Env  # noqa: E402

RT = dataclasses.replace(S.RT, lease_sec=1.0, heartbeat_sec=0.3, claim_idle_ms=1000, job_timeout_sec=20)
real_redis = pytest.mark.skipif(shutil.which("redis-server") is None, reason="redis-server 없음")


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def redis_url():
    if shutil.which("redis-server") is None:
        pytest.skip("redis-server 없음")
    port = _free_port()
    p = subprocess.Popen(["redis-server", "--port", str(port), "--save", "", "--appendonly", "no"],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(50):
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.1).close()
            break
        except OSError:
            time.sleep(0.1)
    yield f"redis://127.0.0.1:{port}/0"
    p.terminate()
    p.wait(5)


class RealEnv(Env):
    def __init__(self, tmp_path, url, **kw):
        super().__init__(tmp_path, **kw)
        self.r = aioredis.from_url(url, decode_responses=True)

    async def __aenter__(self):
        await self.r.flushall()
        return await super().__aenter__()

    async def __aexit__(self, *exc):
        await super().__aexit__(*exc)
        await self.r.aclose()


# ── 1. 배정기 여러 개 ────────────────────────────────────────────
def test_concurrent_allocators_never_reserve_a_job_twice(tmp_path):
    async def t():
        async with Env(tmp_path, allocator=False) as e:
            jobs = [await e.job({"text": str(i)}) for i in range(60)]
            async with McpClient(env=e.env) as m2, McpClient(env=e.env) as m3:
                got = []
                for _ in range(4):
                    res = await asyncio.gather(*[c.tool("job_reserve_pending", limit=7, stream=RT.stream)
                                                 for c in (e.api, m2, m3) for _ in range(3)])
                    got += [j["job_id"] for r in res for j in r["jobs"]]
            assert len(got) == len(set(got))                          # 두 번 허가된 작업 없음
            assert set(got) <= set(jobs)
    asyncio.run(t())


# ── 4. XAUTOCLAIM 탐색 위치 ───────────────────────────────────────
@real_redis
def test_reclaim_reaches_dead_messages_behind_ten_live_ones(redis_url):
    async def t():
        r = aioredis.from_url(redis_url, decode_responses=True)
        await r.flushall()
        await r.xgroup_create(RT.stream, S.GROUP, id="0", mkstream=True)
        ids = [await r.xadd(RT.stream, {"job_id": f"j{i}"}) for i in range(15)]
        await r.xreadgroup(S.GROUP, "rt-dead", {RT.stream: ">"}, count=15)
        await asyncio.sleep(1.2)                                       # 15개 모두 방치 1.2초
        await r.xclaim(RT.stream, S.GROUP, "rt-alive", 0, ids[:12], justid=True)   # 앞 12개는 살아 있음 (방치 0)
        w = Worker("B", RT, r, mcp=None, block_ms=0)
        found = None
        for _ in range(3):
            if m := await w._claim(RT):
                found = m
                break
        await r.aclose()
        assert found is not None and found[1] in {"j12", "j13", "j14"}
    asyncio.run(t())


# ── 5. Redis 통째 유실 ──────────────────────────────────────────
@real_redis
def test_worker_recreates_group_after_redis_wipe(tmp_path, redis_url):
    async def t():
        async with RealEnv(tmp_path, redis_url) as e:
            e.start_worker(cfg=RT)
            j1 = await e.job({"text": "before"})
            assert await e.wait_state(j1, ("done",))
            await e.r.flushall()                                       # Redis 초기화 (그룹·메시지 모두 사라짐)
            j2 = await e.job({"text": "after"})
            assert await e.wait_state(j2, ("done",), timeout=15)
    asyncio.run(t())


@real_redis
def test_orphaned_jobs_are_resent_after_redis_wipe(tmp_path, redis_url):
    """worker도 죽고 Redis도 초기화돼 DB에만 running·sent가 남음 → scheduler가 Redis 유실을 알아채고 다시 보낸다.
    마감이 지난 작업은 다시 보내지 않고 실패 확정. 후속 요약도 막히지 않는다."""
    from notree.scheduler.periodic import recover_after_redis_loss

    async def t():
        async with RealEnv(tmp_path, redis_url, allocator=False) as e:
            await recover_after_redis_loss(e.r, e.api)                 # 첫 실행: 표시 키를 남김
            running = await e.job({"text": "orphan-running"})
            sent = await e.job({"text": "orphan-sent"})
            expired = (await e.api.tool("job_create", kind="qa", payload={"question_id": "q", "deadline_at": time.time() - 1},
                                        user_id="u", stream=RT.stream))["job_id"]
            await e.api.tool("job_reserve_pending", limit=10, stream=RT.stream)
            for j in (running, sent, expired):
                await e.api.tool("job_mark_sent", job_id=j)
            await e.api.tool("job_start", job_id=running, worker="dead", lease_sec=0.2)   # 죽은 worker
            await e.api.tool("job_start", job_id=expired, worker="dead", lease_sec=0.2)
            await asyncio.sleep(0.3)
            await e.r.flushall()
            assert await recover_after_redis_loss(e.r, e.api) == 2     # running·sent 2개 다시 보냄
            assert await recover_after_redis_loss(e.r, e.api) == 0     # 표시 키가 있으니 다시 안 함
            ex = await e.row(expired)
            assert ex["state"] == "failed" and "마감" in ex["fail_reason"]
            e.start_worker(cfg=RT)
            assert await e.wait_state(running, ("done",)) and await e.wait_state(sent, ("done",))
            assert (await e.row(running))["attempts"] == 2
    asyncio.run(t())
