"""[feat/taskiq] 직접 만든 루프(loop) vs taskiq 버전(taskiq)을 같은 시나리오로 비교한다.

두 구현 모두 진짜 redis-server(테스트마다 새 포트)에서 돈다. redis-server가 없으면 건너뛴다.
재배송 기준은 두 구현에 같은 값을 준다 (loop: claim_idle_ms, taskiq: idle_timeout).
"""
import asyncio
import dataclasses
import os
import shutil
import socket
import subprocess
import sys
import time
from contextlib import AsyncExitStack
from pathlib import Path

import pytest
import redis.asyncio as aioredis

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from notree.common import settings as S  # noqa: E402
from notree.scheduler.allocator import run_allocator  # noqa: E402
from notree.worker.loop import Worker  # noqa: E402
from notree.worker.mcp_client import McpClient  # noqa: E402
from notree.worker_tq import TqWorker, make_broker, run_allocator_tq  # noqa: E402
from test_robustness import FlakyMcp  # noqa: E402

pytestmark = pytest.mark.skipif(shutil.which("redis-server") is None, reason="redis-server 없음")

IMPLS = ["loop", "taskiq", "taskiq+reclaim"]   # taskiq+reclaim = 회수 방식을 고친 broker
# taskiq-redis 1.2.4 원본 broker는 새 메시지를 받은 뒤에만 방치 메시지를 회수한다 → 재배송에 기대는 시나리오는 실패.
# 알려진 한계로 표시 (strict: 고쳐지면 표시를 지워야 통과)
QUIET_RECLAIM = pytest.mark.xfail(strict=True, reason="taskiq-redis 원본: 조용한 스트림에서 방치 메시지 회수 안 함")
IMPLS_NEED_RECLAIM = ["loop", pytest.param("taskiq", marks=QUIET_RECLAIM), "taskiq+reclaim"]
RT = dataclasses.replace(S.RT, lease_sec=1.0, heartbeat_sec=0.3, claim_idle_ms=1200, job_timeout_sec=5,
                         max_attempts=2, concurrency=3)


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Env:
    def __init__(self, tmp_path, impl: str, cfg=RT):
        self.impl, self.cfg = impl, cfg
        self.db = tmp_path / "cmp.db"
        self.env = {**os.environ, "NOTREE_DB": str(self.db), "NOTREE_REQUIRE_PERMIT": "1"}
        self.port = _free_port()
        self.url = f"redis://127.0.0.1:{self.port}/0"
        self.stop = asyncio.Event()
        self.workers: dict[str, tuple] = {}

    async def __aenter__(self):
        self.proc = subprocess.Popen(["redis-server", "--port", str(self.port), "--save", "", "--appendonly", "no"],
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.r = aioredis.from_url(self.url, decode_responses=True)
        for _ in range(50):
            try:
                await self.r.ping()
                break
            except Exception:
                await asyncio.sleep(0.05)
        self.stack = AsyncExitStack()
        self.api = await self.stack.enter_async_context(McpClient(env=self.env))
        self.m_s = await self.stack.enter_async_context(McpClient(env=self.env))
        self.m_w = await self.stack.enter_async_context(McpClient(env=self.env))
        if self.impl == "loop":
            alloc = run_allocator(self.r, self.m_s, self.stop, interval=0.1)
        else:
            self.kick_broker = make_broker(self.cfg, self.url)
            alloc = run_allocator_tq({self.cfg.stream: self.kick_broker}, self.m_s, self.stop, interval=0.1)
        self.tasks = [asyncio.create_task(alloc)]
        return self

    def start_worker(self, name="A", mcp=None, stop=None, **kw):
        stop = stop or self.stop
        if self.impl == "loop":
            w = Worker(name, self.cfg, self.r, mcp or self.m_w, block_ms=200, **kw)
        else:
            w = TqWorker(name, self.cfg, self.url, mcp or self.m_w, idle_ms=self.cfg.claim_idle_ms, block_ms=200,
                         reclaim=self.impl == "taskiq+reclaim", **kw)
        t = asyncio.create_task(w.run(stop))
        self.tasks.append(t)
        self.workers[name] = (w, t)
        return w, t

    async def job(self, payload, kind="echo"):
        return (await self.api.tool("job_create", kind=kind, payload=payload, user_id="u-t",
                                    stream=self.cfg.stream))["job_id"]

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
        try:
            return (await self.r.xpending(self.cfg.stream, S.GROUP))["pending"]
        except Exception:
            return 0

    async def __aexit__(self, *exc):
        self.stop.set()
        await asyncio.wait(self.tasks, timeout=10)
        for t in self.tasks:
            t.cancel()
        await self.stack.aclose()
        if self.impl != "loop":
            await self.kick_broker.shutdown()
        await self.r.aclose()
        self.proc.terminate()
        self.proc.wait(timeout=5)


def run(coro):
    return asyncio.run(coro)


@pytest.mark.parametrize("impl", IMPLS_NEED_RECLAIM)
def test_outcomes(tmp_path, impl):
    """성공 / 영구 실패 / 1번 실패 후 성공 / 한도 초과 / 핸들러 없음."""
    async def t():
        async with Env(tmp_path, impl) as e:
            e.start_worker()
            ok, fatal = await e.job({"text": "hi"}), await e.job({"fail": "fatal"})
            retry, always = await e.job({"fail": "retry:1"}), await e.job({"fail": "always"})
            missing = await e.job({}, kind="no_such_kind")
            t0 = time.time()
            for j in (ok, fatal, retry, always, missing):
                assert await e.wait_state(j, timeout=20), f"{j} 끝나지 않음: {await e.row(j)}"
            elapsed = time.time() - t0
            assert (await e.row(ok))["state"] == "done"
            assert (await e.row(fatal))["state"] == "failed"
            r = await e.row(retry)
            assert r["state"] == "done" and r["attempts"] == 2
            a = await e.row(always)
            assert a["state"] == "failed" and a["attempts"] == RT.max_attempts
            assert "핸들러 없음" in (await e.row(missing))["fail_reason"]
            await asyncio.sleep(0.3)
            assert await e.pending() == 0
            print(f"[{impl}] 결과 5종 처리 {elapsed:.1f}초")
    run(t())


@pytest.mark.parametrize("impl", IMPLS)
def test_long_job_runs_once(tmp_path, impl, capsys):
    """재배송 기준(1.2초)보다 오래 걸리는 작업(3초) + worker 2개 → 한 번만 실행."""
    async def t():
        async with Env(tmp_path, impl) as e:
            e.start_worker("A"); e.start_worker("B")
            j = await e.job({"text": "only-once", "sleep": 3})
            assert await e.wait_state(j, ("done",), timeout=15)
            await asyncio.sleep(0.5)
            assert (await e.row(j))["attempts"] == 1
            assert await e.pending() == 0
    run(t())
    out = capsys.readouterr().out
    assert out.count("'only-once'") == 1, "핸들러가 두 번 실행됨"
    print(f"[{impl}] 'leased'로 건너뛴 횟수: {out.count('lease 보유')}")


@pytest.mark.parametrize("impl", IMPLS_NEED_RECLAIM)
def test_dead_worker_job_is_taken_over_on_quiet_stream(tmp_path, impl):
    """A가 실행 중 죽고, 그 뒤로 새 작업이 하나도 안 들어올 때 B가 이어받는가."""
    async def t():
        async with Env(tmp_path, impl) as e:
            stop_a = asyncio.Event()
            e.start_worker("A", stop=stop_a)
            j = await e.job({"text": "survivor", "die_on": f"{RT.key}-A", "sleep": 0.3})
            assert await e.wait_state(j, ("running",))
            w_a, t_a = e.workers["A"]
            assert await asyncio.wait_for(t_a, 5) == "died"
            t0 = time.time()
            e.start_worker("B")
            done = await e.wait_state(j, ("done",), timeout=12)
            print(f"[{impl}] 조용한 상태에서 인계: {'%.1f초' % (time.time() - t0) if done else '12초 안에 안 됨'}")
            assert done, f"B가 이어받지 못함: {await e.row(j)} pending={await e.pending()}"
    run(t())


@pytest.mark.parametrize("impl", IMPLS)
def test_dead_worker_job_is_taken_over_when_traffic_arrives(tmp_path, impl):
    """같은 상황에서 새 작업이 하나 들어오면 이어받는가 (taskiq는 새 메시지를 받은 뒤에만 회수한다)."""
    async def t():
        async with Env(tmp_path, impl) as e:
            stop_a = asyncio.Event()
            e.start_worker("A", stop=stop_a)
            j = await e.job({"text": "survivor", "die_on": f"{RT.key}-A", "sleep": 0.3})
            assert await e.wait_state(j, ("running",))
            assert await asyncio.wait_for(e.workers["A"][1], 5) == "died"
            e.start_worker("B")
            await asyncio.sleep(2.0)                     # 재배송 기준(1.2초)이 지난 뒤 새 작업
            t0 = time.time()
            await e.job({"text": "traffic"})
            done = await e.wait_state(j, ("done",), timeout=12)
            print(f"[{impl}] 새 작업 도착 후 인계: {'%.1f초' % (time.time() - t0) if done else '안 됨'}")
            assert done
    run(t())


@pytest.mark.parametrize("impl", IMPLS_NEED_RECLAIM)
def test_finish_failure_does_not_lose_the_job(tmp_path, impl):
    """job_finish가 4번 연속 실패(확정 시도 3번을 넘김) → ACK 안 함 → 재배송으로 다시 처리되어 done."""
    async def t():
        async with Env(tmp_path, impl) as e:
            e.start_worker("A", mcp=FlakyMcp(e.m_w, {"job_finish": 4}))
            j = await e.job({"text": "finish-me"})
            done = await e.wait_state(j, ("done",), timeout=15)
            print(f"[{impl}] 완료 기록 장애 후: {(await e.row(j))['state']}, pending={await e.pending()}")
            assert done and await e.pending() == 0
    run(t())


@pytest.mark.parametrize("impl", IMPLS)
def test_graceful_shutdown_waits_for_running_job(tmp_path, impl):
    async def t():
        async with Env(tmp_path, impl) as e:
            stop_a = asyncio.Event()
            _, task = e.start_worker("A", stop=stop_a, shutdown_grace_sec=5)
            j = await e.job({"sleep": 1.0})
            assert await e.wait_state(j, ("running",))
            stop_a.set()
            await asyncio.wait_for(task, 10)
            assert (await e.row(j))["state"] == "done"
            assert await e.pending() == 0
    run(t())


@pytest.mark.parametrize("impl", IMPLS)
def test_shutdown_grace_exceeded_releases_lease_with_full_slots(tmp_path, impl):
    """슬롯이 꽉 찬 상태에서 종료 신호 → 유예 0.3초 뒤 lease 반납, 메시지는 남음."""
    cfg = dataclasses.replace(RT, concurrency=1, job_timeout_sec=30)

    async def t():
        async with Env(tmp_path, impl, cfg=cfg) as e:
            stop_a = asyncio.Event()
            _, task = e.start_worker("A", stop=stop_a, shutdown_grace_sec=0.3)
            j = await e.job({"sleep": 10})
            await e.job({"sleep": 10})                         # 슬롯이 꽉 찬 상태로 하나 더 대기
            assert await e.wait_state(j, ("running",))
            t0 = time.time()
            stop_a.set()
            await asyncio.wait_for(task, 8)
            took = time.time() - t0
            row = await e.row(j)
            print(f"[{impl}] 종료까지 {took:.1f}초")
            assert took < 3
            assert row["state"] == "sent" and row["lease_until"] is None
            assert await e.pending() >= 1
    run(t())


@pytest.mark.parametrize("impl", IMPLS)
def test_needless_reclaims_of_running_job(tmp_path, impl, capfd):
    """재배송 기준(1.2초)보다 긴 작업(4.5초)을 실행하는 동안, 살아 있는 작업을 다른 worker나 자신이 다시 가져가는 횟수.
    loop는 heartbeat마다 방치 시간을 초기화하므로 0. taskiq는 메시지 ID를 작업에 주지 않아 초기화할 수 없다."""
    async def t():
        async with Env(tmp_path, impl) as e:
            e.start_worker("A"); e.start_worker("B")
            j = await e.job({"text": "long", "sleep": 4.5})
            assert await e.wait_state(j, ("done",), timeout=20)
            assert (await e.row(j))["attempts"] == 1
    run(t())
    out = capfd.readouterr().out
    skipped = out.count("lease 보유") + out.count("이미 실행 중")
    assert out.count("'long'") == 1
    if impl == "loop":
        assert skipped == 0
    with capfd.disabled():
        print(f"\n[{impl}] 4.5초 작업 중 불필요한 재회수 {skipped}번")
