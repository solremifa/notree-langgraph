"""Redis 소유 키 + 원자적 heartbeat (NOTREE_LIVENESS=redis). **실제 redis-server**로 돈다 (fakeredis는 Lua를 못 돌림).
redis-server가 없으면 건너뛴다.

  1. Lua 규칙: 토큰이 같고, 원본 메시지가 PEL에 내 것으로 있고, Stream에 실제로 있을 때만 갱신. 해제도 토큰 비교
  2. worker 통합 (한 프로세스): 결과 처리, 죽은 worker 인계·이어서 실행, 느리지만 살아 있는 작업 보호,
     heartbeat가 회수 기준보다 늦어진 경우(갱신 지연), DB heartbeat 호출 0번
  3. 별도 프로세스 SIGKILL: MCP 서버(HTTP)·worker를 따로 띄우고 worker를 강제 종료
"""
import asyncio
import dataclasses
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest
import redis.asyncio as aioredis

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from notree.common import settings as S  # noqa: E402
from notree.graphs.course_update import llm  # noqa: E402
from notree.worker import ownership as O  # noqa: E402
from notree.worker.mcp_client import McpClient  # noqa: E402
from test_skeleton import Env  # noqa: E402

pytestmark = pytest.mark.skipif(shutil.which("redis-server") is None, reason="redis-server 없음")

# heartbeat 0.3 < 소유 TTL 1.0 < 회수 기준 1.2 (settings의 규칙과 같은 순서, 시간만 줄임)
RT = dataclasses.replace(S.RT, lease_sec=1.0, heartbeat_sec=0.3, claim_idle_ms=1200, job_timeout_sec=20)
BATCH = dataclasses.replace(S.BATCH, lease_sec=1.0, heartbeat_sec=0.3, claim_idle_ms=1200, job_timeout_sec=20)


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def redis_url():
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


@pytest.fixture
def redis_mode(monkeypatch):
    monkeypatch.setattr(S, "LIVENESS", "redis")


async def _client(url):
    r = aioredis.from_url(url, decode_responses=True)
    await r.flushall()
    return r


async def _one_message(r, cfg=RT, consumer="rt-A"):
    await r.xgroup_create(cfg.stream, S.GROUP, id="0", mkstream=True)
    await r.xadd(cfg.stream, {"job_id": "job-1"})
    res = await r.xreadgroup(S.GROUP, consumer, {cfg.stream: ">"}, count=1)
    return res[0][1][0][0]


# ── 1. Lua 규칙 ────────────────────────────────────────────────────
def test_heartbeat_extends_only_for_current_owner(redis_url):
    async def t():
        r = await _client(redis_url)
        m1 = await _one_message(r)
        a = O.Ownership(r, "job-1", "rt-A", RT, m1)
        b = O.Ownership(r, "job-1", "rt-B", RT, m1)
        assert await a.acquire() and not await b.acquire()
        assert b.holder.startswith("rt-A:")
        await asyncio.sleep(0.4)
        assert await a.heartbeat() == 1
        assert await r.pttl(a.key) > 900                         # 키 연장됨
        assert (await r.xpending_range(RT.stream, S.GROUP, m1, m1, 1))[0]["time_since_delivered"] < 100   # 방치 시간 0
        assert not await b.release() and await r.exists(a.key)   # 남의 토큰으로는 못 지움
        assert await a.release() and not await r.exists(a.key)
        await r.aclose()
    asyncio.run(t())


def test_expired_owner_cannot_renew_or_delete_new_owners_key(redis_url):
    async def t():
        r = await _client(redis_url)
        m1 = await _one_message(r)
        short = dataclasses.replace(RT, lease_sec=0.3)
        a = O.Ownership(r, "job-1", "rt-A", short, m1)
        assert await a.acquire()
        await asyncio.sleep(0.4)                                   # A의 키 만료 (A는 멈춰 있었음)
        await r.xautoclaim(RT.stream, S.GROUP, "rt-B", min_idle_time=0, start_id="0-0", count=1)
        b = O.Ownership(r, "job-1", "rt-B", RT, m1)
        assert await b.acquire()
        assert await a.heartbeat() == 0                            # 토큰 불일치
        assert not await a.release()
        assert (await r.get(a.key)) == b.value
        await r.aclose()
    asyncio.run(t())


def test_heartbeat_does_not_steal_back_a_reclaimed_message(redis_url):
    """XCLAIM은 소유권을 옮기는 명령이라, 검사 없이 부르면 다른 worker가 회수한 메시지를 되찾아 온다."""
    async def t():
        r = await _client(redis_url)
        m1 = await _one_message(r)
        a = O.Ownership(r, "job-1", "rt-A", RT, m1)
        assert await a.acquire()
        await r.pexpire(a.key, 500)
        await r.xautoclaim(RT.stream, S.GROUP, "rt-B", min_idle_time=0, start_id="0-0", count=1)  # B가 회수
        assert await a.heartbeat() == -1
        assert (await r.xpending_range(RT.stream, S.GROUP, m1, m1, 1))[0]["consumer"] == "rt-B"   # 그대로 B
        assert await r.pttl(a.key) <= 500                          # 키도 연장 안 함
        await r.aclose()
    asyncio.run(t())


def test_heartbeat_fails_when_original_message_is_gone(redis_url):
    """원본 메시지가 Stream에서 지워졌으면(잘라내기·XDEL) 키만 연장하지 않는다."""
    async def t():
        r = await _client(redis_url)
        m1 = await _one_message(r)
        a = O.Ownership(r, "job-1", "rt-A", RT, m1)
        assert await a.acquire()
        await r.pexpire(a.key, 500)
        await r.xdel(RT.stream, m1)
        assert await a.heartbeat() == -2
        assert await r.pttl(a.key) <= 500
        await r.aclose()
    asyncio.run(t())


# ── 2. worker 통합 (한 프로세스, 실제 Redis) ─────────────────────────
class RealEnv(Env):
    def __init__(self, tmp_path, url):
        super().__init__(tmp_path)
        self.r = aioredis.from_url(url, decode_responses=True)

    async def __aenter__(self):
        await self.r.flushall()
        return await super().__aenter__()

    async def __aexit__(self, *exc):
        await super().__aexit__(*exc)
        await self.r.aclose()

    async def own_keys(self):
        return [k async for k in self.r.scan_iter("notree:own:*")]

    async def stats(self, job_id):
        return (await self.api.tool("tool_stats", thread_id=job_id))["counts"]


def test_outcomes_without_db_heartbeat(tmp_path, redis_url, redis_mode):
    async def t():
        async with RealEnv(tmp_path, redis_url) as e:
            e.start_worker(cfg=RT)
            ok = await e.job({"text": "hi", "sleep": 1.0})        # heartbeat 3번 이상 도는 길이
            fatal = await e.job({"fail": "fatal"})
            retry = await e.job({"fail": "retry:1"})
            always = await e.job({"fail": "always"})
            for j in (ok, fatal, retry, always):
                assert await e.wait_state(j, timeout=20)
            assert (await e.row(ok))["state"] == "done"
            assert (await e.row(fatal))["state"] == "failed"
            r = await e.row(retry)
            assert r["state"] == "done" and r["attempts"] == 2      # 반납 → 회수 기준 뒤 재배송 → 키 다시 획득
            assert (await e.row(always))["state"] == "failed"
            assert (await e.stats(ok)).get("job_heartbeat", 0) == 0  # DB heartbeat 없음
            await asyncio.sleep(0.2)
            assert await e.pending() == 0 and await e.own_keys() == []
    asyncio.run(t())


async def _takeover(tmp_path, url, payload, *, slow=False, monkeypatch=None):
    llm.CALLS.clear()
    async with RealEnv(tmp_path, url) as e:
        async with McpClient(env=e.env) as m_b:
            a = e.start_worker(cfg=BATCH, name="A")
            await asyncio.sleep(0.3)
            if slow:
                monkeypatch.setattr(llm, "FAKE_DELAY", 1.6)     # LLM 호출 하나가 회수 기준(1.2초)보다 김
            j = (await e.api.tool("job_create", kind="course_update", payload=payload, user_id="u-t"))["job_id"]
            assert await e.wait_state(j, ("running", "done"))
            b = e.start_worker(cfg=BATCH, name="B", mcp=m_b)
            assert await e.wait_state(j, ("done",), timeout=20)
            row, note = await e.row(j), await e.api.tool("note_get", idem_key=j)
            await asyncio.sleep(0.3)
            pending = (await e.r.xpending(BATCH.stream, S.GROUP))["pending"]
            keys, stats = await e.own_keys(), await e.stats(j)
            e.stop.set()
            await asyncio.gather(*e.tasks)
    return a, b, row, note, pending, keys, stats


def test_dead_worker_is_taken_over_and_resumed(tmp_path, redis_url, redis_mode):
    a, b, row, note, pending, keys, stats = asyncio.run(
        _takeover(tmp_path, redis_url, {"sources": ["hello", "big", "world"], "die_at": "extract:world"}))
    assert a.dead and not b.dead
    assert row["attempts"] == 2 and row["lease_owner"] == "batch-B" and row["exec_epoch"] == 2
    assert note["note"] == "HELLO BIG WORLD!" and pending == 0 and keys == []
    assert sorted(llm.CALLS) == ["extract:big", "extract:hello", "extract:world", "merge#1"]   # 끝낸 단계는 다시 안 함
    assert stats.get("job_heartbeat", 0) == 0


def test_slow_but_alive_worker_is_not_robbed(tmp_path, redis_url, redis_mode, monkeypatch):
    a, b, row, _, pending, keys, _ = asyncio.run(
        _takeover(tmp_path, redis_url, {"sources": ["hello", "big", "world"]}, slow=True, monkeypatch=monkeypatch))
    assert not a.dead and pending == 0 and keys == []
    assert row["attempts"] == 1 and row["lease_owner"] == "batch-A"
    assert llm.CALLS.count("extract:hello") == 1


def test_late_heartbeat_hands_over_and_old_runner_stops(tmp_path, redis_url, redis_mode, monkeypatch):
    """갱신 지연: A의 heartbeat 하나만 2초 늦게 실행됨 (TTL 1초·회수 기준 1.2초보다 김).
    그 사이 키가 만료되고 메시지가 회수돼 2번째 시도가 실행 세대를 올린다. 첫 시도는 늦은 heartbeat에서
    소유권을 잃은 걸 알고 멈춘다 (완료 저장 안 함). 2번째 시도는 A나 B 누구든 될 수 있다 —
    A의 메시지 받기 루프는 살아 있어서 먼저 회수할 수 있다. 프로세스 전체가 멈추는 경우는 SIGKILL 시험이 본다."""
    orig = O.Ownership.heartbeat
    stalled = []

    async def slow_beat(self):
        if self.consumer == "rt-A" and not stalled:
            stalled.append(time.monotonic())
            await asyncio.sleep(2.0)
        return await orig(self)

    monkeypatch.setattr(O.Ownership, "heartbeat", slow_beat)

    async def t():
        async with RealEnv(tmp_path, redis_url) as e:
            async with McpClient(env=e.env) as m_b:
                a = e.start_worker(cfg=RT, name="A")
                j = await e.job({"text": "long", "sleep": 3.0})
                assert await e.wait_state(j, ("running",))
                b = e.start_worker(cfg=RT, name="B", mcp=m_b)
                assert await e.wait_state(j, ("done",), timeout=20)
                row = await e.row(j)
                await asyncio.sleep(0.3)
                assert row["attempts"] == 2 and row["exec_epoch"] == 2
                assert stalled                                         # 지연이 실제로 일어남
                stats = await e.stats(j)
                assert stats.get("job_finish", 0) == 1                 # 첫 시도는 완료를 시도하지 않음
                assert await e.pending() == 0 and await e.own_keys() == []
                assert not a.dead and not b.dead
    asyncio.run(t())


# ── 3. 별도 프로세스: SIGKILL(죽음)·SIGSTOP(프로세스 전체 정지 후 재개) ─────
def _proc_scenario(tmp_path, redis_url, action: str, sleep: float):
    """MCP 서버(HTTP)·배정기·worker A를 별도 프로세스로 띄우고, A가 작업을 잡으면 action을 건 뒤 worker B를 띄운다.
    action = "kill": SIGKILL / "stop": SIGSTOP → B가 이어받아 실행 중이 되면 SIGCONT로 A를 깨움."""
    port = _free_port()
    env = {**os.environ, "NOTREE_DB": str(tmp_path / "proc.db"), "NOTREE_MCP_PORT": str(port),
           "NOTREE_MCP_URL": f"http://127.0.0.1:{port}/mcp", "NOTREE_REDIS_URL": redis_url,
           "NOTREE_LIVENESS": "redis", "NOTREE_REQUIRE_PERMIT": "1",
           "NOTREE_RT_LEASE_SEC": "1.0", "NOTREE_RT_HEARTBEAT_SEC": "0.3", "NOTREE_RT_CLAIM_IDLE_SEC": "1.2",
           "NOTREE_ALLOC_INTERVAL_SEC": "0.1", "PYTHONPATH": str(ROOT)}
    procs: dict[str, subprocess.Popen] = {}

    def spawn(name, *args):
        procs[name] = subprocess.Popen([sys.executable, "-m", *args], env=env, cwd=ROOT,
                                       stdout=open(tmp_path / f"{name}.log", "w"), stderr=subprocess.STDOUT)

    async def t():
        r = await _client(redis_url)
        spawn("mcp", "notree.mcp_server", "--http")
        for _ in range(100):
            try:
                socket.create_connection(("127.0.0.1", port), timeout=0.1).close()
                break
            except OSError:
                await asyncio.sleep(0.1)
        spawn("sched", "notree.scheduler.allocator")
        spawn("A", "notree.worker", "--stream", "rt", "--name", "A")
        async with McpClient(url=env["NOTREE_MCP_URL"]) as api:
            j = (await api.tool("job_create", kind="echo", payload={"text": "x", "sleep": sleep}, user_id="u",
                                stream="notree:rt"))["job_id"]

            async def wait(pred, timeout):
                end = time.time() + timeout
                while time.time() < end:
                    row = await api.tool("job_get", job_id=j)
                    if pred(row):
                        return row
                    await asyncio.sleep(0.1)
                raise AssertionError(f"시간 초과: {row}")

            await wait(lambda x: x["state"] == "running" and x["lease_owner"] == "rt-A", 20)
            await asyncio.sleep(1.0)                                   # heartbeat 몇 번 돈 뒤
            if action == "kill":
                procs["A"].send_signal(signal.SIGKILL)
                procs["A"].wait(5)
            else:
                procs["A"].send_signal(signal.SIGSTOP)
            hit = time.time()
            spawn("B", "notree.worker", "--stream", "rt", "--name", "B")
            await wait(lambda x: x["state"] == "running" and x["lease_owner"] == "rt-B", 20)
            took = time.time() - hit
            if action == "stop":
                procs["A"].send_signal(signal.SIGCONT)                 # A가 깨어남: 늦은 heartbeat → 소유권 없음
            row = await wait(lambda x: x["state"] == "done", 30)
            await asyncio.sleep(1.0)                                   # 깨어난 A가 할 일을 다 하도록
            stats = (await api.tool("tool_stats", thread_id=j))["counts"]
        pending = (await r.xpending("notree:rt", S.GROUP))["pending"]
        keys = [k async for k in r.scan_iter("notree:own:*")]
        await r.aclose()
        return row, took, stats, pending, keys

    try:
        out = asyncio.run(t())
    finally:
        for p in procs.values():
            if p.poll() is None:
                p.send_signal(signal.SIGCONT)
                p.terminate()
        for p in procs.values():
            try:
                p.wait(5)
            except subprocess.TimeoutExpired:
                p.kill()
    return (*out, (tmp_path / "A.log").read_text())


def test_sigkill_worker_process_is_taken_over(tmp_path, redis_url):
    row, took, stats, pending, keys, _ = _proc_scenario(tmp_path, redis_url, "kill", sleep=6)
    print(f"\nSIGKILL 뒤 B가 이어받기까지 {took:.2f}초 (회수 기준 1.2초 + B 기동)")
    assert row["attempts"] == 2 and row["lease_owner"] == "rt-B" and row["exec_epoch"] == 2
    assert stats.get("job_heartbeat", 0) == 0 and pending == 0 and keys == []
    assert took < 10


def test_stopped_process_resumes_without_overwriting(tmp_path, redis_url):
    """SIGSTOP으로 A 프로세스 전체를 멈춤(긴 GC·스왑·VM 정지 흉내) → B가 이어받음 → A를 깨움.
    깨어난 A는 늦은 heartbeat에서 소유권을 잃었음을 알고 멈추거나, 먼저 저장을 시도해도 실행 세대가 달라 거절된다."""
    row, took, stats, pending, keys, a_log = _proc_scenario(tmp_path, redis_url, "stop", sleep=8)
    print(f"\nSIGSTOP 뒤 B가 이어받기까지 {took:.2f}초")
    assert row["attempts"] == 2 and row["lease_owner"] == "rt-B" and row["exec_epoch"] == 2
    assert "소유권을 잃음" in a_log or "완료 확정 못 받음(실행권 없음)" in a_log
    assert "완료 → XACK" not in a_log                                   # A는 ACK하지 않음
    assert stats.get("job_heartbeat", 0) == 0 and pending == 0 and keys == []
