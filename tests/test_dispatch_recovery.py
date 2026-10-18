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

ROOT = Path(__file__).resolve().parents[1]
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


# ── 리뷰 3차: 복구 '발견'과 '완료'를 나눔 ──────────────────────────
@real_redis
def test_job_with_live_lease_at_first_recovery_is_rechecked_after_it_expires(tmp_path, redis_url):
    """DB 모드: worker가 죽고 Redis도 유실됐는데 DB lease는 아직 남아 있음 → 첫 복구는 보류,
    lease가 끝난 뒤의 주기에서 다시 보낸다 (표시 키가 생겼다고 복구가 끝난 게 아님)."""
    from notree.scheduler.periodic import recover_after_redis_loss

    async def t():
        async with RealEnv(tmp_path, redis_url, allocator=False) as e:
            await recover_after_redis_loss(e.r, e.api)
            j = await e.job({"text": "held"})
            await e.api.tool("job_reserve_pending", limit=5, stream=RT.stream)
            await e.api.tool("job_mark_sent", job_id=j)
            await e.api.tool("job_start", job_id=j, worker="dead", lease_sec=1.5)   # 죽었지만 lease는 1.5초 남음
            await e.r.flushall()
            assert await recover_after_redis_loss(e.r, e.api) == 0       # 보류
            assert (await e.row(j))["state"] == "running"
            await asyncio.sleep(1.7)
            assert await recover_after_redis_loss(e.r, e.api) == 1       # lease가 끝난 뒤 다시 확인해 보냄
            assert await recover_after_redis_loss(e.r, e.api) == 0       # 복구 끝
            e.start_worker(cfg=RT)
            assert await e.wait_state(j, ("done",))
    asyncio.run(t())


@real_redis
def test_recovery_walks_past_the_page_limit(tmp_path, redis_url):
    from notree.scheduler.periodic import recover_after_redis_loss

    async def t():
        async with RealEnv(tmp_path, redis_url, allocator=False) as e:
            await recover_after_redis_loss(e.r, e.api)
            jobs = [await e.job({"text": str(i)}) for i in range(7)]
            await e.api.tool("job_reserve_pending", limit=10, stream=RT.stream)
            for j in jobs:
                await e.api.tool("job_mark_sent", job_id=j)
            await e.r.flushall()
            assert await recover_after_redis_loss(e.r, e.api, page=3) == 7   # 3개씩 끝까지
            assert await e.r.xlen(RT.stream) == 7
    asyncio.run(t())


@real_redis
def test_jobs_started_after_the_wipe_are_not_touched_by_an_open_recovery(tmp_path, redis_url):
    """복구가 열려 있는 동안(보류 작업 있음) 새로 시작한 작업은 대상이 아니다 — Redis 소유 키 모드처럼
    lease_sec=0이라 DB lease로는 살아 있는지 알 수 없는 작업도 다시 보내지 않는다."""
    from notree.scheduler.periodic import recover_after_redis_loss

    async def t():
        async with RealEnv(tmp_path, redis_url, allocator=False) as e:
            await recover_after_redis_loss(e.r, e.api)
            held = await e.job({"text": "held"})
            await e.api.tool("job_reserve_pending", limit=5, stream=RT.stream)
            await e.api.tool("job_start", job_id=held, worker="dead", lease_sec=5)
            await e.r.flushall()
            assert await recover_after_redis_loss(e.r, e.api) == 0       # 복구 열림 (held 보류)
            await asyncio.sleep(0.05)
            new = await e.job({"text": "new"})
            await e.api.tool("job_reserve_pending", limit=5, stream=RT.stream)
            await e.api.tool("job_start", job_id=new, worker="alive", lease_sec=0)
            assert await recover_after_redis_loss(e.r, e.api) == 0
            assert (await e.row(new))["state"] == "running" and (await e.row(new))["attempts"] == 1
    asyncio.run(t())


# ── 리뷰 4차: '최근 DB에서 시작됨' ≠ '새 Redis에 메시지가 있음' ─────────────
@real_redis
def test_late_start_of_a_pre_wipe_message_does_not_escape_recovery(tmp_path, redis_url):
    """worker가 초기화 전 Redis에서 메시지를 읽음 → job_start 지연 → Redis 초기화 → 복구 시작 → 늦은 job_start 도착
    → worker가 그 뒤 멈춤. 이전 전달 세대의 시작은 거절돼야 하고, 작업은 새 Redis로 다시 보내져 끝나야 한다."""
    from notree.scheduler.periodic import recover_after_redis_loss

    async def t():
        async with RealEnv(tmp_path, redis_url, allocator=False) as e:
            await recover_after_redis_loss(e.r, e.api)
            j = await e.job({"text": "late"})
            got = (await e.api.tool("job_reserve_pending", limit=5, stream=RT.stream))["jobs"]
            old_gen = got[0].get("gen")
            await e.api.tool("job_mark_sent", job_id=j)                  # 초기화 전 Redis에 전달됨 (worker가 읽음)
            await e.r.flushall()
            await recover_after_redis_loss(e.r, e.api)                   # 복구 시작
            st = await e.api.tool("job_start", job_id=j, worker="late", lease_sec=0.3, dispatch_gen=old_gen)
            assert st["status"] == "stale_dispatch"                      # 이전 전달의 시작은 거절
            await asyncio.sleep(0.4)                                     # (그 worker는 여기서 멈췄다고 가정)
            await recover_after_redis_loss(e.r, e.api)
            e.start_worker(cfg=RT)
            assert await e.wait_state(j, ("done",), timeout=15)
    asyncio.run(t())


@real_redis
def test_old_start_before_recovery_is_held_then_resent(tmp_path, redis_url):
    """늦은 시작이 복구 시작보다 **먼저** 도착한 경우: running(이전 세대)으로 남아 있다가, DB 모드 lease가 끝나면 다시 보낸다."""
    from notree.scheduler.periodic import recover_after_redis_loss

    async def t():
        async with RealEnv(tmp_path, redis_url, allocator=False) as e:
            await recover_after_redis_loss(e.r, e.api)
            j = await e.job({"text": "early"})
            got = (await e.api.tool("job_reserve_pending", limit=5, stream=RT.stream))["jobs"]
            await e.api.tool("job_mark_sent", job_id=j)
            await e.r.flushall()
            st = await e.api.tool("job_start", job_id=j, worker="dead", lease_sec=0.5, dispatch_gen=got[0].get("gen"))
            assert st["status"] == "started"                             # 아직 복구 전이라 받아들여짐
            assert await recover_after_redis_loss(e.r, e.api) == 0       # lease가 살아 있어 보류
            await asyncio.sleep(0.6)
            assert await recover_after_redis_loss(e.r, e.api) == 1       # 이전 세대 전달 → 다시 보냄
            e.start_worker(cfg=RT)
            assert await e.wait_state(j, ("done",), timeout=15)
    asyncio.run(t())


@real_redis
def test_stale_generation_message_in_the_new_redis_is_dropped(tmp_path, redis_url):
    """배정기 경쟁으로 이전 세대 메시지가 새 Redis에 들어온 경우: worker는 시작하지 않고 ACK만, 작업은 새 세대 메시지로 1번 완료."""
    from notree.scheduler.periodic import recover_after_redis_loss

    async def t():
        async with RealEnv(tmp_path, redis_url, allocator=False) as e:
            await recover_after_redis_loss(e.r, e.api)
            j = await e.job({"text": "race"})
            old = (await e.api.tool("job_reserve_pending", limit=5, stream=RT.stream))["jobs"][0]["gen"]
            await e.r.flushall()
            assert await recover_after_redis_loss(e.r, e.api) == 1         # 새 세대로 다시 보냄
            await e.r.xadd(RT.stream, {"job_id": j, "gen": str(old)})       # 늦게 도착한 이전 세대 메시지
            e.start_worker(cfg=RT)
            assert await e.wait_state(j, ("done",), timeout=15)
            row = await e.row(j)
            assert row["attempts"] == 1                                     # 이전 세대 메시지로는 시작 안 함
            for _ in range(40):                                             # 함께 읽힌 중복은 "실행 중"이라 남았다가
                if (await e.r.xpending(RT.stream, S.GROUP))["pending"] == 0:   # 회수 기준 뒤 다시 와서 ACK됨
                    break
                await asyncio.sleep(0.1)
            assert (await e.r.xpending(RT.stream, S.GROUP))["pending"] == 0
            assert (await e.row(j))["attempts"] == 1
    asyncio.run(t())


@real_redis
def test_generation_is_remembered_per_stream_not_just_per_message_id(redis_url):
    """Stream 메시지 ID는 Stream마다 따로 매겨진다 → 빌려 쓰는 worker가 두 Stream에서 같은 ID를 함께 들 수 있다.
    세대를 ID만으로 기억하면 한쪽이 덮어써 정상 메시지를 '이전 세대'로 잘못 거절할 수 있다."""
    QA = dataclasses.replace(S.QA, claim_idle_ms=60000)

    async def t():
        r = aioredis.from_url(redis_url, decode_responses=True)
        await r.flushall()
        for cfg in (QA, RT):
            await r.xgroup_create(cfg.stream, S.GROUP, id="0", mkstream=True)
        await r.xadd(QA.stream, {"job_id": "qa-job", "gen": "2"}, id="5-0")
        await r.xadd(RT.stream, {"job_id": "rt-job", "gen": "1"}, id="5-0")    # 같은 ID, 다른 Stream·세대
        w = Worker("A", QA, r, mcp=None, block_ms=0, borrow=RT, borrow_slots=1)
        a = await w._read(QA, None)
        b = await w._read(RT, None)
        await r.aclose()
        assert a[0] == b[0] == "5-0"
        assert w._pop_gen(QA, "5-0") == 2 and w._pop_gen(RT, "5-0") == 1
    asyncio.run(t())


# ── 리뷰 5차: 전달 세대 없는 메시지 ──────────────────────────────
def test_message_without_generation_is_refused_then_resent_with_one(tmp_path):
    """운영 worker는 세대 없는 메시지로 시작하지 않는다 (어느 Redis 세대에서 왔는지 모름) → ACK.
    작업은 sent로 남아 재전송이 세대를 붙여 다시 보내고, 그 메시지로 완료된다."""
    from notree.scheduler.periodic import resend_stale

    async def t():
        async with Env(tmp_path, allocator=False) as e:
            j = await e.job({"text": "legacy"})
            await e.api.tool("job_reserve_pending", limit=5, stream=RT.stream)
            await e.api.tool("job_mark_sent", job_id=j)
            await e.r.xgroup_create(RT.stream, S.GROUP, id="0", mkstream=True)
            await e.r.xadd(RT.stream, {"job_id": j})                     # 세대 없음 (예전 형식)
            e.start_worker(cfg=RT)
            await asyncio.sleep(1.0)
            row = await e.row(j)
            assert row["state"] == "sent" and row["attempts"] == 0      # 시작 안 함
            assert await e.pending() == 0                               # ACK함
            assert await resend_stale(e.r, e.api, reserved_sec=0, sent_sec=0) == 1
            assert await e.wait_state(j, ("done",))
    asyncio.run(t())


def test_legacy_messages_are_accepted_only_when_explicitly_allowed(tmp_path):
    async def t():
        async with Env(tmp_path, allocator=False) as e:
            j = await e.job({"text": "legacy"})
            await e.api.tool("job_reserve_pending", limit=5, stream=RT.stream)
            await e.r.xgroup_create(RT.stream, S.GROUP, id="0", mkstream=True)
            await e.r.xadd(RT.stream, {"job_id": j})
            e.start_worker(cfg=RT, allow_legacy=True)                    # 전환 기간·개발 도구에서만
            assert await e.wait_state(j, ("done",))
    asyncio.run(t())


# ── 리뷰 6차: 스냅숏 복원 (표시 키는 돌아오지만 그 뒤 메시지는 없음) ─────────────
def _redis_proc(port, d):
    p = subprocess.Popen(["redis-server", "--port", str(port), "--dir", str(d), "--dbfilename", "dump.rdb",
                          "--save", "", "--appendonly", "no"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(50):
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.1).close()
            return p
        except OSError:
            time.sleep(0.1)
    return p


@real_redis
def test_restore_from_an_old_snapshot_is_detected_as_loss(tmp_path):
    """표시 키가 든 스냅숏이 저장된 뒤 새 작업이 전달됨 → Redis가 그 스냅숏으로 복원 → 표시 키는 있지만 메시지는 없음.
    표시 키만 보면 유실을 못 알아챈다 → 재시작(run_id 변경)도 유실로 보고 다시 보내야 한다."""
    from notree.scheduler.periodic import recover_after_redis_loss
    port = _free_port()
    proc = _redis_proc(port, tmp_path)
    url = f"redis://127.0.0.1:{port}/0"

    async def t():
        nonlocal proc
        async with RealEnv(tmp_path, url, allocator=False) as e:
            await recover_after_redis_loss(e.r, e.api)                   # 표시 키
            await e.r.save()                                             # (잘못 설정된 Redis의) 스냅숏
            j = await e.job({"text": "after-snapshot"})
            got = (await e.api.tool("job_reserve_pending", limit=5, stream=RT.stream))["jobs"]
            await e.r.xadd(RT.stream, {"job_id": j, "gen": str(got[0]["gen"])})
            await e.api.tool("job_mark_sent", job_id=j)
            await e.r.connection_pool.disconnect()
            proc.terminate(); proc.wait(5)                               # 죽음 (스냅숏은 디스크에 남음)
            proc = _redis_proc(port, tmp_path)                           # 과거 스냅숏으로 복원
            assert await e.r.exists("notree:redis-alive")                # 표시 키는 돌아옴
            assert await e.r.xlen(RT.stream) == 0                        # 메시지는 없음
            assert await recover_after_redis_loss(e.r, e.api) == 1       # 그래도 유실로 보고 다시 보냄
            e.start_worker(cfg=RT)
            assert await e.wait_state(j, ("done",), timeout=15)

    try:
        asyncio.run(t())
    finally:
        proc.terminate(); proc.wait(5)


@real_redis
def test_compose_redis_settings_come_back_empty_after_restart(tmp_path):
    """docker-compose.yml의 redis command 인자 그대로 띄우고, 데이터를 쓴 뒤 정상 종료(SHUTDOWN) → 재시작하면 비어 있어야 한다
    (스냅숏·AOF를 남기지 않음). 설정 파일을 읽어 쓰므로 설정이 바뀌면 이 시험도 따라간다."""
    import re as _re
    text = (ROOT / "docker-compose.yml").read_text()
    args = _re.findall(r'"([^"]*)"', _re.search(r"command:\s*\[(.*?)\]", text).group(1))
    assert args[0] == "redis-server" and "--save" in args and "--appendonly" in args
    port = _free_port()

    def start():
        p = subprocess.Popen(args + ["--port", str(port), "--dir", str(tmp_path)],
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(50):
            try:
                socket.create_connection(("127.0.0.1", port), timeout=0.1).close()
                break
            except OSError:
                time.sleep(0.1)
        return p

    async def t():
        p = start()
        r = aioredis.from_url(f"redis://127.0.0.1:{port}/0", decode_responses=True)
        await r.set("k", "v")
        await r.xadd("s", {"a": "1"})
        try:
            await r.shutdown()                                           # 정상 종료 (설정에 따라 저장할 수도 있는 경로)
        except Exception:
            pass
        p.wait(5)
        await r.aclose()
        p = start()
        r = aioredis.from_url(f"redis://127.0.0.1:{port}/0", decode_responses=True)
        n = await r.dbsize()
        await r.aclose()
        p.terminate(); p.wait(5)
        assert n == 0 and not list(tmp_path.glob("*.rdb")) and not list(tmp_path.glob("appendonly*"))
    asyncio.run(t())
