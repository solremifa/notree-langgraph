"""실행 시작 절차: Redis 선점 → DB 조건부 승인 (2026-10-19, docs/changes/2026-10-19-start-protocol.md).
모두 **실제 Redis**를 쓰고, 순서는 **강제로** 만든다(응답을 쥐고 있거나 버리는 대리 객체·키 만료). 자연 발생 빈도는
notree/dev/start_race.py가 따로 잰다 (docs/measurements/2026-10-19-start-protocol).

  1. 재현했던 문제: 같은 live_summary 작업의 중복 메시지를 A·B가 받음 → A의 DB 시작은 반영됐지만 응답이 늦음 →
     (수정 전) B도 키가 없는 것을 보고 DB에서 시작 → 둘 다 시도를 씀 → 총 1회라 요약 함수를 한 번도 부르지 않고 실패
  2. 절차 단위: 인계 뒤 늦은 시작·들은 뒤 키 확인·중복 시작·같은 start_id 재요청·Redis 초기화·사용자 재시도 뒤 세대
  3. 응답 유실 주입: 시작·선점·heartbeat·해제·마지막 저장
  4. 경계 정책: 승인 확정 직후 죽은 실행은 시도 1회로 센다 (실시간 요약은 실패 확정 — 결과가 저장돼 있으면 완료)
"""
import asyncio
import dataclasses
import sqlite3
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from notree.common import settings as S  # noqa: E402
from notree.worker import ownership as O  # noqa: E402
from notree.worker.handlers import live_summary as LS  # noqa: E402
from notree.worker.loop import Worker  # noqa: E402
from notree.worker.mcp_client import McpClient  # noqa: E402
from redis_support import needs_redis  # noqa: E402
from test_dispatch_recovery import RealEnv, redis_url  # noqa: E402,F401
from test_summary_coalesce import L, U, segs  # noqa: E402

pytestmark = needs_redis
RT = dataclasses.replace(S.RT, lease_sec=1.0, heartbeat_sec=0.3, claim_idle_ms=1200, job_timeout_sec=20)


@pytest.fixture(autouse=True)
def _reset(monkeypatch):
    LS.CALLS.clear()
    monkeypatch.setattr(LS, "FAKE_DELAY", 0.0)


class Hooked:
    """McpClient 대리. 지정한 도구 호출을 서버에 **보낸 뒤** 응답을 돌려주기 전에 멈추거나(hold), 응답을 버린다(drop).
    서버에서는 이미 반영된 상태 — '처리됐지만 응답이 늦거나 사라진' 경우를 결정적으로 만든다."""

    def __init__(self, inner: McpClient) -> None:
        self.inner = inner
        self.calls: list[str] = []
        self.hold: dict[str, asyncio.Event] = {}     # 도구 → 풀어 줄 때까지 응답을 쥐고 있음 (한 번)
        self.applied: dict[str, asyncio.Event] = {}  # 도구 → 서버에 반영됐음
        self.drop: dict[str, int] = {}               # 도구 → 앞으로 n번 응답을 버리고 ConnectionError

    async def tool(self, name, **args):
        self.calls.append(name)
        res = await self.inner.tool(name, **args)
        if name in self.applied:
            self.applied[name].set()
        if gate := self.hold.pop(name, None):
            await gate.wait()
        if self.drop.get(name):
            self.drop[name] -= 1
            raise ConnectionError(f"{name} 응답 유실 (시험)")
        return res

    async def call(self, name, **args):
        self.calls.append(name)
        return await self.inner.call(name, **args)


async def _summary_job(e):
    """강의 하나, 전사 1~3 → 대기 요약 1건 → 허가. (job_id, 전달 세대)"""
    await e.api.tool("lecture_start", lecture_id=L, user_id=U)
    j = (await e.api.tool("transcript_commit", lecture_id=L, user_id=U, segments=segs(1, 3)))["enqueued_job_id"]
    gen = (await e.api.tool("job_reserve_pending", limit=5, stream=RT.stream))["jobs"][0]["gen"]
    await e.r.xgroup_create(RT.stream, S.GROUP, id="0", mkstream=True)
    return j, gen


async def _deliver(e, w: Worker, j: str, gen: int):
    """같은 작업의 메시지 하나를 worker w에게 전달 (중복 메시지 = 메시지마다 한 번씩 부름)."""
    await e.r.xadd(RT.stream, {"job_id": j, "gen": str(gen)})
    msg = await w._read(RT, None)
    assert msg and msg[1] == j
    return msg


def _saved(e, j):
    con = sqlite3.connect(e.db)
    n = con.execute("SELECT COUNT(*) FROM summaries WHERE idem_key=?", (j,)).fetchone()[0]
    con.close()
    return n


@pytest.mark.parametrize("mode", ["db", "redis"])
def test_duplicate_delivery_with_delayed_start_response_does_not_burn_the_only_attempt(tmp_path, redis_url, monkeypatch,
                                                                                      mode):
    """강제 순서: A의 DB 시작은 반영됐지만 응답을 쥐고 있음 → 그동안 B가 같은 작업의 중복 메시지를 끝까지 처리 → A의 응답을 풂.
    두 모드 모두 요약 1회·저장 1회·시도 1회로 완료돼야 한다 (중복 전달이 실행 기회를 쓰지 않음)."""
    monkeypatch.setattr(S, "LIVENESS", mode)

    async def t():
        async with RealEnv(tmp_path, redis_url, allocator=False) as e:
            async with McpClient(env=e.env) as m_a, McpClient(env=e.env) as m_b:
                a_mcp = Hooked(m_a)
                a = Worker("A", RT, e.r, a_mcp, block_ms=0)
                b = Worker("B", RT, e.r, m_b, block_ms=0)
                j, gen = await _summary_job(e)
                msg_a = await _deliver(e, a, j, gen)
                msg_b = await _deliver(e, b, j, gen)
                a_mcp.applied["job_start"], a_mcp.hold["job_start"] = asyncio.Event(), asyncio.Event()
                gate = a_mcp.hold["job_start"]
                ta = asyncio.create_task(a._guarded(*msg_a))
                await asyncio.wait_for(a_mcp.applied["job_start"].wait(), 10)   # A의 시작이 DB에 반영됨 (응답은 아직)
                await asyncio.wait_for(b._guarded(*msg_b), 10)                  # B가 중복 메시지를 끝까지 처리
                gate.set()
                await asyncio.wait_for(ta, 15)
                row = await e.row(j)
                return row, len(LS.CALLS), _saved(e, j)

    row, calls, saved = asyncio.run(t())
    assert (row["state"], row["attempts"], calls, saved) == ("done", 1, 1, 1), (row, calls, saved)


# ── 2. 절차 단위 (Ownership + job_start를 직접 부름) ─────────────────────
SHORT = dataclasses.replace(RT, lease_sec=0.3)


async def _echo_job(e):
    j = (await e.api.tool("job_create", kind="echo", payload={}, user_id="u", stream=RT.stream))["job_id"]
    await e.r.xgroup_create(RT.stream, S.GROUP, id="0", mkstream=True)
    return j


async def _msg(e, consumer, j):
    await e.r.xadd(RT.stream, {"job_id": j})
    res = await e.r.xreadgroup(S.GROUP, consumer, {RT.stream: ">"}, count=1)
    return res[0][1][0][0]


def _own(e, j, consumer, m, cfg=RT):
    return O.Ownership(e.r, j, consumer, cfg, m)


async def _start(e, o, expect, **kw):
    return await e.api.tool("job_start", job_id=o.job_id, worker=o.consumer, lease_sec=0, start_id=o.token,
                            expect_epoch=expect, **kw)


async def _finish(e, o, st):
    return (await e.api.tool("job_finish", job_id=o.job_id, worker=o.consumer, permit=st["permit"]))["finished"]


def test_late_start_after_handover_is_superseded_and_cannot_evict(tmp_path, redis_url):
    """예전 문제(리뷰 2차): A 선점 → A 멈춤(키 만료, DB 시작 전) → B 선점·승인·실행 → A의 늦은 시작.
    A의 시작은 superseded(아무것도 안 바꿈), 들은 뒤 키 확인이 실패해 손 뗌. B의 세대·시도·저장 그대로."""
    async def t():
        async with RealEnv(tmp_path, redis_url, allocator=False) as e:
            j = await _echo_job(e)
            m = await _msg(e, "rt-A", j)
            a = _own(e, j, "rt-A", m, SHORT)
            assert await a.claim()
            await asyncio.sleep(0.4)                                    # A 정지 → 키 만료
            await e.r.xautoclaim(RT.stream, S.GROUP, "rt-B", min_idle_time=0, start_id="0-0", count=1)
            b = _own(e, j, "rt-B", m)
            assert await b.claim()
            sb = await _start(e, b, 0)
            assert (sb["status"], sb["exec_epoch"], sb["attempt"]) == ("started", 1, 1)
            sa = await _start(e, a, 0)                                  # A가 깨어나 늦게 도착
            assert (sa["status"], sa["exec_epoch"]) == ("superseded", 1)
            assert await a.heartbeat() == 0                             # 들은 뒤 키 확인 실패 → A는 손 뗌
            row = await e.row(j)
            assert (row["lease_owner"], row["exec_epoch"], row["attempts"]) == ("rt-B", 1, 1)
            assert await b.heartbeat() == 1 and await _finish(e, b, sb)
    asyncio.run(t())


def test_epoch_learned_without_the_key_is_never_used(tmp_path, redis_url):
    """A가 키를 가진 채 superseded(1)을 들음 → 확인 전에 A의 키가 만료 → B가 선점·확인·승인(2) → A의 확인은 실패.
    확인 없이 늦게 보낸 A의 시작(expect=1)도 superseded(2) — B를 쫓아내지 못한다."""
    async def t():
        async with RealEnv(tmp_path, redis_url, allocator=False) as e:
            j = await _echo_job(e)
            await e.api.tool("job_start", job_id=j, worker="dead", lease_sec=0, start_id="dead", expect_epoch=0)
            m = await _msg(e, "rt-A", j)
            a = _own(e, j, "rt-A", m, SHORT)
            assert await a.claim()
            assert (await _start(e, a, 0))["status"] == "superseded"
            await asyncio.sleep(0.4)                                    # A의 키 만료 (확인 전)
            await e.r.xautoclaim(RT.stream, S.GROUP, "rt-B", min_idle_time=0, start_id="0-0", count=1)
            b = _own(e, j, "rt-B", m)
            assert await b.claim()
            s = await _start(e, b, 0)
            assert (s["status"], s["exec_epoch"]) == ("superseded", 1)
            assert await b.heartbeat() == 1                             # 키를 가진 채로 들었음을 확인
            sb = await _start(e, b, 1)
            assert (sb["status"], sb["exec_epoch"]) == ("started", 2)
            assert await a.heartbeat() == 0                             # A: 확인 실패 → 다시 청하지 않음
            assert (await _start(e, a, 1))["status"] == "superseded"   # 확인을 건너뛰고 보내도 B는 그대로
            row = await e.row(j)
            assert (row["lease_owner"], row["exec_epoch"]) == ("rt-B", 2)
            assert await _finish(e, b, sb)
    asyncio.run(t())


def test_duplicate_start_does_not_evict_or_count(tmp_path, redis_url):
    """A가 실행 중(키 보유). 중복 메시지의 B는 선점에서 짐 → DB를 부르지 않는다.
    B가 어떻게든 시작을 보내도(expect=0) superseded — 세대·시도·소유자 그대로."""
    async def t():
        async with RealEnv(tmp_path, redis_url, allocator=False) as e:
            j = await _echo_job(e)
            a = _own(e, j, "rt-A", await _msg(e, "rt-A", j))
            assert await a.claim()
            sa = await _start(e, a, 0)
            b = _own(e, j, "rt-B", await _msg(e, "rt-B", j))
            assert not await b.claim() and b.holder.startswith("rt-A:")
            assert (await _start(e, b, 0))["status"] == "superseded"
            row = await e.row(j)
            assert (row["lease_owner"], row["exec_epoch"], row["attempts"]) == ("rt-A", 1, 1)
            assert await _finish(e, a, sa)
    asyncio.run(t())


def test_same_start_id_gets_the_same_grant(tmp_path, redis_url):
    """시작 응답을 잃고 같은 start_id로 다시 보내면 같은 승인 (세대·시도 그대로). 다른 start_id는 superseded."""
    async def t():
        async with RealEnv(tmp_path, redis_url, allocator=False) as e:
            j = await _echo_job(e)
            a = _own(e, j, "rt-A", await _msg(e, "rt-A", j))
            assert await a.claim()
            first = await _start(e, a, 0)
            again = await _start(e, a, 0)
            assert (again["status"], again["replay"], again["exec_epoch"], again["attempt"]) == ("started", True, 1, 1)
            assert not first["replay"]
            other = await e.api.tool("job_start", job_id=j, worker="rt-A", lease_sec=0, start_id="x", expect_epoch=0)
            assert other["status"] == "superseded"
            row = await e.row(j)
            assert (row["exec_epoch"], row["attempts"]) == (1, 1)
            assert await _finish(e, a, again)                           # 다시 받은 permit도 같은 세대라 유효
    asyncio.run(t())


def test_redis_wipe_costs_a_rerun_and_epoch_never_goes_back(tmp_path, redis_url):
    """A가 세대 1로 실행 중 → Redis 초기화 → A는 heartbeat에서 잃음. 다시 보낸 메시지로 B가 선점 → superseded(1) → 확인 → 세대 2.
    A의 저장은 거절. 세대는 DB에만 있어 초기화로 되돌아가지 않는다. 사용자 재시도 뒤에도 세대는 이어서 오른다."""
    async def t():
        async with RealEnv(tmp_path, redis_url, allocator=False) as e:
            j = await _echo_job(e)
            a = _own(e, j, "rt-A", await _msg(e, "rt-A", j))
            assert await a.claim()
            sa = await _start(e, a, 0)
            await e.r.flushall()
            await e.r.xgroup_create(RT.stream, S.GROUP, id="0", mkstream=True)
            assert await a.heartbeat() == 0
            b = _own(e, j, "rt-B", await _msg(e, "rt-B", j))
            assert await b.claim()
            assert (await _start(e, b, 0))["exec_epoch"] == 1
            assert await b.heartbeat() == 1
            sb = await _start(e, b, 1)
            assert (sb["exec_epoch"], sb["attempt"]) == (2, 2)
            assert not await _finish(e, a, sa)
            await e.api.tool("job_fail", job_id=j, worker="rt-B", reason="x", user_retryable=True, permit=sb["permit"])
            assert (await e.api.tool("job_retry", job_id=j))["revived"]
            await e.r.flushall()
            await e.r.xgroup_create(RT.stream, S.GROUP, id="0", mkstream=True)
            c = _own(e, j, "rt-C", await _msg(e, "rt-C", j))
            assert await c.claim()
            assert (await _start(e, c, 0))["exec_epoch"] == 2             # 재시도·초기화로 되돌아가지 않음
            assert await c.heartbeat() == 1
            sc = await _start(e, c, 2)
            assert (sc["exec_epoch"], sc["attempt"]) == (3, 1)           # 시도는 사용자 재시도로 0부터, 세대는 이어서
            assert await _finish(e, c, sc)
    asyncio.run(t())


# ── 3. 응답 유실 주입 (worker 단위) ───────────────────────────────────
def _stats(e, j):
    con = sqlite3.connect(e.db)
    rows = dict(con.execute("SELECT tool, COUNT(*) FROM tool_log WHERE thread_id=? GROUP BY tool", (j,)).fetchall())
    con.close()
    return rows


async def _reserved_echo(e):
    j = await _echo_job(e)
    gen = (await e.api.tool("job_reserve_pending", limit=5, stream=RT.stream))["jobs"][0]["gen"]
    return j, gen


@pytest.mark.parametrize("mode", ["db", "redis"])
def test_lost_start_response_is_replayed_not_recounted(tmp_path, redis_url, monkeypatch, mode):
    """job_start가 반영된 뒤 응답만 사라짐 → 같은 start_id로 다시 보냄 → 같은 승인. 시도 1·세대 1, DB 시작 호출은 2번."""
    monkeypatch.setattr(S, "LIVENESS", mode)

    async def t():
        async with RealEnv(tmp_path, redis_url, allocator=False) as e:
            async with McpClient(env=e.env) as m_a:
                hooked = Hooked(m_a)
                hooked.drop["job_start"] = 1
                a = Worker("A", RT, e.r, hooked, block_ms=0)
                j, gen = await _reserved_echo(e)
                await a._guarded(*await _deliver(e, a, j, gen))
                return await e.row(j), _stats(e, j), await e.r.xpending(RT.stream, S.GROUP)

    row, stats, pending = asyncio.run(t())
    assert (row["state"], row["attempts"], row["exec_epoch"]) == ("done", 1, 1)
    assert stats["job_start"] == 2 and pending["pending"] == 0


def test_start_never_answered_is_a_boundary_attempt(tmp_path, redis_url, monkeypatch):
    """경계 정책: 승인은 확정됐는데 응답을 끝내 못 받음 → A는 키를 풀고 ACK하지 않음 → 재배송된 B가 superseded → 확인 → 새 승인.
    DB는 A가 무엇을 했는지 모르므로 A의 승인도 시도 1회로 센다 (echo: 한도 2 안이라 완료, 시도 2)."""
    monkeypatch.setattr(S, "LIVENESS", "redis")

    async def t():
        async with RealEnv(tmp_path, redis_url, allocator=False) as e:
            async with McpClient(env=e.env) as m_a, McpClient(env=e.env) as m_b:
                hooked = Hooked(m_a)
                hooked.drop["job_start"] = 99
                a = Worker("A", RT, e.r, hooked, block_ms=0)
                b = Worker("B", RT, e.r, m_b, block_ms=0)
                j, gen = await _reserved_echo(e)
                await a._guarded(*await _deliver(e, a, j, gen))
                assert not await e.r.exists(O.own_key(j))               # A는 키를 풀었다
                await asyncio.sleep(RT.claim_idle_ms / 1000 + 0.1)
                await b._guarded(*await b._claim(RT))
                return await e.row(j), _stats(e, j)

    row, stats = asyncio.run(t())
    assert (row["state"], row["attempts"], row["exec_epoch"], row["lease_owner"]) == ("done", 2, 2, "rt-B")
    assert stats["job_start"] == 3 + 2                                  # A 3번(모두 같은 승인) + B (superseded, 승인)


class _FlakyOwnership(O.Ownership):
    """Redis 명령은 실행되지만 응답이 사라지는 경우: 지정한 메서드를 실행한 뒤 n번까지 ConnectionError (worker A만)."""
    lose: dict[str, int] = {}

    async def _maybe_lose(self, name, coro):
        res = await coro
        if self.consumer == "rt-A" and _FlakyOwnership.lose.get(name):
            _FlakyOwnership.lose[name] -= 1
            raise ConnectionError(f"{name} 응답 유실 (시험)")
        return res

    async def claim(self):
        return await self._maybe_lose("claim", super().claim())

    async def heartbeat(self):
        return await self._maybe_lose("heartbeat", super().heartbeat())

    async def release(self):
        return await self._maybe_lose("release", super().release())


@pytest.mark.parametrize("lost", ["claim", "heartbeat", "release"])
def test_lost_redis_responses_do_not_burn_attempts(tmp_path, redis_url, monkeypatch, lost):
    """선점·heartbeat·해제의 응답 유실 (명령은 Redis에서 실행됨).
      claim     : A는 실패로 보고 손 뗌(ACK 안 함) → 키는 TTL로 풀림 → 재배송된 메시지로 완료. 시도 1
      heartbeat : TTL 안의 한두 번은 잃은 것이 아님 → 그대로 완료. 시도 1
      release   : 완료·ACK는 이미 끝남. 키는 TTL로 풀림. 시도 1"""
    import notree.worker.loop as loop_mod
    monkeypatch.setattr(S, "LIVENESS", "redis")
    monkeypatch.setattr(loop_mod, "Ownership", _FlakyOwnership)
    _FlakyOwnership.lose = {lost: 2 if lost == "heartbeat" else 1}

    async def t():
        async with RealEnv(tmp_path, redis_url) as e:
            e.start_worker(cfg=RT, name="A")
            j = await e.job({"text": "x", "sleep": 1.0})
            assert await e.wait_state(j, ("done",), timeout=20)
            await asyncio.sleep(RT.lease_sec + 0.2)
            return await e.row(j), _stats(e, j), await e.pending(), [k async for k in e.r.scan_iter("notree:own:*")]

    row, stats, pending, keys = asyncio.run(t())
    assert row["attempts"] == 1 and row["exec_epoch"] == 1, row
    assert stats.get("job_heartbeat", 0) == 0 and pending == 0 and keys == []
    assert _FlakyOwnership.lose[lost] == 0                              # 유실이 실제로 일어남


# ── 4. 경계 정책·실시간 요약 총 1회 ─────────────────────────────────────
@pytest.mark.parametrize("dies_after", ["job_start", "summary_publish"])
def test_worker_dying_right_after_a_grant_uses_the_only_summary_attempt(tmp_path, redis_url, monkeypatch, dies_after):
    """경계 정책 (총 1회): A가 승인을 받은 직후 죽음(정리 없음) → 재배송된 B는 시도 2 > 한도 1이라 요약 함수를 부르지 않고 확정만.
      job_start 직후      : 결과 없음 → 실패 확정 (다시 시도 가능). 요약 함수 0회
      summary_publish 직후: 결과 있음 → 완료로 확정 (done_instead). 요약 함수 1회·저장 1회 (다시 부르지 않음)"""
    monkeypatch.setattr(S, "LIVENESS", "redis")

    async def t():
        async with RealEnv(tmp_path, redis_url, allocator=False) as e:
            async with McpClient(env=e.env) as m_a, McpClient(env=e.env) as m_b:
                hooked = Hooked(m_a)
                hooked.applied[dies_after], hooked.hold[dies_after] = asyncio.Event(), asyncio.Event()
                a = Worker("A", RT, e.r, hooked, block_ms=0)
                b = Worker("B", RT, e.r, m_b, block_ms=0)
                j, gen = await _summary_job(e)
                ta = asyncio.create_task(a._guarded(*await _deliver(e, a, j, gen)))
                await hooked.applied[dies_after].wait()
                a.dead = True                                       # 죽음: 키 해제·ACK·job_release 없음
                ta.cancel()
                await asyncio.gather(ta, return_exceptions=True)
                assert await e.r.exists(O.own_key(j))
                t0 = time.monotonic()
                await asyncio.sleep(RT.claim_idle_ms / 1000 + 0.1)
                await b._guarded(*await b._claim(RT))
                return await e.row(j), len(LS.CALLS), _saved(e, j), time.monotonic() - t0

    row, calls, saved, took = asyncio.run(t())
    if dies_after == "job_start":
        assert (row["state"], row["user_retryable"], row["attempts"], calls, saved) == ("failed", True, 2, 0, 0)
    else:
        assert (row["state"], row["attempts"], calls, saved) == ("done", 2, 1, 1)
    assert took < 3


@pytest.mark.parametrize("mode", ["db", "redis"])
def test_last_attempt_save_response_lost_finishes_as_done(tmp_path, redis_url, monkeypatch, mode):
    """실시간 요약(총 1회)의 저장은 됐는데 응답이 모두 사라짐 → 같은 idem_key 재전송도 응답 없음 → 시도 1 = 한도 →
    job_fail이 같은 트랜잭션에서 결과를 보고 완료로 확정 (done_instead). 요약 함수 1회·저장 1회."""
    monkeypatch.setattr(S, "LIVENESS", mode)
    monkeypatch.setattr(LS, "PUBLISH_CONFIRM", (0.05, 0.05, 0.05))

    async def t():
        async with RealEnv(tmp_path, redis_url, allocator=False) as e:
            async with McpClient(env=e.env) as m_a:
                hooked = Hooked(m_a)
                hooked.drop["summary_publish"] = 99
                a = Worker("A", RT, e.r, hooked, block_ms=0)
                j, gen = await _summary_job(e)
                await a._guarded(*await _deliver(e, a, j, gen))
                return await e.row(j), len(LS.CALLS), _saved(e, j), await e.r.xpending(RT.stream, S.GROUP)

    row, calls, saved, pending = asyncio.run(t())
    assert (row["state"], row["attempts"], calls, saved, pending["pending"]) == ("done", 1, 1, 1, 0)
