"""실행 안정성 회귀 테스트 (2026-10-15 외부 분석에서 지적된 네 가지).

1. 같은 worker가 자기 실행 중인 메시지를 XAUTOCLAIM으로 다시 가져와 중복 실행
2. 완료(job_finish) 기록이 실패했는데 ACK해서 작업이 버려짐
3. lease를 잃은 이전 실행자의 permit으로 쓰기가 성공함 (실행 세대 검증 없음)
4. 슬롯이 꽉 찬 상태에서 종료 신호를 받으면 유예 시간이 바로 시작되지 않음

지금은 모두 '알려진 버그'(xfail strict)로 표시돼 있다. 고치면 strict 때문에 XPASS가 실패로 잡히므로
고친 항목의 표시를 반드시 지우게 된다.
"""
import asyncio
import dataclasses
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from notree.common import settings as S  # noqa: E402
from notree.contracts.envelope import ToolError  # noqa: E402
from notree.worker.loop import Worker  # noqa: E402
from test_skeleton import RT, Env  # noqa: E402


def known_bug(n: int):
    return pytest.mark.xfail(strict=True, reason=f"알려진 버그 {n}번 (CHANGELOG 2026-10-15 참고)")


async def job_rt(e: Env, payload: dict) -> str:
    return (await e.api.tool("job_create", kind="echo", payload=payload, user_id="u-t", stream=RT.stream))["job_id"]


# ── 1. 자기 메시지 재회수로 인한 중복 실행 ─────────────────────────
@known_bug(1)
def test_worker_does_not_reclaim_its_own_running_job(tmp_path, capsys):
    """재배송 기준(0.6초)보다 오래 걸리는 작업 + 빈 슬롯 → 같은 worker가 다시 가져가면 안 된다."""
    cfg = dataclasses.replace(RT, lease_sec=3.0, heartbeat_sec=0.3, claim_idle_ms=600, job_timeout_sec=10,
                              concurrency=3)

    async def t():
        async with Env(tmp_path) as e:
            e.start_worker(cfg=cfg, name="A")
            j = await job_rt(e, {"text": "only-once", "sleep": 2.5})
            assert await e.wait_state(j, ("done",), timeout=15)
            await asyncio.sleep(0.5)
            row = await e.row(j)
            assert row["attempts"] == 1, f"같은 작업이 {row['attempts']}번 시작됨"
            assert await e.pending() == 0
    asyncio.run(t())
    assert capsys.readouterr().out.count("'only-once'") == 1, "핸들러가 두 번 실행됨"


@known_bug(1)
def test_job_start_rejects_live_lease_even_for_same_worker(tmp_path):
    async def t():
        async with Env(tmp_path, allocator=False) as e:
            j = await job_rt(e, {})
            await e.api.tool("job_reserve_pending")
            first = await e.api.tool("job_start", job_id=j, worker="rt-A", lease_sec=30)
            again = await e.api.tool("job_start", job_id=j, worker="rt-A", lease_sec=30)
            assert first["status"] == "started"
            assert again["status"] == "leased", "살아 있는 lease인데 같은 이름이라고 다시 시작을 허용함"
    asyncio.run(t())


# ── 2. 완료 기록 실패 후 ACK ──────────────────────────────────────
class FlakyMcp:
    """지정한 도구를 처음 n번 실패시키는 MCP 클라이언트 래퍼."""

    def __init__(self, inner, fail: dict[str, int]):
        self.inner, self.fail = inner, dict(fail)

    async def tool(self, name, **args):
        if self.fail.get(name, 0) > 0:
            self.fail[name] -= 1
            raise ToolError(name, "unavailable", "주입된 장애", True)
        return await self.inner.tool(name, **args)

    def __getattr__(self, item):
        return getattr(self.inner, item)


@known_bug(2)
def test_finish_failure_does_not_lose_the_job(tmp_path):
    """job_finish가 일시적으로 실패해도 작업이 'DB는 running, Redis에선 사라짐' 상태로 버려지면 안 된다.
    최종적으로 done이 되고 ACK도 끝나야 한다 (재시도든 재배송이든)."""
    cfg = dataclasses.replace(RT, lease_sec=1.0, heartbeat_sec=0.3, claim_idle_ms=1200, job_timeout_sec=5)

    async def t():
        async with Env(tmp_path) as e:
            e.start_worker(cfg=cfg, name="A", mcp=FlakyMcp(e.m_w, {"job_finish": 4}))
            j = await job_rt(e, {"text": "finish-me"})
            done = await e.wait_state(j, ("done",), timeout=15)
            row, pending = await e.row(j), await e.pending()
            assert done, f"완료되지 않음: state={row['state']} pending={pending} (running인데 ACK됐으면 영영 버려진 것)"
            assert pending == 0
    asyncio.run(t())


# ── 3. 이전 실행자의 permit ───────────────────────────────────────
@known_bug(3)
def test_stale_permit_cannot_write_after_takeover(tmp_path):
    async def t():
        async with Env(tmp_path, allocator=False) as e:
            j = await job_rt(e, {})
            await e.api.tool("job_reserve_pending")
            a = await e.api.tool("job_start", job_id=j, worker="rt-A", lease_sec=0.3)
            await asyncio.sleep(0.5)                                           # A의 lease 만료
            b = await e.api.tool("job_start", job_id=j, worker="rt-B", lease_sec=30)
            assert b["status"] == "started"

            stale_pub = await e.api.call("note_publish", idem_key=j, course="c", note="from A", status="완료",
                                         permit=a["permit"])
            assert stale_pub["ok"] is False and stale_pub["code"] == "permit_invalid", \
                f"lease를 잃은 A의 permit으로 발행이 성공함: {stale_pub}"
            stale_ck = await e.api.call("checkpoint_put", thread_id=j, ns="", checkpoint_id="c1", parent_id=None,
                                        checkpoint=["t", ""], metadata=["t", ""], permit=a["permit"])
            assert stale_ck["ok"] is False and stale_ck["code"] == "permit_invalid", \
                f"lease를 잃은 A의 permit으로 체크포인트 저장이 성공함: {stale_ck}"

            ok_pub = await e.api.call("note_publish", idem_key=j, course="c", note="from B", status="완료",
                                      permit=b["permit"])
            assert ok_pub["ok"] is True
            assert (await e.api.tool("note_get", idem_key=j))["note"] == "from B"
    asyncio.run(t())


@known_bug(3)
def test_previous_attempt_permit_is_rejected_for_same_worker(tmp_path):
    """같은 worker 이름이라도 이전 시도(실행 세대)의 permit은 무효여야 한다."""
    async def t():
        async with Env(tmp_path, allocator=False) as e:
            j = await job_rt(e, {})
            await e.api.tool("job_reserve_pending")
            first = await e.api.tool("job_start", job_id=j, worker="rt-A", lease_sec=0.3)
            await asyncio.sleep(0.5)
            second = await e.api.tool("job_start", job_id=j, worker="rt-A", lease_sec=30)
            assert second["status"] == "started" and second["attempt"] == 2
            res = await e.api.call("note_publish", idem_key=j, course="c", note="old", status="완료",
                                   permit=first["permit"])
            assert res["ok"] is False and res["code"] == "permit_invalid", f"이전 시도의 permit이 통함: {res}"
    asyncio.run(t())


# ── 4. 슬롯이 꽉 찬 상태의 종료 ───────────────────────────────────
@known_bug(4)
def test_shutdown_grace_starts_immediately_when_slots_are_full(tmp_path):
    cfg = dataclasses.replace(RT, concurrency=1, job_timeout_sec=30)

    async def t():
        async with Env(tmp_path) as e:
            stop = asyncio.Event()
            w = Worker("A", cfg, e.r, e.m_w, block_ms=0, shutdown_grace_sec=0.3)
            task = asyncio.create_task(w.run(stop))
            j = await job_rt(e, {"sleep": 10})
            assert await e.wait_state(j, ("running",))
            t0 = time.time()
            stop.set()
            try:
                await asyncio.wait_for(task, timeout=8)
            finally:
                stop.set(); task.cancel()
            took = time.time() - t0
            assert took < 2.0, f"종료에 {took:.1f}초 걸림 (유예 0.3초)"
            row = await e.row(j)
            assert row["state"] == "sent" and await e.pending() == 1         # 반납 → 다른 worker가 이어받을 수 있음
    asyncio.run(t())
