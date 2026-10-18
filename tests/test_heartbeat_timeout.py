"""리뷰 6차 ⑤ (2026-10-19): heartbeat가 오류 대신 응답을 멈춰도 lease 만료를 판단해 작업을 멈춘다.
실제 네트워크 단절이 아니라 무응답을 주입한 시험 (Redis·MCP 서버 필요 없음)."""
import asyncio
import dataclasses
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from notree.common import settings as S  # noqa: E402
from notree.worker.lease import Lease, LeaseLost  # noqa: E402
from notree.worker.ownership import RedisLease  # noqa: E402
from notree.worker.registry import JobContext  # noqa: E402

CFG = dataclasses.replace(S.RT, lease_sec=0.1, heartbeat_sec=0.03)


class Hang:
    """처음 n번은 정상 응답, 그 뒤로는 영원히 응답하지 않는다."""

    def __init__(self, ok_calls=0, ok_value=None):
        self.n, self.ok_calls, self.ok_value = 0, ok_calls, ok_value

    async def __call__(self, *a, **k):
        self.n += 1
        if self.n <= self.ok_calls:
            return self.ok_value
        await asyncio.Event().wait()


class _Mcp:
    def __init__(self, hang):
        self.tool = hang


class _Own:
    def __init__(self, hang):
        self.heartbeat = hang


def _ctx(mcp=None):
    return JobContext(job_id="j", kind="echo", payload={}, attempt=1, permit="p", worker="w", cfg=CFG, mcp=mcp)


async def _run_until_lost(lease) -> float:
    t0 = time.monotonic()
    with pytest.raises(LeaseLost):
        async with lease:
            await asyncio.sleep(5)            # 핸들러: 계속 실행 중 (외부 호출을 계속 한다고 보면 됨)
    return time.monotonic() - t0


@pytest.mark.parametrize("ok_calls", [0, 3])          # 처음부터 무응답 / 몇 번 갱신한 뒤 무응답
def test_redis_heartbeat_that_never_answers_still_loses_the_lease(ok_calls):
    hang = Hang(ok_calls, ok_value=1)
    took = asyncio.run(_run_until_lost(RedisLease(_ctx(), _Own(hang))))
    assert took < 0.25 + ok_calls * CFG.heartbeat_sec * 1.5, took   # 리뷰 재현: lease 0.1초인데 0.25초에도 실행 중이었다


@pytest.mark.parametrize("ok_calls", [0, 3])
def test_db_heartbeat_that_never_answers_still_loses_the_lease(ok_calls):
    hang = Hang(ok_calls, ok_value={"renewed": True})
    took = asyncio.run(_run_until_lost(Lease(_ctx(_Mcp(hang)))))
    assert took < 0.25 + ok_calls * CFG.heartbeat_sec * 1.5, took


def test_one_slow_heartbeat_inside_the_lease_is_not_a_loss():
    """느린 응답 하나(TTL 안)는 잃은 것이 아니다 → 다음 박동이 갱신하면 계속 실행."""
    class SlowOnce:
        def __init__(self):
            self.n = 0

        async def __call__(self):
            self.n += 1
            if self.n == 2:
                await asyncio.sleep(CFG.lease_sec * 0.5)
            return 1

    async def t():
        async with RedisLease(_ctx(), _Own(SlowOnce())) as lease:
            await asyncio.sleep(0.5)
        assert not lease.lost
    asyncio.run(t())
