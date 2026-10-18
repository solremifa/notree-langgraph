"""결합 장애 (실제 redis-server, 통합 브랜치). 리뷰 2차 권장 순서의 마지막 단계.

Redis 소유 키 모드에서 실시간 요약이 실행 중일 때 Redis가 통째로 초기화되고(FLUSHALL), 그사이 새 전사가 들어온다.
  - 실행 중이던 worker는 heartbeat가 실패해 소유권을 잃고, 늦은 저장은 거절된다 (exec_epoch)
  - scheduler가 Redis 유실을 알아채 고립된 작업을 정리한다 (실시간 요약은 총 1회라 실패 확정)
  - worker는 consumer group을 다시 만들고, 다시 보낸 작업은 DB가 새 실행 세대를 발급해 소유 키를 다시 쓴다
  - 후속 요약이 처리 위치부터 읽어 전사 전체를 한 번에 요약한다 → 구간 빠짐·겹침·중복 저장 없음, 멈춘 작업 없음
"""
import asyncio
import dataclasses
import shutil
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from notree.common import settings as S  # noqa: E402
from notree.scheduler.periodic import recover_after_redis_loss  # noqa: E402
from notree.worker.handlers import live_summary as LS  # noqa: E402
from test_dispatch_recovery import RealEnv, redis_url  # noqa: E402,F401
from test_summary_coalesce import L, U, lec, ranges, segs, summary_jobs, wait_lec  # noqa: E402

pytestmark = pytest.mark.skipif(shutil.which("redis-server") is None, reason="redis-server 없음")
RT = dataclasses.replace(S.RT, lease_sec=1.0, heartbeat_sec=0.3, claim_idle_ms=1200, job_timeout_sec=20)


def test_redis_wipe_during_summary_in_redis_liveness_mode(tmp_path, redis_url, monkeypatch):  # noqa: F811
    monkeypatch.setattr(S, "LIVENESS", "redis")
    monkeypatch.setattr(LS, "FAKE_DELAY", 1.5)
    LS.CALLS.clear()

    async def t():
        async with RealEnv(tmp_path, redis_url) as e:
            await recover_after_redis_loss(e.r, e.api)                     # 표시 키
            w = e.start_worker(cfg=RT)
            await e.api.tool("lecture_start", lecture_id=L, user_id=U)
            first = (await e.api.tool("transcript_commit", lecture_id=L, user_id=U, segments=segs(1, 5)))["enqueued_job_id"]
            await wait_lec(e, lambda x: x["attempted_until"] == 5)          # 첫 요약이 1~5를 읽고 요약 중
            await e.r.flushall()                                            # Redis 통째 유실
            await e.api.tool("transcript_commit", lecture_id=L, user_id=U, segments=segs(6, 8))
            await recover_after_redis_loss(e.r, e.api)
            x = await wait_lec(e, lambda x: x["processed_until"] == 8, timeout=25)
            await asyncio.sleep(1.5)
            row = await e.row(first)
            assert row["state"] == "failed" and "Redis 유실" in row["fail_reason"]   # 총 1회 → 다시 보내지 않고 실패 확정
            assert ranges(e) == [(1, 8)]                                    # 첫 요약의 늦은 저장은 없음, 후속이 전체를 한 번에
            assert x["rolling_summary"].startswith("[1-8]")
            assert summary_jobs(e, ("pending", "reserved", "sent", "running")) == []   # 멈춘 작업 없음
            assert not w.dead
            assert [k async for k in e.r.scan_iter("notree:own:*")] == []
    asyncio.run(t())


def test_resent_orphan_runs_with_a_new_epoch_after_redis_wipe(tmp_path, redis_url, monkeypatch):  # noqa: F811
    """시도가 남은 작업(echo, 총 2회)이 실행 중일 때 Redis 초기화 → 유실 복구가 다시 보냄 → DB가 세대 2를 발급해 소유 키를 씀
    → 완료. 첫 실행은 소유 키가 사라져 잃고 저장하지 않는다 (완료 1번). 번호가 DB 세대라 초기화로 되돌아가지 않는다."""
    monkeypatch.setattr(S, "LIVENESS", "redis")

    async def t():
        async with RealEnv(tmp_path, redis_url) as e:
            await recover_after_redis_loss(e.r, e.api)
            e.start_worker(cfg=RT)
            j = await e.job({"text": "long", "sleep": 2.0})
            assert await e.wait_state(j, ("running",))
            await asyncio.sleep(0.3)
            await e.r.flushall()
            assert await recover_after_redis_loss(e.r, e.api) == 1
            assert await e.wait_state(j, ("done",), timeout=20)
            await asyncio.sleep(0.5)
            row = await e.row(j)
            assert row["attempts"] == 2 and row["exec_epoch"] == 2
            stats = (await e.api.tool("tool_stats", thread_id=j))["counts"]
            assert stats.get("job_finish", 0) == 1
    asyncio.run(t())
