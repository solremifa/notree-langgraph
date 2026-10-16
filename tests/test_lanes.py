"""레인 분리: Q&A·채점은 qa 레인, 실시간 요약은 rt 레인. 빌려 쓰기는 자기 몫 슬롯을 남긴다."""
import asyncio
import dataclasses
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from notree.common import settings as S  # noqa: E402
from notree.worker.loop import Worker  # noqa: E402
from test_skeleton import Env, run  # noqa: E402

FAST = dict(lease_sec=1.0, heartbeat_sec=0.3, claim_idle_ms=1200, job_timeout_sec=5, max_attempts=2)
QA = dataclasses.replace(S.QA, concurrency=2, **FAST)
RT = dataclasses.replace(S.RT, concurrency=2, **FAST)


async def job(e, stream, **payload):
    return (await e.api.tool("job_create", kind="echo", payload=payload, user_id="u-test",
                             stream=stream.stream))["job_id"]


async def pending(e, cfg):
    return (await e.r.xpending(cfg.stream, S.GROUP))["pending"]


def test_kinds_route_to_lanes(tmp_path):
    async def t():
        async with Env(tmp_path, allocator=False) as e:
            for kind, stream in [("qa", "notree:qa"), ("grading", "notree:qa"),
                                 ("live_summary", "notree:rt"), ("final_note", "notree:batch")]:
                jid = (await e.api.tool("job_create", kind=kind, payload={}, user_id="u"))["job_id"]
                assert (await e.row(jid))["stream"] == stream, kind
    run(t())


def test_qa_is_not_blocked_by_rt_backlog(tmp_path):
    """rt가 밀려 있어도 qa 작업은 바로 처리된다 (실측: 800명에서 38.8초 → 4.1초의 원리)."""
    async def t():
        async with Env(tmp_path) as e:
            e.start_worker(RT, name="r")
            e.start_worker(QA, name="q")
            backlog = [await job(e, RT, sleep=1.5) for _ in range(8)]   # rt 슬롯 2개 → 6초 넘게 밀림
            await asyncio.sleep(0.3)
            t0 = time.time()
            q = await job(e, QA, text="질문")
            assert await e.wait_state(q, timeout=5)
            assert time.time() - t0 < 1.5
            states = [(await e.row(j))["state"] for j in backlog]
            assert states.count("done") < len(backlog)                  # rt는 아직 밀려 있음
    run(t())


def test_fixed_lane_does_not_take_other_work(tmp_path):
    async def t():
        async with Env(tmp_path) as e:
            e.start_worker(QA, name="q")                                  # 빌려 쓰기 없음 (기본)
            r = await job(e, RT, text="요약")
            await asyncio.sleep(1.0)
            assert (await e.row(r))["state"] != "done"
    run(t())


def test_borrow_takes_idle_slots_but_keeps_own_share(tmp_path):
    """qa가 비면 rt 작업을 빌려 쓰되 borrow_slots개까지만. 나머지 슬롯은 qa 몫으로 남는다."""
    async def t():
        async with Env(tmp_path) as e:
            qa3 = dataclasses.replace(QA, concurrency=3)
            w = e.start_worker(qa3, name="q", borrow=RT, borrow_slots=2)
            rts = [await job(e, RT, sleep=1.0) for _ in range(5)]
            peak = 0
            t_end = time.time() + 1.0
            while time.time() < t_end:                                    # 빌린 작업 수가 2를 넘지 않는지
                peak = max(peak, w.borrowed)
                assert w.borrowed <= 2
                await asyncio.sleep(0.02)
            assert peak == 2
            t0 = time.time()
            q = await job(e, QA, text="질문")                             # 빌린 2개가 도는 중에도 바로
            assert await e.wait_state(q, timeout=3)
            assert time.time() - t0 < 1.0
            for j in rts:
                assert await e.wait_state(j, timeout=10)
            assert await pending(e, RT) == 0 and await pending(e, QA) == 0  # 빌린 작업은 rt Stream에서 ACK
    run(t())


def test_borrowed_job_uses_its_own_lane_settings(tmp_path):
    """빌린 작업의 시도 한도·재배송은 원래 레인(rt) 설정을 따른다."""
    async def t():
        async with Env(tmp_path) as e:
            rt1 = dataclasses.replace(RT, max_attempts=1)
            e.start_worker(QA, name="q", borrow=rt1, borrow_slots=1)
            j = await job(e, rt1, fail="always")
            assert await e.wait_state(j, timeout=5)
            row = await e.row(j)
            assert row["state"] == "failed" and row["attempts"] == 1
    run(t())


def test_borrow_slots_never_take_the_last_slot():
    w_cfg = dataclasses.replace(QA, concurrency=2)
    w = Worker("x", w_cfg, None, None, borrow=RT, borrow_slots=5)
    assert w.borrow_slots == 1
    assert Worker("x", w_cfg, None, None, borrow=RT, borrow_slots=0).borrow is None
