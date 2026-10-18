"""실시간 요약: 쌓지 않고 범위를 합친다 (설계 v0.2 ③).

  - 강의당 대기 요약 1개 (DB 유일 인덱스) → 동시 커밋도 1건만 등록
  - 같은 강의 요약은 동시에 하나만 실행 (배정 조건) → 대기 1개가 그사이 들어온 전사를 한꺼번에 읽음
  - 요약 저장과 위치 이동은 한 트랜잭션, 위치는 앞으로만
  - 녹음 종료(세대 +1) 뒤 늦은 요약은 저장 안 됨
  - 누락 복구는 '아무도 읽어 가지 않은 전사'가 있을 때만 → 실패한 구간을 혼자 끝없이 다시 등록하지 않음
"""
import asyncio
import dataclasses
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from notree.common import settings as S  # noqa: E402
from notree.contracts.envelope import ToolError  # noqa: E402
from notree.worker.handlers import live_summary as LS  # noqa: E402
from notree.worker.mcp_client import McpClient  # noqa: E402
from test_skeleton import Env  # noqa: E402

RT = dataclasses.replace(S.RT, lease_sec=1.0, heartbeat_sec=0.3, claim_idle_ms=1200, job_timeout_sec=20)
U, L = "u-1", "lec-1"


@pytest.fixture(autouse=True)
def _reset(monkeypatch):
    LS.CALLS.clear()
    monkeypatch.setattr(LS, "FAKE_DELAY", 0.0)


def segs(a, b, text="말"):
    return [{"seq": i, "text": f"{text}{i}"} for i in range(a, b + 1)]


def summary_jobs(e, states=None):
    con = sqlite3.connect(e.db)
    q = "SELECT job_id, state FROM jobs WHERE kind='live_summary' AND lecture_id=?"
    rows = con.execute(q + " ORDER BY created", (L,)).fetchall()
    con.close()
    return [r for r in rows if states is None or r[1] in states]


def ranges(e):
    con = sqlite3.connect(e.db)
    rows = con.execute("SELECT from_seq, upto_seq FROM summaries WHERE lecture_id=? ORDER BY version", (L,)).fetchall()
    con.close()
    return rows


async def lec(e):
    return await e.api.tool("lecture_get", lecture_id=L, user_id=U)


async def wait_lec(e, pred, timeout=15):
    for _ in range(int(timeout / 0.1)):
        x = await lec(e)
        if pred(x):
            return x
        await asyncio.sleep(0.1)
    raise AssertionError(f"시간 초과: {await lec(e)}")


def test_commits_coalesce_into_one_pending(tmp_path):
    async def t():
        async with Env(tmp_path, allocator=False) as e:
            await e.api.tool("lecture_start", lecture_id=L, user_id=U)
            first = await e.api.tool("transcript_commit", lecture_id=L, user_id=U, segments=segs(1, 3))
            assert first["enqueued_job_id"] and not first["coalesced"]
            for a, b in ((4, 5), (6, 6), (5, 7)):                     # (5,7): 5·6은 다시 보낸 것 (멱등)
                r = await e.api.tool("transcript_commit", lecture_id=L, user_id=U, segments=segs(a, b))
                assert r["coalesced"] and r["enqueued_job_id"] is None
            assert (await lec(e))["saved_until"] == 7
            assert len(summary_jobs(e)) == 1
            with pytest.raises(ToolError) as ex:                        # 빈틈은 거절
                await e.api.tool("transcript_commit", lecture_id=L, user_id=U, segments=segs(9, 9))
            assert ex.value.code == "invalid_args"
            con = sqlite3.connect(e.db)                                 # DB 제약 자체: 대기 2개는 못 들어감
            with pytest.raises(sqlite3.IntegrityError):
                con.execute("INSERT INTO jobs(job_id, kind, state, lecture_id) VALUES('x','live_summary','pending',?)", (L,))
            con.close()
    asyncio.run(t())


def test_concurrent_commits_register_one_job(tmp_path):
    """MCP 서버 프로세스 3개(같은 DB)에서 같은 묶음을 동시에 여러 번 보냄 (gateway 재전송 흉내)."""
    async def t():
        async with Env(tmp_path, allocator=False) as e:
            await e.api.tool("lecture_start", lecture_id=L, user_id=U)
            async with McpClient(env=e.env) as m2, McpClient(env=e.env) as m3:
                clients = [e.api, m2, m3]
                for a, b in ((1, 4), (5, 9), (10, 12)):
                    res = await asyncio.gather(*[c.tool("transcript_commit", lecture_id=L, user_id=U, segments=segs(a, b))
                                                 for c in clients for _ in range(4)])
                    assert all(r["saved_until"] == b for r in res)
            assert len(summary_jobs(e)) == 1 and (await lec(e))["saved_until"] == 12
    asyncio.run(t())


def test_follow_up_waits_while_summary_is_in_flight(tmp_path):
    async def t():
        async with Env(tmp_path, allocator=False) as e:
            await e.api.tool("lecture_start", lecture_id=L, user_id=U)
            j1 = (await e.api.tool("transcript_commit", lecture_id=L, user_id=U, segments=segs(1, 2)))["enqueued_job_id"]
            got = (await e.api.tool("job_reserve_pending", stream=RT.stream))["jobs"]
            assert [x["job_id"] for x in got] == [j1]
            j2 = (await e.api.tool("transcript_commit", lecture_id=L, user_id=U, segments=segs(3, 4)))["enqueued_job_id"]
            assert j2 and j2 != j1                                      # 실행 중 1 + 대기 1
            assert (await e.api.tool("transcript_commit", lecture_id=L, user_id=U, segments=segs(5, 5)))["coalesced"]
            assert (await e.api.tool("job_reserve_pending", stream=RT.stream))["jobs"] == []   # j1이 끝날 때까지 허가 안 함
            st = await e.api.tool("job_start", job_id=j1, worker="w", lease_sec=5)
            await e.api.tool("job_finish", job_id=j1, worker="w", permit=st["permit"])
            got = (await e.api.tool("job_reserve_pending", stream=RT.stream))["jobs"]
            assert [x["job_id"] for x in got] == [j2]
    asyncio.run(t())


def test_commits_during_a_summary_become_one_follow_up(tmp_path, monkeypatch):
    monkeypatch.setattr(LS, "FAKE_DELAY", 1.0)

    async def t():
        async with Env(tmp_path) as e:
            e.start_worker(cfg=RT)
            await e.api.tool("lecture_start", lecture_id=L, user_id=U)
            await e.api.tool("transcript_commit", lecture_id=L, user_id=U, segments=segs(1, 5))
            await wait_lec(e, lambda x: x["attempted_until"] == 5)      # 첫 요약이 1~5를 읽고 요약 중
            for i in range(6, 16):                                      # 그사이 커밋 10번
                await e.api.tool("transcript_commit", lecture_id=L, user_id=U, segments=segs(i, i))
            x = await wait_lec(e, lambda x: x["processed_until"] == 15)
            await asyncio.sleep(0.3)
            assert len(summary_jobs(e)) == 2 and LS.CALLS == [(1, 5), (6, 15)]
            assert ranges(e) == [(1, 5), (6, 15)]
            assert "[1-5]" in x["rolling_summary"] and "[6-15]" in x["rolling_summary"]
    asyncio.run(t())


def test_commit_and_completion_at_the_same_time(tmp_path, monkeypatch):
    """전사가 0.05초마다 들어오는 동안 요약(0.3초)이 계속 끝남 → 구간이 겹치거나 빠지지 않고, 작업은 커밋보다 훨씬 적다."""
    monkeypatch.setattr(LS, "FAKE_DELAY", 0.3)

    async def t():
        async with Env(tmp_path) as e:
            e.start_worker(cfg=RT)
            await e.api.tool("lecture_start", lecture_id=L, user_id=U)
            n = 60
            for i in range(1, n + 1):
                await e.api.tool("transcript_commit", lecture_id=L, user_id=U, segments=segs(i, i))
                await asyncio.sleep(0.05)
            await wait_lec(e, lambda x: x["processed_until"] == n, timeout=20)
            await asyncio.sleep(0.3)
            rs = ranges(e)
            assert rs[0][0] == 1 and rs[-1][1] == n
            assert all(rs[k][1] + 1 == rs[k + 1][0] for k in range(len(rs) - 1))   # 빈틈·겹침 없음
            print(f"\n커밋 {n}번 → 요약 {len(rs)}번: {rs}")
            # 앞 요약이 배정만 되고 아직 읽기 전에 커밋이 오면 대기 1건이 더 생기고, 앞 요약이 그 전사까지 읽어 가면
            # 뒤 요약은 할 일이 없어 LLM을 부르지 않고 끝난다 → 작업 수 ≥ 요약 수, LLM 호출 수 = 요약 수
            assert len(LS.CALLS) == len(rs) <= len(summary_jobs(e)) < n / 3
            assert summary_jobs(e, ("pending", "reserved", "sent", "running")) == []
    asyncio.run(t())


def test_summary_running_when_lecture_ends_is_not_saved(tmp_path, monkeypatch):
    monkeypatch.setattr(LS, "FAKE_DELAY", 1.0)

    async def t():
        async with Env(tmp_path) as e:
            e.start_worker(cfg=RT)
            await e.api.tool("lecture_start", lecture_id=L, user_id=U)
            j = (await e.api.tool("transcript_commit", lecture_id=L, user_id=U, segments=segs(1, 3)))["enqueued_job_id"]
            await wait_lec(e, lambda x: x["attempted_until"] == 3)
            ended = await e.api.tool("lecture_end", lecture_id=L, user_id=U)
            assert ended["state"] == "ended" and ended["generation"] == 2
            assert await e.wait_state(j, ("failed",))
            assert "generation_mismatch" in (await e.row(j))["fail_reason"]
            x = await lec(e)
            assert x["processed_until"] == 0 and x["rolling_summary"] == "" and ranges(e) == []
            with pytest.raises(ToolError) as ex:
                await e.api.tool("transcript_commit", lecture_id=L, user_id=U, segments=segs(4, 4))
            assert ex.value.code == "generation_mismatch"
            assert (await e.api.tool("summary_recover"))["registered"] == []
    asyncio.run(t())


def test_failed_range_is_retried_by_the_next_summary_not_by_recovery(tmp_path, monkeypatch):
    orig = LS.summarize
    fails = []

    async def flaky(rolling, new_text, a, b):
        if not fails:
            fails.append((a, b))
            LS.CALLS.append((a, b))
            raise LS.RetryableError("요약 LLM 실패 (시험)")
        return await orig(rolling, new_text, a, b)

    monkeypatch.setattr(LS, "summarize", flaky)

    async def t():
        async with Env(tmp_path) as e:
            e.start_worker(cfg=RT)
            await e.api.tool("lecture_start", lecture_id=L, user_id=U)
            j = (await e.api.tool("transcript_commit", lecture_id=L, user_id=U, segments=segs(1, 2)))["enqueued_job_id"]
            assert await e.wait_state(j, ("failed",))                    # 실시간 요약은 총 1회
            for _ in range(3):                                          # 새 전사가 없으면 복구가 다시 등록하지 않음
                assert (await e.api.tool("summary_recover"))["registered"] == []
            await e.api.tool("transcript_commit", lecture_id=L, user_id=U, segments=segs(3, 3))
            await wait_lec(e, lambda x: x["processed_until"] == 3)
            assert ranges(e) == [(1, 3)]                                # 실패 구간 1~2를 함께 처리
    asyncio.run(t())


def test_recovery_registers_once_when_a_job_is_lost(tmp_path):
    async def t():
        async with Env(tmp_path, allocator=False) as e:
            await e.api.tool("lecture_start", lecture_id=L, user_id=U)
            j = (await e.api.tool("transcript_commit", lecture_id=L, user_id=U, segments=segs(1, 3)))["enqueued_job_id"]
            con = sqlite3.connect(e.db)                                 # 등록된 작업이 사라졌다고 가정
            con.execute("DELETE FROM jobs WHERE job_id=?", (j,))
            con.commit()
            con.close()
            got = (await e.api.tool("summary_recover"))["registered"]
            assert len(got) == 1
            assert (await e.api.tool("summary_recover"))["registered"] == []   # 대기 작업이 있으니 다시 안 함
            assert len(summary_jobs(e)) == 1
    asyncio.run(t())
