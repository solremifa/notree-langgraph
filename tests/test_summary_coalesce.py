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


# ── 리뷰에서 재현된 문제 (2026-10-18) ───────────────────────────────
async def _start(e, job_id, worker="w", lease=5.0):
    return await e.api.tool("job_start", job_id=job_id, worker=worker, lease_sec=lease)


def test_permit_of_another_job_cannot_publish_or_read_a_lecture(tmp_path):
    """Alice의 다른 작업(echo) permit, Alice의 다른 강의 요약 permit으로 Bob의 강의 요약을 바꾸거나 읽지 못한다.
    이미 저장된 결과를 돌려주는 duplicate 경로도 강의 연결을 확인한다."""
    async def t():
        async with Env(tmp_path, allocator=False) as e:
            await e.api.tool("lecture_start", lecture_id="alice-L", user_id="alice")
            await e.api.tool("lecture_start", lecture_id="bob-L", user_id="bob")
            await e.api.tool("transcript_commit", lecture_id="bob-L", user_id="bob", segments=segs(1, 2))
            ja = (await e.api.tool("transcript_commit", lecture_id="alice-L", user_id="alice",
                                   segments=segs(1, 2)))["enqueued_job_id"]
            echo = (await e.api.tool("job_create", kind="echo", payload={}, user_id="alice"))["job_id"]
            for job in (echo, ja):
                p = (await _start(e, job, lease=0.01))["permit"]
                with pytest.raises(ToolError) as ex:
                    await e.api.tool("summary_publish", idem_key=job, lecture_id="bob-L", expected_generation=1,
                                     from_seq=1, upto_seq=2, text="탈취", permit=p)
                assert ex.value.code == "permit_invalid"
                with pytest.raises(ToolError):
                    await e.api.tool("transcript_range", lecture_id="bob-L", permit=p)
            bob = await e.api.tool("lecture_get", lecture_id="bob-L", user_id="bob")
            assert bob["processed_until"] == 0 and bob["rolling_summary"] == "" and bob["attempted_until"] == 0
            await asyncio.sleep(0.05)                                 # 이전 lease 만료 뒤 다시 시작해 새 permit
            pa = (await _start(e, ja, worker="w2"))["permit"]
            await e.api.tool("transcript_range", lecture_id="alice-L", permit=pa)
            ok = await e.api.tool("summary_publish", idem_key=ja, lecture_id="alice-L", expected_generation=1,
                                  from_seq=1, upto_seq=2, text="정상", permit=pa)
            assert ok["status"] == "published"
            with pytest.raises(ToolError) as ex:                      # 같은 idem_key로 다른 강의 → duplicate로 새지 않음
                await e.api.tool("summary_publish", idem_key=ja, lecture_id="bob-L", expected_generation=1,
                                 from_seq=1, upto_seq=2, text="정상", permit=pa)
            assert ex.value.code == "permit_invalid"
    asyncio.run(t())


def test_resending_the_same_batch_does_not_reregister_a_failed_summary(tmp_path):
    async def t():
        async with Env(tmp_path, allocator=False) as e:
            await e.api.tool("lecture_start", lecture_id=L, user_id=U)
            j = (await e.api.tool("transcript_commit", lecture_id=L, user_id=U, segments=segs(1, 1)))["enqueued_job_id"]
            p = (await _start(e, j))["permit"]
            await e.api.tool("transcript_range", lecture_id=L, permit=p)
            await e.api.tool("job_fail", job_id=j, worker="w", reason="요약 실패", user_retryable=False, permit=p)
            assert (await e.api.tool("summary_recover"))["registered"] == []
            for batch in (segs(1, 1), []):                             # 재전송·빈 묶음
                r = await e.api.tool("transcript_commit", lecture_id=L, user_id=U, segments=batch)
                assert r["enqueued_job_id"] is None and r["added"] == 0 and not r["coalesced"]
            assert len(summary_jobs(e)) == 1
            r = await e.api.tool("transcript_commit", lecture_id=L, user_id=U, segments=segs(1, 2))   # 2번이 새로 옴
            assert r["added"] == 1 and r["enqueued_job_id"]
    asyncio.run(t())


def test_stale_permit_cannot_move_attempted_until(tmp_path):
    async def t():
        async with Env(tmp_path, allocator=False) as e:
            await e.api.tool("lecture_start", lecture_id=L, user_id=U)
            j = (await e.api.tool("transcript_commit", lecture_id=L, user_id=U, segments=segs(1, 2)))["enqueued_job_id"]
            old = (await _start(e, j, worker="A", lease=0.01))["permit"]
            await asyncio.sleep(0.05)
            new = (await _start(e, j, worker="B"))["permit"]           # B가 새 실행권 (실행 세대 2)
            await e.api.tool("transcript_commit", lecture_id=L, user_id=U, segments=segs(3, 3))
            with pytest.raises(ToolError) as ex:
                await e.api.tool("transcript_range", lecture_id=L, permit=old)
            assert ex.value.code == "permit_invalid"
            assert (await lec(e))["attempted_until"] == 0
            assert (await e.api.tool("transcript_range", lecture_id=L, permit=new))["last_seq"] == 3
            assert (await lec(e))["attempted_until"] == 3
    asyncio.run(t())


# ── 리뷰 2차 (2026-10-18): 사용자 재시도·저장 응답 유실 ───────────────
def test_user_retry_does_not_collide_with_a_follow_up_summary(tmp_path):
    async def t():
        async with Env(tmp_path, allocator=False) as e:
            await e.api.tool("lecture_start", lecture_id=L, user_id=U)
            a = (await e.api.tool("transcript_commit", lecture_id=L, user_id=U, segments=segs(1, 1)))["enqueued_job_id"]
            p = (await _start(e, a))["permit"]
            await e.api.tool("job_fail", job_id=a, worker="w", reason="요약 실패", user_retryable=True, permit=p)
            b = (await e.api.tool("transcript_commit", lecture_id=L, user_id=U, segments=segs(2, 2)))["enqueued_job_id"]
            assert b
            r = await e.api.tool("job_retry", job_id=a)               # 오류 없이, 되살리지 않고 이유를 알려 줌
            assert r["revived"] is False and "후속" in r["reason"]
            assert [s for _, s in summary_jobs(e)] == ["failed", "pending"]
    asyncio.run(t())


def test_user_retry_revives_only_when_no_other_summary_exists(tmp_path):
    async def t():
        async with Env(tmp_path, allocator=False) as e:
            await e.api.tool("lecture_start", lecture_id=L, user_id=U)
            a = (await e.api.tool("transcript_commit", lecture_id=L, user_id=U, segments=segs(1, 1)))["enqueued_job_id"]
            p = (await _start(e, a))["permit"]
            await e.api.tool("job_fail", job_id=a, worker="w", reason="요약 실패", user_retryable=True, permit=p)
            assert (await e.api.tool("job_retry", job_id=a))["revived"] is True     # 다른 요약이 없으면 되살림
            p = (await _start(e, a, worker="w2"))["permit"]
            await e.api.tool("job_fail", job_id=a, worker="w2", reason="요약 실패", user_retryable=True, permit=p)
            await e.api.tool("lecture_end", lecture_id=L, user_id=U)
            r = await e.api.tool("job_retry", job_id=a)               # 녹음이 끝났으면 되살리지 않음
            assert r["revived"] is False and "녹음" in r["reason"]
    asyncio.run(t())


class LostResponseMcp:
    """지정한 도구를 실제로 실행한 뒤, 응답만 잃어버린 것처럼 처음 n번 예외를 던진다."""

    def __init__(self, inner, lose: dict[str, int]):
        self.inner, self.lose = inner, dict(lose)

    async def tool(self, name, **args):
        res = await self.inner.tool(name, **args)
        if self.lose.get(name, 0) > 0:
            self.lose[name] -= 1
            raise ToolError(name, "unavailable", "응답 유실 (주입)", True)
        return res

    def __getattr__(self, item):
        return getattr(self.inner, item)


def test_lost_publish_response_ends_as_done_not_failed(tmp_path):
    async def t():
        async with Env(tmp_path) as e:
            e.start_worker(cfg=RT, mcp=LostResponseMcp(e.m_w, {"summary_publish": 1}))
            await e.api.tool("lecture_start", lecture_id=L, user_id=U)
            j = (await e.api.tool("transcript_commit", lecture_id=L, user_id=U, segments=segs(1, 3)))["enqueued_job_id"]
            assert await e.wait_state(j)
            assert (await e.row(j))["state"] == "done"                 # 요약은 저장됐으니 작업도 완료
            assert (await lec(e))["processed_until"] == 3 and ranges(e) == [(1, 3)]
            assert LS.CALLS == [(1, 3)]                                # LLM은 한 번만
    asyncio.run(t())


# ── 리뷰 3차 (2026-10-18): 재시도와 커밋이 동시에 ───────────────────
def test_retry_and_commit_racing_never_break_the_pending_rule(tmp_path):
    """사용자 재시도(MCP 서버 A)와 새 전사 커밋(MCP 서버 B)을 동시에 30번. 매번 오류 없이, 대기 요약은 많아야 1개."""
    async def t():
        async with Env(tmp_path, allocator=False) as e:
            async with McpClient(env=e.env) as m2:
                await e.api.tool("lecture_start", lecture_id=L, user_id=U)
                seq = 0
                for k in range(30):
                    seq += 1
                    j = (await e.api.tool("transcript_commit", lecture_id=L, user_id=U,
                                          segments=segs(seq, seq)))["enqueued_job_id"]
                    p = (await _start(e, j, worker=f"w{k}"))["permit"]
                    await e.api.tool("job_fail", job_id=j, worker=f"w{k}", reason="실패", user_retryable=True, permit=p)
                    seq += 1
                    r, c = await asyncio.gather(e.api.tool("job_retry", job_id=j),
                                                m2.tool("transcript_commit", lecture_id=L, user_id=U, segments=segs(seq, seq)))
                    assert len(summary_jobs(e, ("pending",))) == 1
                    assert r["revived"] != bool(c["enqueued_job_id"])      # 둘 중 정확히 하나만 대기 요약을 만듦
                    for jid, st in summary_jobs(e, ("pending",)):            # 다음 바퀴를 위해 정리
                        p = (await _start(e, jid, worker=f"x{k}"))["permit"]
                        await e.api.tool("job_finish", job_id=jid, worker=f"x{k}", permit=p)
    asyncio.run(t())


def test_id_collision_is_not_mistaken_for_a_pending_summary(tmp_path, monkeypatch):
    """리뷰 6차 ④: 요약 등록의 IntegrityError는 '대기 요약이 이미 있음'일 때만 합치기로 본다.
    작업 ID가 겹치면(앞 8자리만 쓰던 때 가능) 조용히 합쳐진 척하지 않고 오류. ID는 이제 UUID 전체."""
    import sqlite3
    import uuid

    from notree.mcp_server import server as srv
    monkeypatch.setattr(srv, "DB", tmp_path / "ids.db")
    srv._init_db()
    with srv.tx() as con:
        con.execute("INSERT INTO lectures(lecture_id, user_id, state, updated_at) VALUES('L','u','recording',0)")
        con.execute("INSERT INTO lectures(lecture_id, user_id, state, updated_at) VALUES('M','u','recording',0)")
        first = srv._enqueue_summary(con, con.execute("SELECT * FROM lectures WHERE lecture_id='L'").fetchone())
        assert first and len(first) == len("op-") + 32
        assert srv._enqueue_summary(con, con.execute("SELECT * FROM lectures WHERE lecture_id='L'").fetchone()) is None
    fixed = uuid.UUID(first[3:])
    monkeypatch.setattr(srv.uuid, "uuid4", lambda: fixed)         # 다른 강의인데 같은 작업 ID가 나옴
    with pytest.raises(sqlite3.IntegrityError, match="jobs.job_id"):
        with srv.tx() as con:
            srv._enqueue_summary(con, con.execute("SELECT * FROM lectures WHERE lecture_id='M'").fetchone())
