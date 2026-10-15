"""worker 프로세스가 죽었을 때 다른 worker가 이어받는지 (poc-v1의 test_worker.py를 notree 구조로 옮김)."""
import asyncio
import dataclasses
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from notree.common import settings as S  # noqa: E402
from notree.graphs.course_update import llm  # noqa: E402
from notree.worker.mcp_client import McpClient  # noqa: E402
from test_skeleton import Env  # noqa: E402

BATCH = dataclasses.replace(S.BATCH, lease_sec=1.0, heartbeat_sec=0.3, claim_idle_ms=1200, job_timeout_sec=20)


async def scenario(tmp_path, payload, monkeypatch=None, slow=False):
    """worker-A가 작업을 받은 뒤 worker-B를 띄운다. (A, B, job row, note, pending, note_publish 호출 수)"""
    llm.CALLS.clear()
    async with Env(tmp_path) as e:
        async with McpClient(env=e.env) as m_b:                 # B는 별도 연결 (다른 프로세스 역할)
            a = e.start_worker(cfg=BATCH, name="A")
            await asyncio.sleep(0.3)
            if slow:
                monkeypatch.setattr(llm, "FAKE_DELAY", 1.6)     # LLM 호출 하나가 재배송 기준(1.2초)보다 오래 걸림
            j = (await e.api.tool("job_create", kind="course_update", payload=payload, user_id="u-t"))["job_id"]
            assert await e.wait_state(j, ("running", "done"))
            b = e.start_worker(cfg=BATCH, name="B", mcp=m_b)
            assert await e.wait_state(j, ("done",), timeout=20)
            row = await e.row(j)
            note = await e.api.tool("note_get", idem_key=j)
            stats = (await e.api.tool("tool_stats", thread_id=j))["counts"]
            pending = (await e.r.xpending(BATCH.stream, S.GROUP))["pending"]
            e.stop.set()
            await asyncio.gather(*e.tasks)
    return a, b, row, note, pending, stats


def test_dead_worker_job_is_taken_over_and_resumed(tmp_path):
    a, b, row, note, pending, _ = asyncio.run(
        scenario(tmp_path, {"sources": ["hello", "big", "world"], "die_at": "extract:world"}))
    assert a.dead and not b.dead
    assert row["attempts"] == 2 and row["lease_owner"] == "batch-B"
    assert note["note"] == "HELLO BIG WORLD!" and pending == 0
    # A가 끝낸 hello·big은 B가 다시 부르지 않음
    assert sorted(llm.CALLS) == ["extract:big", "extract:hello", "extract:world", "merge#1"]


def test_death_right_after_publish_does_not_double_publish(tmp_path):
    a, _, row, note, pending, stats = asyncio.run(
        scenario(tmp_path, {"sources": ["hello", "big", "world"], "die_at": "publish"}))
    assert a.dead and row["attempts"] == 2 and pending == 0
    assert note["note"] == "HELLO BIG WORLD!"
    assert stats["note_publish"] == 2                          # B가 다시 불렀지만 duplicate로 끝남
    assert llm.CALLS.count("merge#1") == 1


def test_slow_but_alive_worker_is_not_robbed(tmp_path, monkeypatch):
    a, b, row, _, pending, _ = asyncio.run(
        scenario(tmp_path, {"sources": ["hello", "big", "world"]}, monkeypatch, slow=True))
    assert not a.dead and pending == 0
    assert row["attempts"] == 1 and row["lease_owner"] == "batch-A"   # B는 재배송을 받아도 lease 때문에 손대지 않음
    assert llm.CALLS.count("extract:hello") == 1
