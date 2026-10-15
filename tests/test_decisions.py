"""2026-10-15 팀 결정 사항이 코드에 반영됐는지 확인한다 (docs/team-decisions.md)."""
import asyncio
import dataclasses
import os
import re
import sqlite3
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import llm  # noqa: E402
from notree.common import settings as S  # noqa: E402
from notree.contracts.envelope import ToolError  # noqa: E402
from notree.contracts.tools import NO_PERMIT, TOOLS  # noqa: E402
from notree.dev import gen_tool_docs  # noqa: E402
from notree.scheduler.periodic import run_periodic  # noqa: E402
from notree.worker.mcp_client import McpClient  # noqa: E402
from notree.worker.registry import JobContext, ToolRejected, ToolUnavailable  # noqa: E402
from test_skeleton import RT, Env  # noqa: E402

BATCH = dataclasses.replace(S.BATCH, lease_sec=1.0, heartbeat_sec=0.3, claim_idle_ms=1200, job_timeout_sec=10)


async def batch_job(e: Env, payload: dict) -> str:
    return (await e.api.tool("job_create", kind="course_update", payload=payload, user_id="u-test"))["job_id"]


# ── 8번: 계약 ────────────────────────────────────────────────────
def test_server_tools_match_contract():
    os.environ.setdefault("NOTREE_DB", "/tmp/notree-contract-test.db")
    import mcp_server

    tools = {t.name: t for t in asyncio.run(mcp_server.mcp.list_tools())}
    implemented = {n for n, s in TOOLS.items() if s.status == "implemented"}
    assert set(tools) == implemented, "서버 도구와 계약(implemented)이 다름"
    for name in implemented:
        params = set(tools[name].inputSchema["properties"])
        expected = set(TOOLS[name].input.model_fields) | (set() if name in NO_PERMIT else {"permit"})
        assert params == expected, f"{name}: 서버 인자 {params} ≠ 계약 {expected}"


def test_tool_names_are_openai_compatible():
    for name in TOOLS:
        assert re.fullmatch(r"[a-z][a-z0-9_]{0,63}", name), f"이름 규칙 위반: {name}"


def test_contract_docs_are_up_to_date():
    assert gen_tool_docs.OUT.read_text("utf-8") == gen_tool_docs.render(), \
        "uv run python -m notree.dev.gen_tool_docs 로 문서를 다시 생성하세요"


def test_ctx_tool_maps_rejections_by_retryable():
    class FakeMcp:
        def __init__(self, retryable):
            self.retryable = retryable

        async def tool(self, name, **args):
            raise ToolError(name, "locked" if self.retryable else "generation_mismatch", "x", self.retryable)

    def ctx(retryable):
        return JobContext(job_id="j", kind="k", payload={}, attempt=1, permit="p", worker="w", cfg=RT,
                          mcp=FakeMcp(retryable))

    with pytest.raises(ToolUnavailable):
        asyncio.run(ctx(True).tool("summary_publish"))
    with pytest.raises(ToolRejected):
        asyncio.run(ctx(False).tool("summary_publish"))


def test_duplicate_publish_is_success(tmp_path):
    async def t():
        env = {**os.environ, "NOTREE_DB": str(tmp_path / "d.db"), "NOTREE_REQUIRE_PERMIT": "0"}
        async with McpClient(env=env) as m:
            first = await m.call("note_publish", idem_key="k1", course="c", note="n", status="완료")
            second = await m.call("note_publish", idem_key="k1", course="c", note="n", status="완료")
            assert first == {"ok": True, "data": {"idem_key": "k1"}}
            assert second == {"ok": True, "data": {"idem_key": "k1"}, "duplicate": True}
    asyncio.run(t())


# ── 2번: 마감 시각 ───────────────────────────────────────────────
def test_deadline_already_passed_fails_without_running(tmp_path, capsys):
    async def t():
        async with Env(tmp_path) as e:
            e.start_worker()
            j = await e.job({"text": "should not run", "deadline_at": time.time() - 1})
            assert await e.wait_state(j)
            row = await e.row(j)
            assert row["state"] == "failed" and row["attempts"] == 1
            assert row["fail_reason"] == "마감 시각 지남" and row["user_retryable"] is True
    asyncio.run(t())
    assert "should not run" not in capsys.readouterr().out


def test_deadline_cuts_running_job_and_skips_retry(tmp_path):
    async def t():
        async with Env(tmp_path) as e:
            e.start_worker()
            j = await e.job({"sleep": 5, "deadline_at": time.time() + 1.0})
            t0 = time.time()
            assert await e.wait_state(j)
            row = await e.row(j)
            assert time.time() - t0 < 3
            assert row["state"] == "failed" and row["attempts"] == 1          # 마감이 지났으니 재시도 안 함
            assert row["fail_reason"].startswith("마감 시각 초과")
    asyncio.run(t())


def test_retry_with_new_deadline(tmp_path):
    async def t():
        async with Env(tmp_path) as e:
            e.start_worker()
            j = await e.job({"text": "second chance", "deadline_at": time.time() - 1})
            assert await e.wait_state(j)
            assert (await e.api.tool("job_retry", job_id=j, deadline_at=time.time() + 30))["revived"]
            assert await e.wait_state(j, ("done",))
    asyncio.run(t())


# ── 3번: 작업 종류별 시도 한도 ────────────────────────────────────
def test_per_kind_max_attempts(tmp_path, monkeypatch):
    monkeypatch.setattr(S, "KIND_MAX_ATTEMPTS", {"echo": 1})

    async def t():
        async with Env(tmp_path) as e:
            e.start_worker()
            j = await e.job({"fail": "always"})
            assert await e.wait_state(j)
            row = await e.row(j)
            assert row["state"] == "failed" and row["attempts"] == 1 and "한도" in row["fail_reason"]
    asyncio.run(t())


# ── 6번: 배정기 누락 복구 ────────────────────────────────────────
def test_stale_reserved_job_is_resent(tmp_path):
    async def t():
        async with Env(tmp_path, allocator=False) as e:      # 배정기 없음 = 허가 직후 배정기가 죽은 상태
            j = await e.job({"text": "recovered"})
            await e.api.tool("job_reserve_pending")       # 허가만 하고 XADD 전에 배정기가 죽었다고 가정
            e.start_worker()
            await asyncio.sleep(1.0)
            assert (await e.row(j))["state"] == "reserved"
            e.tasks.append(asyncio.create_task(
                run_periodic(e.r, e.m_s, e.stop, resend_every=0.3, reserved_sec=0.5, sent_sec=30)))
            assert await e.wait_state(j)
            assert (await e.row(j))["state"] == "done"
    asyncio.run(t())


# ── 7번: 다시 시도 ───────────────────────────────────────────────
def test_user_retry_revives_only_retryable_failures(tmp_path):
    flag = tmp_path / "not-ready"
    flag.write_text("x")

    async def t():
        async with Env(tmp_path) as e:
            e.start_worker()
            j = await e.job({"fail_if_exists": str(flag)})
            fatal = await e.job({"fail": "fatal"})
            assert await e.wait_state(j) and await e.wait_state(fatal)
            assert (await e.row(j))["user_retryable"] is True
            flag.unlink()
            assert (await e.api.tool("job_retry", job_id=j))["revived"] is True
            refused = await e.api.tool("job_retry", job_id=fatal)
            assert refused["revived"] is False and "영구" in refused["reason"]
            assert await e.wait_state(j, ("done",))
            assert (await e.row(j))["attempts"] == 1                       # 시도 횟수 초기화
    asyncio.run(t())


def test_revived_course_update_resumes_from_checkpoint(tmp_path, monkeypatch):
    monkeypatch.setattr(S, "KIND_MAX_ATTEMPTS", {"course_update": 1})
    llm.CALLS.clear()

    async def t():
        async with Env(tmp_path) as e:
            e.start_worker(cfg=BATCH)
            j = await batch_job(e, {"sources": ["hello", "big", "world"], "crash_at": "extract:world"})
            assert await e.wait_state(j)
            assert (await e.row(j))["state"] == "failed"
            assert sorted(llm.CALLS) == ["extract:big", "extract:hello"]
            llm.CALLS.clear()
            assert (await e.api.tool("job_retry", job_id=j))["revived"]
            assert await e.wait_state(j, ("done",))
            assert llm.CALLS == ["extract:world", "merge#1"]                 # 끝난 extract는 다시 안 부름
            # 4번: 사용자 ID는 인자가 아니라 permit에서 꺼내 저장된다
            note = await e.api.tool("note_get", idem_key=j)
            assert note["note"] == "HELLO BIG WORLD!" and note["user_id"] == "u-test"
    asyncio.run(t())


# ── 9번: 체크포인트 정리 ─────────────────────────────────────────
def test_checkpoint_cleanup_removes_old_finished_jobs(tmp_path):
    async def t():
        async with Env(tmp_path) as e:
            e.start_worker(cfg=BATCH)
            j = await batch_job(e, {"sources": ["hello"]})
            assert await e.wait_state(j, ("done",))
            assert (await e.api.tool("checkpoint_cleanup", older_than_sec=3600))["deleted_threads"] == 0
            con = sqlite3.connect(e.db)
            con.execute("UPDATE jobs SET updated_at=? WHERE job_id=?", (time.time() - 7200, j))
            con.commit()
            assert (await e.api.tool("checkpoint_cleanup", older_than_sec=3600))["deleted_threads"] == 1
            assert con.execute("SELECT COUNT(*) FROM checkpoints WHERE thread_id=?", (j,)).fetchone()[0] == 0
            con.close()
    asyncio.run(t())
