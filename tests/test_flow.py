"""같은 시나리오를 두 저장 방식으로 돌린다.
  memory : LangGraph 기본 InMemorySaver (기준선)
  mcp    : A안. 체크포인트·발행을 MCP 서버 프로세스의 도구로만 처리
"""
import asyncio
import os
import sqlite3
import sys
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from langgraph.checkpoint.memory import InMemorySaver

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import llm  # noqa: E402
from graph import FileNoteStore, build_graph  # noqa: E402
from mcp_saver import McpCheckpointSaver, McpClient, McpNoteStore  # noqa: E402


@pytest.fixture(autouse=True)
def reset_calls():
    llm.CALLS.clear()


@asynccontextmanager
async def backend(kind, tmp_path):
    if kind == "mcp":
        env = {**os.environ, "NOTREE_DB": str(tmp_path / "notree.db")}
        async with McpClient(env=env) as c:
            yield McpCheckpointSaver(c), McpNoteStore(c), c
    else:
        yield InMemorySaver(), FileNoteStore(str(tmp_path)), None


def cfg(tmp_path, thread, notes, **kw):
    return {"configurable": {"thread_id": thread, "out_dir": str(tmp_path), "llm": "fake",
                             "notes": notes, **kw}}


async def go(app, inp, config):
    return await app.ainvoke(inp, config, durability="sync")


KINDS = ["memory", "mcp"]


@pytest.mark.parametrize("kind", KINDS)
def test_happy_path(tmp_path, kind):
    async def t():
        async with backend(kind, tmp_path) as (saver, notes, _):
            out = await go(build_graph(saver), {"sources": ["hello", "world"]}, cfg(tmp_path, "t1", notes))
            assert out["draft"] == "HELLO WORLD!" and out["status"] == "완료"
    asyncio.run(t())


@pytest.mark.parametrize("kind", KINDS)
def test_retry_and_needs_review_branches(tmp_path, kind):
    async def t():
        async with backend(kind, tmp_path) as (saver, notes, _):
            app = build_graph(saver)
            once = await go(app, {"sources": ["hello", "world"]}, cfg(tmp_path, "t2", notes, sloppy="once"))
            assert once["attempt"] == 2 and once["status"] == "완료"
            always = await go(app, {"sources": ["hello", "world"]}, cfg(tmp_path, "t3", notes, sloppy="always"))
            assert always["status"] == "확인 필요" and always["issues"] == ["WORLD"]
    asyncio.run(t())


@pytest.mark.parametrize("kind", KINDS)
def test_resume_does_not_redo_finished_extracts(tmp_path, kind):
    async def t():
        async with backend(kind, tmp_path) as (saver, notes, _):
            app = build_graph(saver)
            with pytest.raises(RuntimeError):
                await go(app, {"sources": ["hello", "big", "world"]},
                         cfg(tmp_path, "t4", notes, crash_at="extract:world"))
            assert sorted(llm.CALLS) == ["extract:big", "extract:hello"]
        llm.CALLS.clear()
        # 새 연결(= 다른 worker가 작업을 이어받음)으로 이어하기
        async with backend(kind, tmp_path) as (saver2, notes2, _):
            if kind == "memory":
                saver2 = saver                               # 메모리 저장기는 프로세스 안에서만 유지됨
            out = await go(build_graph(saver2), None, cfg(tmp_path, "t4", notes2))
            assert llm.CALLS == ["extract:world", "merge#1"]  # 끝난 extract는 다시 안 부름
            assert out["draft"] == "HELLO BIG WORLD!"
    asyncio.run(t())


@pytest.mark.parametrize("kind", KINDS)
def test_publish_crash_does_not_double_publish(tmp_path, kind):
    async def t():
        async with backend(kind, tmp_path) as (saver, notes, client):
            app = build_graph(saver)
            with pytest.raises(RuntimeError):
                await go(app, {"sources": ["hello", "world"]}, cfg(tmp_path, "t5", notes, crash_at="publish"))
            await go(app, None, cfg(tmp_path, "t5", notes))
            if client:
                assert (await client.tool("tool_stats", thread_id="t5"))["counts"]["note_publish"] == 2
                assert (await client.tool("note_get", idem_key="t5"))["note"] == "HELLO WORLD!"
    asyncio.run(t())


def test_worker_never_opens_db_in_mcp_mode(tmp_path, monkeypatch):
    """A안의 핵심: worker 프로세스에서 sqlite3.connect를 막아도 그래프가 끝까지 돈다.
    (DB는 MCP 서버 프로세스만 연다)"""
    def blocked(*a, **k):
        raise AssertionError("worker가 DB에 직접 접속하려 함")
    monkeypatch.setattr(sqlite3, "connect", blocked)

    async def t():
        async with backend("mcp", tmp_path) as (saver, notes, client):
            out = await go(build_graph(saver), {"sources": ["hello", "world"]}, cfg(tmp_path, "t6", notes))
            assert out["status"] == "완료"
            assert set(client.calls) >= {"checkpoint_put", "checkpoint_put_writes", "checkpoint_get", "note_publish"}
    asyncio.run(t())
    assert (tmp_path / "notree.db").exists()
