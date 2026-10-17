"""qa_search의 PostgreSQL 구현 (NOTREE_QA_SEARCH=pg). PostgreSQL이 없으면 건너뛴다.

    NOTREE_TEST_PG_ADMIN=postgresql://postgres@127.0.0.1:55432/postgres  (기본값)
테스트용 DB notree_qa_test를 만들어 쓴다.
"""
import asyncio
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from notree.contracts.envelope import ToolError  # noqa: E402
from notree.worker.mcp_client import McpClient  # noqa: E402

ADMIN = os.getenv("NOTREE_TEST_PG_ADMIN", "postgresql://postgres@127.0.0.1:55432/postgres")
TEST_DSN = ADMIN.rsplit("/", 1)[0] + "/notree_qa_test"


def _prepare() -> bool:
    try:
        import psycopg
        with psycopg.connect(ADMIN, autocommit=True, connect_timeout=2) as con:
            if not con.execute("SELECT 1 FROM pg_database WHERE datname='notree_qa_test'").fetchone():
                con.execute("CREATE DATABASE notree_qa_test")
        with psycopg.connect(TEST_DSN, autocommit=True) as con:
            con.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
            con.execute("DROP TABLE IF EXISTS qa_search_docs")
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _prepare(), reason="PostgreSQL 없음")

DOCS_A = [
    {"source": "note", "id": "L1:s1", "lecture_id": "L1", "text": "표준화는 평균과 표준편차로 피처의 척도를 맞춘다."},
    {"source": "note", "id": "L2:s1", "lecture_id": "L2", "text": "표준화 통계는 train에서만 계산한다. 데이터 누수 방지."},
    {"source": "transcript", "id": "L1:t1", "lecture_id": "L1", "text": "자 표준화를 할 때 평균을 빼고 표준편차로 나눕니다."},
    {"source": "transcript", "id": "L2:t1", "lecture_id": "L2", "text": "테스트 데이터에도 트레인 평균을 써야 누수가 안 생깁니다."},
    {"source": "transcript", "id": "L2:t2", "lecture_id": "L2", "text": "오늘 점심은 뭐 먹을까요."},
]
DOCS_B = [{"source": "note", "id": "B:s1", "lecture_id": "BL", "text": "표준화 평균 표준편차 (다른 사용자의 노트)"}]


def run(coro):
    return asyncio.run(coro)


async def _setup(tmp_path, dsn=TEST_DSN):
    env = {**os.environ, "NOTREE_DB": str(tmp_path / "qa.db"), "NOTREE_REQUIRE_PERMIT": "0",
           "NOTREE_QA_SEARCH": "pg", "NOTREE_PG_QA_DSN": dsn}
    return env


def test_pg_search_scope_order_and_ids(tmp_path):
    async def t():
        env = await _setup(tmp_path)
        async with McpClient(env=env) as m:
            await m.tool("qa_seed", user_id="A", course_id="cA", course_name="ML", docs=DOCS_A)
            await m.tool("qa_seed", user_id="B", course_id="cB", course_name="ML", docs=DOCS_B)
            q = await m.tool("qa_ask", user_id="A", course_id="cA", question="표준화는 어떻게 해?")
            notes = (await m.tool("qa_search", question_id=q["question_id"], source="note",
                                  keywords=["표준화", "평균", "표준편차"], limit=5))["hits"]
            assert [h["id"] for h in notes] == ["L1:s1", "L2:s1"]               # 다른 사용자(B) 문서는 안 나옴, 점수순
            assert notes[0]["score"] > notes[1]["score"] and notes[0]["lecture_id"] == "L1"
            trs = (await m.tool("qa_search", question_id=q["question_id"], source="transcript",
                                keywords=["표준화", "평균", "표준편차"], limit=5, lecture_ids=["L2"]))["hits"]
            assert {h["lecture_id"] for h in trs} == {"L2"} and "L1:t1" not in {h["id"] for h in trs}
            assert "L2:t2" not in {h["id"] for h in trs}                          # 점수 0(관련 없음)은 빠짐
    run(t())


def test_pg_search_excludes_deleted_docs(tmp_path):
    import psycopg

    async def t():
        env = await _setup(tmp_path)
        async with McpClient(env=env) as m:
            await m.tool("qa_seed", user_id="A", course_id="cA", course_name="ML", docs=DOCS_A)
            with psycopg.connect(TEST_DSN, autocommit=True) as con:            # 강의 L1이 삭제됐다고 표시
                con.execute("UPDATE qa_search_docs SET deleted_at=now() WHERE lecture_id='L1'")
            q = await m.tool("qa_ask", user_id="A", course_id="cA", question="표준화?")
            hits = (await m.tool("qa_search", question_id=q["question_id"], source="note",
                                 keywords=["표준화", "평균"], limit=5))["hits"]
            assert [h["id"] for h in hits] == ["L2:s1"]
    run(t())


def test_pg_unreachable_is_retryable_unavailable(tmp_path):
    async def t():
        env = await _setup(tmp_path, dsn="postgresql://postgres@127.0.0.1:1/none?connect_timeout=1")
        async with McpClient(env=env) as m:
            await m.tool("qa_seed", user_id="A", course_id="cA", course_name="ML", docs=[])   # 노트·전사가 없으면 PG에 안 붙음
            q = await m.tool("qa_ask", user_id="A", course_id="cA", question="표준화?")
            with pytest.raises(ToolError) as ex:
                await m.tool("qa_search", question_id=q["question_id"], source="note", keywords=["표준화"])
            assert ex.value.code == "unavailable" and ex.value.retryable
    run(t())


def test_slow_pg_does_not_block_writes_and_is_cut_off(tmp_path):
    """PG 검색이 느릴 때(테이블 잠금으로 재현): ① 다른 SQLite 쓰기(별도 연결)와 같은 서버의 job_heartbeat가 기다리지 않고,
    ② 검색은 statement_timeout으로 끊겨 '다시 시도 가능'으로 끝난다."""
    import sqlite3
    import time

    import psycopg

    async def t():
        env = {**(await _setup(tmp_path)), "NOTREE_PG_QA_STATEMENT_TIMEOUT_MS": "1500"}
        async with McpClient(env=env) as m:
            await m.tool("qa_seed", user_id="A", course_id="cA", course_name="ML", docs=DOCS_A)
            q = await m.tool("qa_ask", user_id="A", course_id="cA", question="표준화?")
            with psycopg.connect(TEST_DSN) as locker:                   # 트랜잭션을 연 채 테이블 잠금 → 검색 SELECT가 막힘
                locker.execute("LOCK TABLE qa_search_docs IN ACCESS EXCLUSIVE MODE")
                t0 = time.perf_counter()
                search = asyncio.create_task(m.tool("qa_search", question_id=q["question_id"], source="note",
                                                    keywords=["표준화"]))
                await asyncio.sleep(0.4)                                  # 검색이 PG에서 기다리는 중
                assert not search.done()
                hb0 = time.perf_counter()                                 # ① 같은 서버의 다른 도구 (SQLite 쓰기 포함)
                await asyncio.wait_for(m.tool("job_heartbeat", job_id=q["job_id"], worker="w", lease_sec=30), 1.0)
                assert time.perf_counter() - hb0 < 0.5
                con = sqlite3.connect(env["NOTREE_DB"], timeout=0.2)     # ① 별도 연결의 SQLite 쓰기
                con.execute("INSERT INTO tool_log(tool, thread_id) VALUES('probe','x')")
                con.commit()
                con.close()
                with pytest.raises(ToolError) as ex:                      # ② statement_timeout으로 끊김
                    await asyncio.wait_for(search, 5)
                assert ex.value.code == "unavailable" and ex.value.retryable
                assert time.perf_counter() - t0 < 4
    run(t())


def test_pg_connect_hang_is_cut_off(tmp_path):
    """연결은 받는데 응답하지 않는 PG(네트워크 정체 흉내): connect_timeout(2초)으로 끊겨 '다시 시도 가능'."""
    import socket
    import time

    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    srv.listen(8)                                     # accept도 응답도 안 함
    port = srv.getsockname()[1]

    async def t():
        env = await _setup(tmp_path, dsn=f"postgresql://postgres@127.0.0.1:{port}/none")
        async with McpClient(env=env) as m:
            await m.tool("qa_seed", user_id="A", course_id="cA", course_name="ML", docs=[])
            q = await m.tool("qa_ask", user_id="A", course_id="cA", question="표준화?")
            t0 = time.perf_counter()
            with pytest.raises(ToolError) as ex:
                await asyncio.wait_for(m.tool("qa_search", question_id=q["question_id"], source="note",
                                              keywords=["표준화"]), 8)
            assert ex.value.code == "unavailable" and time.perf_counter() - t0 < 5
    try:
        run(t())
    finally:
        srv.close()
