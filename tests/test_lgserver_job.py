"""실험 2단계: worker 실행기 역할을 LangGraph 노드로 옮긴 qa_job 그래프 (서버 없이, 시험용 MCP 서버에 직접).

질문 등록(qa_ask: 질문 + 작업 한 트랜잭션)은 지금과 같고, Redis · 배정기 · worker 대신 이 그래프가 작업을 실행한다.
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from notree.lgserver.qa_job import build_qa_job_graph  # noqa: E402
from test_qa_worker import USER, ask, docs  # noqa: E402
from test_skeleton import Env, run  # noqa: E402


async def _seeded(e):
    await e.api.tool("qa_seed", user_id=USER, course_id="c1", course_name="자료구조", docs=docs())


async def _run(e, job_id, **conf):
    g = build_qa_job_graph()
    nodes = []
    async for u in g.astream({"job_id": job_id}, {"configurable": {"mcp": e.m_w, "llm_kind": "fake", **conf}},
                             stream_mode="updates"):
        nodes += list(u)
    return nodes


def test_작업이_그래프로_실행되고_답이_저장된다(tmp_path):
    async def t():
        async with Env(tmp_path, allocator=False) as e:
            await _seeded(e)
            q = await ask(e, "스택 오버플로는 왜 생겨?")
            nodes = await _run(e, q["job_id"])
            assert nodes == ["start_job", "run_handler", "settle"]
            assert (await e.row(q["job_id"]))["state"] == "done"
            got = await e.api.tool("qa_get", question_id=q["question_id"], user_id=USER)
            assert got["state"] == "answered" and got["answer"]["label"] == "강의"
    run(t())


def test_이미_끝난_작업은_시작하지_않는다(tmp_path):
    async def t():
        async with Env(tmp_path, allocator=False) as e:
            await _seeded(e)
            q = await ask(e, "스택은 어떤 구조야?")
            await _run(e, q["job_id"])
            assert await _run(e, q["job_id"]) == ["start_job"]      # 두 번째 run: job_start가 done → 끝
    run(t())


def test_다른_실행이_lease를_쥐고_있으면_손대지_않는다(tmp_path):
    async def t():
        async with Env(tmp_path, allocator=False) as e:
            await _seeded(e)
            q = await ask(e, "스택은 어떤 구조야?")
            await e.m_w.tool("job_start", job_id=q["job_id"], worker="other", lease_sec=30)
            assert await _run(e, q["job_id"]) == ["start_job"]
            assert (await e.row(q["job_id"]))["state"] == "running"   # 그대로 다른 실행 몫
    run(t())


def test_삭제된_질문은_다시_시도_불가로_실패(tmp_path):
    async def t():
        async with Env(tmp_path, allocator=False) as e:
            await _seeded(e)
            q = await ask(e, "스택은 어떤 구조야?")
            await e.api.tool("qa_delete", question_id=q["question_id"], user_id=USER)
            assert await _run(e, q["job_id"]) == ["start_job", "run_handler", "settle"]
            row = await e.row(q["job_id"])
            assert row["state"] == "failed" and not row["user_retryable"]
    run(t())
