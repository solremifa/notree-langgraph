"""Q&A worker 통합 (가짜 검색·저장 도구 + 가짜 LLM): 질문 등록 → qa 레인 → 그래프 → 답변 저장 → 화면 조회."""
import asyncio
import dataclasses
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from notree.common import settings as S  # noqa: E402
from notree.contracts.envelope import ToolError  # noqa: E402
from notree.graphs.qa.llm import FakeQALLM  # noqa: E402
from notree.graphs.qa.retriever import SearchForbidden, SearchUnavailable, sample_corpus  # noqa: E402
from notree.worker.handlers.qa import McpRetriever  # noqa: E402
from notree.worker.mcp_client import McpClient  # noqa: E402
from notree.worker.registry import ToolRejected, ToolUnavailable  # noqa: E402
from test_skeleton import Env, run  # noqa: E402

QA = dataclasses.replace(S.QA, lease_sec=2.0, heartbeat_sec=0.5, claim_idle_ms=2500, concurrency=2)
USER = "u-test"
LECTURE = {"n1": "L1", "n2": "L1", "n3": "L2", "t1": "L1", "t2": "L1"}


def docs():
    return [{"source": src, "id": i, "text": t, "lecture_id": LECTURE.get(i)}
            for src, items in sample_corpus().items() for i, t in items]


async def ask(e, question, course="c1"):
    return await e.api.tool("qa_ask", user_id=USER, course_id=course, question=question)


async def answered(e, q):
    assert await e.wait_state(q["job_id"], timeout=20)
    return await e.api.tool("qa_get", question_id=q["question_id"], user_id=USER)


def test_question_goes_from_ask_to_saved_answer(tmp_path):
    async def t():
        async with Env(tmp_path) as e:
            await e.api.tool("qa_seed", user_id=USER, course_id="c1", course_name="자료구조", docs=docs())
            e.start_worker(QA, name="q")
            q = await ask(e, "스택 오버플로는 왜 생겨?")
            got = await answered(e, q)
            assert (await e.row(q["job_id"]))["state"] == "done"
            assert got["state"] == "answered"
            a = got["answer"]
            assert a["status"] == "completed" and a["label"] == "강의" and a["notice_code"] is None
            assert [s["id"] for s in a["sources"]] == ["t1"] and "[^t1]" in a["answer"]
            stats = (await e.api.tool("tool_stats", thread_id=q["job_id"]))["counts"]
            assert stats["qa_context"] == 1 and stats["qa_complete"] == 1
            assert stats["qa_search"] == 2                       # 노트(부족) → 전사(충분)
    run(t())


def test_unrelated_question_is_general_knowledge_and_no_search(tmp_path):
    async def t():
        async with Env(tmp_path) as e:
            await e.api.tool("qa_seed", user_id=USER, course_id="c1", course_name="자료구조", docs=docs())
            e.start_worker(QA, name="q")
            q = await ask(e, "오늘 점심 뭐 먹지?")
            a = (await answered(e, q))["answer"]
            assert a["label"] == "일반 지식" and a["sources"] == []
            assert "qa_search" not in (await e.api.tool("tool_stats", thread_id=q["job_id"]))["counts"]
    run(t())


def test_no_evidence_is_saved_as_notice_and_job_is_done(tmp_path):
    async def t():
        async with Env(tmp_path) as e:
            await e.api.tool("qa_seed", user_id=USER, course_id="c2", course_name="자료구조", docs=[])
            e.start_worker(QA, name="q")
            q = await ask(e, "스택은 어떤 구조야?", course="c2")
            a = (await answered(e, q))["answer"]
            assert (await e.row(q["job_id"]))["state"] == "done"      # 근거 부족도 작업은 정상 종료
            assert a["status"] == "근거 부족" and a["notice_code"] == "not_found" and a["sentences"] == []
            counts = (await e.api.tool("tool_stats", thread_id=q["job_id"]))["counts"]
            assert counts["qa_search"] == 2 and counts["web_search"] == 1   # 노트 → 전사 → 웹 1번
    run(t())


class SlowAnswerLLM(FakeQALLM):
    """검색까지는 바로, 답변만 느리게 (답을 만드는 사이에 질문을 지우려고)."""

    async def answer(self, *a, **k):
        await asyncio.sleep(1.0)
        return await super().answer(*a, **k)


async def _delete_when(e, q, tool):
    """이 작업이 tool을 부른 뒤에 질문을 지운다."""
    for _ in range(200):
        if (await e.api.tool("tool_stats", thread_id=q["job_id"]))["counts"].get(tool):
            break
        await asyncio.sleep(0.02)
    assert (await e.api.tool("qa_delete", question_id=q["question_id"], user_id=USER))["deleted"]


@pytest.mark.parametrize("delete_after, code", [("qa_search", "generation_mismatch"), ("qa_context", "not_found")])
def test_deleted_question_fails_without_retry(tmp_path, delete_after, code):
    """질문이 지워지면: 답을 만드는 중이었으면 저장이 generation_mismatch, 검색 전이었으면 검색이 not_found.
    어느 쪽이든 다시 해도 소용없으므로 시도 1번으로 실패 확정 (다시 시도 불가)."""
    async def t():
        async with Env(tmp_path) as e:
            await e.api.tool("qa_seed", user_id=USER, course_id="c1", course_name="자료구조", docs=docs())
            llm = SlowAnswerLLM() if delete_after == "qa_search" else FakeQALLM(delay=0.8)
            e.start_worker(QA, name="q", extras={"qa_llm": llm})
            q = await ask(e, "스택은 어떤 구조야?")
            await _delete_when(e, q, delete_after)
            assert await e.wait_state(q["job_id"], timeout=15)
            row = await e.row(q["job_id"])
            assert row["state"] == "failed" and row["user_retryable"] is False
            assert code in row["fail_reason"] and row["attempts"] == 1
    run(t())


def test_qa_tools_respect_user_boundary(tmp_path):
    async def t():
        async with Env(tmp_path, allocator=False) as e:
            await e.api.tool("qa_seed", user_id=USER, course_id="c1", course_name="자료구조", docs=docs())
            with pytest.raises(ToolError) as ex:
                await e.api.tool("qa_ask", user_id="someone-else", course_id="c1", question="스택?")
            assert ex.value.code == "not_found"
            q = await ask(e, "스택?")
            assert (await e.api.tool("qa_get", question_id=q["question_id"], user_id="someone-else"))["found"] is False
            with pytest.raises(ToolError) as ex:                     # worker 도구는 permit 없이는 거절
                await e.m_w.tool("qa_context", question_id=q["question_id"])
            assert ex.value.code == "permit_invalid"
    run(t())


def test_qa_complete_is_idempotent_and_does_not_overwrite(tmp_path):
    async def t():
        async with Env(tmp_path, allocator=False) as e:
            await e.api.tool("qa_seed", user_id=USER, course_id="c1", course_name="자료구조", docs=docs())
            q = await ask(e, "스택?")
            env = {**e.env, "NOTREE_REQUIRE_PERMIT": "0"}            # permit 없이 저장 규칙만 본다
            async with McpClient(env=env) as m:
                ctx = await m.tool("qa_context", question_id=q["question_id"])
                body = dict(question_id=q["question_id"], expected_generation=ctx["generation"], status="completed",
                            label="강의", answer="스택은 LIFO [^n1]", sentences=[{"text": "스택은 LIFO", "citations": ["n1"]}],
                            sources=[{"id": "n1", "source": "note", "text": "…", "ref": "n1"}])
                first = await m.tool("qa_complete", idem_key=q["job_id"], **body)
                again = await m.tool("qa_complete", idem_key=q["job_id"], **body)
                assert again["duplicate"] and again["answer_id"] == first["answer_id"]
                with pytest.raises(ToolError) as ex:
                    await m.tool("qa_complete", idem_key="op-other", **body)
                assert ex.value.code == "generation_mismatch"
                with pytest.raises(ToolError) as ex:                 # 결과 형식도 계약대로 검사
                    await m.tool("qa_complete", idem_key="op-x", **{**body, "status": "done"})
                assert ex.value.code == "invalid_args"
    run(t())


class _Ctx:
    def __init__(self, exc):
        self.exc = exc

    async def tool(self, name, **_):
        raise self.exc


def test_retriever_maps_tool_errors_to_graph_errors():
    async def t():
        unavailable = ToolUnavailable(ToolError("qa_search", "unavailable", "DB", True))
        rejected = ToolRejected(ToolError("qa_search", "not_found", "질문 없음", False))
        with pytest.raises(SearchUnavailable):
            await McpRetriever(_Ctx(unavailable), "q").search("note", ["스택"])
        with pytest.raises(SearchForbidden):
            await McpRetriever(_Ctx(rejected), "q").search("transcript", ["스택"])
        with pytest.raises(SearchUnavailable):
            await McpRetriever(_Ctx(ConnectionResetError("끊김")), "q").search("web", ["스택"])
    asyncio.run(t())


def test_deleted_question_returns_no_content_and_copies_are_removed(tmp_path):
    """답변 완료 뒤 삭제: 조회에 질문·답변·출처 원문이 나오지 않고, DB의 복사본도 지워진다 (2026-10-17 리뷰)."""
    import sqlite3

    async def t():
        async with Env(tmp_path) as e:
            await e.api.tool("qa_seed", user_id=USER, course_id="c1", course_name="자료구조", docs=docs())
            e.start_worker(QA, name="q")
            q = await ask(e, "스택 오버플로는 왜 생겨?")
            assert (await answered(e, q))["state"] == "answered"
            assert (await e.api.tool("qa_delete", question_id=q["question_id"], user_id=USER))["deleted"]
            got = await e.api.tool("qa_get", question_id=q["question_id"], user_id=USER)
            assert got["found"] and got["state"] == "deleted"
            assert got["question"] is None and got["answer"] is None
            con = sqlite3.connect(e.db)
            row = con.execute("SELECT question, answer FROM qa_questions WHERE question_id=?",
                              (q["question_id"],)).fetchone()
            con.close()
            assert row == (None, None)
    run(t())


def test_saved_answer_has_citations_in_sentence_order(tmp_path):
    """화면 계약의 전체 citations: 문장별 인용을 순서대로, 중복 없이. 서버가 만든다."""
    async def t():
        async with Env(tmp_path, allocator=False) as e:
            await e.api.tool("qa_seed", user_id=USER, course_id="c1", course_name="자료구조", docs=docs())
            q = await ask(e, "스택?")
            async with McpClient(env={**e.env, "NOTREE_REQUIRE_PERMIT": "0"}) as m:
                ctx = await m.tool("qa_context", question_id=q["question_id"])
                src = [{"id": i, "source": s, "text": "…", "ref": i} for i, s in (("t1", "transcript"), ("n1", "note"))]
                sent = [{"text": "a", "citations": ["t1"]}, {"text": "b", "citations": ["n1", "t1"]}]
                await m.tool("qa_complete", idem_key=q["job_id"], question_id=q["question_id"],
                             expected_generation=ctx["generation"], status="completed", label="강의",
                             answer="a [^t1] b [^n1][^t1]", sentences=sent, sources=src)
                with pytest.raises(ToolError) as ex:                 # 출처에 없는 인용은 저장하지 않는다
                    await m.tool("qa_complete", idem_key="op-y", question_id=q["question_id"],
                                 expected_generation=ctx["generation"], status="completed", label="강의",
                                 answer="c [^x9]", sentences=[{"text": "c", "citations": ["x9"]}], sources=src)
                assert ex.value.code == "invalid_args"
            got = await e.api.tool("qa_get", question_id=q["question_id"], user_id=USER)
            assert got["answer"]["citations"] == ["t1", "n1"]
    run(t())


class LoseCompleteResponse:
    """qa_complete는 서버에 전달(저장 커밋)하고 응답만 잃어버린다. 나머지 도구는 그대로."""

    def __init__(self, mcp):
        self.mcp, self.lost = mcp, 0

    async def tool(self, name, **args):
        res = await self.mcp.tool(name, **args)
        if name == "qa_complete" and self.lost == 0:
            self.lost += 1
            raise ConnectionResetError("응답 유실 (시험)")
        return res


def test_retry_after_lost_save_response_skips_the_graph(tmp_path):
    """저장은 됐는데 응답을 못 받아 재시도되면, 질문 해석·검색·답변을 다시 하지 않고 완료 처리."""
    async def t():
        async with Env(tmp_path) as e:
            await e.api.tool("qa_seed", user_id=USER, course_id="c1", course_name="자료구조", docs=docs())
            llm = FakeQALLM()
            e.start_worker(QA, name="q", mcp=LoseCompleteResponse(e.m_w), extras={"qa_llm": llm})
            q = await ask(e, "스택 오버플로는 왜 생겨?")
            assert await e.wait_state(q["job_id"], timeout=20)
            row = await e.row(q["job_id"])
            assert row["state"] == "done" and row["attempts"] == 2
            assert llm.calls.count("understand") == 1                # 두 번째 시도는 그래프를 건너뜀
            counts = (await e.api.tool("tool_stats", thread_id=q["job_id"]))["counts"]
            assert counts["qa_search"] == 2 and counts["qa_context"] == 2
            assert (await e.api.tool("qa_get", question_id=q["question_id"], user_id=USER))["state"] == "answered"
    run(t())


def test_seed_rejects_the_same_doc_id_in_another_source(tmp_path):
    """인용 ID는 출처 전체에서 유일 (리뷰 6차 ②): 노트에 있는 ID를 전사로 또 넣으면 거절."""
    import os

    from notree.worker.mcp_client import McpClient

    async def t():
        env = {**os.environ, "NOTREE_DB": str(tmp_path / "seed.db"), "NOTREE_REQUIRE_PERMIT": "0"}
        async with McpClient(env=env) as m:
            await m.tool("qa_seed", user_id="u", course_id="c", course_name="자료구조",
                         docs=[{"source": "note", "id": "same", "text": "스택은 LIFO이다"}])
            with pytest.raises(ToolError) as ex:
                await m.tool("qa_seed", user_id="u", course_id="c", course_name="자료구조",
                             docs=[{"source": "transcript", "id": "same", "text": "힙은 완전 이진 트리이다"}])
            assert ex.value.code == "invalid_args"
            await m.tool("qa_seed", user_id="u", course_id="c", course_name="자료구조",          # 같은 출처 덮어쓰기는 그대로 됨
                         docs=[{"source": "note", "id": "same", "text": "스택은 LIFO 구조이다"}])
    asyncio.run(t())


class LoseEveryCompleteResponse(LoseCompleteResponse):
    """qa_complete는 매번 저장하고 응답만 잃는다."""

    async def tool(self, name, **args):
        res = await self.mcp.tool(name, **args)
        if name == "qa_complete":
            self.lost += 1
            raise ConnectionResetError("응답 유실 (시험)")
        return res


def test_lost_save_response_on_the_last_attempt_still_finishes_the_job(tmp_path, monkeypatch):
    """리뷰 6차 ③: 마지막 시도에서 저장 직후 응답만 잃어도 작업은 실패가 아니라 완료 (답변·작업 상태가 같다)."""
    monkeypatch.setitem(S.KIND_MAX_ATTEMPTS, "qa", 1)          # 첫 시도가 곧 마지막 시도
    async def t():
        async with Env(tmp_path) as e:
            await e.api.tool("qa_seed", user_id=USER, course_id="c1", course_name="자료구조", docs=docs())
            llm, mcp = FakeQALLM(), LoseEveryCompleteResponse(e.m_w)
            e.start_worker(QA, name="q", mcp=mcp, extras={"qa_llm": llm})
            q = await ask(e, "스택 오버플로는 왜 생겨?")
            assert await e.wait_state(q["job_id"], timeout=20)
            row = await e.row(q["job_id"])
            assert row["state"] == "done" and row["attempts"] == 1, row
            assert mcp.lost == 1 and llm.calls.count("understand") == 1   # LLM 다시 안 부름, 저장도 한 번
            got = await e.api.tool("qa_get", question_id=q["question_id"], user_id=USER)
            assert got["state"] == "answered" and got["answer"]["status"] == "completed"
    run(t())


def test_last_attempt_without_a_saved_answer_still_fails(tmp_path, monkeypatch):
    """반대 경우: 저장이 실제로 안 됐으면 done_check는 실패를 바꾸지 않는다."""
    monkeypatch.setitem(S.KIND_MAX_ATTEMPTS, "qa", 1)

    class RefuseComplete:
        def __init__(self, mcp):
            self.mcp = mcp

        async def tool(self, name, **args):
            if name == "qa_complete":
                raise ConnectionResetError("저장 전에 끊김 (시험)")
            return await self.mcp.tool(name, **args)

    async def t():
        async with Env(tmp_path) as e:
            await e.api.tool("qa_seed", user_id=USER, course_id="c1", course_name="자료구조", docs=docs())
            e.start_worker(QA, name="q", mcp=RefuseComplete(e.m_w), extras={"qa_llm": FakeQALLM()})
            q = await ask(e, "스택 오버플로는 왜 생겨?")
            assert await e.wait_state(q["job_id"], timeout=20)
            row = await e.row(q["job_id"])
            assert row["state"] == "failed" and "시도 한도 초과" in str(row), row
            assert (await e.api.tool("qa_get", question_id=q["question_id"], user_id=USER))["state"] != "answered"
    run(t())


class SaveThenStallPastDeadline:
    """worker가 보는 마감을 1.5초 뒤로 줄이고(job_start 응답), qa_complete는 저장한 뒤 마감이 지날 때까지 응답을 늦춘다."""

    def __init__(self, mcp):
        self.mcp, self.deadline = mcp, None

    async def tool(self, name, **args):
        import time
        res = await self.mcp.tool(name, **args)
        if name == "job_start" and res.get("status") == "started":
            self.deadline = time.time() + 1.5
            res = {**res, "payload": {**res["payload"], "deadline_at": self.deadline}}
        if name == "qa_complete":
            await asyncio.sleep(max(0.0, self.deadline - time.time()) + 1.0)
        return res


def test_deadline_passing_right_after_the_save_still_finishes_the_job(tmp_path):
    """리뷰 6차 ③: 저장 직후 전체 마감이 지나 시간 초과로 끊겨도, 결과가 저장돼 있으면 완료."""
    async def t():
        async with Env(tmp_path) as e:
            await e.api.tool("qa_seed", user_id=USER, course_id="c1", course_name="자료구조", docs=docs())
            e.start_worker(QA, name="q", mcp=SaveThenStallPastDeadline(e.m_w), extras={"qa_llm": FakeQALLM()})
            q = await ask(e, "스택 오버플로는 왜 생겨?")
            assert await e.wait_state(q["job_id"], timeout=20)
            row = await e.row(q["job_id"])
            assert row["state"] == "done" and row["attempts"] == 1, row
            assert (await e.api.tool("qa_get", question_id=q["question_id"], user_id=USER))["state"] == "answered"
    run(t())


class SaveLostThenFailUnconfirmed(LoseEveryCompleteResponse):
    """리뷰 7차 ②: 저장 응답 유실 + 실패 확정(job_fail)도 첫 확인 묶음(3번)은 서버에 닿지 못함 = '확인 불가'."""

    def __init__(self, mcp):
        super().__init__(mcp)
        self.fail_blocked = 3

    async def tool(self, name, **args):
        if name == "job_fail" and self.fail_blocked > 0:
            self.fail_blocked -= 1
            raise ConnectionResetError("job_fail 전달 실패 (시험)")
        return await super().tool(name, **args)


def test_unconfirmed_outcome_is_not_settled_as_failure_and_llm_is_not_called_again(tmp_path, monkeypatch):
    """확인 불가에서는 실패를 확정하지 않고 ACK도 안 함 → 재배송(시도 > 한도)은 핸들러 없이 확정만 다시 → 서버가 저장을 보고 완료."""
    monkeypatch.setitem(S.KIND_MAX_ATTEMPTS, "qa", 1)
    async def t():
        async with Env(tmp_path) as e:
            await e.api.tool("qa_seed", user_id=USER, course_id="c1", course_name="자료구조", docs=docs())
            llm, mcp = FakeQALLM(), SaveLostThenFailUnconfirmed(e.m_w)
            e.start_worker(QA, name="q", mcp=mcp, extras={"qa_llm": llm})
            q = await ask(e, "스택 오버플로는 왜 생겨?")
            await asyncio.sleep(1.5)                                    # 첫 시도의 확인 묶음이 끝날 때까지
            assert (await e.row(q["job_id"]))["state"] == "running"      # 실패로 확정하지 않음
            assert await e.wait_state(q["job_id"], timeout=20)
            row = await e.row(q["job_id"])
            assert row["state"] == "done" and row["attempts"] == 2, row  # 두 번째 배송은 확정만
            assert llm.calls.count("understand") == 1 and mcp.lost == 1  # LLM·저장 다시 안 함
            counts = (await e.api.tool("tool_stats", thread_id=q["job_id"]))["counts"]
            assert counts["qa_context"] == 1 and counts["qa_search"] == 2
    run(t())
