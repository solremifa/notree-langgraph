"""Q&A 대화 세션 단기 메모리 (2026-10-20). 설계: docs/changes/2026-10-20-qa-conversation-memory.md

질문 등록 → qa 레인 → worker(qa_context: 이전 문답) → 그래프(understand부터 이력) → qa_complete(서버가 이력 반영)를
가짜 LLM(모델 입력 캡처)·가짜 검색으로 돌린다. 실제 OpenAI는 부르지 않는다.

Redis는 두 가지로 따로 돈다 (`redis` 인자):
  fake : fakeredis TCP 서버 (MCP 서버가 하위 프로세스라 TCP가 필요). 재시작 대신 초기화만
  real : 시험 전용 실제 Redis (redis-server 또는 docker redis:7-alpine, RDB/AOF 끔). 없으면 건너뛰고 이유를 남긴다
"""
import asyncio
import dataclasses
import sqlite3
import sys
from pathlib import Path

import pytest
import redis as redis_sync

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from conv_support import FakeRedis, Proxy, RealRedis, real_redis_unavailable  # noqa: E402
from notree.common import settings as S  # noqa: E402
from notree.contracts.envelope import ToolError  # noqa: E402
from notree.graphs.qa.llm import FakeQALLM  # noqa: E402
from notree.worker.mcp_client import McpClient  # noqa: E402
from test_skeleton import Env, run  # noqa: E402

QA = dataclasses.replace(S.QA, lease_sec=2.0, heartbeat_sec=0.5, claim_idle_ms=2500, concurrency=2)
USER, OTHER = "u-conv", "u-other"
COURSE, COURSE2 = "c-db", "c-db2"
FK_DOCS = [
    ("n-fk1", "외래키는 다른 테이블의 기본키를 참조하는 열이다."),
    ("n-fk2", "외래키를 쓰는 이유는 참조 무결성을 지키기 위해서다."),
    ("n-fk3", "외래키가 가리키는 참조되는 행을 삭제하는 경우 ON DELETE 규칙에 따라 거부되거나 함께 삭제된다."),
    ("n-pk1", "기본키는 테이블의 각 행을 유일하게 식별한다."),
]
GLOSSARY = ["외래키", "기본키", "테이블", "무결성"]
CLARIFY_Q = "외래키 제약 자체를 삭제하는 경우인가요, 참조되는 행을 삭제하는 경우인가요?"
AMBIGUOUS = {("외래키", "삭제"): (CLARIFY_Q, ["외래키 제약 삭제", "참조되는 행 삭제"])}

_REAL_SKIP = real_redis_unavailable()


@pytest.fixture(scope="module")
def real_redis():
    if _REAL_SKIP:
        pytest.skip(f"실제 Redis 시험 건너뜀: {_REAL_SKIP}")
    r = RealRedis()
    yield r
    r.close()


@pytest.fixture(scope="module")
def fake_redis():
    r = FakeRedis()
    yield r
    r.close()


@pytest.fixture(params=["fake", "real"])
def redis(request):
    """같은 시험을 가짜·실제 Redis로 따로 돌린다. 실제가 없으면 real 쪽만 건너뜀 (이유가 -rs에 나옴)."""
    r = request.getfixturevalue(f"{request.param}_redis")
    r.reset()
    return r


def llm(**kw) -> "Capture":
    return Capture(glossary=GLOSSARY, ambiguous=AMBIGUOUS, **kw)


class Capture(FakeQALLM):
    """모델 입력을 캡처하는 가짜 LLM. on_answer(질문)·on_understand(질문)로 시험이 끼어들 수 있다."""

    def __init__(self, on_answer=None, on_understand=None, **kw):
        super().__init__(**kw)
        self.on_answer, self.on_understand = on_answer, on_understand

    async def understand(self, question, course, **kw):
        out = await super().understand(question, course, **kw)
        if self.on_understand:
            await self.on_understand(question)
        return out

    async def answer(self, question, *a, **kw):
        out = await super().answer(question, *a, **kw)
        if self.on_answer:
            await self.on_answer(question)
        return out

    def last(self, step: str, question: str) -> dict:
        return [x for x in self.inputs if x["step"] == step and x["question"] == question][-1]


def env(tmp_path, url, allocator=True, **settings) -> Env:
    e = Env(tmp_path, allocator=allocator)
    e.env.update({"NOTREE_CONV_REDIS_URL": url, **{k: str(v) for k, v in settings.items()}})
    return e


async def seed(e, user=USER, course=COURSE):
    docs = [{"source": "note", "id": i, "text": t} for i, t in FK_DOCS]
    await e.api.tool("qa_seed", user_id=user, course_id=course, course_name="데이터베이스", docs=docs)


async def start(e, user=USER, course=COURSE) -> str:
    return (await e.api.tool("qa_conversation_start", user_id=user, course_id=course))["conversation_id"]


async def ask(e, cid, question, user=USER, course=COURSE) -> dict:
    return await e.api.tool("qa_ask", user_id=user, course_id=course, question=question, conversation_id=cid)


async def done(e, q, timeout=25) -> dict:
    assert q["status"] == "accepted", q
    assert await e.wait_state(q["job_id"], timeout=timeout)
    return await e.api.tool("qa_get", question_id=q["question_id"], user_id=USER)


async def conv(e, cid, user=USER) -> dict:
    return await e.api.tool("qa_conversation_get", conversation_id=cid, user_id=user, include_messages=True)


def conv_keys(url, cid) -> list[str]:
    with redis_sync.Redis.from_url(url, decode_responses=True) as c:
        return c.keys(f"notree:conv:{cid}:*")


def roles(history):
    return [(m["role"], m["content"]) for m in history]


# ── 문맥 전달 ────────────────────────────────────────────────────────

def test_followup_understand_and_answer_receive_previous_turn(tmp_path, redis):
    """재현 시험의 수정 확인: 후속 질문의 understand·answer 입력에 이전 문답이 들어가고, 검색이 그 문맥으로 된다.
    현재 질문은 이력에 없다 (별도 입력). (기준 커밋에서는 understand('그럼 그걸 쓰는 이유는?', 과목)만 받아 '일반 지식'으로 답함)"""
    async def t():
        async with env(tmp_path, redis.url) as e:
            await seed(e)
            m = llm()
            e.start_worker(QA, name="q", extras={"qa_llm": m})
            cid = await start(e)
            a1 = (await done(e, await ask(e, cid, "외래키가 뭐야?")))["answer"]
            q2 = await ask(e, cid, "그럼 그걸 쓰는 이유는?")
            assert q2["turn_seq"] == 2
            a2 = (await done(e, q2))["answer"]

            u = m.last("understand", "그럼 그걸 쓰는 이유는?")
            assert u["memory_state"] == "ok"
            assert roles(u["history"]) == [("user", "외래키가 뭐야?"), ("assistant", a1["sentences"][0]["text"])]
            assert all(h["question_id"] != q2["question_id"] for h in u["history"])        # 현재 질문 중복 없음
            ans = m.last("answer", "그럼 그걸 쓰는 이유는?")
            assert roles(ans["history"]) == roles(u["history"]) and "외래키" in ans["search_query"]
            assert a2["status"] == "completed" and a2["label"] == "강의"
            assert [s["id"] for s in a2["sources"]] == ["n-fk2"]                           # 문맥으로 찾은 근거
            assert "[^" not in u["history"][1]["content"]                                   # 이전 답의 인용 표시는 이력에 없음

            c = await conv(e, cid)
            assert c["state"] == "open" and c["turns"] == 2 and c["memory_state"] == "ok"
            assert [x["sequence"] for x in c["messages"]] == [1, 2, 3, 4]
            assert [x["role"] for x in c["messages"]] == ["user", "assistant"] * 2
            assert len({x["message_id"] for x in c["messages"]}) == 4
    run(t())


def test_single_question_call_is_unchanged(tmp_path, fake_redis):
    """conversation_id 없는 기존 호출: 문맥 없음, LLM 인자도 이전과 같고, Redis를 건드리지 않는다."""
    async def t():
        fake_redis.reset()
        async with env(tmp_path, fake_redis.url) as e:
            await seed(e)
            m = llm()
            e.start_worker(QA, name="q", extras={"qa_llm": m})
            q = await e.api.tool("qa_ask", user_id=USER, course_id=COURSE, question="그럼 그걸 쓰는 이유는?")
            assert q["status"] == "accepted" and q["conversation_id"] is None and q["turn_seq"] is None
            a = (await done(e, q))["answer"]
            assert a["label"] == "일반 지식"                                                  # 지시 대상을 모름 (이전과 같음)
            u = m.last("understand", "그럼 그걸 쓰는 이유는?")
            assert u["history"] == [] and u["memory_state"] == "none"
            with redis_sync.Redis.from_url(fake_redis.url) as c:
                assert c.keys("notree:conv:*") == []
    run(t())


def test_ambiguous_reference_asks_for_clarification_then_uses_it(tmp_path, redis):
    """'그걸 삭제하면'이 외래키 제약 삭제인지 참조 행 삭제인지 정할 수 없으면 되묻는다 (검색·답변 없음, 작업 done).
    확인 질문도 이력(assistant, 상태 '확인 필요')에 들어가 다음 답('참조되는 행…')의 해석에 쓰인다."""
    async def t():
        async with env(tmp_path, redis.url) as e:
            await seed(e)
            m = llm()
            e.start_worker(QA, name="q", extras={"qa_llm": m})
            cid = await start(e)
            await done(e, await ask(e, cid, "외래키가 뭐야?"))
            q = await ask(e, cid, "그럼 그걸 삭제하면 어떻게 돼?")
            a = (await done(e, q))["answer"]
            assert (await e.row(q["job_id"]))["state"] == "done"
            assert a["status"] == "확인 필요" and a["label"] == "확인 질문" and a["sources"] == [] and a["citations"] == []
            assert a["clarification"] == {"question": CLARIFY_Q, "options": ["외래키 제약 삭제", "참조되는 행 삭제"]}
            assert a["answer"] == CLARIFY_Q and a["notice_code"] is None
            counts = (await e.api.tool("tool_stats", thread_id=q["job_id"]))["counts"]
            assert "qa_search" not in counts and "web_search" not in counts
            assert not any(x["step"] == "answer" and x["question"] == "그럼 그걸 삭제하면 어떻게 돼?" for x in m.inputs)

            a3 = (await done(e, await ask(e, cid, "참조되는 행을 삭제하는 경우")))["answer"]
            u = m.last("understand", "참조되는 행을 삭제하는 경우")
            assert u["history"][-1]["role"] == "assistant" and u["history"][-1]["status"] == "확인 필요"
            assert u["history"][-1]["content"] == CLARIFY_Q
            assert a3["status"] == "completed" and [s["id"] for s in a3["sources"]] == ["n-fk3"]
    run(t())


def test_contexts_do_not_mix_across_conversations_courses_users(tmp_path, redis):
    async def t():
        async with env(tmp_path, redis.url) as e:
            await seed(e)
            await seed(e, course=COURSE2)
            await seed(e, user=OTHER, course="c-other")
            m = llm()
            e.start_worker(QA, name="q", extras={"qa_llm": m})
            c1 = await start(e)
            await done(e, await ask(e, c1, "외래키가 뭐야?"))
            # 같은 사용자·같은 과목의 새 대화: 이전 대화 문맥 없음
            c2 = await start(e)
            a = (await done(e, await ask(e, c2, "그럼 그걸 쓰는 이유는?")))["answer"]
            assert m.last("understand", "그럼 그걸 쓰는 이유는?")["history"] == [] and a["label"] == "일반 지식"
            # 다른 과목의 대화
            c3 = await start(e, course=COURSE2)
            await done(e, await ask(e, c3, "기본키가 뭐야?", course=COURSE2))
            assert m.last("understand", "기본키가 뭐야?")["history"] == []
            # 대화 id를 다른 과목으로 쓰면 없음
            with pytest.raises(ToolError) as ex:
                await ask(e, c1, "외래키?", course=COURSE2)
            assert ex.value.code == "not_found"
            # 다른 사용자: 남의 대화로 질문·조회·종료 불가 ('없음'과 구별하지 않음)
            with pytest.raises(ToolError) as ex:
                await ask(e, c1, "그거 알려줘", user=OTHER, course=COURSE)
            assert ex.value.code == "not_found"
            assert (await conv(e, c1, user=OTHER))["found"] is False
            with pytest.raises(ToolError) as ex:
                await e.api.tool("qa_conversation_close", conversation_id=c1, user_id=OTHER)
            assert ex.value.code == "not_found"
            with pytest.raises(ToolError) as ex:                                           # 남의 과목으로 대화 시작 불가
                await e.api.tool("qa_conversation_start", user_id=OTHER, course_id=COURSE)
            assert ex.value.code == "not_found"
            assert (await conv(e, c1))["turns"] == 1                                         # 그대로
    run(t())


# ── 동시 요청 ────────────────────────────────────────────────────────

def test_concurrent_asks_in_one_conversation_register_one_job(tmp_path, redis):
    """같은 대화방에 동시에 8번 질문 (MCP 서버 프로세스 4개 → DB 경쟁): 하나만 accepted, 나머지는 작업 없이 busy."""
    async def t():
        async with env(tmp_path, redis.url, allocator=False) as e:
            await seed(e)
            cid = await start(e)
            clients = [McpClient(env=e.env) for _ in range(4)]
            for c in clients:
                await e.stack.enter_async_context(c)
            res = await asyncio.gather(*[c.tool("qa_ask", user_id=USER, course_id=COURSE, question=f"외래키 질문 {i}",
                                                conversation_id=cid) for i, c in enumerate(clients * 2)])
            acc = [r for r in res if r["status"] == "accepted"]
            busy = [r for r in res if r["status"] == "busy"]
            assert len(acc) == 1 and len(busy) == 7
            assert all(r["active_question_id"] == acc[0]["question_id"] and r["job_id"] is None for r in busy)
            con = sqlite3.connect(e.db)
            assert con.execute("SELECT COUNT(*) FROM qa_questions WHERE conversation_id=?", (cid,)).fetchone()[0] == 1
            assert con.execute("SELECT COUNT(*) FROM jobs WHERE kind='qa'").fetchone()[0] == 1
            con.close()
    run(t())


# ── 저장·재시도·장애 ─────────────────────────────────────────────────

class LoseCompleteResponse:
    """qa_complete는 서버에 전달(저장 커밋 + 이력 반영)하고 응답만 잃어버린다 → 재배송. 나머지 도구는 그대로."""

    def __init__(self, mcp):
        self.mcp, self.lost = mcp, 0

    async def tool(self, name, **args):
        res = await self.mcp.tool(name, **args)
        if name == "qa_complete" and self.lost == 0:
            self.lost += 1
            raise ConnectionResetError("응답 유실 (시험)")
        return res


def test_lost_save_response_and_redelivery_do_not_duplicate_history(tmp_path, redis):
    async def t():
        async with env(tmp_path, redis.url) as e:
            await seed(e)
            m = llm()
            e.start_worker(QA, name="q", mcp=LoseCompleteResponse(e.m_w), extras={"qa_llm": m})
            cid = await start(e)
            q1 = await ask(e, cid, "외래키가 뭐야?")
            await done(e, q1)
            assert (await e.row(q1["job_id"]))["attempts"] == 2 and m.calls.count("understand") == 1   # LLM 다시 안 부름
            assert len((await conv(e, cid))["messages"]) == 2
            # 같은 답을 같은 idem_key로 또 저장해도 (재배송) 이력은 그대로
            async with McpClient(env={**e.env, "NOTREE_REQUIRE_PERMIT": "0"}) as raw:
                saved = (await e.api.tool("qa_get", question_id=q1["question_id"], user_id=USER))["answer"]
                again = await raw.tool("qa_complete", idem_key=q1["job_id"], question_id=q1["question_id"],
                                       expected_generation=1, status=saved["status"], label=saved["label"],
                                       answer=saved["answer"], sentences=saved["sentences"], sources=saved["sources"])
                assert again["duplicate"] and again["memory"] == "noop"
            await done(e, await ask(e, cid, "그럼 그걸 쓰는 이유는?"))
            hist = m.last("understand", "그럼 그걸 쓰는 이유는?")["history"]
            assert [h["role"] for h in hist] == ["user", "assistant"]                       # 한 쌍만
            assert len((await conv(e, cid))["messages"]) == 4
    run(t())


def test_redis_failure_after_db_save_keeps_answer_and_repairs_on_next_question(tmp_path, redis):
    """DB 저장은 됐는데 이력 반영 때 Redis 장애: 답은 그대로(작업 done, 시도 1, LLM 다시 안 부름),
    다음 질문의 qa_context가 빠진 차례만 반영해 문맥이 이어진다 (중복 없음)."""
    async def t():
        proxy = Proxy(redis.port)
        try:
            async def cut(question):
                if question == "외래키가 뭐야?":
                    proxy.cut()                                       # 답을 만든 직후, qa_complete 전에 Redis가 끊김
            m = llm(on_answer=cut)
            async with env(tmp_path, proxy.url) as e:
                await seed(e)
                e.start_worker(QA, name="q", extras={"qa_llm": m})
                cid = await start(e)
                q1 = await ask(e, cid, "외래키가 뭐야?")
                got = await done(e, q1)
                row = await e.row(q1["job_id"])
                assert row["state"] == "done" and row["attempts"] == 1 and got["state"] == "answered"
                assert m.calls.count("understand") == 1
                proxy.restore()
                assert (await conv(e, cid))["messages"] == []                    # 아직 반영 안 됨
                await done(e, await ask(e, cid, "그럼 그걸 쓰는 이유는?"))
                u = m.last("understand", "그럼 그걸 쓰는 이유는?")
                assert u["memory_state"] == "ok" and roles(u["history"])[0] == ("user", "외래키가 뭐야?")
                msgs = (await conv(e, cid))["messages"]
                assert [x["sequence"] for x in msgs] == [1, 2, 3, 4]
        finally:
            proxy.close()
    run(t())


class BeforeTool:
    """worker의 MCP 호출 중 이름이 맞는 첫 호출 직전에 fn()을 부른다 (장애를 정확한 지점에 넣기)."""

    def __init__(self, mcp, name, fn):
        self.mcp, self.name, self.fn, self.armed = mcp, name, fn, False

    async def tool(self, name, **args):
        if name == self.name and self.armed:
            self.armed = False
            self.fn()
        return await self.mcp.tool(name, **args)


def test_redis_down_at_context_still_answers_and_marks_unavailable(tmp_path, redis):
    """qa_context 때 Redis 장애: 이력 없이 답하되 memory_state=unavailable로 그래프에 알린다. 복구 뒤 빠진 차례가 반영된다."""
    async def t():
        proxy = Proxy(redis.port)
        try:
            m = llm()
            async with env(tmp_path, proxy.url) as e:
                await seed(e)
                hook = BeforeTool(e.m_w, "qa_context", proxy.cut)
                e.start_worker(QA, name="q", mcp=hook, extras={"qa_llm": m})
                cid = await start(e)
                await done(e, await ask(e, cid, "외래키가 뭐야?"))
                hook.armed = True
                q2 = await ask(e, cid, "그럼 그걸 쓰는 이유는?")                  # 등록까지는 Redis 정상, qa_context 직전에 끊김
                a2 = (await done(e, q2))["answer"]
                u = m.last("understand", "그럼 그걸 쓰는 이유는?")
                assert u["memory_state"] == "unavailable" and u["history"] == []
                assert a2["label"] == "일반 지식"                                  # 문맥 없이 답함 (지어낸 강의 근거 없음)
                assert (await e.row(q2["job_id"]))["state"] == "done"
                with pytest.raises(ToolError) as ex:                              # 장애 중 새 질문은 등록하지 않음
                    await ask(e, cid, "또 질문")
                assert ex.value.code == "unavailable" and ex.value.retryable
                proxy.restore()
                await done(e, await ask(e, cid, "외래키를 쓰는 이유는?"))
                hist = m.last("understand", "외래키를 쓰는 이유는?")["history"]
                assert [h["content"] for h in hist if h["role"] == "user"] == ["외래키가 뭐야?", "그럼 그걸 쓰는 이유는?"]
        finally:
            proxy.close()
    run(t())


# ── 종료·만료 ────────────────────────────────────────────────────────

def test_close_during_answer_keeps_db_result_and_blocks_late_history(tmp_path, redis):
    """답을 만드는 중에 대화 종료: 답은 DB에 저장되고(조회 가능), 이력·Redis 키는 되살아나지 않으며 새 질문은 거절."""
    async def t():
        started, release = asyncio.Event(), asyncio.Event()

        async def hold(question):
            started.set()
            await release.wait()
        m = llm(on_answer=hold)
        async with env(tmp_path, redis.url) as e:
            await seed(e)
            e.start_worker(QA, name="q", extras={"qa_llm": m})
            cid = await start(e)
            q = await ask(e, cid, "외래키가 뭐야?")
            await asyncio.wait_for(started.wait(), 15)
            closed = await e.api.tool("qa_conversation_close", conversation_id=cid, user_id=USER)
            assert closed == {"closed": True, "state": "closed", "memory_cleared": True,
                              "active_question_id": q["question_id"]}
            assert conv_keys(redis.url, cid) == []
            release.set()
            got = await done(e, q)
            assert got["state"] == "answered" and (await e.row(q["job_id"]))["state"] == "done"
            assert conv_keys(redis.url, cid) == []                                     # 늦은 답이 키를 만들지 않음
            c = await conv(e, cid)
            assert c["state"] == "closed" and c["memory_state"] == "ended" and c["messages"] == []
            # 늦은 재배송(같은 idem_key 저장)도 이력을 만들지 않음
            async with McpClient(env={**e.env, "NOTREE_REQUIRE_PERMIT": "0"}) as raw:
                a = got["answer"]
                again = await raw.tool("qa_complete", idem_key=q["job_id"], question_id=q["question_id"],
                                       expected_generation=1, status=a["status"], label=a["label"], answer=a["answer"],
                                       sentences=a["sentences"], sources=a["sources"])
                assert again["duplicate"] and again["memory"] == "skipped"
            assert conv_keys(redis.url, cid) == []
            r = await ask(e, cid, "그럼 그걸 쓰는 이유는?")
            assert r["status"] == "closed" and r["question_id"] is None and r["expire_reason"] == "closed"
            again = await e.api.tool("qa_conversation_close", conversation_id=cid, user_id=USER)
            assert again["closed"] is False and again["state"] == "closed"
    run(t())


def test_ttl_expiry_then_late_answer_does_not_revive(tmp_path, redis):
    """미사용 만료(TTL 2초로 줄임) 뒤 늦은 답: DB에는 저장, 이력·키는 없음, 그 대화로 새 질문은 expired."""
    async def t():
        async def slow(question):
            await asyncio.sleep(3.0)                                  # 그사이 대화 키 만료
        m = llm(on_answer=slow)
        async with env(tmp_path, redis.url, NOTREE_QA_CONV_TTL_SEC=2) as e:
            await seed(e)
            e.start_worker(QA, name="q", extras={"qa_llm": m})
            cid = await start(e)
            q = await ask(e, cid, "외래키가 뭐야?")
            got = await done(e, q)
            assert got["state"] == "answered" and (await e.row(q["job_id"]))["state"] == "done"
            assert conv_keys(redis.url, cid) == []
            assert (await conv(e, cid))["state"] == "expired"
            r = await ask(e, cid, "그럼 그걸 쓰는 이유는?")
            assert r["status"] == "expired" and r["expire_reason"] == "ttl" and r["job_id"] is None
            assert conv_keys(redis.url, cid) == []
    run(t())


@pytest.mark.parametrize("how", ["reset", "restart"])
def test_redis_loss_ends_conversation_without_reviving_old_context(tmp_path, redis, how):
    """Redis 초기화(FLUSHALL)·재시작 뒤: DB 결과·작업 상태는 그대로, 그 대화는 context_lost로 끝나고 되살아나지 않는다.
    새 대화에는 이전 문답이 섞이지 않는다. 재시작은 실제(시험 전용) Redis에서만."""
    if how == "restart" and not redis.can_restart:
        pytest.skip("가짜 Redis는 재시작이 없음 (초기화 시험으로 대신)")

    async def t():
        async with env(tmp_path, redis.url) as e:
            await seed(e)
            m = llm()
            e.start_worker(QA, name="q", extras={"qa_llm": m})
            cid = await start(e)
            q1 = await ask(e, cid, "외래키가 뭐야?")
            await done(e, q1)
            redis.reset() if how == "reset" else redis.restart()
            assert (await e.row(q1["job_id"]))["state"] == "done"
            assert (await e.api.tool("qa_get", question_id=q1["question_id"], user_id=USER))["state"] == "answered"
            r = await ask(e, cid, "그럼 그걸 쓰는 이유는?")
            assert r["status"] == "expired" and r["expire_reason"] == "context_lost" and r["job_id"] is None
            assert conv_keys(redis.url, cid) == []
            assert (await ask(e, cid, "다시"))["status"] == "expired"                    # 계속 끝난 상태
            c2 = await start(e)
            await done(e, await ask(e, c2, "그럼 그걸 쓰는 이유는?"))
            assert m.last("understand", "그럼 그걸 쓰는 이유는?")["history"] == []
    run(t())


# ── 길이 상한·삭제·다시 시도 ──────────────────────────────────────────

def test_long_conversation_keeps_last_turns_only(tmp_path, redis):
    async def t():
        m = llm()
        async with env(tmp_path, redis.url, NOTREE_QA_CONV_MAX_TURNS=3) as e:
            await seed(e)
            e.start_worker(QA, name="q", extras={"qa_llm": m})
            cid = await start(e)
            for i in range(5):
                await done(e, await ask(e, cid, f"외래키 질문 {i}"))
            hist = m.last("understand", "외래키 질문 4")["history"]
            assert [h["content"] for h in hist if h["role"] == "user"] == ["외래키 질문 1", "외래키 질문 2", "외래키 질문 3"]
            msgs = (await conv(e, cid))["messages"]
            assert [x["sequence"] for x in msgs] == [5, 6, 7, 8, 9, 10]                  # 최근 3쌍
    run(t())


def test_history_token_cap_is_applied_to_model_input(tmp_path, fake_redis):
    """모델 입력 상한(추정 토큰)이 작으면 최신 쌍만, 그마저 넘치면 잘라서 한 쌍."""
    async def t():
        fake_redis.reset()
        m = llm()
        async with env(tmp_path, fake_redis.url, NOTREE_QA_HISTORY_MAX_TOKENS=25) as e:
            await seed(e)
            e.start_worker(QA, name="q", extras={"qa_llm": m})
            cid = await start(e)
            await done(e, await ask(e, cid, "외래키가 뭐야?"))
            await done(e, await ask(e, cid, "기본키가 뭐야?"))
            await done(e, await ask(e, cid, "그럼 그걸 쓰는 이유는?"))
            hist = m.last("understand", "그럼 그걸 쓰는 이유는?")["history"]
            assert [h["role"] for h in hist] == ["user", "assistant"] and hist[0]["content"] == "기본키가 뭐야?"
            assert hist[1]["content"].endswith("…(생략)")
            assert len((await conv(e, cid))["messages"]) == 6                            # 저장은 그대로, 입력만 줄임
    run(t())


def test_deleted_question_leaves_short_term_memory(tmp_path, redis):
    async def t():
        m = llm()
        async with env(tmp_path, redis.url) as e:
            await seed(e)
            e.start_worker(QA, name="q", extras={"qa_llm": m})
            cid = await start(e)
            q1 = await ask(e, cid, "외래키가 뭐야?")
            await done(e, q1)
            assert (await e.api.tool("qa_delete", question_id=q1["question_id"], user_id=USER))["deleted"]
            assert (await conv(e, cid))["messages"] == []
            await done(e, await ask(e, cid, "그럼 그걸 쓰는 이유는?"))
            assert m.last("understand", "그럼 그걸 쓰는 이유는?")["history"] == []
    run(t())


def test_failed_turn_frees_slot_and_retry_respects_order(tmp_path, fake_redis):
    """작업 실패는 진행 중 슬롯을 다음 질문 때 비운다. 실패한 질문의 '다시 시도'는 그 뒤 새 질문이 없을 때만 (슬롯을 다시 잡음)."""
    async def t():
        fake_redis.reset()
        async with env(tmp_path, fake_redis.url, allocator=False) as e:
            await seed(e)
            cid = await start(e)

            def fail(job_id):
                con = sqlite3.connect(e.db)
                con.execute("UPDATE jobs SET state='failed', user_retryable=1, fail_reason='시험' WHERE job_id=?", (job_id,))
                con.commit()
                con.close()
            q1 = await ask(e, cid, "외래키가 뭐야?")
            fail(q1["job_id"])
            retried = await e.api.tool("job_retry", job_id=q1["job_id"])
            assert retried["revived"] is True                                           # 마지막 차례 → 되살림, 슬롯 다시 잡음
            assert (await ask(e, cid, "기본키는?"))["status"] == "busy"
            fail(q1["job_id"])
            q2 = await ask(e, cid, "기본키는?")
            assert q2["status"] == "accepted"                                            # 실패한 질문의 슬롯은 비워짐
            fail(q2["job_id"])
            r = await e.api.tool("job_retry", job_id=q1["job_id"])
            assert r["revived"] is False and "새 질문" in r["reason"]
    run(t())
