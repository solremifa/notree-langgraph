"""대화 세션으로 여러 질문을 이어서 해 본다 (화면 대신 개발 CLI, 2026-10-20).

    uv run python -m notree.dev.qa_chat --local "외래키가 뭐야?" "그럼 그걸 쓰는 이유는?" "그럼 그걸 삭제하면 어떻게 돼?" "참조되는 행을 삭제하는 경우"
    uv run python -m notree.dev.qa_chat --local                      # 한 줄씩 입력 (/state /close /new /quit)
    uv run python -m notree.dev.qa_chat "외래키가 뭐야?" ...           # 떠 있는 MCP 서버(--http)·배정기·qa worker에 붙음

--local: 이 프로세스 안에서 MCP 서버(하위 프로세스)·배정기·qa worker·가짜 LLM·가짜 Redis(fakeredis TCP)를 띄운다. 키·외부 서비스 없음.
         --redis URL을 주면 그 Redis를 대화 메모리로 쓴다 (시험 전용 Redis만).
처음 쓰는 과목이면 데모 말뭉치(데이터베이스: 외래키·기본키)를 넣는다. /state는 이력의 역할·순서·길이만 보여 준다 (원문 출력 안 함).
"""
import argparse
import asyncio
import os
import tempfile
import threading
import time
from contextlib import AsyncExitStack
from pathlib import Path

from notree.common import settings as S
from notree.dev.devenv import free_port
from notree.graphs.qa.llm import FakeQALLM
from notree.worker.mcp_client import McpClient, ToolError

DOCS = [
    ("n-fk1", "외래키는 다른 테이블의 기본키를 참조하는 열이다."),
    ("n-fk2", "외래키를 쓰는 이유는 참조 무결성을 지키기 위해서다."),
    ("n-fk3", "외래키가 가리키는 참조되는 행을 삭제하는 경우 ON DELETE 규칙에 따라 거부되거나 함께 삭제된다."),
    ("n-fk4", "외래키 제약 자체를 삭제하면 참조 무결성 검사가 더 이상 일어나지 않는다."),
    ("n-pk1", "기본키는 테이블의 각 행을 유일하게 식별한다."),
]
GLOSSARY = ["외래키", "기본키", "테이블", "무결성"]
AMBIGUOUS = {("외래키", "삭제"): ("외래키 제약 자체를 삭제하는 경우인가요, 참조되는 행을 삭제하는 경우인가요?",
                                ["외래키 제약 삭제", "참조되는 행 삭제"])}


async def local_stack(stack: AsyncExitStack, redis_url: str | None) -> tuple[McpClient, asyncio.Event]:
    """MCP 서버 3개(api·배정기·worker 몫, stdio) + 배정기 + qa worker를 이 프로세스에 띄운다."""
    import fakeredis

    from notree.scheduler.allocator import run_allocator
    from notree.worker.loop import Worker
    if redis_url is None:
        from fakeredis import TcpFakeServer
        port = free_port()
        srv = TcpFakeServer(("127.0.0.1", port), server_type="redis")
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        redis_url = f"redis://127.0.0.1:{port}/0"
    db = Path(tempfile.mkdtemp(prefix="qa_chat_")) / "chat.db"
    env = {**os.environ, "NOTREE_DB": str(db), "NOTREE_REQUIRE_PERMIT": "1", "NOTREE_CONV_REDIS_URL": redis_url}
    api = await stack.enter_async_context(McpClient(env=env))
    sch = await stack.enter_async_context(McpClient(env=env))
    wrk = await stack.enter_async_context(McpClient(env=env))
    streams = fakeredis.FakeAsyncRedis(server=fakeredis.FakeServer(), decode_responses=True)
    stop = asyncio.Event()
    llm = FakeQALLM(glossary=GLOSSARY, ambiguous=AMBIGUOUS)
    tasks = [asyncio.create_task(run_allocator(streams, sch, stop, interval=0.1)),
             asyncio.create_task(Worker("chat", S.QA, streams, wrk, block_ms=0, extras={"qa_llm": llm}).run(stop))]

    async def shutdown():
        stop.set()
        await asyncio.gather(*tasks, return_exceptions=True)
    stack.push_async_callback(shutdown)
    return api, stop


async def show_state(mcp: McpClient, user: str, cid: str) -> None:
    c = await mcp.tool("qa_conversation_get", conversation_id=cid, user_id=user, include_messages=True)
    left = f"{c['expires_at'] - time.time():.0f}초 뒤 만료" if c.get("expires_at") else "만료 시각 없음"
    print(f"   [대화 {cid}] {c['state']} · 질문 {c['turns']}개 · 메모리 {c['memory_state']} · {left}")
    for m in c["messages"]:
        print(f"     #{m['sequence']} {m['role']:<9} {len(m['content'])}자 {m.get('status') or ''}")


async def ask(mcp: McpClient, a, cid: str, text: str) -> str:
    """질문 하나. 끝난 대화면 새 대화를 명시적으로 시작해 돌려준다."""
    q = await mcp.tool("qa_ask", user_id=a.user, course_id=a.course, question=text, conversation_id=cid)
    if q["status"] in ("closed", "expired"):
        print(f"   대화가 끝남 ({q['status']}, {q['expire_reason']}) → /new 로 새 대화를 시작하세요")
        return cid
    if q["status"] == "busy":
        print(f"   진행 중인 질문이 있음 ({q['active_question_id']}) → 답을 기다린 뒤 다시")
        return cid
    print(f"Q{q['turn_seq']}. {text}")
    end = time.time() + a.wait
    while time.time() < end:
        got = await mcp.tool("qa_get", question_id=q["question_id"], user_id=a.user)
        job = await mcp.tool("job_get", job_id=q["job_id"])
        if got["state"] == "answered" or job["state"] == "failed":
            break
        await asyncio.sleep(0.3)
    if got["state"] != "answered":
        print(f"   답 없음: 작업 {job['state']} {job.get('fail_reason') or ''}")
        return cid
    ans = got["answer"]
    print(f"A. [{ans['label']} · {ans['status']}] {ans['answer']}")
    if ans.get("clarification"):
        print(f"   선택지: {ans['clarification']['options']}")
    for s in ans["sources"]:
        print(f"   [^{s['id']}] {s['source']}: {s['text']}")
    return cid


async def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("questions", nargs="*")
    p.add_argument("--local", action="store_true", help="이 프로세스 안에서 전부 띄움 (가짜 LLM·가짜 Redis)")
    p.add_argument("--redis", default=None, help="--local에서 쓸 대화 메모리 Redis (시험 전용)")
    p.add_argument("--user", default="demo-user")
    p.add_argument("--course", default="c-db")
    p.add_argument("--wait", type=float, default=100)
    a = p.parse_args()
    async with AsyncExitStack() as stack:
        if a.local:
            mcp, _ = await local_stack(stack, a.redis)
        else:
            mcp = await stack.enter_async_context(McpClient(url=S.MCP_URL))
        await mcp.tool("qa_seed", user_id=a.user, course_id=a.course, course_name="데이터베이스",
                       docs=[{"source": "note", "id": i, "text": t} for i, t in DOCS])
        cid = (await mcp.tool("qa_conversation_start", user_id=a.user, course_id=a.course))["conversation_id"]
        print(f"새 대화 {cid} (사용자 {a.user}, 과목 {a.course})")
        lines = a.questions or iter(lambda: input("> "), None)
        try:
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                if line == "/quit":
                    break
                if line == "/state":
                    await show_state(mcp, a.user, cid)
                elif line == "/close":
                    print(f"   {await mcp.tool('qa_conversation_close', conversation_id=cid, user_id=a.user)}")
                elif line == "/new":
                    cid = (await mcp.tool("qa_conversation_start", user_id=a.user, course_id=a.course))["conversation_id"]
                    print(f"새 대화 {cid}")
                else:
                    try:
                        cid = await ask(mcp, a, cid, line)
                    except ToolError as e:
                        print(f"   거절: {e.code} {e}")
            if a.questions:
                await show_state(mcp, a.user, cid)
        except (EOFError, KeyboardInterrupt):
            pass


if __name__ == "__main__":
    asyncio.run(main())
