"""실험 2단계: 질문을 업무 서비스에 등록하고, 그 작업을 Agent Server의 qa_job 그래프로 실행한다.

먼저 띄울 것 (Redis · 배정기 · worker는 필요 없다. 이 그래프가 worker 자리를 맡는다)
    uv run python -m notree.mcp_server --http                                          # 업무 서비스 (터미널 1)
    $env:PYTHONUTF8 = "1"                                                             # Windows만
    uv run --group lgserver langgraph dev --config notree/lgserver/langgraph.json      # Agent Server (터미널 2)

그다음 (터미널 3)
    uv run python -m notree.dev.lgserver_ask "스택 오버플로는 왜 생겨?"
    uv run python -m notree.dev.lgserver_ask "힙 정렬의 시간 복잡도는?" --again     # 같은 작업으로 run 한 번 더 → 시작 안 함
    uv run python -m notree.dev.lgserver_ask "스택은 어떤 구조야?" --delete-first    # 질문 삭제 뒤 실행 → 실패 확정

흐름: qa_ask(질문 + 작업 한 트랜잭션, 업무 DB) → 서버에 run {job_id} → start_job → run_handler(Q&A 그래프) → settle
배정기 · worker가 같이 떠 있으면 같은 작업을 서로 잡으려 한다. 그때는 lease · permit 때문에 한쪽만 실행된다.
"""
import argparse
import asyncio
import time

from langgraph_sdk import get_client

from notree.common import settings as S
from notree.dev.qa_ask import LECTURE
from notree.graphs.qa.retriever import sample_corpus
from notree.worker.mcp_client import McpClient


async def run_job(client, assistant_id: str, job_id: str, llm: str) -> None:
    thread = await client.threads.create()                  # 작업 하나 = thread 하나 (State가 이어 붙지 않게)
    t0 = time.perf_counter()
    async for part in client.runs.stream(thread["thread_id"], assistant_id, input={"job_id": job_id},
                                         config={"configurable": {"llm_kind": llm}},
                                         stream_mode="updates", stream_subgraphs=True):
        if part.event.startswith("updates"):
            inner = "|" in part.event   # 안쪽 Q&A 그래프의 노드 (지금은 따로 실행해 안 옴. Runtime context로 바꾸면 보임)
            for node, data in (part.data or {}).items():
                data = data or {}
                show = {k: v for k, v in data.items() if k in ("start_status", "attempt", "outcome", "reason", "settled",
                                                             "status", "label", "citations", "hits")}
                if "hits" in show:
                    show["hits"] = [h["id"] for h in show["hits"]]
                print(f"  {time.perf_counter() - t0:5.2f}s  {'    ' if inner else ''}{node:<18} {show}")
        elif part.event == "error":
            print("  오류:", part.data)


async def main() -> None:
    p = argparse.ArgumentParser(description="업무 서비스에 질문 등록 → Agent Server qa_job 그래프로 실행")
    p.add_argument("question")
    p.add_argument("--url", default="http://127.0.0.1:2024", help="Agent Server 주소")
    p.add_argument("--user", default="demo-user")
    p.add_argument("--course", default="c-ds")
    p.add_argument("--llm", choices=["fake", "openai"], default="fake")
    p.add_argument("--again", action="store_true", help="같은 작업으로 run을 한 번 더 (이미 끝났으니 시작 안 해야 함)")
    p.add_argument("--delete-first", action="store_true", help="질문을 지운 뒤 실행 (실패 확정돼야 함)")
    a = p.parse_args()
    client = get_client(url=a.url)
    found = await client.assistants.search(graph_id="qa_job")
    if not found:
        raise SystemExit("서버에 qa_job 그래프가 없음 — notree/lgserver/langgraph.json으로 띄웠는지 확인")
    async with McpClient(url=S.MCP_URL) as mcp:
        docs = [{"source": s, "id": i, "text": t, "lecture_id": LECTURE.get(i)}
                for s, items in sample_corpus().items() for i, t in items]
        await mcp.tool("qa_seed", user_id=a.user, course_id=a.course, course_name="자료구조", docs=docs)
        q = await mcp.tool("qa_ask", user_id=a.user, course_id=a.course, question=a.question)
        print(f"Q. {a.question}\n  업무 서비스: 질문 {q['question_id']} + 작업 {q['job_id']} (한 트랜잭션)")
        if a.delete_first:
            await mcp.tool("qa_delete", question_id=q["question_id"], user_id=a.user)
            print("  질문 삭제함")
        for i in range(2 if a.again else 1):
            print(f"  ── Agent Server run {i + 1}")
            await run_job(client, found[0]["assistant_id"], q["job_id"], a.llm)
        job = await mcp.tool("job_get", job_id=q["job_id"])
        print(f"  작업 상태: {job['state']} (시도 {job['attempts']})")
        if not a.delete_first:
            got = await mcp.tool("qa_get", question_id=q["question_id"], user_id=a.user)
            ans = got.get("answer") or {}
            print(f"  A. [{ans.get('label')}] {ans.get('answer')}")


if __name__ == "__main__":
    asyncio.run(main())
