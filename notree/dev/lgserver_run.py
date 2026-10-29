"""Agent Server(langgraph dev)에 띄운 Q&A 그래프를 HTTP API로 실행해 본다 (실험 1단계).

먼저 다른 터미널에서 서버:
    $env:PYTHONUTF8 = "1"          # Windows만: 없으면 서버가 cp949 UnicodeDecodeError로 죽음
    uv sync --group lgserver
    uv run --group lgserver langgraph dev --config notree/lgserver/langgraph.json --no-browser

그다음:
    uv run python -m notree.dev.lgserver_run                                   # 기본 질문 4개
    uv run python -m notree.dev.lgserver_run "스택은 어떤 구조야?" --bad-citation  # verify → answer 반복
    uv run python -m notree.dev.lgserver_run "힙 정렬의 시간 복잡도는?" --deadline-sec 15   # 웹 건너뜀
    uv run python -m notree.dev.lgserver_run "스택은 어떤 구조야?" --history     # 서버가 저장한 체크포인트 목록
    uv run python -m notree.dev.lgserver_run --url http://127.0.0.1:2024 ...    # 서버 주소 (기본 2024)

qa_run.py와 같은 그래프 · 같은 가짜 말뭉치를 쓴다. 차이는 실행 위치:
  qa_run.py      이 프로세스 안에서 graph.astream()
  이 스크립트     HTTP로 run을 만들고, 서버의 queue worker가 그래프를 돌리며 노드마다 결과를 SSE로 보낸다
"""
import argparse
import asyncio
import time

from langgraph_sdk import get_client

from notree.dev.qa_run import DEFAULT, describe


async def ask(client, assistant_id: str, q: str, args) -> None:
    thread = await client.threads.create()            # 대화 하나 = thread 하나. 서버가 체크포인트를 thread에 쌓는다
    cfg = {"configurable": {"llm_kind": args.llm, "bad_citation": args.bad_citation,
                            **({"deadline_sec": args.deadline_sec} if args.deadline_sec else {})}}
    print(f"\nQ. {q}   (thread …{thread['thread_id'][-8:]})")
    t0, final = time.perf_counter(), {}
    async for part in client.runs.stream(thread["thread_id"], assistant_id,
                                         input={"question": q, "course": "자료구조"},
                                         config=cfg, stream_mode="updates"):
        if part.event == "metadata":
            print(f"  run …{part.data['run_id'][-8:]} 시작 (서버 queue worker가 받음)")
        elif part.event == "updates":
            for node, data in part.data.items():
                data = data or {}
                final.update(data)
                print(f"  {time.perf_counter() - t0:5.2f}s  {node:<18} {describe(node, data)}")
        elif part.event == "error":
            print("  오류:", part.data)
    print(f"  A. [{final.get('label')}] {final.get('answer')}")
    if args.history:
        print("  서버에 저장된 체크포인트 (최신 → 처음):")
        for st in await client.threads.get_history(thread["thread_id"], limit=20):
            step = st["metadata"].get("step")
            print(f"    step {step:>2}  다음={st['next'] or '끝'}")


async def main() -> None:
    p = argparse.ArgumentParser(description="Agent Server의 Q&A 그래프를 HTTP로 실행")
    p.add_argument("question", nargs="*")
    p.add_argument("--url", default="http://127.0.0.1:2024")
    p.add_argument("--llm", choices=["fake", "openai"], default="fake")
    p.add_argument("--bad-citation", action="store_true", help="첫 답변이 없는 근거를 인용 (가짜 LLM)")
    p.add_argument("--deadline-sec", type=float, help="마감까지 남은 초")
    p.add_argument("--history", action="store_true", help="실행 뒤 서버의 체크포인트 목록 출력")
    args = p.parse_args()
    client = get_client(url=args.url)
    found = await client.assistants.search(graph_id="qa")
    if not found:
        raise SystemExit("서버에 qa 그래프가 없음 — langgraph dev를 notree/lgserver/langgraph.json으로 띄웠는지 확인")
    for q in ([" ".join(args.question)] if args.question else DEFAULT):
        await ask(client, found[0]["assistant_id"], q, args)


if __name__ == "__main__":
    asyncio.run(main())
