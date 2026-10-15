"""Q&A 근거 탐색 그래프를 단계별로 찍어 본다.

    uv run python -m notree.dev.qa_run                      # 기본 질문 4개 (노트 / 전사 / 웹 / 관련 없음)
    uv run python -m notree.dev.qa_run "스택은 어떤 구조야?"
    uv run python -m notree.dev.qa_run "힙 정렬의 시간 복잡도는?" --deadline-in 15   # 웹 건너뛰기
    uv run python -m notree.dev.qa_run "스택은 어떤 구조야?" --bad-citation           # 인용 확인 → 다시 답하기
    uv run python -m notree.dev.qa_run --graph              # Mermaid 그림
    uv run python -m notree.dev.qa_run "..." --llm openai   # 실제 OpenAI (검색은 가짜 말뭉치)
"""
import argparse
import asyncio
import time

from notree.graphs.qa import FakeRetriever, build_qa_graph
from notree.graphs.qa.llm import get_qa_llm
from notree.graphs.qa.retriever import sample_corpus

DEFAULT = ["스택은 어떤 구조야?", "스택 오버플로는 왜 생겨?", "힙 정렬의 시간 복잡도는?", "오늘 점심 뭐 먹지?"]


def describe(node: str, upd: dict) -> str:
    if node == "understand":
        return f"관련={upd['related']} 검색어={upd['keywords']}"
    if node.startswith("search_"):
        hs = upd["hits"]
        return "찾음 " + (", ".join(f"{h['id']}({h['score']:.2f})" for h in hs) if hs else "없음")
    if node.startswith("skip_"):
        return upd["skipped"][0]
    if node == "answer":
        return f"#{upd['answer_attempts']} 라벨={upd['label']} 인용={upd['citations']}"
    if node == "verify":
        return upd["verify_problem"] or "통과"
    if node == "save":
        return f"상태={upd['status']}"
    return str(upd)


async def ask(graph, q: str, args) -> None:
    llm = get_qa_llm(args.llm, **({"bad_citation": True} if args.bad_citation and args.llm == "fake" else {}))
    cfg = {"configurable": {"llm": llm, "retriever": FakeRetriever(sample_corpus()),
                            "deadline_at": time.time() + args.deadline_in if args.deadline_in else None}}
    print(f"\nQ. {q}")
    t0, final = time.perf_counter(), {}
    async for upd in graph.astream({"question": q, "course": "자료구조"}, cfg, stream_mode="updates"):
        for node, data in upd.items():
            data = data or {}
            final.update(data)
            print(f"  {time.perf_counter() - t0:5.2f}s  {node:<18} {describe(node, data)}")
    print(f"  A. [{final.get('label')}] {final.get('answer')}")


async def main() -> None:
    p = argparse.ArgumentParser(description="Q&A 근거 탐색 그래프 데모")
    p.add_argument("question", nargs="*")
    p.add_argument("--deadline-in", type=float, help="마감까지 남은 초 (전사 10초·웹 25초 미만이면 건너뜀)")
    p.add_argument("--bad-citation", action="store_true", help="첫 답변이 없는 근거를 인용 (가짜 LLM)")
    p.add_argument("--llm", choices=["fake", "openai"], default="fake")
    p.add_argument("--graph", action="store_true", help="Mermaid 그림만 출력")
    args = p.parse_args()
    graph = build_qa_graph()
    if args.graph:
        print(graph.get_graph().draw_mermaid())
        return
    for q in ([" ".join(args.question)] if args.question else DEFAULT):
        await ask(graph, q, args)


if __name__ == "__main__":
    asyncio.run(main())
