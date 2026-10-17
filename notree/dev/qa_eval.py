"""Q&A 답변 품질·시간·비용 측정: 가짜 검색(정해진 말뭉치) + 실제 LLM.

    uv run python -m notree.dev.qa_eval --llm openai --repeat 3 --out out/qa_eval.jsonl
    uv run python -m notree.dev.qa_eval --llm openai --price-in 0.15 --price-out 0.60    # 100만 토큰당 달러 → 비용 추정
    uv run python -m notree.dev.qa_eval --llm fake                                       # 키 없이 도구 점검

무엇을 보나
  - 주어진 근거로 답하는 능력: 문장별 JSON 형식, 인용이 받은 근거 안에 있는지, 기대한 근거를 인용했는지,
    라벨·결과 상태(completed / 부분 답변 / 근거 부족 / 시간 초과)가 사례의 기대와 맞는지, 문장에 인용 표시를 직접 썼는지
  - 시간: 질문 하나 전체, LLM 호출별 지연
  - 비용: LLM 호출 수, 입력·출력 토큰 (--price-*를 주면 달러 추정)
무엇을 못 보나
  - 실제 검색 품질 (검색은 sample_corpus에서 키워드 일치로 찾는 가짜). 근거 검색 성공률은 실제 강의 데이터로 따로 잰다

모델은 NOTREE_MODEL (기본 gpt-4o-mini), 키는 OPENAI_API_KEY.
"""
import argparse
import asyncio
import json
import re
import statistics
import time
from pathlib import Path

from notree.graphs.qa import FakeRetriever, build_qa_graph
from notree.graphs.qa.llm import get_qa_llm
from notree.graphs.qa.retriever import sample_corpus

# 사례: 질문, 기대 라벨(들), 기대 상태(들), 꼭 인용해야 하는 근거, 마감(초, 없으면 90)
CASES = [
    {"id": "note", "q": "스택은 어떤 구조야?", "label": {"강의"}, "status": {"completed"}, "cite": {"n1"}},
    {"id": "transcript", "q": "스택 오버플로는 왜 생겨?", "label": {"강의"}, "status": {"completed"}, "cite": {"t1"}},
    {"id": "web", "q": "힙 정렬의 시간 복잡도는?", "label": {"웹"}, "status": {"completed"}, "cite": {"w1"}},
    {"id": "unrelated", "q": "오늘 점심 뭐 먹지?", "label": {"일반 지식"}, "status": {"completed"}, "cite": set()},
    {"id": "not_found", "q": "AVL 트리의 회전은 언제 해?", "label": {"근거 부족", "일반 지식"}, "status": {"근거 부족", "completed"},
     "cite": set(), "note": "자료구조 질문이지만 말뭉치에 근거 없음. 관련 없음으로 판단하면 일반 지식"},
    {"id": "partial", "q": "스택과 큐는 어떻게 다르고 각각 언제 써?", "label": {"강의"}, "status": {"completed", "부분 답변"},
     "cite": {"n1", "n2"}, "note": "차이는 노트(n1·n2), 쓰임은 일부만(t2)"},
    {"id": "tight_deadline", "q": "스택 오버플로는 왜 생겨?", "label": {"근거 부족", "강의"}, "status": {"시간 초과", "completed", "근거 부족"},
     "cite": set(), "deadline": 4, "note": "마감 4초: 검색·질문 이해를 건너뛰고 LLM에 2초만 줌"},
]
MARKER = re.compile(r"\[\^?[A-Za-z]+\d+\]")


def judge(case: dict, final: dict) -> dict:
    hit_ids = {h["id"] for h in final.get("hits", [])}
    cites = final.get("citations", [])
    sentences = final.get("sentences") or []
    checks = {
        "label": final.get("label") in case["label"],
        "status": final.get("status") in case["status"],
        "cites_in_evidence": all(c in hit_ids for c in cites),
        "expected_cited": case["cite"] <= set(cites),
        "no_inline_markers": not any(MARKER.search(s.get("text", "")) for s in sentences),
    }
    return {"ok": all(checks.values()), "checks": checks}


async def run_case(graph, case: dict, llm_kind: str) -> dict:
    llm = get_qa_llm(llm_kind)
    deadline = time.time() + case.get("deadline", 90)
    cfg = {"configurable": {"llm": llm, "retriever": FakeRetriever(sample_corpus()), "deadline_at": deadline}}
    t0 = time.perf_counter()
    err = None
    try:
        final = await graph.ainvoke({"question": case["q"], "course": "자료구조"}, cfg)
    except Exception as e:                      # 형식 오류(ValidationError)·API 오류도 결과로 남긴다
        final, err = {}, f"{type(e).__name__}: {e}"[:300]
    secs = time.perf_counter() - t0
    usage = getattr(llm, "usage", [])
    out = {"case": case["id"], "question": case["q"], "seconds": round(secs, 2), "error": err,
           "status": final.get("status"), "label": final.get("label"), "citations": final.get("citations", []),
           "searched": final.get("searched", []), "skipped": final.get("skipped", []), "answer": final.get("answer"),
           "llm_calls": len(getattr(llm, "calls", [])), "llm": usage,
           "prompt_tokens": sum(u["prompt_tokens"] for u in usage),
           "completion_tokens": sum(u["completion_tokens"] for u in usage)}
    out.update(judge(case, final) if not err else {"ok": False, "checks": {"error": False}})
    return out


def summarize(rows: list[dict], price_in: float | None, price_out: float | None) -> str:
    lines = [f"{'사례':<15}{'통과':>6}{'상태':>16}{'라벨':>10}{'초(중앙)':>9}{'LLM':>5}{'입력':>7}{'출력':>6}  실패한 검사"]
    for cid in dict.fromkeys(r["case"] for r in rows):
        rs = [r for r in rows if r["case"] == cid]
        fails = sorted({k for r in rs for k, v in r["checks"].items() if not v})
        lines.append(f"{cid:<15}{sum(r['ok'] for r in rs):>3}/{len(rs):<2}{str(rs[-1]['status']):>16}{str(rs[-1]['label']):>10}"
                     f"{statistics.median(r['seconds'] for r in rs):>9.2f}{rs[-1]['llm_calls']:>5}"
                     f"{statistics.mean(r['prompt_tokens'] for r in rs):>7.0f}{statistics.mean(r['completion_tokens'] for r in rs):>6.0f}"
                     f"  {', '.join(fails)}")
    calls = [u for r in rows for u in r["llm"]]
    pin, pout = sum(r["prompt_tokens"] for r in rows), sum(r["completion_tokens"] for r in rows)
    lines.append(f"\n전체 {sum(r['ok'] for r in rows)}/{len(rows)} 통과 · 질문당 중앙 {statistics.median(r['seconds'] for r in rows):.2f}초 · "
                 f"LLM 호출 {len(calls)}번"
                 + (f" (호출당 중앙 {statistics.median(u['seconds'] for u in calls):.2f}초)" if calls else "")
                 + f" · 토큰 입력 {pin} / 출력 {pout}")
    if price_in is not None and price_out is not None:
        cost = pin / 1e6 * price_in + pout / 1e6 * price_out
        lines.append(f"추정 비용 ${cost:.5f} (질문당 ${cost / len(rows):.6f}, 100만 토큰당 입력 ${price_in}·출력 ${price_out} 기준)")
    return "\n".join(lines)


async def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--llm", choices=["openai", "fake"], default="openai")
    p.add_argument("--repeat", type=int, default=1)
    p.add_argument("--cases", default="", help="쉼표로 사례 id만 골라서")
    p.add_argument("--out", default="", help="원본 결과 jsonl")
    p.add_argument("--price-in", type=float, default=None, help="입력 100만 토큰당 달러")
    p.add_argument("--price-out", type=float, default=None, help="출력 100만 토큰당 달러")
    a = p.parse_args()
    cases = [c for c in CASES if not a.cases or c["id"] in a.cases.split(",")]
    graph = build_qa_graph()
    rows = []
    for i in range(a.repeat):
        for c in cases:
            r = await run_case(graph, c, a.llm)
            r["round"] = i + 1
            rows.append(r)
            print(f"  {r['case']:<15} {'통과' if r['ok'] else '실패'}  {r['status']} [{r['label']}] {r['seconds']}s  {r['answer']!r:.80}"
                  + (f"  오류: {r['error']}" if r["error"] else ""))
    print()
    print(summarize(rows, a.price_in, a.price_out))
    if a.out:
        Path(a.out).parent.mkdir(parents=True, exist_ok=True)
        with open(a.out, "w", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps({**r, "model": __import__("os").getenv("NOTREE_MODEL", "gpt-4o-mini") if a.llm == "openai" else "fake"},
                                   ensure_ascii=False) + "\n")
        print(f"원본: {a.out}")


if __name__ == "__main__":
    asyncio.run(main())
