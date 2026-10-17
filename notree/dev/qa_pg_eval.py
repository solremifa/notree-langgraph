"""그래프 + 실제 PG 검색으로, LLM에 **실제로 전달된 근거**에 정답이 들어가는지 잰다.

    uv run python -m notree.dev.qa_pg_eval --load                        # data/qa_docs.jsonl → qa_search_docs (사용자 'eval')
    uv run python -m notree.dev.qa_pg_eval --keywords data/qa_keywords.json

검색 정책(노트 → 강의 2개 우선 전사 → 0.3 미만이면 과목 전체 → LLM이 부족하다고 하면 과목 전체 → 웹)과
최종 근거 선택(evidence: 0.3 이상, 점수순, 중복 제거, 최대 5개)을 함께 본다. 검색 결과 순위가 아니라 LLM 입력을 본다.

답변 쪽은 가짜로 두 가지
  accept : 첫 근거로 바로 답함 (LLM이 "충분"이라고 하는 경우) → 첫 전달 근거에 정답이 있었나
  oracle : 정답 근거가 전달될 때까지 "근거가 더 필요"라고 함 → 확장 정책이 예산(LLM 4번) 안에 정답까지 닿나
검색어는 --keywords(LLM이 뽑은 것) 또는 단어 추출. 웹은 없음(빈 결과).
"""
import argparse
import asyncio
import json
import statistics
import time

from notree.graphs.qa import build_qa_graph
from notree.graphs.qa.graph import MAX_EVIDENCE
from notree.graphs.qa.llm import Answer, FakeQALLM, Sentence, Understanding, keywords_of
from notree.graphs.qa.retriever import Hit
from notree.mcp_server import pg_search

USER = "eval"


class PgRetriever:
    def __init__(self, course: str) -> None:
        self.course, self.calls = course, []

    async def search(self, source, keywords, limit=3, lecture_ids=None):
        self.calls.append(source if not lecture_ids else f"{source}@{','.join(lecture_ids)}")
        if source == "web":
            return []
        rows = await asyncio.to_thread(pg_search.search, USER, self.course, source, keywords, limit, lecture_ids)
        return [Hit(r["id"], source, r["text"], r["score"], r["ref"], r["lecture_id"]) for r in rows]


class EvalLLM(FakeQALLM):
    def __init__(self, keywords: list[str], gold_ids: set[str], mode: str) -> None:
        super().__init__()
        self.kw, self.gold, self.mode = keywords, gold_ids, mode
        self.evidence_log: list[list[str]] = []

    async def understand(self, question, course):
        self.calls.append("understand")
        return Understanding(related=True, keywords=self.kw)

    async def answer(self, question, evidence, attempt, problem="", related=True):
        self.calls.append(f"answer#{attempt}")
        ids = [h["id"] for h in evidence]
        self.evidence_log.append(ids)
        if not evidence:
            return Answer(sentences=[Sentence(text="근거 없음")], needs_more_evidence=True)
        hit_gold = [i for i in ids if i in self.gold]
        if self.mode == "oracle" and not hit_gold:
            return Answer(sentences=[Sentence(text="부분", citations=[ids[0]])], needs_more_evidence=True)
        return Answer(sentences=[Sentence(text="답", citations=[(hit_gold or ids)[0]])])


def gold_ids(gold: dict) -> set[str]:
    return {f"{lec}:{p}{c}" for lec, cs in gold.items() for c in cs for p in ("s", "t")}


async def run_one(graph, item: dict, kw: list[str], mode: str) -> dict:
    g = gold_ids(item["gold"])
    llm, ret = EvalLLM(kw, g, mode), PgRetriever(item["course"])
    t0 = time.perf_counter()
    out = await graph.ainvoke({"question": item["q"], "course": item["course"]}, {"configurable": {"llm": llm, "retriever": ret}})
    first = llm.evidence_log[0] if llm.evidence_log else []
    reached = next((i + 1 for i, ev in enumerate(llm.evidence_log) if set(ev) & g), None)
    return {"id": item["id"], "mode": mode, "negative": not g, "keywords": kw, "searches": ret.calls,
            "answers": len(llm.evidence_log), "llm_calls": len(llm.calls), "first_evidence": first,
            "first_has_gold": bool(set(first) & g), "reached_at_answer": reached,
            "first_gold_sources": sorted({i.rsplit(":", 1)[1][0] for i in set(first) & g}),
            "status": out.get("status"), "transcript_lectures": out.get("transcript_lectures", []),
            "ms": round((time.perf_counter() - t0) * 1000, 1)}


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--load", action="store_true")
    ap.add_argument("--docs", default="data/qa_docs.jsonl")
    ap.add_argument("--questions", default="data/qa_questions.json")
    ap.add_argument("--keywords", default="")
    ap.add_argument("--out", default="")
    ap.add_argument("--verbose", action="store_true")
    a = ap.parse_args()
    if a.load:
        by_course: dict[str, list[dict]] = {}
        for line in open(a.docs, encoding="utf-8"):
            d = json.loads(line)
            by_course.setdefault(d["course_id"], []).append(d)
        print("적재:", {c: pg_search.upsert_docs(USER, c, ds) for c, ds in by_course.items()})
    qs = json.load(open(a.questions, encoding="utf-8"))["questions"]
    kws = json.load(open(a.keywords, encoding="utf-8")) if a.keywords else {}
    graph = build_qa_graph()
    rows = []
    for mode in ("accept", "oracle"):
        for item in qs:
            rows.append(await run_one(graph, item, kws.get(item["id"]) or keywords_of(item["q"]), mode))
    print(f"질문 {sum(1 for q in qs if q['gold'])}개 · 검색어 {'LLM' if a.keywords else '단어 추출'} · 근거 최대 {MAX_EVIDENCE}개\n")
    for mode in ("accept", "oracle"):
        rs = [r for r in rows if r["mode"] == mode and not r["negative"]]
        first = sum(r["first_has_gold"] for r in rs) / len(rs)
        reached = sum(1 for r in rs if r["reached_at_answer"]) / len(rs)
        exp = sum(1 for r in rs if any(s == "transcript" for s in r["searches"][1:]) and
                  sum(1 for s in r["searches"] if s.startswith("transcript")) >= 2) / len(rs)
        print(f"[{mode}] 첫 전달 근거에 정답 {first:.0%} · 예산 안에 정답 전달 {reached:.0%} · "
              f"과목 전체 확장 {exp:.0%} · 답변 호출 중앙 {statistics.median(r['answers'] for r in rs)} · "
              f"LLM 호출 중앙 {statistics.median(r['llm_calls'] for r in rs)} · 질문당 {statistics.median(r['ms'] for r in rs):.0f}ms")
        if mode == "accept":
            src = {"s": 0, "t": 0}
            for r in rs:
                for x in r["first_gold_sources"]:
                    src[x] += 1
            print(f"    첫 전달 근거의 정답 출처: 노트 {src['s']}개 질문 · 전사 {src['t']}개 질문")
        miss = [r["id"] for r in rs if not r["reached_at_answer"]]
        print(f"    끝내 정답이 전달되지 않은 질문: {miss}")
        if a.verbose:
            for r in [r for r in rows if r["mode"] == mode]:
                print(f"    {r['id']:<6} 첫{'○' if r['first_has_gold'] else '✕'} 도달={r['reached_at_answer']} 검색={r['searches']} "
                      f"첫 근거={r['first_evidence']}")
    if a.out:
        with open(a.out, "w", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    asyncio.run(main())
