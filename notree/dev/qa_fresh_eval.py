"""새 질문 세트로 **검색 + 실제 답변**을 함께 평가한다 (그래프 + PG 검색 + LLM, 웹 없음).

    uv run python -m notree.dev.lecture_import data/lectures --out data/qa_docs.jsonl  # 강의 .md → 문서
    uv run python -m notree.dev.qa_fresh_eval --load --llm fake                       # PG에 적재 (한 번) + 키 없이 점검
    uv run python -m notree.dev.qa_fresh_eval --llm openai --limits 3,5              # 실제 답변, 근거 3개·5개 비교
    uv run python -m notree.dev.qa_fresh_eval --llm fake                              # 키 없이: 단어 추출 검색어 + 첫 근거로 답함 (검색만 봄)

입력 (data/는 git에 올리지 않음)
  --questions data/fresh/questions.md    질문만. 이것만 LLM에 간다
  --key       data/fresh/answer_key.md   정답 항목·근거 ID·기대 상태·채점 주의. **실행이 끝난 뒤 채점에만** 쓰고 프롬프트에 넣지 않는다

출력
  --out   data/fresh/out/run.jsonl     질문 × 근거 수마다 한 줄: 상태, 답변 문장·인용, LLM에 전달한 근거(시도별), 토큰·시간
  --sheet data/fresh/out/sheet.md      채점표: 질문, 정답 항목·주의, 답변(문장별 인용), 인용한 원문. 의미 채점은 이 표로 사람(또는
                                       답변 모델과 다른 모델)이 한다

자동으로 보는 것 (의미 채점이 아님)
  - 기대 상태와 일치하는지 (completed / 부분 답변 / 근거 부족)
  - 정답 항목별로 그 항목의 근거 ID가 LLM에 전달됐는지(첫 답변 / 전체), 답변이 인용했는지.
    정답표의 근거 ID는 "비완전한 초안"이라 다른 구간으로도 맞게 답할 수 있다 → 이 수치는 하한
  - 근거 수(--limits): 출처마다 검색 상한. 노트에서 멈추면 전달 근거 수 = 이 값 (MAX_EVIDENCE 5는 그대로)
"""
import argparse
import asyncio
import json
import re
import statistics
import time
from pathlib import Path

from notree.dev.qa_pg_eval import USER
from notree.graphs.qa import build_qa_graph
from notree.graphs.qa.llm import Answer, Sentence, Understanding, get_qa_llm, keywords_of
from notree.graphs.qa.retriever import Hit
from notree.mcp_server import pg_search

COURSE = {"머신러닝": "ml", "OOP": "oop"}
COURSE_NAME = {"ml": "머신러닝", "oop": "객체지향 프로그래밍"}
STATUS = {"completed": "completed", "부분 답변": "부분 답변", "근거 부족": "근거 부족"}


def parse_questions(path: str) -> list[dict]:
    out, course = [], None
    for line in open(path, encoding="utf-8"):
        if m := re.match(r"##\s+(.+)", line):
            course = COURSE.get(m.group(1).strip())
        elif m := re.match(r"(fresh\d+)\.\s+(.+)", line):
            out.append({"id": m.group(1), "course": course, "q": m.group(2).strip()})
    return out


def parse_key(path: str) -> dict[str, dict]:
    """{id: {status, items: [{text, gold: [doc_id]}], unanswerable: [..], notes: [..]}}"""
    key, cur = {}, None
    for line in open(path, encoding="utf-8"):
        if m := re.match(r"##\s+(fresh\d+)\s+·\s+(.+)", line):
            cur = key[m.group(1)] = {"status": STATUS[m.group(2).strip()], "items": [], "unanswerable": [], "notes": []}
        elif cur is None:
            continue
        elif m := re.match(r"(\d+)\.\s+(.+)", line):
            cur["items"].append({"text": m.group(2).strip(), "gold": []})
        elif m := re.match(r"\s+근거:\s+(.+)", line):
            cur["items"][-1]["gold"] = [g.split("/", 1)[-1].strip() for g in m.group(1).split(",")]
        elif m := re.match(r"-\s+자료로 확인 불가:\s+(.+)", line):
            cur["unanswerable"].append(m.group(1).strip())
        elif m := re.match(r"-\s+채점 주의:\s+(.+)", line):
            cur["notes"].append(m.group(1).strip())
    return key


class PgRetriever:
    """PG 검색. 출처마다 상한을 limit으로 고정 (근거 3개·5개 비교). 웹은 빈 결과 (강의 자료만 평가)."""

    def __init__(self, course: str, limit: int) -> None:
        self.course, self.limit, self.calls = course, limit, []

    async def search(self, source, keywords, limit=3, lecture_ids=None):
        self.calls.append(source if not lecture_ids else f"{source}@{','.join(lecture_ids)}")
        if source == "web":
            return []
        rows = await asyncio.to_thread(pg_search.search, USER, self.course, source, keywords, self.limit, lecture_ids)
        return [Hit(r["id"], source, r["text"], r["score"], r["ref"], r["lecture_id"]) for r in rows]


class Recorder:
    """LLM을 감싸 understand 결과와 answer에 **실제로 전달된 근거**·원본 문장을 남긴다."""

    def __init__(self, inner) -> None:
        self.inner, self.understood, self.attempts = inner, None, []

    def __getattr__(self, name):
        return getattr(self.inner, name)

    async def understand(self, question, course):
        u = await self.inner.understand(question, course)
        self.understood = u.model_dump()
        return u

    async def answer(self, question, evidence, attempt, problem="", related=True):
        a = await self.inner.answer(question, evidence, attempt, problem=problem, related=related)
        self.attempts.append({"attempt": attempt, "evidence": [h["id"] for h in evidence],
                              "needs_more": a.needs_more_evidence})
        return a


class SearchOnlyLLM:
    """키 없이 검색만 볼 때: 단어 추출 검색어, 첫 근거를 인용해 답함 (항상 '충분')."""

    def __init__(self) -> None:
        self.calls, self.usage = [], []

    async def understand(self, question, course):
        self.calls.append("understand")
        return Understanding(related=True, keywords=keywords_of(question))

    async def answer(self, question, evidence, attempt, problem="", related=True):
        self.calls.append(f"answer#{attempt}")
        if not evidence:
            return Answer(sentences=[Sentence(text="강의 자료에서 근거를 찾지 못했다")], needs_more_evidence=True)
        return Answer(sentences=[Sentence(text="(검색만)", citations=[evidence[0]["id"]])])


async def run_one(graph, q: dict, llm_name: str, limit: int, deadline_sec: float) -> dict:
    inner = SearchOnlyLLM() if llm_name == "fake" else get_qa_llm(llm_name)
    llm, ret = Recorder(inner), PgRetriever(q["course"], limit)
    t0 = time.perf_counter()
    cfg = {"configurable": {"llm": llm, "retriever": ret, "deadline_at": time.time() + deadline_sec}}
    out = await graph.ainvoke({"question": q["q"], "course": COURSE_NAME[q["course"]]}, cfg)
    usage = getattr(inner, "usage", []) or []
    return {"id": q["id"], "limit": limit, "llm": llm_name, "q": q["q"],
            "keywords": (llm.understood or {}).get("keywords", []), "related": (llm.understood or {}).get("related"),
            "searches": ret.calls, "attempts": llm.attempts,
            "status": out.get("status"), "label": out.get("label"), "answer": out.get("answer"),
            "sentences": out.get("sentences") or [], "citations": out.get("citations") or [],
            "sources": [{"id": s["id"], "source": s["source"], "text": s["text"]} for s in out.get("sources") or []],
            "llm_calls": len(inner.calls), "prompt_tokens": sum(u["prompt_tokens"] for u in usage),
            "completion_tokens": sum(u["completion_tokens"] for u in usage),
            "seconds": round(time.perf_counter() - t0, 2)}


def chunk(doc_id: str) -> str:
    """같은 전사 구간의 요약 노트 s{n}과 전사 t{n}은 같은 근거로 센다 (정답표는 f·t만 적음)."""
    lec, part = doc_id.rsplit(":", 1)
    return f"{lec}:t{part[1:]}" if part.startswith("s") else doc_id


def score(row: dict, k: dict) -> dict:
    """자동 채점 (의미 채점 아님). 근거 ID 일치는 하한."""
    first = {chunk(i) for i in row["attempts"][0]["evidence"]} if row["attempts"] else set()
    passed = {chunk(i) for a in row["attempts"] for i in a["evidence"]}
    cited = {chunk(i) for i in row["citations"]}
    items = [{"first": bool({chunk(g) for g in it["gold"]} & first), "passed": bool({chunk(g) for g in it["gold"]} & passed),
              "cited": bool({chunk(g) for g in it["gold"]} & cited)} for it in k["items"]]
    return {"expected": k["status"], "status_ok": row["status"] == k["status"], "items": items}


def summarize(rows: list[dict], key: dict, limit: int) -> None:
    rs = [r for r in rows if r["limit"] == limit]
    print(f"\n[근거 {limit}개] 질문 {len(rs)}개")
    for exp in ("completed", "부분 답변", "근거 부족"):
        g = [r for r in rs if key[r["id"]]["status"] == exp]
        if g:
            got: dict[str, int] = {}
            for r in g:
                got[r["status"]] = got.get(r["status"], 0) + 1
            print(f"  기대 {exp:<6} {len(g):>2}개 → 일치 {sum(r['score']['status_ok'] for r in g):>2}개   실제 {got}")
    its = [it for r in rs for it in r["score"]["items"]]
    if its:
        n = len(its)
        print(f"  정답 항목 {n}개 (근거 ID 일치, 하한): 첫 전달 근거 {sum(i['first'] for i in its)/n:.0%} · "
              f"전달 근거 전체 {sum(i['passed'] for i in its)/n:.0%} · 답변이 인용 {sum(i['cited'] for i in its)/n:.0%}")
    full = [r for r in rs if r["score"]["items"]]
    if full:
        allp = sum(all(i["passed"] for i in r["score"]["items"]) for r in full)
        print(f"  답할 수 있는 질문 {len(full)}개 중 모든 항목의 근거가 전달된 질문 {allp}개 (하한)")
    ev = [len(r["attempts"][0]["evidence"]) for r in rs if r["attempts"]]
    print(f"  첫 전달 근거 수 중앙 {statistics.median(ev) if ev else 0} · 과목 전체 확장 "
          f"{sum('transcript' in r['searches'] and any(s.startswith('transcript@') for s in r['searches']) for r in rs)}개 질문 · "
          f"LLM 호출 중앙 {statistics.median(r['llm_calls'] for r in rs)} · "
          f"입력 토큰 중앙 {statistics.median(r['prompt_tokens'] for r in rs):.0f} · 출력 {statistics.median(r['completion_tokens'] for r in rs):.0f} · "
          f"시간 중앙 {statistics.median(r['seconds'] for r in rs):.1f}s / 최대 {max(r['seconds'] for r in rs):.1f}s")


def write_sheet(path: Path, rows: list[dict], key: dict, qs: list[dict], limits: list[int]) -> None:
    by = {(r["id"], r["limit"]): r for r in rows}
    L = ["# 채점표 (정답표 포함 — 공개 저장소·LLM 프롬프트에 넣지 말 것)", "",
         "항목마다 ○(충족) △(일부) ✕(누락·틀림). 인용 원문이 그 문장을 뒷받침하는지, 채점 주의 위반, 자료에 없는 내용을 지어냈는지 함께 본다.", ""]
    for q in qs:
        k = key[q["id"]]
        L += [f"## {q['id']} · 기대 {k['status']}", "", f"**Q.** {q['q']}", ""]
        for n, it in enumerate(k["items"], 1):
            L.append(f"- 정답 {n}. {it['text']}  `{', '.join(it['gold'])}`")
        L += [f"- 자료로 확인 불가: {u}" for u in k["unanswerable"]] + [f"- 채점 주의: {x}" for x in k["notes"]] + [""]
        for lim in limits:
            r = by.get((q["id"], lim))
            if not r:
                continue
            L += [f"### 근거 {lim}개 → {r['status']} [{r['label']}]  검색어 {r['keywords']}  검색 {r['searches']}", ""]
            for s in r["sentences"]:
                L.append(f"- {s['text']}  {' '.join('[' + c + ']' for c in s.get('citations', []))}")
            if not r["sentences"]:
                L.append(f"- (본문) {r['answer']}")
            L.append("")
            for s in r["sources"]:
                L.append(f"  > [{s['id']}] {s['text'][:300]}")
            L.append("")
    path.write_text("\n".join(L), encoding="utf-8")


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--questions", default="data/fresh/questions.md")
    ap.add_argument("--key", default="data/fresh/answer_key.md")
    ap.add_argument("--llm", default="openai")
    ap.add_argument("--limits", default="3,5")
    ap.add_argument("--deadline", type=float, default=90.0)
    ap.add_argument("--only", default="", help="쉼표로 질문 id 일부만")
    ap.add_argument("--load", action="store_true", help="--docs를 PG에 먼저 적재 (사용자 'eval')")
    ap.add_argument("--docs", default="data/qa_docs.jsonl")
    ap.add_argument("--out", default="data/fresh/out/run.jsonl")
    ap.add_argument("--sheet", default="data/fresh/out/sheet.md")
    a = ap.parse_args()
    if a.load:
        by_course: dict[str, list[dict]] = {}
        for line in open(a.docs, encoding="utf-8"):
            d = json.loads(line)
            by_course.setdefault(d["course_id"], []).append(d)
        print("적재:", {c: pg_search.upsert_docs(USER, c, ds) for c, ds in by_course.items()})
    qs = parse_questions(a.questions)
    if a.only:
        qs = [q for q in qs if q["id"] in a.only.split(",")]
    limits = [int(x) for x in a.limits.split(",")]
    graph, rows = build_qa_graph(), []
    for lim in limits:                         # 답변 생성이 끝날 때까지 정답표는 읽지 않는다
        for q in qs:
            r = await run_one(graph, q, a.llm, lim, a.deadline)
            rows.append(r)
            print(f"[{lim}] {q['id']} {r['status']:<6} 검색={r['searches']} 근거={[x['evidence'] for x in r['attempts']]} "
                  f"{r['seconds']}s", flush=True)
    key = parse_key(a.key)
    for r in rows:
        r["score"] = score(r, key[r["id"]])
    print(f"\n모델 {a.llm} · 검색어 {'단어 추출' if a.llm == 'fake' else 'LLM'} · 웹 없음 · 마감 {a.deadline:.0f}초")
    for lim in limits:
        summarize(rows, key, lim)
    Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    with open(a.out, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    write_sheet(Path(a.sheet), rows, key, qs, limits)
    print(f"\n→ {a.out}\n→ {a.sheet} (채점표)")


if __name__ == "__main__":
    asyncio.run(main())
