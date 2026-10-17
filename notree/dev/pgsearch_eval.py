"""실제 강의 자료로 PG(pg_trgm) 근거 검색을 평가한다: 정답 구간을 찾는가, 강의 2개 우선 검색이 맞는 강의를 고르는가.

    uv run python -m notree.dev.lecture_import data/lectures/*.md --out data/qa_docs.jsonl
    uv run python -m notree.dev.pgsearch_eval --load                     # 문서 적재 (테이블 다시 만듦)
    uv run python -m notree.dev.pgsearch_eval --questions data/qa_questions.json --out out/pgsearch.jsonl

DB: NOTREE_PG_QA_DSN (기본 postgresql://postgres@127.0.0.1:55432/notree_qa). pg_trgm 확장 필요.

검색어: 질문에서 조사를 떼어 낸 단어(keywords_of, LLM이 시간 안에 못 끝났을 때 그래프가 쓰는 대체 방법)와 같은 방식.
  실제 그래프는 LLM이 고른 검색어를 쓰므로 결과가 다를 수 있다 (--keywords로 질문별 검색어를 따로 줄 수 있음).
점수 방식 (0~1)
  kw_mean  : 검색어마다 word_similarity(검색어, 본문)의 평균   ← 가짜 검색의 '몇 개가 들어 있나'에 가장 가까움
  kw_hit   : word_similarity ≥ 0.6(pg_trgm 기본값)인 검색어의 비율
  question : word_similarity(질문 전체, 본문)
범위
  course   : 과목 전체 전사
  top2     : 노트 검색 상위 결과로 강의 2개를 고르고 그 전사만
  expand   : top2에서 최고 점수가 --enough 미만이면 과목 전체를 더 찾아 합침 (우선 검색 2개 + 부족 시 확장)
정답: 질문의 gold {강의: [청크]} 중 하나라도 찾으면 맞음. 노트는 실시간 요약 구간(s청크), 전사는 t청크로 비교한다.
"""
import argparse
import asyncio
import json
import os
import statistics
import time
from pathlib import Path

import asyncpg

from notree.graphs.qa.llm import keywords_of

DSN = os.getenv("NOTREE_PG_QA_DSN", "postgresql://postgres@127.0.0.1:55432/notree_qa")
SCORES = {
    "kw_mean": "(SELECT avg(word_similarity(k, text))::float8 FROM unnest($2::text[]) k)",
    "kw_hit": "(SELECT avg((word_similarity(k, text) >= 0.6)::int)::float8 FROM unnest($2::text[]) k)",
    "question": "word_similarity($3::text, text)",
}

SCHEMA = """
DROP TABLE IF EXISTS qa_docs;
CREATE TABLE qa_docs(doc_id text PRIMARY KEY, course_id text, lecture_id text, source text, kind text, chunk int, text text);
CREATE INDEX qa_docs_scope ON qa_docs(course_id, source, lecture_id);
CREATE INDEX qa_docs_trgm ON qa_docs USING gin (text gin_trgm_ops);
"""


async def load(con, path: str) -> int:
    await con.execute(SCHEMA)
    rows = []
    for line in open(path, encoding="utf-8"):
        d = json.loads(line)
        chunk = int(d["id"].rsplit(":", 1)[1][1:]) if d["id"].rsplit(":", 1)[1][0] in "st" else None
        rows.append((d["id"], d["course_id"], d["lecture_id"], d["source"], d["kind"], chunk, d["text"]))
    await con.executemany("INSERT INTO qa_docs VALUES($1,$2,$3,$4,$5,$6,$7)", rows)
    await con.execute("ANALYZE qa_docs")
    return len(rows)


async def search(con, score: str, course: str, source: str, kw: list[str], q: str, limit: int,
                 lectures: list[str] | None = None) -> list[dict]:
    sql = (f"SELECT doc_id, lecture_id, kind, chunk, {SCORES[score]} AS s FROM qa_docs "
           f"WHERE course_id=$1 AND source='{source}' AND $2::text[] IS NOT NULL AND $3::text IS NOT NULL"   # 안 쓰는 인자도 형을 정해 둠
           + (" AND lecture_id = ANY($5::text[])" if lectures else "")
           + " ORDER BY s DESC NULLS LAST LIMIT $4")
    args = [course, kw, q, limit] + ([lectures] if lectures else [])
    return [dict(r) for r in await con.fetch(sql, *args)]


def is_gold(hit: dict, gold: dict) -> bool:
    return hit["chunk"] is not None and hit["chunk"] in gold.get(hit["lecture_id"], [])


def first_rank(hits: list[dict], gold: dict) -> int | None:
    return next((i + 1 for i, h in enumerate(hits) if is_gold(h, gold)), None)


async def evaluate(con, qs: list[dict], score: str, enough: float, kw_override: dict) -> list[dict]:
    out = []
    for item in qs:
        kw = kw_override.get(item["id"]) or keywords_of(item["q"])
        t0 = time.perf_counter()
        notes = await search(con, score, item["course"], "note", kw, item["q"], 10)
        best_by_lec: dict[str, float] = {}
        for n in notes:
            best_by_lec[n["lecture_id"]] = max(best_by_lec.get(n["lecture_id"], 0), n["s"] or 0)
        top2 = sorted(best_by_lec, key=lambda k: -best_by_lec[k])[:2]
        t_note = time.perf_counter() - t0

        t1 = time.perf_counter()
        course_hits = await search(con, score, item["course"], "transcript", kw, item["q"], 5)
        t_course = time.perf_counter() - t1
        t2 = time.perf_counter()
        top2_hits = await search(con, score, item["course"], "transcript", kw, item["q"], 5, top2) if top2 else []
        expanded = False
        exp_hits = top2_hits
        if not top2_hits or (top2_hits[0]["s"] or 0) < enough:
            expanded = True
            more = await search(con, score, item["course"], "transcript", kw, item["q"], 5)
            seen = {h["doc_id"] for h in top2_hits}
            exp_hits = sorted(top2_hits + [h for h in more if h["doc_id"] not in seen], key=lambda h: -(h["s"] or 0))[:5]
        t_top2 = time.perf_counter() - t2

        g = item["gold"]
        out.append({
            "id": item["id"], "q": item["q"], "keywords": kw, "score": score, "negative": not g,
            "note_rank": first_rank(notes, g), "note_top_score": notes[0]["s"] if notes else None,
            "lecture_at2": bool(set(top2) & set(g)) if g else None, "top2": top2,
            "course_rank": first_rank(course_hits, g), "top2_rank": first_rank(top2_hits, g), "expand_rank": first_rank(exp_hits, g),
            "expanded": expanded,
            "course_top_score": course_hits[0]["s"] if course_hits else None,
            "gold_score": next((h["s"] for h in course_hits if is_gold(h, g)), None),
            "ms": {"note": round(t_note * 1000, 1), "course": round(t_course * 1000, 1), "top2_expand": round(t_top2 * 1000, 1)},
        })
    return out


def at(rows, key, k):
    rs = [r for r in rows if not r["negative"]]
    return sum(1 for r in rs if r[key] is not None and r[key] <= k) / len(rs)


def report(rows_by_score: dict[str, list[dict]]) -> str:
    lines = [f"{'점수 방식':<10}{'노트@3':>8}{'강의@2':>8}{'전사 과목@3':>12}{'우선2@3':>9}{'우선2+확장@3':>13}{'과목@5':>8}{'확장 비율':>9}"]
    for score, rows in rows_by_score.items():
        rs = [r for r in rows if not r["negative"]]
        lec = sum(1 for r in rs if r["lecture_at2"]) / len(rs)
        lines.append(f"{score:<10}{at(rows, 'note_rank', 3):>8.0%}{lec:>8.0%}{at(rows, 'course_rank', 3):>12.0%}{at(rows, 'top2_rank', 3):>9.0%}"
                     f"{at(rows, 'expand_rank', 3):>13.0%}{at(rows, 'course_rank', 5):>8.0%}{sum(r['expanded'] for r in rs) / len(rs):>9.0%}")
    return "\n".join(lines)


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--load", action="store_true")
    ap.add_argument("--docs", default="data/qa_docs.jsonl")
    ap.add_argument("--questions", default="data/qa_questions.json")
    ap.add_argument("--scores", default="kw_mean,kw_hit,question")
    ap.add_argument("--enough", type=float, default=0.6, help="우선 검색에서 이 점수 미만이면 과목 전체로 확장")
    ap.add_argument("--keywords", default="", help="질문 id → 검색어 목록 JSON (LLM 검색어로 다시 볼 때)")
    ap.add_argument("--out", default="")
    ap.add_argument("--verbose", action="store_true")
    a = ap.parse_args()
    con = await asyncpg.connect(DSN)
    if a.load:
        print(f"적재: {await load(con, a.docs)}개")
    qs = json.load(open(a.questions, encoding="utf-8"))["questions"]
    kw_override = json.load(open(a.keywords, encoding="utf-8")) if a.keywords else {}
    by_score = {s: await evaluate(con, qs, s, a.enough, kw_override) for s in a.scores.split(",")}
    print(f"질문 {sum(1 for q in qs if q['gold'])}개 (+ 범위 밖 {sum(1 for q in qs if not q['gold'])}개) · 확장 기준 {a.enough}\n")
    print(report(by_score))
    for score, rows in by_score.items():
        rs = [r for r in rows if not r["negative"]]
        gs = [r["gold_score"] for r in rs if r["gold_score"] is not None]
        neg = [r["course_top_score"] for r in rows if r["negative"]]
        ms = [sum(r["ms"].values()) for r in rows]
        print(f"\n[{score}] 정답 근거 점수 중앙 {statistics.median(gs):.2f} (최소 {min(gs):.2f}) · 범위 밖 질문의 최고 점수 {neg} · "
              f"질문당 검색 {statistics.median(ms):.1f}ms")
        if a.verbose:
            for r in rows:
                mark = "  " if r["negative"] else ("○ " if r["expand_rank"] and r["expand_rank"] <= 3 else "✕ ")
                print(f"  {mark}{r['id']:<6} 노트{r['note_rank']} 과목{r['course_rank']} 우선2{r['top2_rank']} 확장{r['expand_rank']} "
                      f"강의={r['top2']} 검색어={r['keywords']}")
    if a.out:
        Path(a.out).parent.mkdir(parents=True, exist_ok=True)
        with open(a.out, "w", encoding="utf-8") as f:
            for rows in by_score.values():
                for r in rows:
                    f.write(json.dumps(r, ensure_ascii=False) + "\n")
    await con.close()


if __name__ == "__main__":
    asyncio.run(main())
