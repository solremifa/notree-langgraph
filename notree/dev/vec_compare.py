"""검색 방식 오프라인 비교: pg_trgm · 벡터 · 결합(RRF). worker·그래프는 건드리지 않는다.

    uv run python -m notree.dev.vec_compare --check                        # 실행 전 확인값 (문서 수·분할·정답 ID·검색어)
    uv run python -m notree.dev.pgsearch_eval --load --scores kw_mean      # PG에 문서 적재 (한 번)
    uv run python -m notree.dev.vec_compare --emb data/vec/oai-3-small \
        --fresh-keywords data/fresh/out/run-openai.jsonl --out data/vec/compare.jsonl

같은 조건
  문서: data/qa_docs.jsonl 그대로 (분할 · 버전 같음). 범위: 질문의 과목 전체, 출처(노트 / 전사)별로 따로 찾는다.
  후보 수: 방식마다 출처별 상위 --pool개(기본 20)를 만들고 @3 · @5로 자른다. 권한 범위는 과목 하나 (실험 데이터에 사용자가 하나뿐).
방식
  trgm   : 지금 PG 검색과 같은 kw_mean (검색어별 word_similarity 평균). 검색어는 미리 준비한 것을 쓴다
           (기존 33: data/qa_keywords.json의 LLM 검색어 / 새 30: --fresh-keywords로 준 실제 LLM 실행의 검색어, 없으면 단어 추출)
  vec    : 질문 벡터와 문서 벡터의 코사인 (정확 계산, 색인 없음 — pgvector의 정확 검색과 순위가 같다)
  hybrid : 두 방식의 순위를 RRF(1/(60+순위))로 합침. **결합 방법의 후보 하나일 뿐이고, 점수를 0.3 · 0.6 기준에 대입하지 않는다**
지표 (모두 검색 단계. 그래프가 고른 근거 · 답변 품질은 qa_fresh_eval의 run 결과로 따로 본다)
  R@k 노트 / 전사   : 그 출처 상위 k개 안에 정답 근거가 있는 질문 비율
  R@k 둘 중 하나     : 노트 상위 k개 ∪ 전사 상위 k개 (후보 최대 2k개) 안에 있는 비율 — "통합 상위 k"가 아니다
  항목 충족 @k (새 30): 정답표의 항목마다 그 항목 근거가 노트 · 전사 상위 k개 안에 있는 비율 (복합 질문의 부분별)
  범위 밖 질문       : 방식별 최고 점수만 보여 준다. 점수만으로 거절을 정하지 않는다 (실제 거절 판단은 답변 평가에서)
  시간               : 검색만(준비된 검색어 · 질문 벡터로 찾는 시간)과, 질문 처리 전체에 더해지는 준비 시간(질문 임베딩 · LLM 검색어)을 따로
정답표(answer_key.md)는 검색이 끝난 뒤 채점에만 읽는다. 출력에는 문서 ID · 순위 · 점수만 남기고 원문은 남기지 않는다.
"""
import argparse
import asyncio
import collections
import json
import statistics
import time
from pathlib import Path

import asyncpg
import numpy as np

from notree.dev.pgsearch_eval import DSN, SCORES
from notree.dev.qa_fresh_eval import chunk, parse_key, parse_questions
from notree.graphs.qa.llm import keywords_of

RRF_K = 60
SOURCES = ("note", "transcript")


# ── 입력 ────────────────────────────────────────────────────────
def load_sets(a) -> list[dict]:
    """[{set, id, course, q, keywords, kw_from, gold_docs: set|None, gold_chunks: dict|None, items: [[ids]], negative}]"""
    out = []
    if a.old:
        kw = json.load(open(a.old_keywords, encoding="utf-8")) if a.old_keywords and Path(a.old_keywords).exists() else {}
        for q in json.load(open(a.old, encoding="utf-8"))["questions"]:
            out.append({"set": "old", "id": q["id"], "course": q["course"], "q": q["q"],
                        "keywords": kw.get(q["id"]) or keywords_of(q["q"]), "kw_from": "LLM" if q["id"] in kw else "단어 추출",
                        "gold_chunks": q["gold"], "gold_docs": None, "items": None, "negative": not q["gold"]})
    if a.fresh:
        kw = fresh_keywords(a.fresh_keywords)
        for q in parse_questions(a.fresh):
            out.append({"set": "fresh", "id": q["id"], "course": q["course"], "q": q["q"],
                        "keywords": kw.get(q["id"]) or keywords_of(q["q"]), "kw_from": "LLM" if q["id"] in kw else "단어 추출",
                        "gold_chunks": None, "gold_docs": None, "items": None, "negative": None})
    return out


def fresh_keywords(path: str) -> dict:
    """실제 LLM 실행 결과(run.jsonl, llm != fake)의 검색어, 또는 {id: [검색어]} JSON. 가짜 실행의 검색어는 쓰지 않는다."""
    if not path or not Path(path).exists():
        return {}
    if path.endswith(".jsonl"):
        kw = {}
        for line in open(path, encoding="utf-8"):
            r = json.loads(line)
            if r.get("llm") != "fake" and r.get("keywords") and r["id"] not in kw:
                kw[r["id"]] = r["keywords"]
        return kw
    return json.load(open(path, encoding="utf-8"))


def understand_timing(path: str) -> dict | None:
    if not path or not path.endswith(".jsonl") or not Path(path).exists():
        return None
    seen, ms = set(), []
    for line in open(path, encoding="utf-8"):
        r = json.loads(line)
        if r.get("llm") != "fake" and r.get("understand_ms") is not None and r["id"] not in seen:
            seen.add(r["id"])
            ms.append(r["understand_ms"])
    return {"n": len(ms), "median": statistics.median(ms), "max": max(ms)} if ms else None


def attach_fresh_gold(qs: list[dict], key_path: str) -> None:
    """검색이 끝난 뒤 채점에만 쓴다."""
    key = parse_key(key_path)
    for q in qs:
        if q["set"] != "fresh":
            continue
        k = key[q["id"]]
        q["items"] = [{chunk(g) for g in it["gold"]} for it in k["items"] if it["gold"]]   # 요약 노트 s{n} = 전사 t{n}
        q["gold_docs"] = {g for it in q["items"] for g in it}
        q["negative"] = k["status"] == "근거 부족"


def is_gold(q: dict, doc_id: str) -> bool:
    if q["gold_docs"] is not None:
        return chunk(doc_id) in q["gold_docs"]
    if not q["gold_chunks"]:
        return False
    lec, part = doc_id.rsplit(":", 1)
    return part[0] in "st" and int(part[1:]) in q["gold_chunks"].get(lec, [])


# ── 검색 ────────────────────────────────────────────────────────
async def trgm(con, q: dict, source: str, pool: int) -> tuple[list[tuple[str, float]], float]:
    t0 = time.perf_counter()
    rows = await con.fetch(f"SELECT doc_id, {SCORES['kw_mean']} AS s FROM qa_docs WHERE course_id=$1 AND source=$3 "
                           "ORDER BY s DESC NULLS LAST, doc_id LIMIT $4", q["course"], q["keywords"], source, pool)
    return [(r["doc_id"], r["s"] or 0.0) for r in rows], (time.perf_counter() - t0) * 1000


class Vec:
    def __init__(self, path: str, docs: dict[str, dict]) -> None:
        p = Path(path)
        self.meta = json.load(open(p / "meta.json", encoding="utf-8"))
        self.dv, self.qv = np.load(p / "docs.npy"), np.load(p / "questions.npy")
        self.doc_ids = json.load(open(p / "doc_ids.json", encoding="utf-8"))
        self.q_index = {q: i for i, q in enumerate(json.load(open(p / "question_ids.json", encoding="utf-8")))}
        missing = set(docs) ^ set(self.doc_ids)
        if missing:
            raise SystemExit(f"임베딩의 문서 ID가 문서 파일과 다르다 ({len(missing)}개). 같은 qa_docs.jsonl로 다시 만들 것")
        self.masks = {(c, s): np.array([docs[d]["course_id"] == c and docs[d]["source"] == s for d in self.doc_ids])
                      for c in {d["course_id"] for d in docs.values()} for s in SOURCES}

    def search(self, q: dict, source: str, pool: int) -> tuple[list[tuple[str, float]], float]:
        t0 = time.perf_counter()
        qv = self.qv[self.q_index[f"{q['set']}:{q['id']}"]]
        idx = np.flatnonzero(self.masks[(q["course"], source)])
        sims = self.dv[idx] @ qv
        top = idx[np.argsort(-sims)[:pool]]
        res = [(self.doc_ids[i], float(self.dv[i] @ qv)) for i in top]
        return res, (time.perf_counter() - t0) * 1000


def rrf(*lists: list[tuple[str, float]]) -> list[tuple[str, float]]:
    s = collections.defaultdict(float)
    for lst in lists:
        for rank, (d, _) in enumerate(lst, 1):
            s[d] += 1.0 / (RRF_K + rank)
    return sorted(s.items(), key=lambda x: -x[1])


# ── 집계 ────────────────────────────────────────────────────────
def first_rank(q: dict, lst: list[tuple[str, float]]) -> int | None:
    return next((i for i, (d, _) in enumerate(lst, 1) if is_gold(q, d)), None)


def tie_at(q: dict, lst: list[tuple[str, float]], k: int) -> bool:
    """정답의 포함 여부가 k번째 경계의 동점 순서에 달렸는가 (동점 정렬은 임의라 결과가 바뀔 수 있다)"""
    r = first_rank(q, lst)
    if r is None or len(lst) <= k:
        return False
    g = lst[r - 1][1]
    return (r <= k and g == lst[k][1]) or (r > k and g == lst[k - 1][1])


def items_covered(q: dict, lists: dict[str, list], k: int) -> tuple[int, int]:
    cand = {chunk(d) for s in SOURCES for d, _ in lists[s][:k]}
    return sum(1 for it in q["items"] if cand & it), len(q["items"])


def pct(a: int, b: int) -> str:
    return f"{a / b:.0%} ({a}/{b})" if b else "-"


def report(rows: list[dict], methods: list[str], ks: list[int], vec_meta: dict | None, und: dict | None = None) -> str:
    out = []
    for st in ("old", "fresh"):
        rs = [r for r in rows if r["set"] == st]
        if not rs:
            continue
        pos = [r for r in rs if not r["negative"]]
        kwf = collections.Counter(r["kw_from"] for r in rs)
        out.append(f"\n## {'기존 33개 (구간 제목을 참고해 만든 질문)' if st == 'old' else '새 30개 (기준값 조정에 쓰지 않은 AI 생성 질문)'}"
                   f" · 정답 있는 질문 {len(pos)}개 · 범위 밖 {len(rs) - len(pos)}개 · 검색어 {dict(kwf)}")
        head = f"{'방식':<8}" + "".join(f"{f'R@{k} 노트':>12}{f'R@{k} 전사':>12}{f'R@{k} 둘 중':>12}" for k in ks)
        if st == "fresh":
            head += "".join(f"{f'항목@{k}':>12}" for k in ks)
        out.append(head)
        out.append(f"  (칸 = 비율 개수 · R@k 분모 {len(pos)}개 질문"
                   + (f" · 항목 분모 {sum(r['items'][methods[0]][str(ks[0])][1] for r in pos)}개 항목)" if st == "fresh" else ")"))
        for m in methods:
            line = f"{m:<8}"
            for k in ks:
                n = sum(1 for r in pos if (r["rank"][m]["note"] or 99) <= k)
                t = sum(1 for r in pos if (r["rank"][m]["transcript"] or 99) <= k)
                e = sum(1 for r in pos if min(r["rank"][m]["note"] or 99, r["rank"][m]["transcript"] or 99) <= k)
                line += "".join(f"{f'{x / len(pos):.0%} {x}':>12}" for x in (n, t, e))
            if st == "fresh":
                for k in ks:
                    c = sum(r["items"][m][str(k)][0] for r in pos)
                    a = sum(r["items"][m][str(k)][1] for r in pos)
                    line += f"{f'{c / a:.0%} {c}':>12}" if a else f"{'-':>12}"
            out.append(line)
        for m in methods:
            ties = {k: sum(1 for r in pos if r["tie"][m][str(k)]) for k in ks}
            if any(ties.values()):
                out.append(f"  {m}: 정답이 k번째 경계에서 동점이라 순서에 따라 결과가 바뀔 수 있는 질문 " + " · ".join(f"@{k} {v}개" for k, v in ties.items())
                           + " (동점은 doc_id 순으로 고정)")
        neg = [r for r in rs if r["negative"]]
        if neg:
            for m in methods:
                tops = [round(max(r["top"][m]["note"], r["top"][m]["transcript"]), 3) for r in neg]
                gold = [r["gold_best"][m] for r in pos if r["gold_best"][m] is not None]
                g = f" · 정답이 있는 질문의 정답 점수 중앙 {statistics.median(gold):.3f} 최소 {min(gold):.3f}" if gold else ""
                out.append(f"  범위 밖 {len(neg)}개 · {m} 최고 점수 {tops}{g}")
            out.append("  (점수만으로 범위 밖을 거절하지 않는다. 겹치면 LLM · 별도 판단이 필요)")
        for m in methods:
            ms = [r["ms"][m] for r in rs]
            out.append(f"  검색만 {m}: 질문당 중앙 {statistics.median(ms):.1f}ms (노트+전사, 후보 {rows[0]['pool']}개씩)")
    if vec_meta:
        qms = list(vec_meta["question_ms"].values())
        out.append(f"\n## 비용 · 준비 시간 ({vec_meta['model']}{' — 가짜, 결과로 쓰지 말 것' if vec_meta.get('fake') else ''})")
        out.append(f"  초기 색인: 문서 {vec_meta['docs']}개 · {vec_meta['dim']}차원 · {vec_meta['doc_seconds']}초 · 토큰 {vec_meta['doc_tokens']}")
        out.append(f"  질문 임베딩(질문 처리 전체에 더해짐): 중앙 {statistics.median(qms):.0f}ms · 최대 {max(qms):.0f}ms · 토큰 {vec_meta['question_tokens']}")
    if und:
        out.append(f"  LLM 검색어 생성(새 30, {und['n']}개): 중앙 {und['median']:.0f}ms · 최대 {und['max']:.0f}ms (질문 처리 전체에 더해짐)")
    else:
        out.append("  LLM 검색어 생성 시간: 측정값 없음 (qa_fresh_eval --llm openai 실행 결과의 understand_ms를 --fresh-keywords로 주면 집계)")
    return "\n".join(out)


def check(a, docs: dict[str, dict], qs: list[dict]) -> None:
    c = collections.Counter((d["course_id"], d["source"], d["kind"]) for d in docs.values())
    print(f"문서 {len(docs)}개")
    for k, v in sorted(c.items()):
        print(f"  {k[0]:<4} {k[1]:<11} {k[2]:<11} {v}")
    for s in SOURCES:
        L = [len(d["text"]) for d in docs.values() if d["source"] == s]
        print(f"  {s} 길이(자): 중앙 {statistics.median(L):.0f} · 최소 {min(L)} · 최대 {max(L)}")
    for st in ("old", "fresh"):
        rs = [q for q in qs if q["set"] == st]
        if rs:
            print(f"질문 {st}: {len(rs)}개 · 검색어 {dict(collections.Counter(q['kw_from'] for q in rs))}")
    attach_fresh_gold(qs, a.key)
    bad = sorted({g for q in qs if q["gold_docs"] for g in q["gold_docs"] if g not in docs and g.replace(":t", ":s") not in docs})
    bad += [f"{lec}:{c}" for q in qs if q["gold_chunks"] for lec, cs in q["gold_chunks"].items() for c in cs
            if f"{lec}:s{c}" not in docs and f"{lec}:t{c}" not in docs]
    print(f"정답 ID 중 문서에 없는 것: {bad or '없음'}")
    if a.emb:
        v = Vec(a.emb, docs)
        miss = [f"{q['set']}:{q['id']}" for q in qs if f"{q['set']}:{q['id']}" not in v.q_index]
        print(f"임베딩 {v.meta['model']}{' (가짜)' if v.meta.get('fake') else ''}: 문서 ID 일치 · 질문 없음 {miss or '없음'}")


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--docs", default="data/qa_docs.jsonl")
    ap.add_argument("--old", default="data/qa_questions.json")
    ap.add_argument("--old-keywords", default="data/qa_keywords.json")
    ap.add_argument("--fresh", default="data/fresh/questions.md")
    ap.add_argument("--fresh-keywords", default="", help="실제 LLM 실행 run.jsonl 또는 {id: [검색어]} JSON. 없으면 단어 추출")
    ap.add_argument("--key", default="data/fresh/answer_key.md")
    ap.add_argument("--emb", default="", help="vec_embed 출력 폴더. 없으면 trgm만")
    ap.add_argument("--pool", type=int, default=20)
    ap.add_argument("--k", default="3,5")
    ap.add_argument("--check", action="store_true")
    ap.add_argument("--out", default="")
    a = ap.parse_args()
    if a.out and not str(Path(a.out).resolve()).startswith(str(Path("data").resolve())):
        raise SystemExit("--out은 data/ 아래여야 한다")
    docs = {d["id"]: d for d in (json.loads(line) for line in open(a.docs, encoding="utf-8"))}
    qs = load_sets(a)
    if a.check:
        check(a, docs, qs)
        return
    vec = Vec(a.emb, docs) if a.emb else None
    methods = ["trgm"] + (["vec", "hybrid"] if vec else [])
    ks = [int(x) for x in a.k.split(",")]
    con = await asyncpg.connect(DSN)
    if await con.fetchval("SELECT count(*) FROM qa_docs") != len(docs):
        raise SystemExit("PG qa_docs의 문서 수가 문서 파일과 다르다. pgsearch_eval --load로 다시 적재할 것")
    results = []
    for q in qs:                                   # 검색 (정답표는 아직 읽지 않음)
        lists, ms = {"trgm": {}}, {"trgm": 0.0}
        for s in SOURCES:
            lists["trgm"][s], t = await trgm(con, q, s, a.pool)
            ms["trgm"] += t
        if vec:
            lists["vec"], ms["vec"] = {}, 0.0
            for s in SOURCES:
                lists["vec"][s], t = vec.search(q, s, a.pool)
                ms["vec"] += t
            t0 = time.perf_counter()
            lists["hybrid"] = {s: rrf(lists["trgm"][s], lists["vec"][s])[:a.pool] for s in SOURCES}
            ms["hybrid"] = ms["trgm"] + ms["vec"] + (time.perf_counter() - t0) * 1000
        results.append((q, lists, ms))
    await con.close()
    attach_fresh_gold(qs, a.key)                   # 채점
    rows = []
    for q, lists, ms in results:
        r = {"set": q["set"], "id": q["id"], "negative": q["negative"], "kw_from": q["kw_from"], "pool": a.pool,
             "rank": {m: {s: first_rank(q, lists[m][s]) for s in SOURCES} for m in methods},
             "tie": {m: {str(k): any(tie_at(q, lists[m][s], k) for s in SOURCES) for k in ks} for m in methods},
             "top": {m: {s: (lists[m][s][0][1] if lists[m][s] else 0.0) for s in SOURCES} for m in methods},
             "gold_best": {m: max((sc for s in SOURCES for d, sc in lists[m][s] if is_gold(q, d)), default=None) for m in methods},
             "ms": {m: round(v, 2) for m, v in ms.items()},
             "ids": {m: {s: [d for d, _ in lists[m][s][:max(ks)]] for s in SOURCES} for m in methods}}
        if q["items"] is not None:
            r["items"] = {m: {str(k): items_covered(q, lists[m], k) for k in ks} for m in methods}
        rows.append(r)
    print(report(rows, methods, ks, vec.meta if vec else None, understand_timing(a.fresh_keywords)))
    if a.out:
        with open(a.out, "w", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
        print(f"\n질문별 순위 → {a.out} (문서 ID · 순위 · 점수만, 원문 없음)")


if __name__ == "__main__":
    asyncio.run(main())
