"""비교 결과를 질문·항목별로 나눠 본다 (DB·API 없이 결과 파일만 읽음). 공개 가능한 표만 만든다.

    uv run python -m notree.dev.vec_trace --compare data/vec/compare.jsonl \
        --run data/fresh/out/run-openai.jsonl --md data/vec/trace.md

입력
  --compare  vec_compare --out 결과 (질문별 순위 · 상위 문서 ID)
  --run      qa_fresh_eval 실제 LLM 실행 결과 (그래프가 LLM에 실제로 전달한 근거). 없으면 3장 생략
  --key      새 30 정답표 (채점에만)
출력 (마크다운, 질문 ID · 순위 · 개수만. 원문 · 문서 ID · 벡터 없음)
  1. 질문별 첫 정답 순위 (방식 × 노트 / 전사, 후보 20개 안. '-' = 20위 밖)
  2. 방식 간 이기고 진 질문 (R@k 둘 중 하나 기준) — 결합이 놓친 질문은 결합 목록에서 정답이 몇 위였는지
  3. 새 30 항목별 추적: 검색(--search-method 상위 k, 노트 ∪ 전사)과 그래프 전달(--limit, 모든 시도의 근거 합)을 같은 항목에서 비교
     검색 X · 전달 X  → 검색 개선 대상
     검색 O · 전달 X  → 선별 · 중단 조건 개선 대상
     검색 X · 전달 O  → 그래프(우선 강의 · 확장 · 추가 검색)로 얻음
     검색 O · 전달 O
     전달 근거 정답 판정은 qa_fresh_eval과 같다: 문서 ID 일치(요약 노트 s{n} = 전사 t{n}). 동등한 다른 구간은 세지 않으므로 하한일 수 있다
"""
import argparse
import collections
import json
from pathlib import Path

from notree.dev.qa_fresh_eval import chunk, parse_key

SOURCES = ("note", "transcript")
SHORT = {"note": "노트", "transcript": "전사"}


def load_jsonl(path: str) -> list[dict]:
    return [json.loads(line) for line in open(path, encoding="utf-8")]


def found(r: dict, m: str, k: int) -> bool:
    return any((r["rank"][m][s] or 99) <= k for s in SOURCES)


def rank_s(x) -> str:
    return str(x) if x else "-"


def section_ranks(rows: list[dict], methods: list[str], ks: list[int]) -> list[str]:
    out = ["## 1. 질문별 첫 정답 순위", "",
           "후보는 출처별 상위 20개. 숫자 = 정답 문서가 처음 나온 순위, '-' = 20위 안에 없음. 범위 밖 질문은 정답이 없어 생략.", ""]
    for st, title in (("old", "기존 33"), ("fresh", "새 30")):
        rs = [r for r in rows if r["set"] == st and not r["negative"]]
        if not rs:
            continue
        head = "| 질문 | " + " | ".join(f"{m} {SHORT[s]}" for m in methods for s in SOURCES)
        sep = "|---|" + "---|" * (len(methods) * 2)
        if st == "fresh":
            head += " | " + " | ".join(f"{m} 항목@{k}" for m in methods for k in ks)
            sep += "---|" * (len(methods) * len(ks))
        out += [f"### {title} ({len(rs)}개)", "", head + " |", sep]
        for r in rs:
            line = f"| {r['id']} | " + " | ".join(rank_s(r["rank"][m][s]) for m in methods for s in SOURCES)
            if st == "fresh":
                line += " | " + " | ".join(f"{r['items'][m][str(k)][0]}/{r['items'][m][str(k)][1]}" for m in methods for k in ks)
            out.append(line + " |")
        out.append("")
    return out


def section_winloss(rows: list[dict], methods: list[str], ks: list[int]) -> list[str]:
    out = ["## 2. 방식 간 이기고 진 질문", "",
           "R@k 둘 중 하나(노트 상위 k ∪ 전사 상위 k) 기준. 괄호 = 진 쪽 목록에서 정답의 최고 순위(20위 밖이면 '-').", ""]
    pairs = [(a, b) for i, a in enumerate(methods) for b in methods[i + 1:]]
    for st, title in (("old", "기존 33"), ("fresh", "새 30")):
        rs = [r for r in rows if r["set"] == st and not r["negative"]]
        if not rs:
            continue
        out += [f"### {title}", "", "| k | 비교 | 둘 다 | 앞만 찾음 | 뒤만 찾음 | 둘 다 못 찾음 |", "|---|---|---|---|---|---|"]
        for k in ks:
            for a, b in pairs:
                both = [r for r in rs if found(r, a, k) and found(r, b, k)]
                only_a = [r for r in rs if found(r, a, k) and not found(r, b, k)]
                only_b = [r for r in rs if found(r, b, k) and not found(r, a, k)]
                none = len(rs) - len(both) - len(only_a) - len(only_b)
                best = lambda r, m: rank_s(min((r["rank"][m][s] or 99) for s in SOURCES) if any(r["rank"][m][s] for s in SOURCES) else None)  # noqa: E731
                fa = ", ".join(f"{r['id']}({best(r, b)})" for r in only_a) or "-"
                fb = ", ".join(f"{r['id']}({best(r, a)})" for r in only_b) or "-"
                out.append(f"| @{k} | {a} vs {b} | {len(both)} | {len(only_a)}: {fa} | {len(only_b)}: {fb} | {none} |")
        out.append("")
    return out


def section_trace(rows: list[dict], run: list[dict], key: dict, search_m: str, k: int, limit: int, vec_m: str | None) -> list[str]:
    real = [r for r in run if r.get("llm") != "fake" and r.get("limit") == limit]
    if not real:
        return [f"## 3. 항목별 추적", "", f"실제 LLM 실행 결과(limit {limit})가 없어 생략.", ""]
    runs = {r["id"]: r for r in real}
    cmp_ = {r["id"]: r for r in rows if r["set"] == "fresh"}
    cnt, per_q, vec_cover = collections.Counter(), [], collections.Counter()
    for qid, kq in sorted(key.items()):
        if kq["status"] == "근거 부족" or qid not in runs or qid not in cmp_:
            continue
        rr, cr = runs[qid], cmp_[qid]
        delivered = {chunk(d) for a in rr["attempts"] for d in a["evidence"]}
        searched = {chunk(d) for s in SOURCES for d in cr["ids"][search_m][s][:k]}
        vec = {chunk(d) for s in SOURCES for d in cr["ids"][vec_m][s][:k]} if vec_m else set()
        c = collections.Counter()
        for it in kq["items"]:
            g = {chunk(x) for x in it["gold"]}
            if not g:
                continue
            cat = ("검색 O" if g & searched else "검색 X") + " · " + ("전달 O" if g & delivered else "전달 X")
            c[cat] += 1
            if vec_m and cat == "검색 X · 전달 X" and g & vec:
                vec_cover[cat] += 1
        cnt.update(c)
        stop = "노트만" if rr["searches"] == ["note"] else ("과목 전체까지" if "transcript" in rr["searches"] else "우선 강의")
        per_q.append((qid, c, stop))
    total = sum(cnt.values())
    cats = ["검색 X · 전달 X", "검색 O · 전달 X", "검색 X · 전달 O", "검색 O · 전달 O"]
    meaning = {"검색 X · 전달 X": "검색 개선 대상", "검색 O · 전달 X": "선별 · 중단 조건 개선 대상",
               "검색 X · 전달 O": "그래프(우선 강의 · 확장 · 추가 검색)로 얻음", "검색 O · 전달 O": ""}
    out = ["## 3. 새 30 항목별 추적 (검색 vs 그래프 전달)", "",
           f"검색 = {search_m} 상위 {k}개 (노트 ∪ 전사, 과목 전체, 같은 LLM 검색어) · 전달 = 그래프가 근거 {limit}개 설정에서 모든 시도에 넘긴 근거의 합",
           f"분모 = 답할 수 있는 질문의 정답 항목 {total}개. 전달 판정은 문서 ID 일치(하한일 수 있음).", "",
           "| 구분 | 항목 수 | 의미 |", "|---|---|---|"]
    for c in cats:
        extra = f" — 그중 {vec_m} 상위 {k}개에는 있음 {vec_cover[c]}개" if vec_m and c == cats[0] else ""
        out.append(f"| {c} | {cnt[c]} ({cnt[c] / total:.0%}) | {meaning[c]}{extra} |")
    stops = collections.Counter(s for _, _, s in per_q)
    lost_by_stop = collections.Counter(s for _, c, s in per_q for _ in range(c["검색 O · 전달 X"]))
    out += ["", f"그래프 검색 범위: {dict(stops)} (질문 수) · '검색 O · 전달 X' 항목의 질문 범위: {dict(lost_by_stop)}", "",
            "| 질문 | 그래프 검색 범위 | " + " | ".join(cats) + " |", "|---|---|" + "---|" * len(cats)]
    for qid, c, stop in per_q:
        out.append(f"| {qid} | {stop} | " + " | ".join(str(c[x]) for x in cats) + " |")
    out.append("")
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--compare", default="data/vec/compare.jsonl")
    ap.add_argument("--run", default="")
    ap.add_argument("--key", default="data/fresh/answer_key.md")
    ap.add_argument("--k", default="3,5")
    ap.add_argument("--search-method", default="trgm", help="3장에서 '검색'으로 볼 방식 (그래프가 쓰는 것은 trgm)")
    ap.add_argument("--limit", type=int, default=3, help="3장에서 볼 그래프 근거 개수 설정")
    ap.add_argument("--md", default="")
    a = ap.parse_args()
    rows = load_jsonl(a.compare)
    methods = list(rows[0]["rank"])
    ks = [int(x) for x in a.k.split(",")]
    out = [f"# 검색 방식 비교 — 질문별 결과", "", f"방식: {', '.join(methods)} · 후보 출처별 {rows[0]['pool']}개", ""]
    out += section_ranks(rows, methods, ks)
    out += section_winloss(rows, methods, ks)
    if a.run and Path(a.run).exists():
        vec_m = "vec" if "vec" in methods and a.search_method != "vec" else None
        out += section_trace(rows, load_jsonl(a.run), parse_key(a.key), a.search_method, ks[0], a.limit, vec_m)
    text = "\n".join(out)
    if a.md:
        Path(a.md).write_text(text, encoding="utf-8")
        print(f"→ {a.md}")
    else:
        print(text)


if __name__ == "__main__":
    main()
