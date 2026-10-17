"""질문 목록의 검색어를 실제 LLM(그래프의 understand와 같은 프롬프트)으로 뽑아 저장한다.

    uv run python -m notree.dev.qa_keywords --questions data/qa_questions.json --out data/qa_keywords.json
    uv run python -m notree.dev.pgsearch_eval --keywords data/qa_keywords.json     # LLM 검색어로 검색 평가

PG가 없는 PC에서도 돌릴 수 있다 (OpenAI 키만 필요). 결과 JSON은 {질문 id: [검색어]}이고, 그래프와 똑같이 띄어쓰기를 나눈다.
"""
import argparse
import asyncio
import json

from notree.graphs.qa.graph import split_keywords
from notree.graphs.qa.llm import get_qa_llm

COURSE_NAME = {"ml": "머신러닝", "oop": "객체지향 프로그래밍"}


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--questions", default="data/qa_questions.json")
    ap.add_argument("--out", default="data/qa_keywords.json")
    ap.add_argument("--llm", default="openai")
    a = ap.parse_args()
    llm = get_qa_llm(a.llm)
    out = {}
    for q in json.load(open(a.questions, encoding="utf-8"))["questions"]:
        u = await llm.understand(q["q"], COURSE_NAME.get(q["course"], q["course"]))
        out[q["id"]] = split_keywords(u.keywords)
        print(f"{q['id']:<6} 관련={u.related!s:<5} {out[q['id']]}  ← {q['q']}")
    json.dump(out, open(a.out, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(f"→ {a.out}")


if __name__ == "__main__":
    asyncio.run(main())
