"""Q&A 그래프의 LLM 교체 지점.

LLM이 하는 일은 두 가지뿐이다.
- understand: 질문이 과목과 관련 있는지, 무엇으로 검색할지
- answer: 주어진 근거만으로 답하고 인용 ID를 붙이기

어디를 찾을지, 언제 멈출지, 인용이 맞는지는 LLM이 아니라 그래프(규칙)가 정한다.
"""
import json
import os
import re

from pydantic import BaseModel, Field

from notree.graphs.qa.retriever import GLOSSARY
from notree.graphs.qa.state import HitDict

_PARTICLES = ("으로", "은", "는", "이", "가", "을", "를", "의", "야", "에", "로", "와", "과")
_STOP = {"어떤", "왜", "뭐", "뭐야", "무엇", "무엇인가", "생겨", "어떻게", "오늘", "먹지", "알려줘", "설명해줘", "그리고"}


class Understanding(BaseModel):
    related: bool
    keywords: list[str] = Field(default_factory=list)


class Answer(BaseModel):
    answer: str
    citations: list[str] = Field(default_factory=list)


def _keywords(question: str) -> list[str]:
    out = []
    for tok in re.findall(r"[0-9A-Za-z가-힣]+", question):
        for p in _PARTICLES:
            if tok.endswith(p) and len(tok) > len(p):
                tok = tok[: -len(p)]
                break
        if tok and tok not in _STOP and tok not in out:
            out.append(tok)
    return out


class FakeQALLM:
    """키 없이 도는 가짜. 단어 자르기로 검색어를 만들고, 근거 첫 문장을 그대로 답으로 쓴다.

    실제 LLM처럼 '질문에 답이 되는 근거만 인용'하는 흉내로, 최고 근거 점수가 cite_min 이상일 때만 인용한다.
    bad_citation=True면 첫 답변에 없는 근거 ID를 인용한다 (verify → 다시 답하기 분기 시험용).
    bad_citation="always"면 매번 틀린다 (근거 부족으로 끝나는 분기).
    """

    name = "fake"

    def __init__(self, bad_citation: bool | str = False, cite_min: float = 0.6) -> None:
        self.bad_citation = bad_citation
        self.cite_min = cite_min
        self.calls: list[str] = []

    async def understand(self, question: str, course: str) -> Understanding:
        self.calls.append("understand")
        kws = _keywords(question)
        related = any(g.lower() in question.lower() for g in GLOSSARY)
        return Understanding(related=related, keywords=kws if related else [])

    async def answer(self, question: str, evidence: list[HitDict], attempt: int, problem: str = "",
                     related: bool = True) -> Answer:
        self.calls.append(f"answer#{attempt}")
        if not evidence or evidence[0]["score"] < self.cite_min:
            if related:
                return Answer(answer="강의 자료와 검색에서 확실한 근거를 찾지 못했습니다. 교수님께 확인해 보세요.")
            return Answer(answer=f"(일반 지식) '{question}'에 대해 강의 자료 근거 없이 답합니다.")
        top = evidence[0]
        cite = top["id"]
        if self.bad_citation == "always" or (self.bad_citation and attempt == 1):
            cite = "x9"
        return Answer(answer=f"{top['text']} [{cite}]", citations=[cite])


class OpenAIQALLM:
    """JSON으로 받고 pydantic으로 검사한다. (키가 있어야 돌아감, 테스트는 FakeQALLM으로)"""

    name = "openai"

    def __init__(self) -> None:
        from openai import AsyncOpenAI
        self.client = AsyncOpenAI()
        self.model = os.getenv("NOTREE_MODEL", "gpt-4o-mini")
        self.calls: list[str] = []

    async def _json(self, prompt: str) -> dict:
        r = await self.client.chat.completions.create(
            model=self.model, temperature=0, response_format={"type": "json_object"},
            # JSON 모드는 메시지 어딘가에 'json'이라는 단어가 없으면 400. 프롬프트와 상관없이 항상 넣는다
            messages=[{"role": "system", "content": "Respond with a single JSON object only."},
                      {"role": "user", "content": prompt}],
        )
        return json.loads(r.choices[0].message.content or "{}")

    async def understand(self, question: str, course: str) -> Understanding:
        self.calls.append("understand")
        d = await self._json(
            f"과목: {course}\n질문: {question}\n"
            "이 질문이 과목 내용과 관련 있는지 판단하고, 강의 노트 본문에 그대로 나올 법한 핵심 용어 2~4개를 골라 JSON으로 답해. "
            "과목명이나 '개념', '원리', '알고리즘', '메모리' 같은 일반적인 단어는 빼고, 질문에 나온 용어를 우선해. "
            "띄어쓰기가 있는 구는 단어로 나눠. "
            '형식: {"related": true/false, "keywords": ["..."]}')
        return Understanding.model_validate(d)

    async def answer(self, question: str, evidence: list[HitDict], attempt: int, problem: str = "",
                     related: bool = True) -> Answer:
        self.calls.append(f"answer#{attempt}")
        if evidence:
            ev = "\n".join(f"[{h['id']}] {h['text']}" for h in evidence)
            rule = ("아래 근거 중 질문에 실제로 답이 되는 것만 사용해 답하고, 쓴 근거의 ID를 문장 끝에 [ID]로 붙이고 citations에 모두 넣어. "
                    "근거에 없는 내용은 덧붙이지 마. 답이 되는 근거가 하나도 없으면 citations를 빈 배열로 두고, "
                    "내용을 추측하지 말고 '강의 자료에서 근거를 찾지 못했다'고만 답해.")
        elif related:
            ev, rule = "(없음)", ("과목 질문인데 근거를 찾지 못했다. 답의 내용을 추측해서 쓰지 말고, "
                                 "'강의 자료에서 근거를 찾지 못했다'고 짧게 답하고 citations는 빈 배열로 둬.")
        else:
            ev, rule = "(없음)", "과목과 관련 없는 질문이다. 일반 지식으로 짧게 답하고 citations는 빈 배열로 둬."
        fix = f"\n이전 답변의 문제: {problem}. 이것을 고쳐." if problem else ""
        d = await self._json(f"질문: {question}\n근거:\n{ev}\n{rule}{fix}\n"
                             '형식: {"answer": "...", "citations": ["ID", ...]}')
        return Answer.model_validate(d)


def get_qa_llm(kind: str = "fake", **kw):
    return OpenAIQALLM() if kind == "openai" else FakeQALLM(**kw)
