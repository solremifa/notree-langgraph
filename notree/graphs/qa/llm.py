"""Q&A 그래프의 LLM 교체 지점.

LLM이 하는 일은 두 가지뿐이다.
- understand: 질문이 과목과 관련 있는지, 무엇으로 검색할지
- answer: 주어진 근거만으로 문장별로 답하고, 문장마다 쓴 근거 ID를 고르기.
          받은 근거로 답할 수 없으면 needs_more_evidence=true

어디를 찾을지, 언제 멈출지, 인용이 맞는지, 화면에 [ID]를 어떻게 붙일지는 LLM이 아니라 그래프(코드)가 정한다.
"""
import asyncio
import json
import os
import re

from pydantic import BaseModel, Field

from notree.graphs.qa.retriever import GLOSSARY
from notree.graphs.qa.state import HitDict

_PARTICLES = ("으로", "은", "는", "이", "가", "을", "를", "의", "야", "에", "로", "와", "과")
_STOP = {"어떤", "왜", "뭐", "뭐야", "무엇", "무엇인가", "생겨", "어떻게", "오늘", "먹지", "알려줘", "설명해줘", "그리고"}

NOT_FOUND_TEXT = "강의 자료와 검색에서 확실한 근거를 찾지 못했습니다. 교수님께 확인해 보세요."


class Understanding(BaseModel):
    related: bool
    keywords: list[str] = Field(default_factory=list)


class Sentence(BaseModel):
    text: str
    citations: list[str] = Field(default_factory=list)


class Answer(BaseModel):
    sentences: list[Sentence] = Field(default_factory=list)
    needs_more_evidence: bool = False


def keywords_of(question: str) -> list[str]:
    """LLM 없이 질문에서 검색어를 뽑는다 (가짜 LLM, 그리고 understand가 시간 안에 못 끝났을 때의 대체)."""
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
    """키 없이 도는 가짜. 단어 자르기로 검색어를 만들고, 답이 되는 근거의 문장을 그대로 답으로 쓴다.

    - cite_min: 근거 점수가 이 이상이어야 '답이 된다'고 본다 (실제 LLM의 판단 흉내)
    - unhelpful: 점수와 상관없이 답이 안 되는 근거 ID (점수는 높은데 질문에 답이 없는 경우 시험)
    - 답이 되는 근거가 없으면 needs_more_evidence=True
    - bad_citation=True면 첫 답변에 없는 근거 ID를 인용, "always"면 매번 (verify 분기 시험)
    - delay: 호출마다 기다리는 초 (시간 예산 시험)
    """

    name = "fake"

    def __init__(self, bad_citation: bool | str = False, cite_min: float = 0.6,
                 unhelpful: set[str] | None = None, delay: float = 0.0) -> None:
        self.bad_citation, self.cite_min = bad_citation, cite_min
        self.unhelpful, self.delay = unhelpful or set(), delay
        self.calls: list[str] = []

    async def understand(self, question: str, course: str) -> Understanding:
        self.calls.append("understand")
        await asyncio.sleep(self.delay)
        related = any(g.lower() in question.lower() for g in GLOSSARY)
        return Understanding(related=related, keywords=keywords_of(question) if related else [])

    async def answer(self, question: str, evidence: list[HitDict], attempt: int, problem: str = "",
                     related: bool = True) -> Answer:
        self.calls.append(f"answer#{attempt}")
        await asyncio.sleep(self.delay)
        if not related:
            return Answer(sentences=[Sentence(text=f"(일반 지식) '{question}'에 대해 강의 자료 근거 없이 답합니다.")])
        helpful = [h for h in evidence if h["score"] >= self.cite_min and h["id"] not in self.unhelpful]
        if not helpful:
            return Answer(sentences=[Sentence(text=NOT_FOUND_TEXT)], needs_more_evidence=True)
        top = helpful[0]
        cite = "x9" if self.bad_citation == "always" or (self.bad_citation and attempt == 1) else top["id"]
        return Answer(sentences=[Sentence(text=top["text"], citations=[cite])])


class OpenAIQALLM:
    """JSON으로 받고 pydantic으로 검사한다. (키가 있어야 돌아감, 테스트는 가짜 클라이언트로)"""

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
            rule = ("아래 근거 중 질문에 실제로 답이 되는 것만 사용해 문장별로 답해. 문장마다 그 문장에 쓴 근거 ID를 citations에 넣고, "
                    "문장 text에는 [ID]나 [^ID] 같은 인용 표시를 쓰지 마 (코드가 붙인다). 근거에 없는 내용은 덧붙이지 마. "
                    "답이 되는 근거가 하나도 없으면 내용을 추측하지 말고 '강의 자료에서 근거를 찾지 못했다'는 문장 하나만 쓰고 "
                    "needs_more_evidence를 true로 해.")
        elif related:
            ev, rule = "(없음)", ("과목 질문인데 근거를 찾지 못했다. 답의 내용을 추측하지 말고 '강의 자료에서 근거를 찾지 못했다'는 "
                                 "문장 하나만 쓰고 needs_more_evidence를 true로 해.")
        else:
            ev, rule = "(없음)", "과목과 관련 없는 질문이다. 일반 지식으로 짧게 답하고 citations는 비워 둬."
        fix = f"\n이전 답변의 문제: {problem}. 이것을 고쳐." if problem else ""
        d = await self._json(f"질문: {question}\n근거:\n{ev}\n{rule}{fix}\n"
                             '형식: {"sentences": [{"text": "...", "citations": ["ID"]}], "needs_more_evidence": false}')
        return Answer.model_validate(d)


def get_qa_llm(kind: str = "fake", **kw):
    return OpenAIQALLM() if kind == "openai" else FakeQALLM(**kw)
