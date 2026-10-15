"""Q&A 그래프 상태."""
import operator
from typing import Annotated, Literal, TypedDict

Source = Literal["note", "transcript", "web"]
Label = Literal["강의", "웹", "일반 지식", "근거 부족"]


class HitDict(TypedDict):
    id: str            # 근거 ID. 답변의 인용 [id]와 맞아야 함
    source: Source
    text: str
    score: float       # 0~1. 검색 점수 (충분한지 판단에 사용)
    ref: str           # 출처 표시용 (노트 블록, 전사 구간, URL)


class QAState(TypedDict, total=False):
    question: str
    course: str
    related: bool                                      # 질문이 이 과목 노트와 관련 있는가
    keywords: list[str]                                # understand가 만든 검색어
    hits: Annotated[list[HitDict], operator.add]       # 지금까지 찾은 근거 (단계마다 덧붙임)
    searched: Annotated[list[str], operator.add]       # 검색한 곳 (note / transcript / web)
    skipped: Annotated[list[str], operator.add]        # 건너뛴 단계와 이유
    answer: str
    citations: list[str]                               # 답변이 인용한 근거 ID
    label: Label                                       # 화면에 붙일 출처 라벨
    answer_attempts: int
    verify_problem: str                                # 근거 확인 실패 사유 (없으면 "")
    status: str                                        # completed / 근거 부족
