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
    searched: Annotated[list[str], operator.add]       # 검색 단계를 지난 곳 (note / transcript / web). 시간 때문에 못 찾았어도 들어감
    queried: Annotated[list[str], operator.add]        # 실제로 검색 요청을 한 곳 (시간이 없어 요청조차 못 했으면 빠짐)
    skipped: Annotated[list[str], operator.add]        # 건너뛴 단계와 이유
    answer: str                                        # 화면용 본문. 문장 + [ID] 표시를 코드가 만든다
    sentences: list[dict]                              # LLM이 낸 문장별 {text, citations}
    citations: list[str]                               # 답변이 인용한 근거 ID (sentences에서 모은 것)
    needs_more: bool                                   # LLM: "받은 근거로는 답할 수 없음"
    timed_out: bool                                    # 답변을 시간 안에 만들지 못함
    llm_calls: int                                     # 이번 실행의 LLM 호출 수 (상한 MAX_LLM_CALLS)
    label: Label                                       # 화면에 붙일 출처 라벨
    answer_attempts: int
    verify_problem: str                                # 근거 확인 실패 사유 (없으면 "")
    status: str                                        # 결과 상태: completed / 부분 답변 / 근거 부족 / 시간 초과 (작업 상태 done/failed와 별개)
    notice_code: str | None                            # 안내문을 보일 때: not_found / insufficient / timeout (아니면 None)
    sources: list[dict]                                # 인용된 근거만 {id, source, text, ref} (화면·저장용. hits는 내부용)
