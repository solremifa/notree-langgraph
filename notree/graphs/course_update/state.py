"""그래프가 단계마다 주고받는 상태. 실제 설계의 CourseUpdateState를 hello world 크기로 줄인 것."""
import operator
from typing import Annotated, TypedDict


class CourseUpdateState(TypedDict, total=False):
    course: str                                   # 과목 이름
    sources: list[str]                            # snapshot에서 고정한 출처 강의(여기선 단어 하나씩)
    extracts: Annotated[list[dict], operator.add]  # 병렬 extract 결과를 '덧붙이기'로 모음 (reducer)
    draft: str                                    # merge 결과
    attempt: int                                  # merge 시도 횟수
    issues: list[str]                             # validate가 찾은 문제
    status: str                                   # 완료 / 확인 필요


class ExtractInput(TypedDict):
    lecture: str                                  # Send로 extract 하나에 넘기는 값
