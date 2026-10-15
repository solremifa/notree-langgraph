"""근거 검색 인터페이스.

그래프는 Retriever만 안다. worker에서는 MCP 도구(notes_search, 전사 검색, web_search)를 부르는 구현을 꽂고,
테스트·데모에서는 FakeRetriever(메모리 속 작은 말뭉치 + 키워드 일치 점수)를 꽂는다.
"""
from dataclasses import dataclass, field
from typing import Protocol

from notree.graphs.qa.state import HitDict, Source


class Hit(dict):
    """HitDict 생성 도우미."""

    def __init__(self, id: str, source: Source, text: str, score: float, ref: str = "") -> None:
        super().__init__(id=id, source=source, text=text, score=round(score, 3), ref=ref or id)


class Retriever(Protocol):
    async def search(self, source: Source, keywords: list[str], limit: int = 3) -> list[HitDict]: ...


@dataclass
class FakeRetriever:
    """출처별 문서 목록에서 '검색어가 몇 개 들어 있나'로 점수를 매긴다."""

    docs: dict[str, list[tuple[str, str]]]                      # source → [(id, text)]
    calls: list[str] = field(default_factory=list)

    async def search(self, source: Source, keywords: list[str], limit: int = 3) -> list[HitDict]:
        self.calls.append(source)
        if not keywords:
            return []
        hits = []
        for doc_id, text in self.docs.get(source, []):
            found = sum(1 for k in keywords if k.lower() in text.lower())
            if found:
                hits.append(Hit(doc_id, source, text, found / len(keywords)))
        return sorted(hits, key=lambda h: -h["score"])[:limit]


def sample_corpus() -> dict[str, list[tuple[str, str]]]:
    """데모·테스트용 '자료구조' 과목."""
    return {
        "note": [
            ("n1", "스택은 LIFO 구조로, 마지막에 넣은 원소를 먼저 꺼낸다."),
            ("n2", "큐는 FIFO 구조로, 먼저 넣은 원소를 먼저 꺼낸다."),
            ("n3", "힙은 완전 이진 트리이며 부모가 자식보다 작거나(최소 힙) 크다(최대 힙)."),
        ],
        "transcript": [
            ("t1", "교수님: 스택 오버플로는 재귀 호출이 너무 깊어져 스택 영역을 다 쓰면 생깁니다."),
            ("t2", "교수님: 큐는 프린터 대기열처럼 순서대로 처리할 때 씁니다."),
        ],
        "web": [
            ("w1", "힙 정렬의 시간 복잡도는 최선·평균·최악 모두 O(n log n)이다. (출처: 위키백과 힙 정렬)"),
        ],
    }


GLOSSARY = ["스택", "큐", "힙", "트리", "재귀", "정렬", "LIFO", "FIFO", "오버플로"]   # 가짜 '관련성 판단'용
