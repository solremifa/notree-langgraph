"""Q&A 근거 탐색 그래프 (feat/qa-graph 실험).

    understand → search_notes → search_transcript → web_search → answer → verify → save

근거를 찾는 정책(노트 → 전사 → 웹 순서, 충분하면 멈춤, 웹은 1번, 마감이 가까우면 웹 건너뜀, 근거 없는 인용 거절)을
프롬프트가 아니라 그래프 구조로 강제한다. 체크포인트는 쓰지 않는다 (90초 안에 끝나는 rt 작업. 죽으면 처음부터).
"""
from notree.graphs.qa.graph import build_qa_graph  # noqa: F401
from notree.graphs.qa.graph import QAFatal  # noqa: F401
from notree.graphs.qa.retriever import FakeRetriever, Hit, Retriever, SearchForbidden, SearchUnavailable  # noqa: F401
