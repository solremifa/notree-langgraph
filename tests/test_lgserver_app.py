"""실험: Agent Server용 진입점(notree/lgserver/qa_app.py)이 서버 없이도 그래프를 제대로 만드는지.

서버는 run마다 make_qa_graph(config)를 부르고, config의 JSON 값으로 llm · retriever 객체를 붙인다.
"""
import asyncio

from notree.lgserver.qa_app import make_qa_graph


def _run(question: str, configurable: dict) -> dict:
    g = make_qa_graph({"configurable": configurable})
    return asyncio.run(g.ainvoke({"question": question, "course": "자료구조"}))


def test_기본_가짜_llm으로_노트_근거_답변():
    out = _run("스택은 어떤 구조야?", {})
    assert out["status"] == "completed" and out["citations"] == ["n1"]


def test_bad_citation이면_다시_답한다():
    out = _run("스택은 어떤 구조야?", {"bad_citation": True})
    assert out["answer_attempts"] == 2 and out["citations"] == ["n1"]


def test_deadline_sec이_짧으면_웹을_건너뛴다():
    out = _run("힙 정렬의 시간 복잡도는?", {"deadline_sec": 15})
    assert any(s.startswith("web:") for s in out["skipped"])


def test_run_config가_객체를_덮어쓰지_않는다():
    # 서버는 run 요청의 configurable을 그래프 기본 config 위에 합친다. 다른 키 이름이라 객체가 남아야 한다.
    g = make_qa_graph({"configurable": {"llm_kind": "fake"}})
    assert hasattr(g.config["configurable"]["llm"], "understand")
