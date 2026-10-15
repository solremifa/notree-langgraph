"""Q&A 근거 탐색 그래프 (feat/qa-graph). 가짜 LLM·가짜 검색으로 분기마다 확인."""
import asyncio
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from notree.graphs.qa import FakeRetriever, build_qa_graph
from notree.graphs.qa.llm import FakeQALLM
from notree.graphs.qa.retriever import sample_corpus


def run(question, *, deadline_in=None, bad_citation=False, corpus=None):
    llm, ret, saved = FakeQALLM(bad_citation=bad_citation), FakeRetriever(corpus or sample_corpus()), []

    async def sink(result):
        saved.append(result)

    cfg = {"llm": llm, "retriever": ret, "sink": sink,
           "deadline_at": None if deadline_in is None else time.time() + deadline_in}
    out = asyncio.run(build_qa_graph().ainvoke({"question": question, "course": "자료구조"}, {"configurable": cfg}))
    return out, llm, ret, saved


def test_answer_from_notes_stops_early():
    out, llm, ret, saved = run("스택은 어떤 구조야?")
    assert ret.calls == ["note"]                      # 노트로 충분 → 전사·웹은 검색하지 않음
    assert out["label"] == "강의" and out["citations"] == ["n1"] and out["status"] == "completed"
    assert llm.calls == ["understand", "answer#1"]
    assert len(saved) == 1 and saved[0]["citations"] == ["n1"]


def test_falls_back_to_transcript():
    out, _, ret, _ = run("스택 오버플로는 왜 생겨?")
    assert ret.calls == ["note", "transcript"]
    assert out["label"] == "강의" and out["citations"] == ["t1"]


def test_falls_back_to_web_once():
    out, _, ret, _ = run("힙 정렬의 시간 복잡도는?")
    assert ret.calls == ["note", "transcript", "web"]
    assert out["label"] == "웹" and out["citations"] == ["w1"]


def test_unrelated_question_skips_search():
    out, llm, ret, _ = run("오늘 점심 뭐 먹지?")
    assert ret.calls == []
    assert out["label"] == "일반 지식" and out["citations"] == [] and out["status"] == "completed"


def test_web_skipped_when_deadline_is_near():
    out, _, ret, _ = run("힙 정렬의 시간 복잡도는?", deadline_in=15)   # 전사(10초)는 되고 웹(25초)은 안 됨
    assert ret.calls == ["note", "transcript"]
    assert out["label"] == "근거 부족" and out["status"] == "근거 부족" and out["citations"] == []
    assert any(s.startswith("web: 시간 부족") for s in out["skipped"])


def test_transcript_skipped_when_almost_out_of_time():
    out, _, ret, _ = run("스택 오버플로는 왜 생겨?", deadline_in=5)
    assert ret.calls == ["note"]
    assert out["label"] == "근거 부족"


def test_nothing_found_anywhere():
    out, _, ret, _ = run("트리 순회 순서는?")   # 관련 단어(트리)는 있지만 어디에도 충분한 근거 없음
    assert ret.calls == ["note", "transcript", "web"]   # 웹까지 한 번씩만
    assert out["label"] == "근거 부족" and out["status"] == "근거 부족"


def test_bad_citation_is_retried_once():
    out, llm, _, _ = run("스택은 어떤 구조야?", bad_citation=True)
    assert llm.calls == ["understand", "answer#1", "answer#2"]
    assert out["citations"] == ["n1"] and out["status"] == "completed" and out["verify_problem"] == ""


def test_bad_citation_twice_ends_as_insufficient():
    out, llm, _, saved = run("스택은 어떤 구조야?", bad_citation="always")
    assert llm.calls == ["understand", "answer#1", "answer#2"]   # 재시도는 1번까지
    assert out["status"] == "근거 부족" and out["label"] == "근거 부족"
    assert out["citations"] == []                                 # 근거에 없는 인용은 저장하지 않음
    assert saved[0]["status"] == "근거 부족"


@pytest.mark.parametrize("q", ["스택은 어떤 구조야?", "힙 정렬의 시간 복잡도는?", "오늘 점심 뭐 먹지?"])
def test_citations_always_point_to_found_evidence(q):
    out, *_ = run(q)
    found = {h["id"] for h in out.get("hits", [])}
    assert set(out["citations"]) <= found


def test_progress_stream_shows_each_step():
    async def go():
        cfg = {"configurable": {"llm": FakeQALLM(), "retriever": FakeRetriever(sample_corpus())}}
        return [list(u)[0] async for u in build_qa_graph().astream(
            {"question": "스택 오버플로는 왜 생겨?"}, cfg, stream_mode="updates")]
    assert asyncio.run(go()) == ["understand", "search_note", "search_transcript", "answer", "verify", "save"]


def test_openai_requests_always_mention_json():
    """JSON 모드는 메시지에 'json'이 없으면 400 (실제 실행에서 answer 단계가 이걸로 실패했음)."""
    import json as _json
    from types import SimpleNamespace

    from notree.graphs.qa.llm import OpenAIQALLM

    sent = []

    async def create(**kw):
        sent.append(kw["messages"])
        body = '{"related": true, "keywords": ["스택"]}' if len(sent) == 1 else '{"answer": "LIFO [n1]", "citations": ["n1"]}'
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=body))])

    llm = OpenAIQALLM.__new__(OpenAIQALLM)
    llm.client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=create)))
    llm.model, llm.calls = "test", []
    cfg = {"configurable": {"llm": llm, "retriever": FakeRetriever(sample_corpus())}}
    out = asyncio.run(build_qa_graph().ainvoke({"question": "스택은 어떤 구조야?"}, cfg))
    assert out["citations"] == ["n1"]
    assert len(sent) == 2 and all("json" in _json.dumps(m, ensure_ascii=False).lower() for m in sent)
