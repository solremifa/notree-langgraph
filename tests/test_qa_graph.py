"""Q&A 근거 탐색 그래프 (feat/qa-graph). 가짜 LLM·가짜 검색으로 분기마다 확인."""
import asyncio
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from notree.graphs.qa import FakeRetriever, build_qa_graph
from notree.graphs.qa.llm import FakeQALLM
from notree.graphs.qa import QAFatal, SearchForbidden, SearchUnavailable  # noqa: E402
from notree.graphs.qa import graph as G  # noqa: E402
from notree.graphs.qa.graph import INSUFFICIENT_TEXT, TIMEOUT_TEXT, render  # noqa: E402
from notree.graphs.qa.llm import NOT_FOUND_TEXT  # noqa: E402
from notree.graphs.qa.retriever import sample_corpus


def run(question, *, deadline_in=None, bad_citation=False, corpus=None, cite_min=0.6):
    llm, ret, saved = FakeQALLM(bad_citation=bad_citation, cite_min=cite_min), FakeRetriever(corpus or sample_corpus()), []

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
    """남은 10.5초: 질문 이해는 LLM 대신 단어 추출, 노트만 찾고 전사는 건너뜀 (답변 시간을 남김)."""
    out, llm, ret, _ = run("스택 오버플로는 왜 생겨?", deadline_in=10.5)
    assert ret.calls == ["note"]
    assert "understand" not in llm.calls
    assert out["label"] == "근거 부족"


def test_no_search_when_only_answer_time_is_left():
    out, llm, ret, _ = run("스택 오버플로는 왜 생겨?", deadline_in=5)
    assert ret.calls == []                                   # 검색하면 답변할 시간이 없음
    assert any("검색 중단" in s for s in out["skipped"])
    assert out["status"] == "근거 부족" and llm.calls == ["answer#1"]


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
    assert out["answer"] == INSUFFICIENT_TEXT and "x9" not in out["answer"]   # 틀린 본문도 버림
    assert saved[0]["status"] == "근거 부족" and saved[0]["answer"] == INSUFFICIENT_TEXT


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
        body = '{"related": true, "keywords": ["스택"]}' if len(sent) == 1 else ('{"sentences": [{"text": "스택은 LIFO다 [n1]", "citations": ["n1"]}], '
                                                                                   '"needs_more_evidence": false}')
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=body))])

    llm = OpenAIQALLM.__new__(OpenAIQALLM)
    llm.client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=create)))
    llm.model, llm.calls = "test", []
    cfg = {"configurable": {"llm": llm, "retriever": FakeRetriever(sample_corpus())}}
    out = asyncio.run(build_qa_graph().ainvoke({"question": "스택은 어떤 구조야?"}, cfg))
    assert out["citations"] == ["n1"]
    assert out["answer"] == "스택은 LIFO다 [^n1]"         # LLM이 본문에 넣은 [n1]은 지우고 코드가 [^n1]을 붙임
    assert len(sent) == 2 and all("json" in _json.dumps(m, ensure_ascii=False).lower() for m in sent)


def test_borderline_evidence_reaches_llm_and_labels_by_citation():
    """실제 LLM 실행에서 나온 경우: 정답 근거 t1이 0.5점(멈춤 기준 0.6 미만)이라 LLM에게 전달되지 않아 근거 부족이 됐었음.
    이제 0.3 이상이면 LLM에게 주고, LLM이 인용하면 그 출처로 라벨을 붙인다."""
    out, _, ret, _ = run("스택 오버플로 원인 메모리", cite_min=0.3)   # 검색어 4개 → t1 0.5, n1 0.25
    assert ret.calls == ["note", "transcript", "web"]                 # 0.6 미만이라 웹까지는 찾음
    assert out["citations"] == ["t1"] and out["label"] == "강의" and out["status"] == "completed"


def test_label_follows_cited_source_not_top_hit():
    from notree.graphs.qa.graph import label_for
    note = {"id": "n1", "source": "note"}; web = {"id": "w1", "source": "web"}
    assert label_for(True, [web]) == "웹"
    assert label_for(True, [web, note]) == "강의"
    assert label_for(True, []) == "근거 부족"
    assert label_for(False, [note]) == "일반 지식"


# ── 보완 (외부 검토 반영) ───────────────────────────────────────────

class ScriptedRetriever:
    """출처별로 정해진 결과를 돌려주거나, 정해진 순서대로 예외를 던진다."""

    def __init__(self, results: dict, errors: dict | None = None, delay: dict | None = None):
        self.results, self.errors, self.delay = results, {k: list(v) for k, v in (errors or {}).items()}, delay or {}
        self.calls: list[str] = []

    async def search(self, source, keywords, limit=3):
        self.calls.append(source)
        await asyncio.sleep(self.delay.get(source, 0))
        if self.errors.get(source):
            raise self.errors[source].pop(0)
        return list(self.results.get(source, []))


def run_with(question, retriever, llm=None, deadline_in=None):
    llm = llm or FakeQALLM()
    cfg = {"llm": llm, "retriever": retriever,
           "deadline_at": None if deadline_in is None else time.time() + deadline_in}
    out = asyncio.run(build_qa_graph().ainvoke({"question": question, "course": "자료구조"}, {"configurable": cfg}))
    return out, llm


LIFO = {"id": "n1", "source": "note", "text": "스택은 LIFO 자료구조다.", "score": 0.95, "ref": "n1"}
CAUSE = {"id": "t1", "source": "transcript", "text": "스택 오버플로는 재귀 호출이 너무 깊을 때 생긴다.", "score": 0.9, "ref": "t1"}


def test_high_score_but_unanswerable_goes_to_next_source():
    """외부 검토의 재현: 노트 점수 0.95지만 답이 없음 → 예전엔 노트에서 멈추고 근거 부족. 이제 전사까지 찾아 답한다."""
    ret = ScriptedRetriever({"note": [LIFO], "transcript": [CAUSE]})
    out, llm = run_with("스택 오버플로의 원인은?", ret, FakeQALLM(unhelpful={"n1"}))
    assert ret.calls == ["note", "transcript"]
    assert out["citations"] == ["t1"] and out["label"] == "강의" and out["status"] == "completed"
    assert llm.calls == ["understand", "answer#1", "answer#1"]   # 출처가 늘면 답변 시도 수는 새로 셈


def test_llm_call_cap_stops_the_chain():
    """매번 '답할 수 없음' → 노트·전사·웹을 차례로 찾되 LLM 호출은 MAX_LLM_CALLS번에서 멈춘다."""
    ret = ScriptedRetriever({"note": [LIFO], "transcript": [dict(CAUSE, id="t9")],
                             "web": [dict(CAUSE, id="w9", source="web")]})
    out, llm = run_with("스택 오버플로의 원인은?", ret, FakeQALLM(unhelpful={"n1", "t9", "w9"}))
    assert ret.calls == ["note", "transcript", "web"]
    assert len(llm.calls) <= G.MAX_LLM_CALLS
    assert out["status"] == "근거 부족"


def test_transient_search_error_is_retried_once():
    ret = ScriptedRetriever({"note": [LIFO]}, errors={"note": [SearchUnavailable("연결 끊김")]})
    out, _ = run_with("스택은 어떤 구조야?", ret)
    assert ret.calls == ["note", "note"]
    assert out["citations"] == ["n1"] and any("다시 시도" in s for s in out["skipped"])


def test_repeated_search_error_moves_to_next_source():
    """노트 검색이 두 번 다 실패 → 그래프가 멈추지 않고 전사로 넘어가 답한다."""
    ret = ScriptedRetriever({"transcript": [CAUSE]},
                            errors={"note": [SearchUnavailable("과부하"), SearchUnavailable("과부하")]})
    out, _ = run_with("스택 오버플로는 왜 생겨?", ret)
    assert ret.calls == ["note", "note", "transcript"]
    assert out["citations"] == ["t1"] and any("note: 일시 장애" in s and "건너뜀" in s for s in out["skipped"])


def test_forbidden_search_stops_the_graph():
    ret = ScriptedRetriever({}, errors={"note": [SearchForbidden("삭제된 과목")]})
    with pytest.raises(QAFatal):
        run_with("스택은 어떤 구조야?", ret)


def test_slow_search_times_out_and_moves_on(monkeypatch):
    monkeypatch.setattr(G, "SEARCH_TIMEOUT_SEC", 0.2)
    ret = ScriptedRetriever({"note": [LIFO], "transcript": [CAUSE]}, delay={"note": 1.0})
    out, _ = run_with("스택 오버플로는 왜 생겨?", ret)
    assert ret.calls == ["note", "note", "transcript"]
    assert out["citations"] == ["t1"] and any("note: 시간 초과" in s for s in out["skipped"])


def test_slow_llm_answer_saves_timeout_message(monkeypatch):
    monkeypatch.setattr(G, "LLM_TIMEOUT_SEC", 0.2)
    ret = ScriptedRetriever({"note": [LIFO]})
    out, _ = run_with("스택은 어떤 구조야?", ret, FakeQALLM(delay=0.5))
    assert out["status"] == "시간 초과" and out["answer"] == TIMEOUT_TEXT and out["citations"] == []
    assert any("understand: LLM 시간 초과" in s for s in out["skipped"])   # 이해 단계도 단어 추출로 대체됨


def test_expired_deadline_does_not_call_llm():
    ret = ScriptedRetriever({"note": [LIFO]})
    out, llm = run_with("스택은 어떤 구조야?", ret, deadline_in=-1)
    assert llm.calls == [] and ret.calls == []
    assert out["status"] == "시간 초과" and out["answer"] == TIMEOUT_TEXT


def test_render_builds_markers_from_citations():
    text, cites = render([{"text": "스택은 LIFO다 [x9].", "citations": ["n1", "n1"]},
                          {"text": "재귀가 깊으면 넘친다", "citations": ["t1", "n1"]},
                          {"text": "정리하면 위와 같다", "citations": []}])
    assert text == "스택은 LIFO다 [x9]. [^n1] 재귀가 깊으면 넘친다 [^t1] [^n1] 정리하면 위와 같다"   # 모르는 [x9]는 내용으로 둠
    assert cites == ["n1", "t1"]


# ── 2차 검토 (2026-10-15) ─────────────────────────────────────────

def test_render_keeps_code_and_math_brackets():
    text, cites = render([{"text": "배열 a = [1, 2, 3], 첫 원소 a[0], 구간 [0, 1].", "citations": ["n1"]}],
                         known_ids={"n1"})
    assert text == "배열 a = [1, 2, 3], 첫 원소 a[0], 구간 [0, 1]. [^n1]"
    assert cites == ["n1"]


def test_render_strips_only_citation_markers():
    text, _ = render([{"text": "스택은 LIFO다 [n1] [^t1], 예: s[0]", "citations": ["n1"]}], known_ids={"n1", "t1"})
    assert text == "스택은 LIFO다, 예: s[0] [^n1]"


class PartialLLM(FakeQALLM):
    """일부만 답하고(정상 인용) 매번 '근거가 더 필요'라고 하는 LLM."""

    async def answer(self, question, evidence, attempt, problem="", related=True):
        from notree.graphs.qa.llm import Answer, Sentence
        self.calls.append(f"answer#{attempt}")
        return Answer(sentences=[Sentence(text="스택은 LIFO 구조다.", citations=[evidence[0]["id"]])],
                      needs_more_evidence=True)


def test_partial_answer_is_not_saved_as_completed():
    ret = ScriptedRetriever({"note": [LIFO], "transcript": [CAUSE], "web": [dict(CAUSE, id="w1", source="web")]})
    out, _ = run_with("스택 오버플로의 원인은?", ret, PartialLLM())
    assert out["citations"] and out["status"] == "부분 답변"


class UncitedLLM(FakeQALLM):
    """근거를 받고도 인용 없이 지어낸 답을 내는 LLM."""

    async def answer(self, question, evidence, attempt, problem="", related=True):
        from notree.graphs.qa.llm import Answer, Sentence
        self.calls.append(f"answer#{attempt}")
        return Answer(sentences=[Sentence(text="스택 오버플로는 우주선 때문이다.")])


def test_uncited_course_answer_is_replaced_by_notice():
    ret = ScriptedRetriever({"note": [LIFO]})
    out, _ = run_with("스택은 어떤 구조야?", ret, UncitedLLM())
    assert out["status"] == "근거 부족" and "우주선" not in out["answer"]
    assert out["answer"] == NOT_FOUND_TEXT


def test_web_search_is_requested_at_most_once_even_on_error():
    ret = ScriptedRetriever({"web": [dict(CAUSE, id="w1", source="web")]},
                            errors={"web": [SearchUnavailable("timeout")]})
    out, _ = run_with("힙 정렬의 시간 복잡도는?", ret)
    assert ret.calls.count("web") == 1
    assert any("web: 일시 장애" in s and "건너뜀" in s for s in out["skipped"])


# ── 결과 계약 (3차 검토) ───────────────────────────────────────────

class MarkedLLM(FakeQALLM):
    """문장 text에 인용 표시를 섞어 내는 LLM."""

    async def answer(self, question, evidence, attempt, problem="", related=True):
        from notree.graphs.qa.llm import Answer, Sentence
        self.calls.append(f"answer#{attempt}")
        return Answer(sentences=[Sentence(text="스택은 LIFO다 [n1] [^x9]. 예: s[0]", citations=["n1"])])


def test_sentences_text_is_cleaned_like_answer():
    out, _ = run_with("스택은 어떤 구조야?", ScriptedRetriever({"note": [LIFO]}), MarkedLLM())
    assert out["sentences"] == [{"text": "스택은 LIFO다. 예: s[0]", "citations": ["n1"]}]
    assert out["answer"] == "스택은 LIFO다. 예: s[0] [^n1]"


def test_sources_hold_only_cited_hits_and_notice_code_is_set():
    ret = ScriptedRetriever({"note": [LIFO, dict(CAUSE, id="n2", source="note", score=0.4)]})
    out, _ = run_with("스택은 어떤 구조야?", ret)
    assert out["notice_code"] is None
    assert out["sources"] == [{"id": "n1", "source": "note", "text": LIFO["text"], "ref": "n1"}]
    out2, _ = run_with("스택은 어떤 구조야?", ScriptedRetriever({"note": [LIFO]}), UncitedLLM())
    assert out2["notice_code"] == "not_found" and out2["sources"] == [] and out2["sentences"] == []
    out3, _ = run_with("스택은 어떤 구조야?", ScriptedRetriever({"note": [LIFO]}), deadline_in=-1)
    assert out3["notice_code"] == "timeout"
