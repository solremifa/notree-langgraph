"""Q&A 품질 측정 도구(notree.dev.qa_eval)의 판정 로직."""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from notree.dev.qa_eval import CASES, judge, run_case, summarize  # noqa: E402
from notree.graphs.qa import build_qa_graph  # noqa: E402

CASE = {c["id"]: c for c in CASES}


def test_fake_llm_passes_note_case_and_records_fields():
    r = asyncio.run(run_case(build_qa_graph(), CASE["note"], "fake"))
    assert r["ok"] and r["citations"] == ["n1"] and r["error"] is None
    assert {"seconds", "llm_calls", "prompt_tokens", "completion_tokens", "checks"} <= r.keys()
    assert "전체 1/1 통과" in summarize([r], 0.15, 0.6)


def test_judge_flags_inline_markers_and_citations_outside_evidence():
    final = {"label": "강의", "status": "completed", "citations": ["x9"], "hits": [{"id": "n1"}],
             "sentences": [{"text": "스택은 LIFO [n1]", "citations": ["x9"]}]}
    j = judge(CASE["note"], final)
    assert not j["ok"]
    assert j["checks"]["no_inline_markers"] is False and j["checks"]["cites_in_evidence"] is False
    assert j["checks"]["expected_cited"] is False
