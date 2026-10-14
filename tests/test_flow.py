import json
import sys
from pathlib import Path

import pytest
from langgraph.checkpoint.memory import InMemorySaver

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import llm  # noqa: E402
from graph import build_graph  # noqa: E402


def cfg(tmp_path, thread, **kw):
    return {"configurable": {"thread_id": thread, "out_dir": str(tmp_path), "llm": "fake", **kw}}


@pytest.fixture(autouse=True)
def reset_calls():
    llm.CALLS.clear()


def run(app, inp, config):
    return app.invoke(inp, config, durability="sync")


def test_happy_path(tmp_path):
    app = build_graph(InMemorySaver())
    out = run(app, {"sources": ["hello", "world"]}, cfg(tmp_path, "t1"))
    assert out["draft"] == "HELLO WORLD!" and out["status"] == "완료"


def test_retry_branch(tmp_path):
    app = build_graph(InMemorySaver())
    out = run(app, {"sources": ["hello", "world"]}, cfg(tmp_path, "t2", sloppy="once"))
    assert out["attempt"] == 2 and out["status"] == "완료"


def test_needs_review_branch(tmp_path):
    app = build_graph(InMemorySaver())
    out = run(app, {"sources": ["hello", "world"]}, cfg(tmp_path, "t3", sloppy="always"))
    assert out["status"] == "확인 필요" and out["issues"] == ["WORLD"]


def test_resume_does_not_redo_finished_extracts(tmp_path):
    app = build_graph(InMemorySaver())
    with pytest.raises(RuntimeError):
        run(app, {"sources": ["hello", "big", "world"]}, cfg(tmp_path, "t4", crash_at="extract:world"))
    assert sorted(llm.CALLS) == ["extract:big", "extract:hello"]
    llm.CALLS.clear()
    out = run(app, None, cfg(tmp_path, "t4"))              # 이어하기
    assert llm.CALLS == ["extract:world", "merge#1"]        # 끝난 extract는 다시 안 부름
    assert out["draft"] == "HELLO BIG WORLD!"


def test_publish_crash_does_not_double_publish(tmp_path):
    app = build_graph(InMemorySaver())
    with pytest.raises(RuntimeError):
        run(app, {"sources": ["hello", "world"]}, cfg(tmp_path, "t5", crash_at="publish"))
    run(app, None, cfg(tmp_path, "t5"))
    published = json.loads((tmp_path / "published.json").read_text("utf-8"))
    assert list(published) == ["t5"]
