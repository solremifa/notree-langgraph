"""과목 노트 업데이트와 같은 모양의 LangGraph.

    snapshot ─┬─ extract(hello) ─┐
              └─ extract(world) ─┴─ merge ─ validate ─┬─ publish ─ END
                                    ▲                  ├─ (문제 + 1회) → merge 재시도
                                    └──────────────────┘
                                                       └─ (문제 + 2회) → needs_review ─ publish
"""
import json
import time
from pathlib import Path

from langchain_core.runnables import RunnableConfig
from langgraph.graph import END, START, StateGraph
from langgraph.types import Send

from notree.common.log import log
from notree.graphs.course_update.llm import get_llm
from notree.graphs.course_update.state import CourseUpdateState, ExtractInput

MAX_MERGE = 2


class SimulatedCrash(RuntimeError):
    """--crash-at / die_at 으로 일부러 일으키는 중단. worker는 이걸 '프로세스가 죽었다'로 취급한다."""


def opt(config: RunnableConfig, key: str, default=None):
    return config.get("configurable", {}).get(key, default)


def maybe_crash(config: RunnableConfig, where: str) -> None:
    if opt(config, "crash_at") == where:
        log("💥", f"{where} 에서 강제 중단 (worker가 죽었다고 가정)")
        raise SimulatedCrash(f"강제 중단: {where}")


# ── 노드 ─────────────────────────────────────────────────────────
def snapshot(state: CourseUpdateState, config: RunnableConfig):
    sources = state.get("sources") or ["hello", "world"]
    log("snapshot", f"출처 강의 고정: {sources}  (실제: 출처 버전·기준 노트 버전 고정 + 편집 잠금)")
    return {"sources": sources, "attempt": 0}


def fan_out(state: CourseUpdateState):
    """조건부 edge에서 Send 목록을 돌려주면 extract가 출처 수만큼 병렬로 뜬다."""
    return [Send("extract", {"lecture": s}) for s in state["sources"]]


def extract(inp: ExtractInput, config: RunnableConfig):
    lecture = inp["lecture"]
    if opt(config, "crash_at") == f"extract:{lecture}":
        time.sleep(0.3)                      # 다른 extract가 먼저 끝나 저장되도록 잠깐 기다렸다가 죽음
        maybe_crash(config, f"extract:{lecture}")
    word = get_llm(opt(config, "llm", "fake")).extract(lecture)
    log("extract", f"{lecture!r} → {word!r}")
    return {"extracts": [{"lecture": lecture, "word": word}]}


def merge(state: CourseUpdateState, config: RunnableConfig):
    attempt = state.get("attempt", 0) + 1
    order = {s: i for i, s in enumerate(state["sources"])}
    words = [e["word"] for e in sorted(state["extracts"], key=lambda e: order[e["lecture"]])]
    draft = get_llm(opt(config, "llm", "fake")).merge(words, attempt, opt(config, "sloppy"))
    log("merge", f"{attempt}차 병합 → {draft!r}")
    return {"draft": draft, "attempt": attempt}


def validate(state: CourseUpdateState):
    draft = state["draft"].upper()
    issues = [e["word"] for e in state["extracts"] if e["word"].upper() not in draft]
    log("validate", "통과" if not issues else f"누락 {issues}")
    return {"issues": issues}


def route_after_validate(state: CourseUpdateState):
    if not state["issues"]:
        return "publish"
    if state["attempt"] < MAX_MERGE:
        log("route", f"문제 있음 → merge 재시도 ({state['attempt']}/{MAX_MERGE})")
        return "merge"
    log("route", "재시도 한도 도달 → 확인 필요")
    return "needs_review"


def needs_review(state: CourseUpdateState):
    log("needs_review", f"'확인 필요' 표시: {state['issues']}")
    return {"status": "확인 필요"}


class FileNoteStore:
    """--saver sqlite 일 때 쓰는 발행 저장소 (out/published.json)."""

    def __init__(self, out_dir: str) -> None:
        self.db = Path(out_dir) / "published.json"
        self.db.parent.mkdir(parents=True, exist_ok=True)

    async def publish(self, key: str, course: str, note: str, status: str) -> bool:
        published = json.loads(self.db.read_text("utf-8")) if self.db.exists() else {}
        if key in published:
            return False
        published[key] = {"course": course, "note": note, "status": status}
        self.db.write_text(json.dumps(published, ensure_ascii=False, indent=2), "utf-8")
        return True


async def publish(state: CourseUpdateState, config: RunnableConfig):
    """멱등키(= thread_id)로 한 번만 발행. --saver mcp면 MCP note_publish, 아니면 파일."""
    store = opt(config, "notes") or FileNoteStore(opt(config, "out_dir", "out"))
    key = opt(config, "thread_id")
    status = state.get("status") or "완료"
    if await store.publish(key, state.get("course", ""), state["draft"], status):
        log("publish", f"새 버전 저장: {state['draft']!r} [{status}]")
    else:
        log("publish", f"이미 발행됨(멱등키 {key}) → 중복 발행 막음")
    maybe_crash(config, "publish")         # 커밋 '직후' 죽는 경우
    return {"status": status}


def build_graph(checkpointer=None):
    g = StateGraph(CourseUpdateState)
    g.add_node("snapshot", snapshot)
    g.add_node("extract", extract)
    g.add_node("merge", merge)
    g.add_node("validate", validate)
    g.add_node("needs_review", needs_review)
    g.add_node("publish", publish)

    g.add_edge(START, "snapshot")
    g.add_conditional_edges("snapshot", fan_out, ["extract"])
    g.add_edge("extract", "merge")                       # 모든 extract가 끝나야 merge 실행
    g.add_edge("merge", "validate")
    g.add_conditional_edges("validate", route_after_validate, ["publish", "merge", "needs_review"])
    g.add_edge("needs_review", "publish")
    g.add_edge("publish", END)
    return g.compile(checkpointer=checkpointer)
