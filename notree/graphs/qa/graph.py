"""Q&A 근거 탐색 그래프.

    START → understand ─(관련 없음)────────────────────────────────┐
                │                                                    ▼
                └→ search_notes ─(충분)──────────────────────────→ answer → verify ─(통과)→ save → END
                        │                                            ▲   ▲      │
                        └(부족·시간 있음)→ search_transcript ─(충분)──┘   │      └(인용 문제·재시도 남음)→ answer
                                               │                         │       (재시도 소진 → save, 근거 부족)
                                               └(부족·시간 있음)→ web_search ┘

규칙
- 검색을 멈추는 기준: 최고 점수 ≥ ENOUGH_SCORE
- LLM에게 주는 근거: 점수 ≥ EVIDENCE_MIN인 것 전부 (멈춤 기준보다 낮게. 애매한 점수의 정답 근거도 LLM이 판단하게)
- 전사 검색은 남은 시간 ≥ TRANSCRIPT_MIN_SEC, 웹 검색은 ≥ WEB_MIN_SEC일 때만 (웹은 그래프 구조상 최대 1번)
- 답변의 인용은 모두 LLM에게 준 근거 ID여야 함
- 라벨은 LLM이 실제로 인용한 근거의 출처로 정한다: 관련 없음 → 일반 지식, 강의 근거 인용 → 강의,
  웹 근거만 인용 → 웹, 인용 없음 → 근거 부족

실행에 필요한 것은 config["configurable"]로 받는다: llm, retriever, deadline_at(epoch 초, 없으면 무제한), sink(선택).
체크포인트는 쓰지 않는다.
"""
import time

from langgraph.graph import END, START, StateGraph
from langchain_core.runnables import RunnableConfig

from notree.graphs.qa.state import HitDict, QAState

ENOUGH_SCORE = 0.6     # 이 이상이면 더 찾지 않음
EVIDENCE_MIN = 0.3     # 이 이상이면 LLM에게 근거로 줌
MAX_EVIDENCE = 5
TRANSCRIPT_MIN_SEC = 10.0
WEB_MIN_SEC = 25.0
MAX_ANSWER_ATTEMPTS = 2


def _cfg(config: RunnableConfig) -> dict:
    return config.get("configurable", {})


def time_left(config: RunnableConfig) -> float:
    d = _cfg(config).get("deadline_at")
    return float("inf") if d is None else d - time.time()


def best(hits: list[HitDict], sources: tuple[str, ...] | None = None) -> HitDict | None:
    hs = [h for h in hits if sources is None or h["source"] in sources]
    return max(hs, key=lambda h: h["score"], default=None)


def enough(hits: list[HitDict]) -> bool:
    b = best(hits)
    return b is not None and b["score"] >= ENOUGH_SCORE


def evidence(hits: list[HitDict]) -> list[HitDict]:
    """답변에 넘길 근거: EVIDENCE_MIN 이상, 점수 순, 최대 MAX_EVIDENCE개."""
    return sorted((h for h in hits if h["score"] >= EVIDENCE_MIN), key=lambda h: -h["score"])[:MAX_EVIDENCE]


def label_for(related: bool, cited: list[HitDict]) -> str:
    if not related:
        return "일반 지식"
    if any(h["source"] in ("note", "transcript") for h in cited):
        return "강의"
    return "웹" if cited else "근거 부족"


# ── 노드 ─────────────────────────────────────────────────────────

async def understand(state: QAState, config: RunnableConfig) -> dict:
    u = await _cfg(config)["llm"].understand(state["question"], state.get("course", ""))
    return {"related": u.related, "keywords": u.keywords, "answer_attempts": 0}


def _search(source: str):
    async def node(state: QAState, config: RunnableConfig) -> dict:
        hits = await _cfg(config)["retriever"].search(source, state.get("keywords", []))
        return {"hits": hits, "searched": [source]}
    node.__name__ = f"search_{source}"
    return node


async def answer(state: QAState, config: RunnableConfig) -> dict:
    attempt = state.get("answer_attempts", 0) + 1
    ev = evidence(state.get("hits", [])) if state.get("related") else []
    a = await _cfg(config)["llm"].answer(state["question"], ev, attempt, state.get("verify_problem", ""),
                                         related=bool(state.get("related")))
    by_id = {h["id"]: h for h in ev}
    label = label_for(bool(state.get("related")), [by_id[c] for c in a.citations if c in by_id])
    return {"answer": a.answer, "citations": a.citations, "label": label, "answer_attempts": attempt}


async def verify(state: QAState) -> dict:
    known = {h["id"] for h in evidence(state.get("hits", []))} if state.get("related") else set()
    cites = state.get("citations", [])
    unknown = [c for c in cites if c not in known]
    if unknown:
        problem = f"근거에 없는 인용 {unknown}"
    else:
        problem = ""
    return {"verify_problem": problem}


async def save(state: QAState, config: RunnableConfig) -> dict:
    out: dict = {"status": "completed"}
    if state.get("verify_problem"):          # 재시도까지 실패 → 틀린 인용은 빼고 근거 부족으로 저장
        known = {h["id"] for h in state.get("hits", [])}
        out.update(status="근거 부족", label="근거 부족",
                   citations=[c for c in state.get("citations", []) if c in known])
    elif state.get("label") == "근거 부족":
        out["status"] = "근거 부족"
    sink = _cfg(config).get("sink")
    if sink is not None:
        await sink({**state, **out})
    return out


# ── 분기 ─────────────────────────────────────────────────────────

def after_understand(state: QAState) -> str:
    return "search_note" if state.get("related") else "answer"


def after_notes(state: QAState, config: RunnableConfig) -> str:
    if enough(state.get("hits", [])):
        return "answer"
    return "search_transcript" if time_left(config) >= TRANSCRIPT_MIN_SEC else "skip_transcript"


def after_transcript(state: QAState, config: RunnableConfig) -> str:
    if enough(state.get("hits", [])):
        return "answer"
    return "search_web" if time_left(config) >= WEB_MIN_SEC else "skip_web"


def after_verify(state: QAState) -> str:
    if state.get("verify_problem") and state.get("answer_attempts", 0) < MAX_ANSWER_ATTEMPTS:
        return "answer"
    return "save"


def _skip(what: str, why: str):
    async def node(state: QAState, config: RunnableConfig) -> dict:
        return {"skipped": [f"{what}: {why} (남은 {time_left(config):.0f}초)"]}
    node.__name__ = f"skip_{what}"
    return node


def build_qa_graph():
    g = StateGraph(QAState)
    g.add_node("understand", understand)
    g.add_node("search_note", _search("note"))
    g.add_node("search_transcript", _search("transcript"))
    g.add_node("search_web", _search("web"))
    g.add_node("skip_transcript", _skip("transcript", "시간 부족"))
    g.add_node("skip_web", _skip("web", "시간 부족"))
    g.add_node("answer", answer)
    g.add_node("verify", verify)
    g.add_node("save", save)

    g.add_edge(START, "understand")
    g.add_conditional_edges("understand", after_understand, ["search_note", "answer"])
    g.add_conditional_edges("search_note", after_notes, ["answer", "search_transcript", "skip_transcript"])
    g.add_conditional_edges("search_transcript", after_transcript, ["answer", "search_web", "skip_web"])
    g.add_edge("skip_transcript", "answer")
    g.add_edge("skip_web", "answer")
    g.add_edge("search_web", "answer")
    g.add_edge("answer", "verify")
    g.add_conditional_edges("verify", after_verify, ["answer", "save"])
    g.add_edge("save", END)
    return g.compile()
