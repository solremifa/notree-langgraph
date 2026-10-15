"""Q&A 근거 탐색 그래프.

    START → understand ─(관련 없음)──────────────────────────────────────────┐
                └→ search_note ─(충분)────────────────────────────────────→ answer → verify ─(통과)→ save → END
                       ├(부족)→ search_transcript ─(충분)─────────────────→   ▲         │
                       │              ├(부족)→ search_web ───────────────→   │         ├(인용 문제·재시도 남음)→ answer
                       │              └(시간 없음)→ skip_web ──────────────→   │         └(답할 수 없음·남은 출처·시간)→ 다음 search_*
                       └(시간 없음)→ skip_transcript ──────────────────────→   ┘

규칙
- 검색을 멈추는 기준: 최고 점수 ≥ ENOUGH_SCORE
- LLM에게 주는 근거: 점수 ≥ EVIDENCE_MIN인 것 (멈춤 기준보다 낮게. 애매한 점수의 정답 근거도 LLM이 판단하게)
- 점수가 높아도 LLM이 "답할 수 없음"(needs_more_evidence)이라고 하면, 남은 출처와 시간이 있을 때 다음 출처를 찾는다
- 웹은 그래프 구조상 최대 1번. LLM 호출은 실행 전체에서 MAX_LLM_CALLS번까지
- 답변 본문의 [ID] 표시는 LLM이 아니라 코드가 문장별 citations로 만든다 → 본문과 인용 목록이 어긋날 수 없다
- 인용 ID가 받은 근거에 없으면 1번 다시 답하고, 그래도 틀리면 본문을 버리고 정해진 "근거 부족" 안내문으로 저장
- 라벨은 실제로 인용한 근거의 출처로: 관련 없음 → 일반 지식, 강의 근거 → 강의, 웹만 → 웹, 인용 없음 → 근거 부족
- 검색 장애: SearchUnavailable·타임아웃 → 시간이 되면 1번 다시, 안 되면 다음 출처 / SearchForbidden → QAFatal로 멈춤
- 시간 예산 (deadline_at 기준)
  · 검색은 답변에 쓸 시간(ANSWER_RESERVE_SEC)을 남기는 범위에서만, 한 번에 최대 SEARCH_TIMEOUT_SEC
  · LLM 호출은 최대 LLM_TIMEOUT_SEC, 저장에 쓸 시간(SAVE_RESERVE_SEC)은 남김
  · 답변할 시간이 MIN_ANSWER_SEC보다 적거나 LLM이 제한 시간을 넘기면 "시간 초과" 안내문으로 저장

실행에 필요한 것은 config["configurable"]로 받는다: llm, retriever, deadline_at(epoch 초, 없으면 무제한), sink(선택).
체크포인트는 쓰지 않는다.
"""
import asyncio
import re
import time

from langchain_core.runnables import RunnableConfig
from langgraph.graph import END, START, StateGraph

from notree.graphs.qa.llm import Understanding, keywords_of
from notree.graphs.qa.retriever import SearchForbidden, SearchUnavailable
from notree.graphs.qa.state import HitDict, QAState

ENOUGH_SCORE = 0.6       # 이 이상이면 더 찾지 않음
EVIDENCE_MIN = 0.3       # 이 이상이면 LLM에게 근거로 줌
MAX_EVIDENCE = 5
TRANSCRIPT_MIN_SEC = 10.0
WEB_MIN_SEC = 25.0
MAX_ANSWER_ATTEMPTS = 2  # 인용 문제로 다시 답하기는 출처 한 묶음당 1번
MAX_LLM_CALLS = 4        # understand 1 + answer 최대 3
SEARCH_TIMEOUT_SEC = 8.0
LLM_TIMEOUT_SEC = 25.0
ANSWER_RESERVE_SEC = 8.0
SAVE_RESERVE_SEC = 2.0
MIN_ANSWER_SEC = 3.0

INSUFFICIENT_TEXT = "근거를 확인할 수 있는 답을 만들지 못했습니다. 질문을 조금 더 구체적으로 해 주세요."
TIMEOUT_TEXT = "시간 안에 답을 만들지 못했습니다. 잠시 뒤 다시 물어봐 주세요."
SOURCES = ("note", "transcript", "web")
MIN_SEC = {"note": 0.0, "transcript": TRANSCRIPT_MIN_SEC, "web": WEB_MIN_SEC}


class QAFatal(Exception):
    """다시 해도 안 되는 실패 (검색 권한 없음·자료 삭제). worker 핸들러는 FatalError로 바꾼다."""


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


_MARK = re.compile(r"\s*\[[^\[\]]{1,40}\]")


def render(sentences: list[dict]) -> tuple[str, list[str]]:
    """문장별 {text, citations} → (화면용 본문, 인용 목록). LLM이 text에 넣은 [..] 표시는 지우고 코드가 다시 붙인다."""
    parts, cites = [], []
    for s in sentences:
        text = _MARK.sub("", s["text"]).strip()
        ids = [c for c in dict.fromkeys(s.get("citations") or [])]
        parts.append(text + "".join(f" [{c}]" for c in ids))
        cites += [c for c in ids if c not in cites]
    return " ".join(p for p in parts if p), cites


def _llm_timeout(config: RunnableConfig) -> float:
    return min(LLM_TIMEOUT_SEC, time_left(config) - SAVE_RESERVE_SEC)


def _can_call_llm(state: QAState, config: RunnableConfig) -> bool:
    return state.get("llm_calls", 0) < MAX_LLM_CALLS and time_left(config) >= MIN_ANSWER_SEC


def next_source(state: QAState, config: RunnableConfig) -> str | None:
    """아직 찾지 않았고, 남은 시간으로 찾을 수 있는 다음 출처."""
    done = set(state.get("searched", []))
    for s in SOURCES:
        if s not in done and time_left(config) >= max(MIN_SEC[s], ANSWER_RESERVE_SEC + MIN_ANSWER_SEC):
            return s
    return None


# ── 노드 ─────────────────────────────────────────────────────────

async def understand(state: QAState, config: RunnableConfig) -> dict:
    q = state["question"]
    calls = state.get("llm_calls", 0)
    fallback = Understanding(related=True, keywords=keywords_of(q))   # 판단을 못 하면 '관련 있음'으로 보고 찾는다
    if time_left(config) < MIN_ANSWER_SEC + ANSWER_RESERVE_SEC:
        return {"related": fallback.related, "keywords": fallback.keywords, "answer_attempts": 0, "llm_calls": calls,
                "skipped": ["understand: 시간 부족 → 단어 추출로 대신"]}
    try:
        u = await asyncio.wait_for(_cfg(config)["llm"].understand(q, state.get("course", "")),
                                   timeout=_llm_timeout(config))
        note = []
    except TimeoutError:
        u, note = fallback, ["understand: LLM 시간 초과 → 단어 추출로 대신"]
    return {"related": u.related, "keywords": u.keywords, "answer_attempts": 0, "llm_calls": calls + 1,
            "skipped": note}


def _search(source: str):
    async def node(state: QAState, config: RunnableConfig) -> dict:
        retriever, notes = _cfg(config)["retriever"], []
        reset = {"answer_attempts": 0, "verify_problem": "", "needs_more": False}
        for i in range(2):
            limit = min(SEARCH_TIMEOUT_SEC, time_left(config) - ANSWER_RESERVE_SEC)
            if limit <= 0:
                notes.append(f"{source}: 답변할 시간을 남기려고 검색 중단")
                break
            try:
                hits = await asyncio.wait_for(retriever.search(source, state.get("keywords", [])), timeout=limit)
                return {"hits": hits, "searched": [source], "skipped": notes, **reset}
            except (SearchUnavailable, TimeoutError) as e:
                why = "시간 초과" if isinstance(e, TimeoutError) else f"일시 장애: {e}"
                notes.append(f"{source}: {why} → {'다시 시도' if i == 0 else '건너뜀'}")
            except SearchForbidden as e:
                raise QAFatal(f"{source} 검색 불가: {e}") from e
        return {"hits": [], "searched": [source], "skipped": notes, **reset}
    node.__name__ = f"search_{source}"
    return node


async def answer(state: QAState, config: RunnableConfig) -> dict:
    attempt = state.get("answer_attempts", 0) + 1
    calls = state.get("llm_calls", 0)
    related = bool(state.get("related"))
    ev = evidence(state.get("hits", [])) if related else []
    timed_out = {"answer": TIMEOUT_TEXT, "sentences": [], "citations": [], "label": "근거 부족",
                 "answer_attempts": attempt, "needs_more": False, "timed_out": True}
    if not _can_call_llm(state, config):
        return {**timed_out, "llm_calls": calls}
    try:
        a = await asyncio.wait_for(
            _cfg(config)["llm"].answer(state["question"], ev, attempt, state.get("verify_problem", ""),
                                       related=related),
            timeout=_llm_timeout(config))
    except TimeoutError:
        return {**timed_out, "llm_calls": calls + 1}
    sentences = [s.model_dump() for s in a.sentences]
    text, cites = render(sentences)
    by_id = {h["id"]: h for h in ev}
    return {"answer": text, "sentences": sentences, "citations": cites,
            "label": label_for(related, [by_id[c] for c in cites if c in by_id]),
            "answer_attempts": attempt, "llm_calls": calls + 1,
            "needs_more": related and a.needs_more_evidence, "timed_out": False}


async def verify(state: QAState) -> dict:
    """인용 ID 유효성 검사: 인용이 모두 이번에 LLM에게 준 근거인가. (내용이 근거와 맞는지까지는 보지 않는다)"""
    if state.get("timed_out"):
        return {"verify_problem": ""}
    known = {h["id"] for h in evidence(state.get("hits", []))} if state.get("related") else set()
    unknown = [c for c in state.get("citations", []) if c not in known]
    return {"verify_problem": f"근거에 없는 인용 {unknown}" if unknown else ""}


async def save(state: QAState, config: RunnableConfig) -> dict:
    out: dict = {"status": "completed"}
    if state.get("timed_out"):
        out.update(status="시간 초과", label="근거 부족", answer=TIMEOUT_TEXT, citations=[], sentences=[])
    elif state.get("verify_problem"):          # 재시도까지 실패 → 틀린 본문은 버리고 정해진 안내문으로
        out.update(status="근거 부족", label="근거 부족", answer=INSUFFICIENT_TEXT, citations=[], sentences=[])
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
    return "search_transcript" if next_source(state, config) == "transcript" else "skip_transcript"


def after_transcript(state: QAState, config: RunnableConfig) -> str:
    if enough(state.get("hits", [])):
        return "answer"
    return "search_web" if next_source(state, config) == "web" else "skip_web"


def after_verify(state: QAState, config: RunnableConfig) -> str:
    if state.get("timed_out"):
        return "save"
    if state.get("verify_problem"):
        if state.get("answer_attempts", 0) < MAX_ANSWER_ATTEMPTS and _can_call_llm(state, config):
            return "answer"
        return "save"
    if state.get("needs_more") and state.get("llm_calls", 0) < MAX_LLM_CALLS:
        nxt = next_source(state, config)
        if nxt:
            return f"search_{nxt}"
    return "save"


def _skip(what: str, why: str):
    async def node(state: QAState, config: RunnableConfig) -> dict:
        return {"skipped": [f"{what}: {why} (남은 {time_left(config):.0f}초)"]}
    node.__name__ = f"skip_{what}"
    return node


def build_qa_graph():
    g = StateGraph(QAState)
    g.add_node("understand", understand)
    for s in SOURCES:
        g.add_node(f"search_{s}", _search(s))
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
    g.add_conditional_edges("verify", after_verify, ["answer", "save", "search_transcript", "search_web"])
    g.add_edge("save", END)
    return g.compile()
