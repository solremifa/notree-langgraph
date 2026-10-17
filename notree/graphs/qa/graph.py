"""Q&A 근거 탐색 그래프.

    START → understand ─(관련 없음)──────────────────────────────────────────────────┐
                └→ search_note ─(충분)────────────────────────────────────────────→ answer → verify ─(통과)→ save → END
                       ├(부족)→ search_transcript ─(충분)─────────────────────────→   ▲         │
                       │        (노트로 고른 강의 2개)  ├(우선 결과 < EXPAND_MIN)→ search_transcript_all ┐ ├(인용 문제·재시도 남음)→ answer
                       │                              ├(부족)→ search_web ──────→   │  (과목 전체)    │ └(답할 수 없음·남은 출처·시간)→ 다음 search_*
                       │                              └(시간 없음)→ skip_web ────→   │ ←──(충분/부족·웹)┘
                       └(시간 없음)→ skip_transcript ──────────────────────────────→   ┘

규칙
- 검색을 멈추는 기준: 최고 점수 ≥ ENOUGH_SCORE
- 전사는 두 단계 (2026-10-17, 실제 강의 7개·질문 33개로 정한 잠정값. docs/measurements/2026-10-17-pg-search):
  · search_transcript: 노트 결과의 강의별 최고 점수로 PRIORITY_LECTURES(2)개 강의를 골라 그 안에서만. 노트 결과에 강의가 없으면 과목 전체
  · 우선 결과의 최고 점수 < EXPAND_MIN(0.3)이면 바로 search_transcript_all(과목 전체)
  · EXPAND_MIN 이상(충분 기준 미만)이면 웹으로 가지 않고 먼저 답한다. LLM이 "근거가 더 필요"하다고 하면
    남은 시간·LLM 호출 예산 안에서 다음 출처로 search_transcript_all → 웹
  · 노트 결과에 강의 정보가 없어 과목 전체로 찾은 경우는 예전과 같음 (부족하면 웹)
  · 확장 기준(EXPAND_MIN)과 충분 기준(ENOUGH_SCORE)은 다른 판단. 최종 답변 가능 여부는 점수가 아니라 LLM이 판단 (정확성 보장은 아님)
- LLM에게 주는 근거: 점수 ≥ EVIDENCE_MIN인 것 (멈춤 기준보다 낮게. 애매한 점수의 정답 근거도 LLM이 판단하게)
- 점수가 높아도 LLM이 "답할 수 없음"(needs_more_evidence)이라고 하면, 남은 출처와 시간이 있을 때 다음 출처를 찾는다
- 웹은 단계도 요청도 최대 1번 (장애가 나도 다시 요청하지 않음). LLM 호출은 실행 전체에서 MAX_LLM_CALLS번까지
- 답변 본문의 인용 표시 [^ID]는 LLM이 아니라 코드가 문장별 citations로 만든다 → 본문과 인용 목록이 어긋날 수 없다.
  LLM이 본문에 넣은 표시는 [^..] 형식이거나 이번 근거 ID인 [ID]만 지운다 (a[0], [1, 2] 같은 코드·수식은 그대로)
- 인용 ID가 받은 근거에 없으면 1번 다시 답하고, 그래도 틀리면 본문을 버리고 정해진 "근거 부족" 안내문으로 저장
- 라벨(어디서 왔나)은 실제로 인용한 근거의 출처로: 관련 없음 → 일반 지식, 강의 근거 → 강의, 웹만 → 웹, 인용 없음 → 근거 부족
- 상태(얼마나 답했나)는 라벨과 따로: completed / 부분 답변(인용은 있으나 LLM이 근거가 더 필요하다고 함) /
  근거 부족(과목 질문인데 인용 없음 → 본문은 정해진 안내문) / 시간 초과(답변 시간 초과, 또는 시간이 없어 검색을 하나도 못 함)
- LLM이 낸 검색어에 띄어쓰기가 있으면 단어로 나눈다 (split_keywords)
- 검색 장애: SearchUnavailable·타임아웃 → 노트·전사는 시간이 되면 1번 다시, 웹은 다시 안 함. 안 되면 다음 출처 /
  SearchForbidden → QAFatal로 멈춤
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

from notree.graphs.qa.llm import NOT_FOUND_TEXT, Understanding, keywords_of
from notree.graphs.qa.retriever import SearchForbidden, SearchUnavailable
from notree.graphs.qa.state import HitDict, QAState

ENOUGH_SCORE = 0.6       # 이 이상이면 더 찾지 않음
EXPAND_MIN = 0.3         # 우선 강의 전사의 최고 점수가 이 미만이면 과목 전체 전사로 바로 넓힘 (잠정)
PRIORITY_LECTURES = 2    # 노트 결과로 고르는 전사 우선 강의 수 (잠정)
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
SOURCES = ("note", "transcript", "transcript_all", "web")
MIN_SEC = {"note": 0.0, "transcript": TRANSCRIPT_MIN_SEC, "transcript_all": TRANSCRIPT_MIN_SEC, "web": WEB_MIN_SEC}
SEARCH_RETRIES = {"note": 1, "transcript": 1, "transcript_all": 1, "web": 0}   # 장애 때 다시 요청하는 횟수. 웹은 요청 비용 때문에 0


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
    """답변에 넘길 근거: EVIDENCE_MIN 이상, 점수 순, 최대 MAX_EVIDENCE개. 같은 ID는 한 번만 (단계마다 다시 찾은 경우).
    검색 단계의 순서(우선 강의 먼저)는 여기서 유지되지 않는다 — 점수순으로 다시 고른다 (측정으로 확인할 정책)."""
    by_id: dict[str, HitDict] = {}
    for h in hits:
        if h["score"] >= EVIDENCE_MIN and (h["id"] not in by_id or h["score"] > by_id[h["id"]]["score"]):
            by_id[h["id"]] = h
    return sorted(by_id.values(), key=lambda h: -h["score"])[:MAX_EVIDENCE]


def priority_lectures(hits: list[HitDict]) -> list[str]:
    """노트 결과에서 강의별 최고 점수가 높은 순으로 PRIORITY_LECTURES개."""
    best_by: dict[str, float] = {}
    for h in hits:
        lec = h.get("lecture_id")
        if h["source"] == "note" and lec:
            best_by[lec] = max(best_by.get(lec, 0.0), h["score"])
    return sorted(best_by, key=lambda k: -best_by[k])[:PRIORITY_LECTURES]


def label_for(related: bool, cited: list[HitDict]) -> str:
    if not related:
        return "일반 지식"
    if any(h["source"] in ("note", "transcript") for h in cited):
        return "강의"
    return "웹" if cited else "근거 부족"


_FOOTNOTE = re.compile(r"\s*\[\^[^\[\]\s]{1,40}\]")


def clean_sentences(sentences: list[dict], known_ids: set[str] | frozenset = frozenset()) -> list[dict]:
    """LLM이 문장 text에 넣은 인용 표시를 지운 문장 목록. 화면은 이 text를 그대로 그리고, 출처는 citations로만 만든다.
    지우는 것: [^..] 형식, 이번 근거·인용 ID와 정확히 같은 [ID]. 그 밖의 대괄호(a[0], [1, 2, 3], 구간 [0, 1])는 내용이라 둔다."""
    out = []
    for s in sentences:
        ids = list(dict.fromkeys(s.get("citations") or []))
        text = _FOOTNOTE.sub("", s["text"])
        for i in set(known_ids) | set(ids):
            text = re.sub(r"\s*\[" + re.escape(i) + r"\]", "", text)
        if text.strip():
            out.append({"text": text.strip(), "citations": ids})
    return out


def render(sentences: list[dict], known_ids: set[str] | frozenset = frozenset()) -> tuple[str, list[str]]:
    """문장별 {text, citations} → (파생 본문 answer, 인용 목록). 문장을 정제한 뒤 끝에 [^ID]를 붙여 잇는다.
    answer는 미리보기·알림·로그용이고, 화면의 주 본문은 정제된 sentences로 그린다."""
    cleaned = clean_sentences(sentences, known_ids)
    cites: list[str] = []
    for s in cleaned:
        cites += [c for c in s["citations"] if c not in cites]
    return " ".join(s["text"] + "".join(f" [^{c}]" for c in s["citations"]) for s in cleaned), cites


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
    return {"related": u.related, "keywords": split_keywords(u.keywords), "answer_attempts": 0, "llm_calls": calls + 1,
            "skipped": note}


def split_keywords(keywords: list[str]) -> list[str]:
    """LLM이 '데이터 구조'처럼 띄어쓰기 있는 구를 내도 단어로 나눈다 (프롬프트로 시켜도 섞여 나옴, 2026-10-17 실측).
    구 하나가 통째로 어디에도 안 맞으면 그 검색어 몫만큼 모든 근거의 점수가 깎인다."""
    out: list[str] = []
    for k in keywords:
        for w in str(k).split():
            if w and w not in out:
                out.append(w)
    return out


def _search(source: str):
    async def node(state: QAState, config: RunnableConfig) -> dict:
        retriever, notes, attempted = _cfg(config)["retriever"], [], False
        reset = {"answer_attempts": 0, "verify_problem": "", "needs_more": False}
        kind, extra, scope = source, {}, None
        if source == "transcript":                 # 노트로 고른 강의 안에서 먼저
            scope = priority_lectures(state.get("hits", [])) or None
            extra = {"transcript_lectures": scope or []}
            done = [source] if scope else [source, "transcript_all"]     # 과목 전체로 찾았으면 확장 단계는 필요 없음
        elif source == "transcript_all":           # 과목 전체 (우선 강의에서 찾은 것과 겹치면 evidence가 하나로 합침)
            kind, done = "transcript", [source]
        else:
            done = [source]
        for i in range(1 + SEARCH_RETRIES[source]):
            limit = min(SEARCH_TIMEOUT_SEC, time_left(config) - ANSWER_RESERVE_SEC)
            if limit <= 0:
                notes.append(f"{source}: 답변할 시간을 남기려고 검색 중단")
                break
            attempted = True
            try:
                call = (retriever.search(kind, state.get("keywords", []), lecture_ids=scope) if scope
                        else retriever.search(kind, state.get("keywords", [])))
                hits = await asyncio.wait_for(call, timeout=limit)
                return {"hits": hits, "searched": done, "queried": [source], "skipped": notes, **reset, **extra}
            except (SearchUnavailable, TimeoutError) as e:
                why = "시간 초과" if isinstance(e, TimeoutError) else f"일시 장애: {e}"
                notes.append(f"{source}: {why} → {'다시 시도' if i < SEARCH_RETRIES[source] else '건너뜀'}")
            except SearchForbidden as e:
                raise QAFatal(f"{source} 검색 불가: {e}") from e
        return {"hits": [], "searched": done, "queried": [source] if attempted else [], "skipped": notes, **reset, **extra}
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
    by_id = {h["id"]: h for h in ev}
    sentences = clean_sentences([s.model_dump() for s in a.sentences], known_ids=set(by_id))
    text, cites = render(sentences)
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


NOTICE_TEXT = {"timeout": TIMEOUT_TEXT, "insufficient": INSUFFICIENT_TEXT, "not_found": NOT_FOUND_TEXT}


def sources_for(citations: list[str], hits: list[HitDict]) -> list[dict]:
    """인용된 근거만 hits에서 골라 화면·저장용으로. 전체 hits는 내부 처리용이다."""
    by_id = {h["id"]: h for h in hits}
    return [{k: by_id[c][k] for k in ("id", "source", "text", "ref")} for c in citations if c in by_id]


async def save(state: QAState, config: RunnableConfig) -> dict:
    """결과 상태(status)와 안내문 코드(notice_code)를 정한다. 작업 실행 상태(done/failed)와는 다른 값이다:
    시간 초과·근거 부족도 그래프는 정상 종료하므로 작업은 done이 된다."""
    notice = None
    out: dict = {"status": "completed"}
    if state.get("timed_out"):
        notice, out["status"] = "timeout", "시간 초과"
    elif state.get("verify_problem"):          # 재시도까지 실패 → 틀린 본문은 버리고 정해진 안내문으로
        notice, out["status"] = "insufficient", "근거 부족"
    elif state.get("related") and not state.get("citations") and not state.get("queried"):
        # 시간이 없어 검색을 하나도 못 했다 → "자료에 없다"(근거 부족)가 아니라 시간 초과로 알린다 (2026-10-17 결정)
        notice, out["status"] = "timeout", "시간 초과"
    elif state.get("related") and not state.get("citations"):
        # 과목 질문인데 인용이 하나도 없음 → LLM 본문(근거 없이 지어냈을 수 있음)은 버리고 안내문만
        notice, out["status"] = "not_found", "근거 부족"
    elif state.get("related") and state.get("needs_more"):
        out["status"] = "부분 답변"            # 인용은 있지만 LLM이 근거가 더 필요하다고 했고, 더 찾지 못함 (원인은 여러 가지)
    if notice:
        out.update(label="근거 부족", answer=NOTICE_TEXT[notice], citations=[], sentences=[])
    out["notice_code"] = notice
    out["sources"] = sources_for(out.get("citations", state.get("citations", [])), state.get("hits", []))
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
    hits = state.get("hits", [])
    if enough(hits):
        return "answer"
    if state.get("transcript_lectures"):           # 강의 우선으로 찾은 경우
        lecs = set(state["transcript_lectures"])
        top = best([h for h in hits if h.get("lecture_id") in lecs], ("transcript",))
        if top is None or top["score"] < EXPAND_MIN:
            if next_source(state, config) == "transcript_all":
                return "search_transcript_all"     # 우선 강의에서 거의 못 찾음 → 바로 과목 전체
        else:
            return "answer"                        # 관련 근거는 있음 → 답해 보고, LLM이 부족하다고 하면 과목 전체 → 웹
    return "search_web" if next_source(state, config) == "web" else "skip_web"


def after_transcript_all(state: QAState, config: RunnableConfig) -> str:
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
    g.add_conditional_edges("search_transcript", after_transcript, ["answer", "search_transcript_all", "search_web", "skip_web"])
    g.add_conditional_edges("search_transcript_all", after_transcript_all, ["answer", "search_web", "skip_web"])
    g.add_edge("skip_transcript", "answer")
    g.add_edge("skip_web", "answer")
    g.add_edge("search_web", "answer")
    g.add_edge("answer", "verify")
    g.add_conditional_edges("verify", after_verify, ["answer", "save", "search_transcript", "search_transcript_all", "search_web"])
    g.add_edge("save", END)
    return g.compile()
