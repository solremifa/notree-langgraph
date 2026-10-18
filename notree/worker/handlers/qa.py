"""Q&A (qa 레인). 그래프 notree/graphs/qa를 worker에서 돌린다.

    payload = {"question_id": ..., "deadline_at": 요청 + 90초}     (api의 qa_ask가 만든다)

한 번의 실행
  1. qa_context   → 질문·과목 이름·세대. 이 작업의 답이 이미 저장돼 있으면(answered) 여기서 완료
  2. 그래프 실행   → 검색은 McpRetriever가 qa_search(노트·전사)·web_search(웹)로. 시간 예산은 deadline_at 기준
  3. qa_complete  → 결과 저장 (idem_key = job_id, expected_generation = 1의 세대)

오류 변환
  검색 도구 retryable 거절·연결 문제 → SearchUnavailable (그래프가 1번 다시 / 다음 출처로)
  검색 도구 영구 거절 (not_found·permit_invalid) → SearchForbidden → 그래프 QAFatal → FatalError (다시 해도 안 됨)
  qa_complete 거절: generation_mismatch(질문 삭제·수정) → FatalError / unavailable → RetryableError
    (같은 시도 안에서 저장만 다시 하지 않고 루프의 재시도에 맡긴다. 저장이 이미 됐으면 다음 시도의 1단계에서 완료)
    다음 시도가 없으면(마지막 시도·마감 초과): 루프가 실패를 확정하기 전에 qa_saved(done_check)로 저장 여부를 보고
    저장돼 있으면 완료로 확정한다 (2026-10-19 리뷰 6차 ③ — 답변은 있는데 작업은 실패로 남던 문제)
  LLM 오류(429 등)는 그래프 밖으로 나오면 루프가 재시도. 그래프 안의 시간 초과는 '시간 초과' 결과로 정상 저장된다
  시간 초과·근거 부족도 작업은 done (결과 상태 status는 작업 상태와 별개, 팀 결정 10번)

LLM은 extras["qa_llm"] → 없으면 NOTREE_QA_LLM (fake | openai, 기본 fake).
노트 반영 제안(proposal)·사용자 승인은 첫 통합에서 뺐다.
"""
import os

from notree.common.log import log
from notree.graphs.qa import build_qa_graph
from notree.graphs.qa.graph import QAFatal
from notree.graphs.qa.llm import get_qa_llm
from notree.graphs.qa.retriever import Hit, SearchForbidden, SearchUnavailable
from notree.graphs.qa.state import HitDict, Source
from notree.worker.registry import FatalError, JobContext, RetryableError, ToolRejected, ToolUnavailable, handler

_graph = None


def _get_graph():
    global _graph
    if _graph is None:
        _graph = build_qa_graph()
    return _graph


class McpRetriever:
    """그래프의 Retriever를 MCP 도구로. 검색 범위(사용자·과목)는 서버가 질문으로 정한다."""

    def __init__(self, ctx: JobContext, question_id: str) -> None:
        self.ctx = ctx
        self.qid = question_id
        self.calls: list[str] = []

    async def search(self, source: Source, keywords: list[str], limit: int = 3) -> list[HitDict]:
        self.calls.append(source)
        try:
            if source == "web":
                res = await self.ctx.tool("web_search", question_id=self.qid, query=" ".join(keywords), limit=limit)
                return [Hit(r["id"], "web", r["snippet"], r["score"], r["url"]) for r in res["results"]]
            res = await self.ctx.tool("qa_search", question_id=self.qid, source=source, keywords=keywords, limit=limit)
            return [Hit(h["id"], h["source"], h["text"], h["score"], h["ref"]) for h in res["hits"]]
        except ToolRejected as e:
            raise SearchForbidden(f"{e.code}: {e}") from e
        except ToolUnavailable as e:
            raise SearchUnavailable(f"{e.code}: {e}") from e
        except (ConnectionError, OSError) as e:
            raise SearchUnavailable(f"{type(e).__name__}: {e}") from e


async def qa_saved(ctx: JobContext) -> bool:
    """done_check: 이 작업(idem_key = job_id)의 답이 이미 저장됐나. 읽기만."""
    qid = ctx.payload.get("question_id")
    return bool(qid) and bool((await ctx.tool("qa_context", question_id=qid)).get("answered"))


@handler("qa", done_check=qa_saved)
async def qa(ctx: JobContext) -> None:
    qid = ctx.payload.get("question_id")
    if not qid:
        raise FatalError("payload에 question_id 없음")
    c = await ctx.tool("qa_context", question_id=qid)
    if c.get("answered"):                     # 저장은 됐는데 응답을 못 받고 재시도됨 → 다시 만들지 않는다
        log("qa", f"{qid}: 이 작업의 답이 이미 저장돼 있음 → 그래프 건너뛰고 완료")
        return
    llm = ctx.extras.get("qa_llm") or get_qa_llm(os.getenv("NOTREE_QA_LLM", "fake"))
    retriever = McpRetriever(ctx, qid)
    cfg = {"configurable": {"llm": llm, "retriever": retriever, "deadline_at": ctx.deadline_at}}
    try:
        final = await _get_graph().ainvoke({"question": c["question"], "course": c["course_name"]}, cfg)
    except QAFatal as e:
        raise FatalError(str(e)) from e
    try:
        res = await ctx.tool("qa_complete", idem_key=ctx.job_id, question_id=qid, expected_generation=c["generation"],
                             status=final["status"], label=final["label"], answer=final["answer"],
                             sentences=final.get("sentences") or [], sources=final.get("sources") or [],
                             notice_code=final.get("notice_code"))
    except ToolUnavailable as e:
        raise RetryableError(f"답변 저장 실패: {e}") from e
    except ToolRejected as e:
        raise FatalError(f"답변 저장 거절({e.code}): {e}") from e
    log("qa", f"{qid}: {final['status']} [{final['label']}] 검색={retriever.calls} 인용={final.get('citations', [])} "
              f"→ {res['answer_id']}")
