"""Agent Server용 'Q&A 작업' 그래프 (실험 2단계). worker의 실행기 역할을 LangGraph 노드로 옮겨 본다.

    START → start_job ─(started)→ run_handler → settle → END
                       └(done · failed · leased · unknown)→ END

  start_job    업무 서비스 job_start → permit · 실행 세대 발급 (늦게 끝난 이전 실행은 지금처럼 DB가 거절)
  run_handler  지금 worker의 qa handler를 그대로 부른다: qa_context → Q&A 그래프 → qa_complete (멱등키 = job_id)
               실행 내내 heartbeat(Lease)로 DB lease를 연장, 마감(deadline_at)을 지킨다
  settle       결과에 따라 job_finish / job_fail / job_release

run 입력은 {"job_id": ...} 하나. 질문 저장 + 작업 등록은 지금처럼 업무 서비스(qa_ask)가 한 트랜잭션으로 한다.
즉 이 그래프는 'scheduler가 XADD 대신 서버에 run을 만들면' 쓰일 자리다.

config.configurable (모두 선택)
  mcp_url   업무 서비스 MCP 주소 (기본 NOTREE_MCP_URL, http://127.0.0.1:8765/mcp)
  llm_kind  "fake" | "openai"   (기본 NOTREE_QA_LLM, 없으면 fake)
  mcp       (시험 전용) 이미 연 McpClient 객체. 서버 run에서는 쓸 수 없다

주의 (실험이라 그대로 둔 것)
  - permit을 State에 둔다 → 서버 체크포인트(서버 쪽 저장소)에 남는다. 운영이면 State 밖에 둬야 한다.
  - 안쪽 Q&A 그래프는 하위 그래프가 아니라 따로 실행한다 (run_handler 주석). 그래서 Studio · 스트림에 안쪽 노드가 안 보인다.
  - 재시도: run_handler가 일시 오류면 job_release만 하고 끝난다. 다시 실행하는 쪽(지금은 Redis 재배송)이 없다.
"""
import asyncio
import contextvars
import os
import traceback
from contextlib import asynccontextmanager
from typing import TypedDict

from langchain_core.runnables import RunnableConfig
from langgraph.graph import END, START, StateGraph

from notree.common import settings as S
from notree.common.log import log
from notree.graphs.qa.llm import get_qa_llm
from notree.worker.handlers.qa import qa as qa_handler
from notree.worker.lease import Lease, LeaseLost
from notree.worker.mcp_client import McpClient
from notree.worker.registry import FatalError, JobContext


class JobState(TypedDict, total=False):
    job_id: str
    worker: str
    start_status: str      # started | done | failed | leased | unknown | stale_dispatch
    permit: str            # 주의: 체크포인트에 저장됨 (위 설명)
    kind: str
    payload: dict
    attempt: int
    exec_epoch: int
    outcome: str           # ok | fatal | retry | fail | lost
    reason: str
    settled: str           # finished | failed | released | not_confirmed


def _c(config: RunnableConfig) -> dict:
    return config.get("configurable", {}) or {}


@asynccontextmanager
async def _mcp(config: RunnableConfig):
    c = _c(config)
    if c.get("mcp") is not None:                    # 시험: 이미 연 클라이언트
        yield c["mcp"]
        return
    async with McpClient(url=c.get("mcp_url") or S.MCP_URL) as m:
        yield m


def _worker_name(config: RunnableConfig) -> str:
    run_id = str(_c(config).get("run_id") or "")
    return "lgserver-" + (run_id[-8:] if run_id else "local")


async def start_job(state: JobState, config: RunnableConfig) -> dict:
    worker = _worker_name(config)
    async with _mcp(config) as m:
        st = await m.tool("job_start", job_id=state["job_id"], worker=worker, lease_sec=S.QA.lease_sec)
    out = {"worker": worker, "start_status": st["status"]}
    if st["status"] == "started":
        out.update(permit=st["permit"], kind=st["kind"], payload=st["payload"], attempt=st["attempt"],
                   exec_epoch=st["exec_epoch"])
    log("lgserver", f"{state['job_id']}: job_start → {st['status']}")
    return out


def after_start(state: JobState) -> str:
    return "run_handler" if state["start_status"] == "started" else END


async def run_handler(state: JobState, config: RunnableConfig) -> dict:
    if state["kind"] != "qa":
        return {"outcome": "fatal", "reason": f"이 실험 그래프는 qa만 처리 (받은 kind: {state['kind']})"}
    kind = _c(config).get("llm_kind") or os.getenv("NOTREE_QA_LLM", "fake")
    async with _mcp(config) as m:
        ctx = JobContext(job_id=state["job_id"], kind="qa", payload=state["payload"], attempt=state["attempt"],
                         permit=state["permit"], worker=state["worker"], cfg=S.QA, mcp=m,
                         max_attempts=S.max_attempts_for("qa", S.QA), deadline_at=state["payload"].get("deadline_at"),
                         extras={"qa_llm": get_qa_llm(kind)})
        timeout = S.job_timeout_for("qa", S.QA)
        left = ctx.time_left()
        if left is not None:
            if left <= 0:
                return {"outcome": "fail", "reason": "마감 시각 지남"}
            timeout = min(timeout, left)
        try:
            async with Lease(ctx):                  # 지금 worker와 같은 heartbeat
                # 안쪽 Q&A 그래프를 이 run의 하위 그래프로 묶지 않는다 (빈 contextvars로 따로 실행).
                # 묶이면 서버 체크포인터를 물려받아, handler가 config에 넣은 LLM · MCP 검색기 객체까지 저장하려다
                # 'cannot pickle async_generator'로 실패한다 (서버 런타임에서만 재현, 2026-10-29).
                # 근본 해결: Q&A 그래프가 객체를 config 대신 LangGraph context(Runtime, 저장 안 됨)에서 읽게 바꾸기.
                inner = asyncio.create_task(asyncio.wait_for(qa_handler(ctx), timeout=timeout),
                                            context=contextvars.Context())
                await inner
        except LeaseLost:
            return {"outcome": "lost", "reason": "lease를 잃음 (다른 실행이 가져감)"}
        except FatalError as e:
            return {"outcome": "fatal", "reason": str(e)}
        except Exception as e:                      # RetryableError · TimeoutError 등
            reason = "timeout" if isinstance(e, TimeoutError) else f"{type(e).__name__}: {e}"
            log("lgserver", f"{state['job_id']}: run_handler 예외\n{traceback.format_exc()}")
            last = state["attempt"] >= ctx.max_attempts or (ctx.time_left() is not None and ctx.time_left() <= 0)
            return {"outcome": "fail" if last else "retry", "reason": reason}
    return {"outcome": "ok", "reason": ""}


async def settle(state: JobState, config: RunnableConfig) -> dict:
    o, base = state["outcome"], dict(permit=state["permit"], job_id=state["job_id"], worker=state["worker"])
    if o == "lost":                                 # 실행권이 이미 다른 실행에 있다 → 아무것도 표시하지 않는다
        return {"settled": "not_confirmed"}
    async with _mcp(config) as m:
        if o == "ok":
            res = await m.tool("job_finish", **base)
            settled = "finished" if res.get("finished") else "not_confirmed"
        elif o in ("fatal", "fail"):
            res = await m.tool("job_fail", **base, reason=state["reason"], user_retryable=(o == "fail"))
            settled = "failed" if res.get("failed") else "not_confirmed"
        else:
            await m.tool("job_release", **base, reason=state["reason"])
            settled = "released"
    log("lgserver", f"{state['job_id']}: {o} → {settled}")
    return {"settled": settled}


def build_qa_job_graph(checkpointer=None):
    g = StateGraph(JobState)
    g.add_node("start_job", start_job)
    g.add_node("run_handler", run_handler)
    g.add_node("settle", settle)
    g.add_edge(START, "start_job")
    g.add_conditional_edges("start_job", after_start, ["run_handler", END])
    g.add_edge("run_handler", "settle")
    g.add_edge("settle", END)
    return g.compile(checkpointer=checkpointer)


# 서버가 불러 가는 그래프 (객체를 붙일 필요가 없어 팩토리 대신 컴파일된 그래프를 그대로 내보낸다)
graph = build_qa_job_graph()
