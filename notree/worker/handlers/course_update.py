"""과목 노트 업데이트 (batch, LangGraph).

thread_id = job_id 라서 재시도·재배송 때 체크포인트가 있으면 거기서 이어간다.
그래프 자체(graph.py)는 아직 hello world 모양이다. 실제 노드는 이 그래프의 노드 함수만 바꾸면 된다.
"""
from graph import SimulatedCrash, build_graph
from notree.common.log import log
from notree.worker.checkpointer import McpCheckpointSaver, McpNoteStore
from notree.worker.registry import JobContext, RetryableError, handler


@handler("course_update")
async def course_update(ctx: JobContext) -> None:
    app = build_graph(McpCheckpointSaver(ctx.mcp, permit=ctx.permit))
    config = {"configurable": {
        "thread_id": ctx.job_id, "llm": ctx.payload.get("llm", "fake"),
        "notes": McpNoteStore(ctx.mcp, permit=ctx.permit),
    }}
    snap = await app.aget_state(config)
    if not snap.values:
        # 시험용: 처음 실행할 때만 지정 지점에서 실패 → 재시도·되살리기 때 체크포인트에서 이어가는지 확인
        config["configurable"]["crash_at"] = ctx.payload.get("crash_at")
        inp = {"course": ctx.payload.get("course", ""), "sources": ctx.payload["sources"]}
    elif snap.next:
        done = [e["lecture"] for e in snap.values.get("extracts", [])]
        log("graph", f"체크포인트에서 이어감: 남은 노드={list(snap.next)} 끝난 extract={done}")
        inp = None
    else:
        log("graph", "그래프는 이미 끝나 있음 → 마무리만")
        return
    try:
        await app.ainvoke(inp, config, durability="sync")
    except SimulatedCrash as e:
        raise RetryableError(str(e)) from e
