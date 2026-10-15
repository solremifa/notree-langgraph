"""worker-batch 프로세스.

    uv run worker.py --name A                       # 계속 돌면서 작업을 받는다 (Ctrl+C로 종료)
    uv run worker.py --name A --die-at extract:world  # 그 지점에서 '죽는' worker

한 바퀴:
  1) XAUTOCLAIM : ACK 없이 CLAIM_IDLE_MS 넘게 방치된 메시지(= 죽은 worker의 작업)를 내 것으로 가져옴
  2) XREADGROUP : 없으면 새 메시지를 기다림 (메시지에는 job_id만 있음)
  3) MCP job_start : 실행 허가 확인 + lease 획득 (다른 worker가 살아서 하고 있으면 거절)
  4) 핸들러 실행 : 과목 노트면 LangGraph. thread_id = job_id 라서 체크포인트가 있으면 거기서 이어감
     실행 중에는 HEARTBEAT_SEC마다 lease 연장
  5) MCP job_finish → XACK
죽으면 4)에서 멈추고 ACK도 heartbeat도 없음 → lease 만료 → 다른 worker의 1)이 가져감
"""
import argparse
import asyncio
import sys

import redis.asyncio as aioredis
from redis.exceptions import ResponseError

import settings as S
from graph import ACTOR, SimulatedCrash, build_graph, log
from mcp_saver import McpCheckpointSaver, McpClient, McpNoteStore


class WorkerDied(Exception):
    pass


# ── 작업 종류별 핸들러 ────────────────────────────────────────────
async def course_update(mcp: McpClient, job_id: str, payload: dict, die_at: str | None) -> None:
    app = build_graph(McpCheckpointSaver(mcp))
    config = {"configurable": {"thread_id": job_id, "llm": payload.get("llm", "fake"),
                               "notes": McpNoteStore(mcp), "crash_at": die_at}}
    snap = await app.aget_state(config)
    if not snap.values:
        log("handler", f"과목 노트 업데이트 시작 (thread_id={job_id})")
        inp = {"course": payload.get("course", ""), "sources": payload["sources"]}
    elif snap.next:
        done = [e["lecture"] for e in snap.values.get("extracts", [])]
        log("handler", f"체크포인트에서 이어감: 남은 노드={list(snap.next)}  이미 끝난 extract={done}")
        inp = None
    else:
        log("handler", "그래프는 이미 끝나 있음 (완료 표시 전에 죽었던 경우) → 마무리만")
        return
    try:
        await app.ainvoke(inp, config, durability="sync")
    except SimulatedCrash as e:
        raise WorkerDied(str(e)) from e


HANDLERS = {"course_update": course_update}


# ── worker 루프 ──────────────────────────────────────────────────
async def heartbeat(mcp: McpClient, job_id: str, name: str) -> None:
    while True:
        await asyncio.sleep(S.HEARTBEAT_SEC)
        if not (await mcp.call("job_heartbeat", job_id=job_id, worker=name, lease_sec=S.LEASE_SEC))["ok"]:
            log("lease", "lease를 잃음 (다른 worker에게 넘어감)")
            return


async def handle(r, mcp: McpClient, name: str, stream: str, msg_id: str, fields: dict, how: str,
                 die_at: str | None) -> bool:
    """처리 후 계속 살아 있으면 True, 죽었으면 False."""
    job_id = fields["job_id"]
    log("redis", f"{how} 메시지 {msg_id} → job {job_id}")
    st = await mcp.call("job_start", job_id=job_id, worker=name, lease_sec=S.LEASE_SEC)
    if not st["ok"]:
        if st["reason"] in ("done", "unknown"):
            await r.xack(stream, S.GROUP, msg_id)
            log("permit", f"이미 끝난 작업({st['reason']}) → ACK만 하고 넘어감")
        else:
            log("permit", f"{st['owner']}의 lease가 {st['left']}초 남음 → 건드리지 않음")
        return True

    prev = f", 이전 담당={st['previous_owner']}" if st.get("previous_owner") not in (None, name) else ""
    log("permit", f"실행 허가 + lease {S.LEASE_SEC:g}초 획득 (시도 {st['attempt']}회째{prev})")
    hb = asyncio.create_task(heartbeat(mcp, job_id, name))
    try:
        await HANDLERS[st["kind"]](mcp, job_id, st["payload"], die_at)
    except WorkerDied as e:
        hb.cancel()
        log("💀", f"{e} → 프로세스 죽음. ACK·완료 표시·heartbeat 모두 없음")
        return False
    except Exception as e:  # 진짜 오류: 이 PoC에선 ACK 안 하고 재배송에 맡김 (실제론 재시도 한도·dead letter)
        hb.cancel()
        log("error", f"{type(e).__name__}: {e} → ACK 안 함, 재배송 대기")
        return True
    hb.cancel()
    await mcp.call("job_finish", job_id=job_id, worker=name)
    await r.xack(stream, S.GROUP, msg_id)
    log("redis", f"job_finish → XACK {msg_id}")
    return True


async def run_worker(name: str, r, mcp: McpClient, die_at: str | None = None,
                     stop: asyncio.Event | None = None, block_ms: int = 1000) -> str:
    """block_ms=0이면 막지 않고 짧게 쉬며 폴링 (fakeredis는 BLOCK이 이벤트 루프를 멈추게 함)"""
    ACTOR.set(f"worker-{name}")
    stop = stop or asyncio.Event()
    try:
        await r.xgroup_create(S.BATCH_STREAM, S.GROUP, id="0", mkstream=True)
    except ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise
    log("start", f"대기 시작 (stream={S.BATCH_STREAM}, group={S.GROUP})" + (f"  die-at={die_at}" if die_at else ""))
    while not stop.is_set():
        # 1) 방치된 메시지 회수
        _, claimed, _ = await r.xautoclaim(S.BATCH_STREAM, S.GROUP, f"worker-{name}",
                                           min_idle_time=S.CLAIM_IDLE_MS, start_id="0-0", count=1)
        if claimed:
            msg_id, fields = claimed[0]
            how = "재배송(XAUTOCLAIM)"
        else:
            # 2) 새 메시지
            res = await r.xreadgroup(S.GROUP, f"worker-{name}", {S.BATCH_STREAM: ">"}, count=1,
                                     block=block_ms or None)
            if not res:
                if not block_ms:
                    await asyncio.sleep(0.2)
                continue
            msg_id, fields = res[0][1][0]
            how = "신규(XREADGROUP)"
        if not await handle(r, mcp, f"worker-{name}", S.BATCH_STREAM, msg_id, fields, how, die_at):
            return "died"
    log("stop", "종료")
    return "stopped"


async def main() -> None:
    p = argparse.ArgumentParser(description="worker-batch")
    p.add_argument("--name", default="A")
    p.add_argument("--die-at", default=None, help="extract:<강의> 또는 publish 지점에서 죽음")
    a = p.parse_args()
    r = aioredis.from_url(S.REDIS_URL, decode_responses=True)
    async with McpClient(url=S.MCP_URL) as mcp:
        result = await run_worker(a.name, r, mcp, die_at=a.die_at)
    await r.aclose()
    sys.exit(1 if result == "died" else 0)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
