"""scheduler 주기 작업.

  resend_stale     (팀 결정 6번)  허가 후 XADD 전에 죽었거나 메시지가 유실돼 멈춘 의도를 다시 XADD.
                                  중복 메시지는 worker의 job_start가 done/leased로 걸러 내므로 넉넉하게 보내도 안전하다.
  cleanup_checkpoints (결정 9번)  완료·실패 후 보관 기간(기본 7일)이 지난 작업의 체크포인트 삭제.

재배송(죽은 worker의 메시지 회수)은 여기서 하지 않는다. worker가 XAUTOCLAIM으로 직접 한다 (결정 1번).
"""
import asyncio

from notree.common import settings as S
from notree.common.log import ACTOR, log
from notree.worker.mcp_client import McpClient


async def resend_stale(r, mcp: McpClient, reserved_sec: float = S.RESEND_RESERVED_SEC,
                       sent_sec: float = S.RESEND_SENT_SEC) -> int:
    jobs = (await mcp.tool("job_resend_stale", reserved_older_than_sec=reserved_sec,
                           sent_older_than_sec=sent_sec))["jobs"]
    for j in jobs:
        msg_id = await r.xadd(j["stream"], {"job_id": j["job_id"]})
        log("resend", f"{j['job_id']} [{j['kind']}] 멈춰 있음 → 다시 XADD {j['stream']} {msg_id}")
    return len(jobs)


async def cleanup_checkpoints(mcp: McpClient, keep_sec: float = S.CHECKPOINT_KEEP_SEC) -> int:
    n = (await mcp.tool("checkpoint_cleanup", older_than_sec=keep_sec))["deleted_threads"]
    if n:
        log("cleanup", f"체크포인트 정리: 작업 {n}개")
    return n


async def run_periodic(r, mcp: McpClient, stop: asyncio.Event | None = None, *,
                       resend_every: float = S.RESEND_INTERVAL_SEC, cleanup_every: float = S.CLEANUP_INTERVAL_SEC,
                       reserved_sec: float = S.RESEND_RESERVED_SEC, sent_sec: float = S.RESEND_SENT_SEC,
                       keep_sec: float = S.CHECKPOINT_KEEP_SEC) -> None:
    ACTOR.set("scheduler")
    stop = stop or asyncio.Event()
    loop = asyncio.get_running_loop()
    next_resend = next_cleanup = loop.time()
    while not stop.is_set():
        now = loop.time()
        try:
            if now >= next_resend:
                await resend_stale(r, mcp, reserved_sec, sent_sec)
                next_resend = now + resend_every
            if now >= next_cleanup:
                await cleanup_checkpoints(mcp, keep_sec)
                next_cleanup = now + cleanup_every
        except Exception as e:  # 주기 작업이 죽으면 안 된다
            log("error", f"주기 작업 실패({type(e).__name__}: {e}) → 다음 주기에 재시도")
        try:
            await asyncio.wait_for(stop.wait(), min(next_resend, next_cleanup) - loop.time() + 0.01)
        except asyncio.TimeoutError:
            pass
