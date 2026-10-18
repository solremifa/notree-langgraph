"""배정기(allocator): 대기 중인 의도 → 실행 허가 → Redis Stream에 job_id만 XADD → 전송 완료.

    uv run python -m notree.scheduler.allocator      (주기 작업 periodic.py도 같이 돈다)

DB에는 "이 레인의 대기 의도를 생성 순서로 N개 고르고 허가로 바꾼다"(job_reserve_pending stream=…)만 맡기고,
어느 레인을 얼마나 먼저 고를지는 여기서 정한다 (settings.ALLOC_LANES).

한 주기
  바퀴마다 qa → rt → batch 순으로 레인별 몫만큼 고름
  어느 레인이든 몫을 다 채웠으면(더 남았을 수 있음) 쉬지 않고 다음 바퀴, 한 주기 최대 ALLOC_MAX_PER_TICK개
  → rt에 요약 2,000개가 쌓여 있어도 qa 의도는 다음 바퀴(수 ms~수십 ms) 안에 나간다
lanes=()이면 레인 구분 없이 생성 순서로 고른다 (레인 분리 전 방식, 비교용).
"""
import asyncio

import redis.asyncio as aioredis

from notree.common import settings as S
from notree.common.log import ACTOR, log
from notree.worker.mcp_client import McpClient


def _fields(j: dict) -> dict:
    """XADD 필드: 작업 ID와 전달 세대 (세대가 있으면). worker는 세대를 job_start에 넘겨 Redis 유실 전 메시지를 거른다."""
    return {"job_id": j["job_id"], **({"gen": str(j["gen"])} if j.get("gen") is not None else {})}


async def _send(r, mcp: McpClient, j: dict) -> None:
    msg_id = await r.xadd(j["stream"], _fields(j))
    await mcp.tool("job_mark_sent", job_id=j["job_id"])
    log("allocator", f"{j['job_id']} [{j['kind']}] 허가 → XADD {j['stream']} {msg_id}")


async def allocate_once(r, mcp: McpClient, lanes=S.ALLOC_LANES, max_per_tick: int = S.ALLOC_MAX_PER_TICK) -> int:
    """한 주기 분량을 배정하고 보낸 수를 돌려준다."""
    if not lanes:                                        # 레인 구분 없음 (비교용)
        jobs = (await mcp.tool("job_reserve_pending", limit=10))["jobs"]
        for j in jobs:
            await _send(r, mcp, j)
        return len(jobs)
    sent = 0
    while sent < max_per_tick:
        more = False
        for key, quota in lanes:
            n = min(quota, max_per_tick - sent)
            if n <= 0:
                break
            jobs = (await mcp.tool("job_reserve_pending", limit=n, stream=S.STREAMS[key].stream))["jobs"]
            for j in jobs:
                await _send(r, mcp, j)
            sent += len(jobs)
            more |= len(jobs) == n
        if not more:
            break
    return sent


async def run_allocator(r, mcp: McpClient, stop: asyncio.Event | None = None,
                        interval: float = S.ALLOC_INTERVAL_SEC, lanes=S.ALLOC_LANES) -> None:
    ACTOR.set("scheduler")
    stop = stop or asyncio.Event()
    while not stop.is_set():
        try:
            await allocate_once(r, mcp, lanes)
        except Exception as e:  # 한 번 실패해도 배정기는 죽지 않는다.
            # 허가(reserved) 뒤 XADD 전에 실패하면 그 의도는 reserved에 머문다 → periodic.resend_stale이 다시 보냄
            log("error", f"배정 실패({type(e).__name__}: {e}) → 다음 주기에 재시도")
        try:
            await asyncio.wait_for(stop.wait(), interval)
        except asyncio.TimeoutError:
            pass


async def main() -> None:
    """scheduler 프로세스: 배정기 + 주기 작업(누락 복구·체크포인트 정리)."""
    from notree.scheduler.periodic import run_periodic
    r = aioredis.from_url(S.REDIS_URL, decode_responses=True)
    async with McpClient(url=S.MCP_URL) as mcp:
        await asyncio.gather(run_allocator(r, mcp), run_periodic(r, mcp))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
