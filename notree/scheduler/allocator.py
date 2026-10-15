"""배정기(allocator): 대기 중인 의도 → 실행 허가 → Redis Stream에 job_id만 XADD → 전송 완료.

    uv run python -m notree.scheduler.allocator

실제 설계에서는 PG에 직접 붙어 SKIP LOCKED로 의도를 고르고, 사용자·과목 동시 상한과 예산을 확인한다.
이 PoC는 같은 일을 MCP 서버의 job_reserve_pending / job_mark_sent 도구로 흉내 낸다.
작업 종류별 Stream(rt/batch)은 의도에 기록된 stream 값을 따른다.
"""
import asyncio

import redis.asyncio as aioredis

from notree.common import settings as S
from notree.common.log import ACTOR, log
from notree.worker.mcp_client import McpClient


async def run_allocator(r, mcp: McpClient, stop: asyncio.Event | None = None, interval: float = 0.5) -> None:
    ACTOR.set("scheduler")
    stop = stop or asyncio.Event()
    while not stop.is_set():
        try:
            for j in (await mcp.call("job_reserve_pending", limit=10))["jobs"]:
                msg_id = await r.xadd(j["stream"], {"job_id": j["job_id"]})
                await mcp.call("job_mark_sent", job_id=j["job_id"])
                log("allocator", f"{j['job_id']} [{j['kind']}] 허가 → XADD {j['stream']} {msg_id}")
        except Exception as e:  # 한 번 실패해도 배정기는 죽지 않는다.
            # 주의: 허가(reserved) 뒤 XADD 전에 실패하면 그 의도는 reserved에 머문다 → 실제 구현에선
            #       'reserved인데 오래된 의도 다시 보내기' 주기 작업이 필요 (team-decisions.md)
            log("error", f"배정 실패({type(e).__name__}: {e}) → 다음 주기에 재시도")
        try:
            await asyncio.wait_for(stop.wait(), interval)
        except asyncio.TimeoutError:
            pass


async def main() -> None:
    r = aioredis.from_url(S.REDIS_URL, decode_responses=True)
    async with McpClient(url=S.MCP_URL) as mcp:
        await run_allocator(r, mcp)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
