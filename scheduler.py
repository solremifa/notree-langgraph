"""scheduler 프로세스의 배정기(allocator).

    python scheduler.py

대기 중인 의도(intent)를 골라 실행 허가(reserved)를 기록하고, Redis Stream에 job_id만 XADD한 뒤
'전송 완료'로 표시한다. api가 Redis에 직접 넣지 않는 이유는 설계서 2절(outbox) 참고.

재배송은 이 PoC에서는 worker가 XAUTOCLAIM으로 직접 한다 (Redis Streams의 표준 방식).
설계서처럼 scheduler 주기 작업으로 옮겨도 동작은 같다.
"""
import asyncio

import redis.asyncio as aioredis

import settings as S
from graph import ACTOR, log
from mcp_saver import McpClient


async def run_allocator(r, mcp: McpClient, stop: asyncio.Event | None = None, interval: float = 0.5) -> None:
    ACTOR.set("scheduler")
    stop = stop or asyncio.Event()
    while not stop.is_set():
        for j in (await mcp.call("job_reserve_pending", limit=10))["jobs"]:
            log("allocator", f"의도 {j['job_id']} → 실행 허가(reserved)")
            msg_id = await r.xadd(j["stream"], {"job_id": j["job_id"]})
            await mcp.call("job_mark_sent", job_id=j["job_id"])
            log("allocator", f"XADD {j['stream']} {msg_id} → 전송 완료(sent)")
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
