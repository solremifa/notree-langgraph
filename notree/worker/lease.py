"""lease = '이 작업은 지금 내가 하고 있다'는 DB 표시.

    async with Lease(ctx):
        await handler(ctx)

들어갈 때 heartbeat 태스크를 띄우고, 나올 때 멈춘다. heartbeat가 거절되면(다른 worker에게 넘어감)
핸들러를 취소하고 LeaseLost를 던진다 → 루프는 ACK도 완료 표시도 하지 않고 손을 뗀다.
"""
import asyncio

from notree.common.log import log
from notree.worker.registry import JobContext


class LeaseLost(Exception):
    pass


class Lease:
    def __init__(self, ctx: JobContext) -> None:
        self.ctx = ctx
        self.lost = False
        self._owner: asyncio.Task | None = None
        self._hb: asyncio.Task | None = None

    async def __aenter__(self) -> "Lease":
        self._owner = asyncio.current_task()
        self._hb = asyncio.create_task(self._beat())
        return self

    async def _beat(self) -> None:
        c = self.ctx
        while True:
            await asyncio.sleep(c.cfg.heartbeat_sec)
            try:
                res = await c.tool("job_heartbeat", job_id=c.job_id, worker=c.worker, lease_sec=c.cfg.lease_sec)
            except Exception as e:  # MCP 일시 장애: 이번 박동만 건너뜀. lease가 만료되기 전 다음 박동에 기대
                log("lease", f"heartbeat 실패({type(e).__name__}) → 다음 주기에 재시도")
                continue
            if not res.get("ok"):
                self.lost = True
                log("lease", f"lease를 잃음({res.get('reason', '다른 worker에게 넘어감')}) → 작업 취소")
                self._owner.cancel()
                return

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        self._hb.cancel()
        if self.lost and exc_type is asyncio.CancelledError:
            self._owner.uncancel()          # 우리가 건 취소를 거둬들이고 LeaseLost로 바꿔 던진다
            raise LeaseLost() from exc
        return False
