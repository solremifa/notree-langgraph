"""lease = '이 작업은 지금 내가 하고 있다'는 DB 표시.

    async with Lease(ctx):
        await handler(ctx)

들어갈 때 heartbeat 태스크를 띄우고, 나올 때 멈춘다. heartbeat가 거절되면(다른 worker에게 넘어감)
핸들러를 취소하고 LeaseLost를 던진다 → 루프는 ACK도 완료 표시도 하지 않고 손을 뗀다.
"""
import asyncio

from notree.common.log import log
from notree.contracts.envelope import ErrorCode, ToolError
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
                res = await c.mcp.tool("job_heartbeat", permit=c.permit, job_id=c.job_id, worker=c.worker,
                                       lease_sec=c.cfg.lease_sec)
                renewed, why = res["renewed"], "다른 worker에게 넘어감"
            except ToolError as e:
                if e.code != ErrorCode.PERMIT_INVALID:          # 일시 장애: 이번 박동만 건너뜀
                    log("lease", f"heartbeat 실패({e.code}) → 다음 주기에 재시도")
                    continue
                renewed, why = False, e.message
            except Exception as e:                              # 연결 문제 등도 이번 박동만 건너뜀
                log("lease", f"heartbeat 실패({type(e).__name__}) → 다음 주기에 재시도")
                continue
            if not renewed:
                self.lost = True
                log("lease", f"lease를 잃음({why}) → 작업 취소")
                self._owner.cancel()
                return

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        self._hb.cancel()
        if self.lost and exc_type is asyncio.CancelledError:
            self._owner.uncancel()          # 우리가 건 취소를 거둬들이고 LeaseLost로 바꿔 던진다
            raise LeaseLost() from exc
        return False
