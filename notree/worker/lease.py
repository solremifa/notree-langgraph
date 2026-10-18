"""lease = '이 작업은 지금 내가 하고 있다'는 DB 표시.

    async with Lease(ctx):
        await handler(ctx)

들어갈 때 heartbeat 태스크를 띄우고, 나올 때 멈춘다. heartbeat가 거절되면(다른 worker에게 넘어감)
또는 lease 시간 동안 한 번도 갱신하지 못하면(오류·무응답 — 한 번의 호출은 lease의 남은 시간까지만 기다린다)
핸들러를 취소하고 LeaseLost를 던진다 → 루프는 ACK도 완료 표시도 하지 않고 손을 뗀다.

heartbeat가 성공할 때마다 touch()도 부른다. 루프는 여기서 Redis 메시지의 '방치 시간'을 0으로 되돌린다
(XCLAIM … JUSTID). 그래야 살아서 실행 중인 메시지가 XAUTOCLAIM 재배송 대상에 오르지 않는다.
"""
import asyncio
import time
from collections.abc import Awaitable, Callable

from notree.common.log import log
from notree.contracts.envelope import ErrorCode, ToolError
from notree.worker.registry import JobContext


class LeaseLost(Exception):
    pass


class Lease:
    def __init__(self, ctx: JobContext, touch: Callable[[], Awaitable[object]] | None = None) -> None:
        self.ctx = ctx
        self.touch = touch
        self.lost = False
        self._owner: asyncio.Task | None = None
        self._hb: asyncio.Task | None = None

    async def __aenter__(self) -> "Lease":
        self._owner = asyncio.current_task()
        self._hb = asyncio.create_task(self._beat())
        return self

    async def _beat(self) -> None:
        c, last_ok = self.ctx, time.monotonic()
        while True:
            await asyncio.sleep(c.cfg.heartbeat_sec)
            left = c.cfg.lease_sec - (time.monotonic() - last_ok)
            try:                                                # 응답이 멈춰도 lease가 남은 동안만 기다린다
                res = await asyncio.wait_for(
                    c.mcp.tool("job_heartbeat", permit=c.permit, job_id=c.job_id, worker=c.worker,
                               lease_sec=c.cfg.lease_sec), timeout=max(left, 0.01))
                renewed, why = res["renewed"], "다른 worker에게 넘어감"
            except Exception as e:
                if isinstance(e, ToolError) and e.code == ErrorCode.PERMIT_INVALID:
                    renewed, why = False, e.message
                elif time.monotonic() - last_ok < c.cfg.lease_sec:   # 일시 장애·무응답: lease 안이면 이번 박동만 건너뜀
                    log("lease", f"heartbeat 실패({getattr(e, 'code', None) or type(e).__name__}) → 다음 주기에 재시도")
                    continue
                else:                                           # lease가 끝날 때까지 한 번도 갱신 못 함 → 이미 남의 것일 수 있다
                    renewed, why = False, f"{c.cfg.lease_sec:g}초 넘게 갱신 못 함"
            if renewed:
                last_ok = time.monotonic()
            if not renewed:
                self.lost = True
                log("lease", f"lease를 잃음({why}) → 작업 취소")
                self._owner.cancel()
                return
            if self.touch:
                try:
                    await self.touch()
                except Exception as e:                          # 다음 박동에 다시
                    log("lease", f"Redis 방치 시간 초기화 실패({type(e).__name__})")

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        self._hb.cancel()
        if self.lost and exc_type is asyncio.CancelledError:
            self._owner.uncancel()          # 우리가 건 취소를 거둬들이고 LeaseLost로 바꿔 던진다
            raise LeaseLost() from exc
        return False
