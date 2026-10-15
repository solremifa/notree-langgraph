"""worker 루프: Redis에서 작업 ID를 받아 핸들러에 넘기고, 결과에 따라 ACK한다.

    Worker(name, cfg, redis, mcp).run(stop_event)

한 메시지의 처리 (process)
  job_start ─┬─ done / failed / unknown → XACK (이미 끝난 작업)
             ├─ leased                  → 손대지 않음 (다른 worker가 살아서 하는 중)
             └─ ok → Lease 안에서 핸들러 실행 (job_timeout 제한)
                      ├─ 성공            → job_finish → XACK
                      ├─ FatalError      → job_fail   → XACK
                      ├─ 그 밖의 실패     → 시도 < 한도: job_release (ACK 안 함 → claim_idle 뒤 재배송)
                      │                    시도 = 한도: job_fail → XACK
                      ├─ LeaseLost       → 아무것도 안 함 (이미 다른 worker 것)
                      └─ 종료 신호로 취소 → job_release (ACK 안 함 → 다른 worker가 곧 이어받음)
"""
import asyncio

from redis.exceptions import ResponseError

from notree.common import settings as S
from notree.common.log import ACTOR, log
from notree.common.settings import StreamConfig
from notree.worker.lease import Lease, LeaseLost
from notree.worker.mcp_client import McpClient
from notree.worker.registry import FatalError, JobContext, load_handlers


class Worker:
    def __init__(self, name: str, cfg: StreamConfig, redis, mcp: McpClient, *,
                 concurrency: int | None = None, block_ms: int = 1000, extras: dict | None = None,
                 shutdown_grace_sec: float | None = None) -> None:
        self.name = f"{cfg.key}-{name}"          # consumer 이름 = lease 소유자 이름
        self.cfg = cfg
        self.r = redis
        self.mcp = mcp
        self.concurrency = concurrency or cfg.concurrency
        self.block_ms = block_ms                  # 0이면 BLOCK 없이 폴링 (fakeredis용)
        self.extras = extras or {}
        self.grace = S.SHUTDOWN_GRACE_SEC if shutdown_grace_sec is None else shutdown_grace_sec
        self.handlers = load_handlers()
        self.running: set[asyncio.Task] = set()

    # ── 메인 루프 ──
    async def run(self, stop: asyncio.Event) -> None:
        ACTOR.set(self.name)
        await self._ensure_group()
        log("start", f"stream={self.cfg.stream} 동시 {self.concurrency}개 · lease {self.cfg.lease_sec:g}s · "
                     f"재배송 {self.cfg.claim_idle_ms / 1000:g}s · 한도 {self.cfg.max_attempts}회 · "
                     f"핸들러 {sorted(self.handlers)}")
        slots = asyncio.Semaphore(self.concurrency)
        while not stop.is_set():
            await slots.acquire()                         # 빈 자리가 있을 때만 메시지를 가져온다
            try:
                msg = None if stop.is_set() else await self._next()
            except Exception as e:                        # Redis·네트워크 일시 장애: 루프는 죽지 않는다
                slots.release()
                log("error", f"메시지 가져오기 실패({type(e).__name__}: {e}) → 1초 뒤 재시도")
                await asyncio.sleep(1)
                continue
            if msg is None:
                slots.release()
                if not self.block_ms:
                    await asyncio.sleep(0.2)
                continue
            t = asyncio.create_task(self._guarded(*msg))
            self.running.add(t)
            t.add_done_callback(lambda t: (self.running.discard(t), slots.release()))
        await self._drain()

    async def _ensure_group(self) -> None:
        try:
            await self.r.xgroup_create(self.cfg.stream, S.GROUP, id="0", mkstream=True)
        except ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    async def _next(self):
        """(msg_id, job_id, how) 또는 None. 방치된 메시지 회수가 새 메시지보다 먼저다."""
        _, claimed, _ = await self.r.xautoclaim(self.cfg.stream, S.GROUP, self.name,
                                                min_idle_time=self.cfg.claim_idle_ms, start_id="0-0", count=1)
        if claimed:
            msg_id, fields = claimed[0]
            return msg_id, fields["job_id"], "재배송"
        res = await self.r.xreadgroup(S.GROUP, self.name, {self.cfg.stream: ">"}, count=1,
                                      block=self.block_ms or None)
        if not res:
            return None
        msg_id, fields = res[0][1][0]
        return msg_id, fields["job_id"], "신규"

    async def _drain(self) -> None:
        if self.running:
            log("stop", f"종료 신호: 새 작업 중단, 진행 중 {len(self.running)}개를 최대 {self.grace:g}초 기다림")
            done, pending = await asyncio.wait(self.running, timeout=self.grace)
            for t in pending:
                t.cancel()                                 # process()가 lease를 반납한다
            if pending:
                await asyncio.wait(pending, timeout=5)
                log("stop", f"{len(pending)}개는 끝내지 못하고 반납 → 다른 worker가 이어받음")
        log("stop", "종료")

    # ── 메시지 하나 ──
    async def _guarded(self, msg_id: str, job_id: str, how: str) -> None:
        """process 바깥의 마지막 안전망. 여기까지 온 오류는 ACK 없이 두므로 재배송된다."""
        try:
            await self.process(msg_id, job_id, how)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log("error", f"{job_id}: 처리 중 예외({type(e).__name__}: {e}) → ACK 안 함, 재배송 대기")

    async def process(self, msg_id: str, job_id: str, how: str) -> None:
        ACTOR.set(self.name)
        st = await self.mcp.call("job_start", job_id=job_id, worker=self.name, lease_sec=self.cfg.lease_sec)
        if not st["ok"]:
            if st["reason"] in ("done", "failed", "unknown"):
                await self._ack(msg_id)
                log("skip", f"{job_id}: 이미 {st['reason']} → ACK만")
            else:
                log("skip", f"{job_id}: {st['owner']}가 lease 보유({st['left']}초 남음) → 손대지 않음")
            return

        ctx = JobContext(job_id=job_id, kind=st["kind"], payload=st["payload"], attempt=st["attempt"],
                         permit=st["permit"], worker=self.name, cfg=self.cfg, mcp=self.mcp, extras=self.extras)
        fn = self.handlers.get(ctx.kind)
        log("job", f"{how} {job_id} [{ctx.kind}] 시도 {ctx.attempt}/{self.cfg.max_attempts}")
        try:
            if fn is None:
                raise FatalError(f"핸들러 없음: {ctx.kind}")
            async with Lease(ctx):
                await asyncio.wait_for(fn(ctx), timeout=self.cfg.job_timeout_sec)
        except LeaseLost:
            log("job", f"{job_id}: lease를 잃어 손을 뗌 (ACK 안 함)")
        except asyncio.CancelledError:
            await self._safe(ctx.tool("job_release", job_id=job_id, worker=self.name, reason="shutdown"))
            log("job", f"{job_id}: 종료로 중단 → lease 반납")
            raise
        except FatalError as e:
            await self._fail(ctx, msg_id, f"fatal: {e}")
        except Exception as e:  # RetryableError, TimeoutError, 예상 못 한 오류
            reason = "timeout" if isinstance(e, TimeoutError) else f"{type(e).__name__}: {e}"
            if ctx.attempt >= self.cfg.max_attempts:
                await self._fail(ctx, msg_id, f"시도 한도 초과 ({reason})")
            else:
                await ctx.tool("job_release", job_id=job_id, worker=self.name, reason=reason)
                log("retry", f"{job_id}: {reason} → lease 반납, {self.cfg.claim_idle_ms / 1000:g}초 뒤 재배송")
        else:
            await ctx.tool("job_finish", job_id=job_id, worker=self.name)
            await self._ack(msg_id)
            log("done", f"{job_id} 완료 → XACK")

    async def _fail(self, ctx: JobContext, msg_id: str, reason: str) -> None:
        await ctx.tool("job_fail", job_id=ctx.job_id, worker=self.name, reason=reason)
        await self._ack(msg_id)
        log("fail", f"{ctx.job_id}: 실패 확정 ({reason}) → XACK")

    async def _ack(self, msg_id: str) -> None:
        await self.r.xack(self.cfg.stream, S.GROUP, msg_id)

    @staticmethod
    async def _safe(coro) -> None:
        try:
            await coro
        except Exception:
            pass
