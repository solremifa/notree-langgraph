"""worker 루프: Redis에서 작업 ID를 받아 핸들러에 넘기고, 결과에 따라 ACK한다.

    Worker(name, cfg, redis, mcp).run(stop_event)

한 메시지의 처리 (process)
  이미 이 worker가 실행 중인 job → 손대지 않음 (보조 방어)
  [LIVENESS=redis] 소유 키 SET NX 실패 → 손대지 않음 (다른 실행이 살아 있음. ACK도 안 함 — 지금의 보수적 규칙)
                   성공하면 job_start(lease_sec=0)로 실행 세대만 올리고, heartbeat는 Redis Lua 한 번 (ownership.py)
                   끝나면(죽은 경우 제외) 소유 키를 토큰 비교로 해제
  job_start ─┬─ done / failed / unknown → XACK (이미 끝난 작업)   ※ 결과는 data.status
             ├─ leased                  → 손대지 않음 (살아 있는 lease. 같은 worker라도)
             └─ started → Lease 안에서 핸들러 실행 (job_timeout과 마감 시각 중 짧은 쪽으로 제한)
                      │   heartbeat마다 Redis 방치 시간도 초기화 → 실행 중인 메시지는 재배송되지 않음
                      ├─ 성공            → job_finish가 finished=true로 **확정되면** XACK
                      ├─ FatalError      → job_fail(다시 시도 불가)가 확정되면 XACK
                      ├─ 마감 시각 지남   → job_fail(다시 시도 가능)가 확정되면 XACK
                      ├─ 그 밖의 실패     → 시도 < 한도: job_release (ACK 안 함 → claim_idle 뒤 재배송)
                      │                    시도 = 한도: job_fail(다시 시도 가능)가 확정되면 XACK
                      ├─ LeaseLost       → 아무것도 안 함 (이미 다른 worker 것)
                      └─ 종료 신호로 취소 → job_release (ACK 안 함 → 다른 worker가 곧 이어받음)
  완료·실패 확정을 못 받으면(장애·실행권 없음) ACK하지 않는다 → 메시지가 남아 재배송으로 다시 처리된다.

빌려 쓰기 (borrow, borrow_slots)
  자기 Stream에 회수할 것도 새것도 없을 때만 borrow Stream의 메시지를 가져온다.
  빌린 작업은 동시에 borrow_slots개까지만 → 나머지 슬롯은 언제나 자기 레인 몫으로 남는다.
  빌린 작업의 lease·재배송·시도 한도·ACK는 원래 레인(borrow)의 설정을 따른다.
"""
import asyncio

from redis.exceptions import ResponseError

from notree.common import settings as S
from notree.common.log import ACTOR, log
from notree.common.settings import StreamConfig
from notree.worker.lease import Lease, LeaseLost
from notree.worker.mcp_client import McpClient
from notree.worker.ownership import Ownership, RedisLease
from notree.worker.registry import FatalError, JobContext, SimulatedDeath, load_handlers


CONFIRM_BACKOFF = (0.2, 0.5, 1.0)   # 완료·실패 표시 재시도 간격 (마지막 값은 쓰이지 않음)
BORROW_POLL_MS = 200                # 빌려 쓸 수 있을 때 자기 Stream을 기다리는 최대 시간 (그 뒤 빌릴 것을 다시 본다)


class DeadlinePassed(Exception):
    """payload의 deadline_at이 이미 지남 → 실행하지 않고 실패 확정 (사용자는 다시 시도 가능)."""


class Worker:
    def __init__(self, name: str, cfg: StreamConfig, redis, mcp: McpClient, *,
                 concurrency: int | None = None, block_ms: int = 1000, extras: dict | None = None,
                 shutdown_grace_sec: float | None = None,
                 borrow: StreamConfig | None = None, borrow_slots: int = 0) -> None:
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
        self.active_jobs: set[str] = set()        # 이 프로세스가 지금 실행 중인 job_id (중복 실행 보조 방어)
        self.dead = False                         # SimulatedDeath (시험·데모용 프로세스 죽음)
        self.borrow = borrow if borrow_slots > 0 else None
        self.borrow_slots = min(borrow_slots, self.concurrency - 1) if self.borrow else 0   # 자기 몫 최소 1개
        self.borrowed = 0                         # 지금 실행 중인 빌린 작업 수

    # ── 메인 루프 ──
    async def run(self, stop: asyncio.Event) -> str:
        """정상 종료면 "stopped", SimulatedDeath면 "died"."""
        ACTOR.set(self.name)
        await self._ensure_group()
        log("start", f"stream={self.cfg.stream} 동시 {self.concurrency}개 · lease {self.cfg.lease_sec:g}s · "
                     f"재배송 {self.cfg.claim_idle_ms / 1000:g}s · 기본 한도 {self.cfg.max_attempts}회(종류별 설정 우선) · "
                     f"핸들러 {sorted(self.handlers)}"
                     + (f" · {self.borrow.stream}에서 최대 {self.borrow_slots}개 빌려 씀" if self.borrow else ""))
        slots = asyncio.Semaphore(self.concurrency)
        while not stop.is_set() and not self.dead:
            if not await self._acquire_or_stop(slots, stop):   # 빈 자리가 나거나 종료 신호가 오거나
                break
            if self.dead:
                slots.release()
                break
            try:
                msg = None if stop.is_set() else await self._next()
            except Exception as e:                        # Redis·네트워크 일시 장애: 루프는 죽지 않는다
                slots.release()
                log("error", f"메시지 가져오기 실패({type(e).__name__}: {e}) → 1초 뒤 재시도")
                await asyncio.sleep(1)
                continue
            if msg is not None and self.dead:             # 가져온 사이에 죽었으면 손대지 않는다 (재배송됨)
                slots.release()
                break
            if msg is None:
                slots.release()
                if not self.block_ms:
                    await asyncio.sleep(0.2)
                continue
            borrowed = msg[3] is not self.cfg
            if borrowed:
                self.borrowed += 1
            t = asyncio.create_task(self._guarded(*msg))
            self.running.add(t)
            t.add_done_callback(lambda t, b=borrowed: self._done(t, slots, b))
        if self.dead:
            for t in self.running:                        # 죽은 프로세스는 아무것도 정리하지 않는다
                t.cancel()
            log("💀", "worker 프로세스 죽음 (ACK·완료 표시·lease 반납·heartbeat 없음)")
            return "died"
        await self._drain()
        return "stopped"

    def _done(self, t: asyncio.Task, slots: asyncio.Semaphore, borrowed: bool) -> None:
        self.running.discard(t)
        if borrowed:
            self.borrowed -= 1
        slots.release()

    @staticmethod
    async def _acquire_or_stop(slots: asyncio.Semaphore, stop: asyncio.Event) -> bool:
        """슬롯이 비면 True, 그 전에 종료 신호가 오면 False. 슬롯이 꽉 찬 채로 종료 신호를 못 보는 일을 막는다."""
        if stop.is_set():
            return False
        acq = asyncio.ensure_future(slots.acquire())
        stp = asyncio.ensure_future(stop.wait())
        await asyncio.wait({acq, stp}, return_when=asyncio.FIRST_COMPLETED)
        stp.cancel()
        if acq.done() and not acq.cancelled():
            if stop.is_set():                              # 동시에 끝났으면 종료가 우선. 받은 슬롯은 돌려준다
                slots.release()
                return False
            return True
        acq.cancel()
        return False

    async def _ensure_group(self) -> None:
        for cfg in filter(None, (self.cfg, self.borrow)):
            try:
                await self.r.xgroup_create(cfg.stream, S.GROUP, id="0", mkstream=True)
            except ResponseError as e:
                if "BUSYGROUP" not in str(e):
                    raise

    async def _next(self):
        """(msg_id, job_id, how, cfg) 또는 None. 방치된 메시지 회수가 새 메시지보다, 자기 레인이 빌린 레인보다 먼저다."""
        if m := await self._claim(self.cfg):
            return m
        if not (self.borrow and self.borrowed < self.borrow_slots):
            return await self._read(self.cfg, self.block_ms or None)
        if m := await self._read(self.cfg, None):
            return m
        if m := await self._claim(self.borrow) or await self._read(self.borrow, None):
            return m
        return await self._read(self.cfg, min(self.block_ms, BORROW_POLL_MS)) if self.block_ms else None

    def _how(self, cfg: StreamConfig, how: str) -> str:
        return how if cfg is self.cfg else f"빌림·{how}"

    async def _claim(self, cfg: StreamConfig):
        _, claimed, _ = await self.r.xautoclaim(cfg.stream, S.GROUP, self.name,
                                                min_idle_time=cfg.claim_idle_ms, start_id="0-0", count=1)
        if not claimed:
            return None
        msg_id, fields = claimed[0]
        return msg_id, fields["job_id"], self._how(cfg, "재배송"), cfg

    async def _read(self, cfg: StreamConfig, block: int | None):
        res = await self.r.xreadgroup(S.GROUP, self.name, {cfg.stream: ">"}, count=1, block=block)
        if not res:
            return None
        msg_id, fields = res[0][1][0]
        return msg_id, fields["job_id"], self._how(cfg, "신규"), cfg

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
    async def _guarded(self, msg_id: str, job_id: str, how: str, cfg: StreamConfig) -> None:
        """process 바깥의 마지막 안전망. 여기까지 온 오류는 ACK 없이 두므로 재배송된다."""
        if job_id in self.active_jobs:
            log("skip", f"{job_id}: 이 worker가 이미 실행 중 → 손대지 않음")
            return
        self.active_jobs.add(job_id)
        try:
            await self.process(msg_id, job_id, how, cfg)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log("error", f"{job_id}: 처리 중 예외({type(e).__name__}: {e}) → ACK 안 함, 재배송 대기")
        finally:
            self.active_jobs.discard(job_id)

    async def process(self, msg_id: str, job_id: str, how: str, cfg: StreamConfig | None = None) -> None:
        ACTOR.set(self.name)
        cfg = cfg or self.cfg                     # 이 메시지가 온 레인의 설정 (빌린 작업이면 빌려 온 레인)
        if S.LIVENESS != "redis":
            return await self._process(msg_id, job_id, how, cfg, None)
        own = Ownership(self.r, job_id, self.name, cfg, msg_id)
        if not await own.acquire():
            log("skip", f"{job_id}: 소유 키를 {(own.holder or '?').split('|')[0]}가 가짐 → 손대지 않음")
            return
        try:
            await self._process(msg_id, job_id, how, cfg, own)
        finally:
            if not self.dead:                     # 죽은 프로세스는 정리하지 않는다 (키는 TTL로 풀림)
                try:
                    await own.release()
                except Exception as e:
                    log("error", f"{job_id}: 소유 키 해제 실패({type(e).__name__}) → TTL로 풀림")

    async def _process(self, msg_id: str, job_id: str, how: str, cfg: StreamConfig, own: Ownership | None) -> None:
        st = await self.mcp.tool("job_start", job_id=job_id, worker=self.name,
                                 lease_sec=0 if own else cfg.lease_sec)
        if st["status"] in ("done", "failed", "unknown"):
            await self._ack(msg_id, cfg)
            log("skip", f"{job_id}: 이미 {st['status']} → ACK만")
            return
        if st["status"] == "leased":
            log("skip", f"{job_id}: {st['owner']}가 lease 보유({st['lease_left_sec']}초 남음) → 손대지 않음")
            return

        kind = st["kind"]
        ctx = JobContext(job_id=job_id, kind=kind, payload=st["payload"], attempt=st["attempt"],
                         permit=st["permit"], worker=self.name, cfg=cfg, mcp=self.mcp,
                         max_attempts=S.max_attempts_for(kind, cfg),
                         deadline_at=st["payload"].get("deadline_at"), extras=self.extras)
        fn = self.handlers.get(kind)
        left = ctx.time_left()
        log("job", f"{how} {job_id} [{kind}] 시도 {ctx.attempt}/{ctx.max_attempts}"
                   + (f" · 마감까지 {left:.0f}초" if left is not None else ""))
        try:
            if fn is None:
                raise FatalError(f"핸들러 없음: {kind}")
            timeout = S.job_timeout_for(kind, cfg)
            if left is not None:
                if left <= 0:
                    raise DeadlinePassed()
                timeout = min(timeout, left)
            lease = RedisLease(ctx, own) if own else Lease(ctx, touch=lambda: self._touch(msg_id, cfg))
            async with lease:
                await asyncio.wait_for(fn(ctx), timeout=timeout)
        except LeaseLost:
            log("job", f"{job_id}: lease를 잃어 손을 뗌 (ACK 안 함)")
        except SimulatedDeath as e:
            self.dead = True
            log("💀", f"{job_id}: {e}")
        except asyncio.CancelledError:
            if not self.dead:
                await self._ctl(ctx, "job_release", reason="shutdown")
                log("job", f"{job_id}: 종료로 중단 → lease 반납")
            raise
        except DeadlinePassed:
            await self._fail(ctx, msg_id, "마감 시각 지남", user_retryable=True)
        except FatalError as e:
            await self._fail(ctx, msg_id, f"fatal: {e}", user_retryable=False)
        except Exception as e:  # RetryableError, TimeoutError, 예상 못 한 오류
            reason = "timeout" if isinstance(e, TimeoutError) else f"{type(e).__name__}: {e}"
            left = ctx.time_left()
            if left is not None and left <= 0:
                await self._fail(ctx, msg_id, f"마감 시각 초과 ({reason})", user_retryable=True)
            elif ctx.attempt >= ctx.max_attempts:
                await self._fail(ctx, msg_id, f"시도 한도 초과 ({reason})", user_retryable=True)
            else:
                await self._ctl(ctx, "job_release", reason=reason)
                log("retry", f"{job_id}: {reason} → lease 반납, {cfg.claim_idle_ms / 1000:g}초 뒤 재배송")
        else:
            confirmed = await self._confirm(ctx, "job_finish", "finished")
            if confirmed:
                await self._ack(msg_id, cfg)
                log("done", f"{job_id} 완료 → XACK")
            else:
                log("done", f"{job_id}: 완료 확정 못 받음({self._why(confirmed)}) → ACK 안 함, 재배송으로 다시 처리")

    async def _fail(self, ctx: JobContext, msg_id: str, reason: str, *, user_retryable: bool) -> None:
        confirmed = await self._confirm(ctx, "job_fail", "failed", reason=reason, user_retryable=user_retryable)
        if confirmed:
            await self._ack(msg_id, ctx.cfg)
            log("fail", f"{ctx.job_id}: 실패 확정 ({reason}){' · 다시 시도 가능' if user_retryable else ''} → XACK")
        else:
            log("fail", f"{ctx.job_id}: 실패 확정 못 받음({self._why(confirmed)}) → ACK 안 함, 재배송으로 다시 처리")

    async def _confirm(self, ctx: JobContext, name: str, flag: str, **args) -> bool | None:
        """완료·실패 표시를 확정 응답까지 몇 번 시도한다.
        True = 확정됨(ACK 해도 됨), False = 서버가 거부(실행권 없음), None = 끝내 응답을 못 받음."""
        for i, wait in enumerate(CONFIRM_BACKOFF):
            try:
                res = await self.mcp.tool(name, permit=ctx.permit, job_id=ctx.job_id, worker=self.name, **args)
                return bool(res.get(flag))
            except Exception as e:
                log("error", f"{ctx.job_id}: {name} 실패({type(e).__name__}: {e})"
                             + (f" → {wait:g}초 뒤 다시" if i < len(CONFIRM_BACKOFF) - 1 else ""))
                if i < len(CONFIRM_BACKOFF) - 1:
                    await asyncio.sleep(wait)
        return None

    @staticmethod
    def _why(confirmed: bool | None) -> str:
        return "실행권 없음" if confirmed is False else "응답 없음"

    async def _ctl(self, ctx: JobContext, name: str, **args) -> None:
        """결과가 ACK에 영향을 주지 않는 제어 호출(job_release). 실패해도 루프는 멈추지 않는다."""
        try:
            await self.mcp.tool(name, permit=ctx.permit, job_id=ctx.job_id, worker=self.name, **args)
        except Exception as e:
            log("error", f"{ctx.job_id}: {name} 실패({type(e).__name__}: {e})")

    async def _touch(self, msg_id: str, cfg: StreamConfig) -> None:
        """Redis 메시지의 방치 시간을 0으로 (소유자는 그대로 나). 살아 있는 작업이 재배송되지 않게 한다."""
        await self.r.xclaim(cfg.stream, S.GROUP, self.name, 0, [msg_id], justid=True)

    async def _ack(self, msg_id: str, cfg: StreamConfig) -> None:
        await self.r.xack(cfg.stream, S.GROUP, msg_id)

