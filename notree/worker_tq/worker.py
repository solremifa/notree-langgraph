"""taskiq 버전 worker.

    TqWorker(name, cfg, redis_url, mcp).run(stop_event)     # Worker와 같은 사용법

바뀐 것 (loop.py 대비)
- 메시지 받기·동시 실행 수·종료 대기 → taskiq Receiver (max_async_tasks, wait_tasks_timeout)
- Redis Stream 읽기·방치 메시지 회수 → taskiq-redis RedisStreamBroker (idle_timeout)
- ACK → 수동 ACK (AcknowledgeType.MANUAL + Context.ack). 완료가 확정된 뒤에만 부른다
- heartbeat의 Redis 방치 시간 초기화(XCLAIM JUSTID) → 할 수 없음. taskiq는 메시지 ID를 작업에 넘기지 않는다
그대로인 것
- Worker.process: job_start, Lease(heartbeat), 시간 제한, 시도 한도, 완료·실패 확정 뒤 ACK, 종료 시 lease 반납
- 배정기는 Redis에 job_id 대신 taskiq 형식 메시지를 넣어야 한다 → run_allocator_tq
"""
import asyncio

from redis.asyncio import Redis
from taskiq import AckableMessage, AcknowledgeType, Context, TaskiqDepends
from taskiq.kicker import AsyncKicker
from taskiq.receiver import Receiver
from taskiq_redis import RedisStreamBroker

from notree.common import settings as S
from notree.common.log import ACTOR, log
from notree.common.settings import StreamConfig
from notree.worker.loop import Worker
from notree.worker.mcp_client import McpClient

TASK = "run_job"


class ReclaimingStreamBroker(RedisStreamBroker):
    """RedisStreamBroker 보완판. 원본(taskiq-redis 1.2.4)은 XREADGROUP이 새 메시지를 돌려줬을 때만 XAUTOCLAIM을 하므로
    새 작업이 안 들어오는 동안에는 죽은 worker의 메시지를 영영 회수하지 않는다. 여기서는 loop.py처럼 매번 회수부터 한다.
    (XAUTOCLAIM은 Redis에서 원자적이라 원본의 별도 잠금도 필요 없다)"""

    async def listen(self):
        async with Redis(connection_pool=self.connection_pool) as r:
            while True:
                _, claimed, _ = await r.xautoclaim(self.queue_name, self.consumer_group_name, self.consumer_name,
                                                   min_idle_time=self.idle_timeout, start_id="0-0",
                                                   count=self.unacknowledged_batch_size)
                for msg_id, msg in claimed:
                    yield AckableMessage(data=msg[b"data"], ack=self._ack_generator(id=msg_id, queue_name=self.queue_name))
                fetched = await r.xreadgroup(self.consumer_group_name, self.consumer_name, {self.queue_name: ">"},
                                             block=self.block, count=self.count)
                for _, msgs in fetched or []:
                    for msg_id, msg in msgs:
                        yield AckableMessage(data=msg[b"data"],
                                             ack=self._ack_generator(id=msg_id, queue_name=self.queue_name))


def make_broker(cfg: StreamConfig, url: str, consumer: str | None = None, idle_ms: int | None = None,
                block_ms: int = 500, reclaim: bool = False) -> RedisStreamBroker:
    cls = ReclaimingStreamBroker if reclaim else RedisStreamBroker
    return cls(
        url=url, queue_name=cfg.stream, consumer_group_name=S.GROUP, consumer_name=consumer,
        consumer_id="0",                         # 그룹 만들기 전에 들어온 메시지도 읽는다
        idle_timeout=idle_ms if idle_ms is not None else cfg.claim_idle_ms,
        xread_block=block_ms, xread_count=1, unacknowledged_batch_size=1, unacknowledged_lock_timeout=5,
    )


class TqWorker(Worker):
    def __init__(self, name: str, cfg: StreamConfig, redis_url: str, mcp: McpClient, *,
                 idle_ms: int | None = None, block_ms: int = 500, reclaim: bool = False, **kw) -> None:
        super().__init__(name, cfg, None, mcp, **kw)
        self.broker = make_broker(cfg, redis_url, consumer=self.name, idle_ms=idle_ms, block_ms=block_ms,
                                  reclaim=reclaim)
        worker = self

        @self.broker.task(task_name=TASK)
        async def run_job(job_id: str, context: Context = TaskiqDepends()) -> None:
            t = asyncio.current_task()
            worker.running.add(t)
            try:
                await worker._guarded(context, job_id, "taskiq")   # msg_id 자리에 Context를 넘긴다 (_ack에서 씀)
            finally:
                worker.running.discard(t)

    # loop.py의 Redis 직접 호출 두 개만 바꾼다
    async def _ack(self, context: Context) -> None:
        await context.ack()

    async def _touch(self, context: Context) -> None:
        return None                                # taskiq에서는 방치 시간을 초기화할 방법이 없다

    async def run(self, stop: asyncio.Event) -> str:
        ACTOR.set(self.name)
        log("start", f"[taskiq] stream={self.cfg.stream} 동시 {self.concurrency}개 · "
                     f"재배송 {self.broker.idle_timeout / 1000:g}s · 한도 {self.cfg.max_attempts}회")
        finish = asyncio.Event()
        recv = Receiver(self.broker, max_async_tasks=self.concurrency, ack_type=AcknowledgeType.MANUAL,
                        wait_tasks_timeout=self.grace, propagate_exceptions=False)

        async def watch() -> None:
            while not stop.is_set() and not self.dead:
                await asyncio.sleep(0.05)
            if self.dead:                          # 죽은 프로세스는 정리 없이 바로 멈춘다
                for t in list(self.running):
                    t.cancel()
                log("💀", "worker 프로세스 죽음 (ACK·완료 표시·lease 반납·heartbeat 없음)")
            else:
                log("stop", f"종료 신호: 진행 중 {len(self.running)}개를 최대 {self.grace:g}초 기다림")
            finish.set()

        watcher = asyncio.create_task(watch())
        try:
            await recv.listen(finish)
        finally:
            watcher.cancel()
            left = [t for t in self.running if not t.done()]
            for t in left:                          # 유예 시간을 넘긴 작업: 취소 → process가 lease 반납
                t.cancel()
            if left:
                await asyncio.wait(left, timeout=5)
                if not self.dead:
                    log("stop", f"{len(left)}개는 끝내지 못하고 반납 → 다른 worker가 이어받음")
            await self.broker.shutdown()
        log("stop", "종료")
        return "died" if self.dead else "stopped"


async def run_allocator_tq(brokers: dict[str, RedisStreamBroker], mcp: McpClient,
                           stop: asyncio.Event | None = None, interval: float = 0.5) -> None:
    """allocator.run_allocator와 같고, XADD 대신 taskiq 형식으로 보낸다 (stream → broker)."""
    ACTOR.set("scheduler")
    stop = stop or asyncio.Event()
    kickers = {s: AsyncKicker(task_name=TASK, broker=b, labels={}) for s, b in brokers.items()}
    while not stop.is_set():
        try:
            for j in (await mcp.tool("job_reserve_pending", limit=10))["jobs"]:
                await kickers[j["stream"]].kiq(j["job_id"])
                await mcp.tool("job_mark_sent", job_id=j["job_id"])
                log("allocator", f"{j['job_id']} [{j['kind']}] 허가 → taskiq {j['stream']}")
        except Exception as e:
            log("error", f"배정 실패({type(e).__name__}: {e}) → 다음 주기에 재시도")
        try:
            await asyncio.wait_for(stop.wait(), interval)
        except asyncio.TimeoutError:
            pass
