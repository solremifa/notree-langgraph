"""Redis 소유 키 + 원자적 heartbeat (NOTREE_LIVENESS=redis). DB heartbeat 대신 쓴다.

    own = Ownership(redis, job_id, consumer, cfg, msg_id)
    if await own.acquire():           # SET NX PX — 같은 작업을 동시에 두 곳에서 시작하지 않게
        async with RedisLease(ctx, own): ...
    await own.release()               # 토큰이 같을 때만 지움

소유권 규칙 (하나)
  - 소유 키 notree:own:{job_id} = "{토큰}|{stream}|{msg_id}", TTL = lease_sec
  - heartbeat 한 번 = Lua 한 번. 다음을 모두 만족할 때만 원본 메시지의 방치 시간을 0으로 하고(XCLAIM JUSTID) 키를 연장한다
      1. 소유 키 값이 내 토큰과 같다 (만료됐거나 다른 worker에게 넘어갔으면 거절)
      2. 원본 메시지가 처리 중 목록(PEL)에 있고 소유 consumer가 나다 (다른 worker가 회수했으면 거절 — 되찾아 오지 않음)
      3. XCLAIM이 그 메시지를 실제로 돌려줬다 (Stream에서 지워진 메시지면 거절 — 키만 연장하지 않음)
  - 시간 관계: heartbeat_sec < lease_sec(키 TTL) < claim_idle(회수 기준). settings의 기존 규칙 그대로
    → 방치 시간이 회수 기준을 넘었다면 마지막 heartbeat가 TTL보다 오래전이라 키도 이미 만료됐다
  - 키 해제도 토큰 비교 (만료 뒤 다른 worker가 잡은 키를 이전 worker가 지우지 못하게)
  - Redis에 닿지 못하는 시간이 TTL을 넘으면 소유권을 장담할 수 없으므로 잃은 것으로 보고 작업을 취소한다
  - 최종 쓰기는 이 키가 아니라 DB의 exec_epoch(permit)로 막는다. 키는 '동시에 두 번 시작하지 않기'와 '살아 있는 작업 재배송 막기'용
  - 펜싱 번호: 키를 얻을 때 notree:fence:{job_id}를 INCR해 받는다 (SET NX와 같은 Lua). job_start(fence=)로 DB에 넘기면
    DB는 지금까지 본 가장 큰 번호 이하를 stale_fence로 거절한다 → "A가 키를 얻고 DB 시작이 늦어지는 사이 키가 만료되고
    B가 키·DB 시작을 마친 뒤 A의 늦은 시작이 도착"해도 A가 실행자가 되지 못한다.
    Redis가 초기화되면 카운터가 1부터 다시 시작하므로, stale_fence가 알려 준 값까지 카운터를 올리고 다시 얻는다 (raise_fence).

한 Redis 인스턴스 전제 (소유 키와 Stream이 다른 슬롯이라 Redis Cluster에서는 이 Lua를 그대로 못 쓴다).
"""
import asyncio
import time
import uuid

from notree.common import settings as S
from notree.common.log import log
from notree.common.settings import StreamConfig
from notree.worker.lease import Lease

HEARTBEAT_LUA = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
local p = redis.call('XPENDING', KEYS[2], ARGV[2], ARGV[4], ARGV[4], 1)
if #p == 0 or p[1][2] ~= ARGV[3] then return -1 end
local c = redis.call('XCLAIM', KEYS[2], ARGV[2], ARGV[3], 0, ARGV[4], 'JUSTID')
if #c == 0 then return -2 end
redis.call('PEXPIRE', KEYS[1], ARGV[5])
return 1
"""
ACQUIRE_LUA = """
if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2]) then
  local f = redis.call('INCR', KEYS[2])
  redis.call('PEXPIRE', KEYS[2], ARGV[3])
  return f
end
return 0
"""
FENCE_KEEP_MS = 24 * 3600 * 1000     # 펜싱 카운터 보관 (작업마다 키 하나). 만료돼 작아지면 stale_fence → raise_fence로 복원
RAISE_FENCE_LUA = """
if tonumber(redis.call('GET', KEYS[1]) or '0') < tonumber(ARGV[1]) then redis.call('SET', KEYS[1], ARGV[1]) end
return 1
"""
RELEASE_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0
"""
BEAT_RESULT = {1: "갱신", 0: "소유 키가 내 것이 아님(만료·다른 worker)", -1: "메시지를 다른 worker가 회수함",
               -2: "원본 메시지가 Stream에 없음"}


def own_key(job_id: str) -> str:
    return f"notree:own:{job_id}"


def fence_key(job_id: str) -> str:
    return f"notree:fence:{job_id}"


class Ownership:
    def __init__(self, redis, job_id: str, consumer: str, cfg: StreamConfig, msg_id: str) -> None:
        self.r, self.job_id, self.consumer, self.cfg, self.msg_id = redis, job_id, consumer, cfg, msg_id
        self.key = own_key(job_id)
        self.value = f"{consumer}:{uuid.uuid4().hex[:8]}|{cfg.stream}|{msg_id}"
        self.ttl_ms = int(cfg.lease_sec * 1000)
        self.holder: str | None = None
        self.fence: int | None = None
        self._acquire = redis.register_script(ACQUIRE_LUA)
        self._raise = redis.register_script(RAISE_FENCE_LUA)
        self._beat = redis.register_script(HEARTBEAT_LUA)
        self._release = redis.register_script(RELEASE_LUA)

    async def acquire(self) -> bool:
        f = int(await self._acquire(keys=[self.key, fence_key(self.job_id)], args=[self.value, self.ttl_ms, FENCE_KEEP_MS]))
        if f:
            self.fence = f
            return True
        self.holder = await self.r.get(self.key)          # 기록용 (원자적이지 않아도 됨)
        return False

    async def raise_fence(self, at_least: int) -> None:
        """Redis 초기화 등으로 카운터가 DB보다 작아졌을 때 DB가 본 값까지 올린다 (내리지는 않음)."""
        await self._raise(keys=[fence_key(self.job_id)], args=[at_least])

    async def heartbeat(self) -> int:
        """1 = 갱신, 0·-1·-2 = 소유권 없음 (BEAT_RESULT)."""
        return int(await self._beat(keys=[self.key, self.cfg.stream],
                                    args=[self.value, S.GROUP, self.consumer, self.msg_id, self.ttl_ms]))

    async def release(self) -> bool:
        return bool(await self._release(keys=[self.key], args=[self.value]))


class RedisLease(Lease):
    """Lease와 같은 모양(실행권을 잃으면 작업을 취소하고 LeaseLost). 갱신은 Redis Lua 한 번, DB는 부르지 않는다."""

    def __init__(self, ctx, own: Ownership) -> None:
        super().__init__(ctx)
        self.own = own

    async def _beat(self) -> None:
        c, last_ok = self.ctx, time.monotonic()
        while True:
            await asyncio.sleep(c.cfg.heartbeat_sec)
            try:
                res = await self.own.heartbeat()
            except Exception as e:                        # Redis 일시 장애: TTL 안이면 다음 박동에 다시
                if time.monotonic() - last_ok < c.cfg.lease_sec:
                    log("lease", f"heartbeat 실패({type(e).__name__}) → 다음 주기에 재시도")
                    continue
                res, why = 0, f"Redis에 {c.cfg.lease_sec:g}초 넘게 닿지 못함"
            else:
                why = BEAT_RESULT.get(res, str(res))
            if res == 1:
                last_ok = time.monotonic()
                continue
            self.lost = True
            log("lease", f"소유권을 잃음({why}) → 작업 취소")
            self._owner.cancel()
            return
