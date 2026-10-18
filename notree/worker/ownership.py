"""Redis 소유 키 + 원자적 heartbeat (NOTREE_LIVENESS=redis). DB heartbeat 대신 쓴다.

    own = Ownership(redis, job_id, consumer, cfg, msg_id)
    if await own.held(): 건너뜀                       # 살아 있는 실행이 있음 (중복 메시지)
    st = job_start(lease_sec=0)                      # DB가 실행 세대(exec_epoch)를 발급 — 영속·단조
    if await own.acquire(st["exec_epoch"]):          # 키에 세대를 담는다. 더 큰 세대만 덮어쓸 수 있다
        async with RedisLease(ctx, own): ...
    await own.release()                              # 토큰이 같을 때만 지움

번호는 DB만 발급한다 (2026-10-18 리뷰 3차로 바꿈)
  전에는 Redis INCR로 펜싱 번호를 받아 DB에 넘겼다. 그러면 "Redis가 A에게 2를 발급 → A의 DB 시작 지연 → Redis 초기화
  → B가 1을 받아 시작 → A가 2로 도착"에서 A도 허용됐다 (초기화 전에 발급됐지만 DB가 아직 못 본 번호).
  지금은 DB의 exec_epoch가 유일한 번호다. Redis가 초기화돼도 되돌아가지 않는다.

소유권 규칙
  - 소유 키 notree:own:{job_id} = "{토큰}|{stream}|{msg_id}|e{세대}", TTL = lease_sec
  - 얻기(Lua): 키가 없거나, 키에 담긴 세대가 내 세대보다 작을 때만 쓴다 → 키는 더 큰 세대 쪽으로만 바뀐다.
    세대가 같거나 큰 키가 있으면 거절 (나는 이미 낡은 실행). DB가 세대를 발급한 뒤 키를 쓰기 전까지는 Redis 소유자와
    DB 실행자가 잠시 다를 수 있다 — 최종 쓰기는 DB의 현재 실행 세대만 허용하므로 그 사이의 쓰기는 거절된다
      · A가 세대 1을 받고 키 쓰기가 늦어짐 → B가 세대 2로 씀 → A의 쓰기는 거절
      · B가 세대 1로 쓰고 실행 중 → A의 늦은 DB 시작이 세대 2를 받음 → A가 덮어씀, B는 다음 heartbeat에서 잃고 멈춤
        (B의 쓰기는 어차피 DB가 세대로 거절) → 키를 쓴 뒤로는 Redis 소유자·DB 실행자 모두 A
  - heartbeat 한 번 = Lua 한 번. 다음을 모두 만족할 때만 원본 메시지의 방치 시간을 0으로 하고(XCLAIM JUSTID) 키를 연장한다
      1. 소유 키 값이 내 값과 같다 (만료·다른 실행에게 넘어갔으면 거절)
      2. 원본 메시지가 처리 중 목록(PEL)에 있고 소유 consumer가 나다 (다른 worker가 회수했으면 거절 — 되찾아 오지 않음)
      3. XCLAIM이 그 메시지를 실제로 돌려줬다 (Stream에서 지워진 메시지면 거절 — 키만 연장하지 않음)
  - 시간 관계: heartbeat_sec < lease_sec(키 TTL) < claim_idle(회수 기준)
  - 키 해제도 토큰 비교. Redis에 TTL보다 오래 닿지 못하면 잃은 것으로 보고 작업을 취소한다
  - 최종 쓰기는 DB의 exec_epoch(permit)로 막는다

남는 비용 (안전 문제는 아님): Redis가 초기화된 직후에는 키가 비어 있어서 낡은 세대도 키를 얻을 수 있다. 그 실행은
첫 DB 쓰기에서 거절돼 멈추고(키 해제), 진짜 실행자는 키를 잃어 멈춘다 → 회수 기준 뒤 새 세대로 다시 실행된다.
시작 확인(held)과 DB 시작 사이에 두 worker가 함께 들어오면 진 쪽도 시도 횟수를 하나 쓴다 (시도 한도를 줄이는 비용).

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
local v = redis.call('GET', KEYS[1])
if v then
  local e = tonumber(string.match(v, '|e(%d+)$')) or 0
  if e >= tonumber(ARGV[3]) then return 0 end
end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
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


class Ownership:
    def __init__(self, redis, job_id: str, consumer: str, cfg: StreamConfig, msg_id: str) -> None:
        self.r, self.job_id, self.consumer, self.cfg, self.msg_id = redis, job_id, consumer, cfg, msg_id
        self.key = own_key(job_id)
        self.token = f"{consumer}:{uuid.uuid4().hex[:8]}"
        self.value: str | None = None                  # acquire 때 세대를 담아 정해진다
        self.epoch: int | None = None
        self.ttl_ms = int(cfg.lease_sec * 1000)
        self.holder: str | None = None
        self._acquire = redis.register_script(ACQUIRE_LUA)
        self._beat = redis.register_script(HEARTBEAT_LUA)
        self._release = redis.register_script(RELEASE_LUA)

    async def held(self) -> bool:
        """살아 있는 소유 키가 있나 (중복 메시지면 DB 시작 전에 건너뛰려고). 원자적이지 않다 — 최종 판정은 acquire."""
        self.holder = await self.r.get(self.key)
        return self.holder is not None

    async def acquire(self, epoch: int) -> bool:
        """DB가 발급한 세대를 담아 키를 쓴다. 키가 없거나 담긴 세대가 더 작을 때만."""
        value = f"{self.token}|{self.cfg.stream}|{self.msg_id}|e{epoch}"
        if int(await self._acquire(keys=[self.key], args=[value, self.ttl_ms, epoch])):
            self.value, self.epoch = value, epoch
            return True
        self.holder = await self.r.get(self.key)
        return False

    async def heartbeat(self) -> int:
        """1 = 갱신, 0·-1·-2 = 소유권 없음 (BEAT_RESULT)."""
        if self.value is None:
            return 0
        return int(await self._beat(keys=[self.key, self.cfg.stream],
                                    args=[self.value, S.GROUP, self.consumer, self.msg_id, self.ttl_ms]))

    async def release(self) -> bool:
        if self.value is None:
            return False
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
