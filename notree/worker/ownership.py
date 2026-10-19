"""Redis 소유 키 + 원자적 heartbeat (NOTREE_LIVENESS=redis, 기본). DB heartbeat 대신 쓴다.

    own = Ownership(redis, job_id, consumer, cfg, msg_id)
    if not await own.claim(): 건너뜀                  # 다른 실행(또는 시작 중인 선점)이 있음 — DB를 부르지 않는다
    async with RedisLease(cfg, own):                 # 시작 중에도 키를 연장 (DB 응답이 느려도 남이 선점하지 못하게)
        st = job_start(start_id=own.token, expect_epoch=E, lease_sec=0)   # DB가 조건부로 승인 (세대·시도는 승인 때만)
    async with RedisLease(cfg, own): 핸들러
    await own.release()                              # 값이 같을 때만 지움

시작 절차 (2026-10-19, docs/changes/2026-10-19-start-protocol.md)
  1. 선점: SET key value NX PX ttl. 값의 토큰은 선점마다 새로 → "내 값이 지금 있다" = 선점한 뒤로 끊김 없이 내 것
  2. 승인: DB가 exec_epoch == E일 때만 세대 +1·시도 +1. E는 0이거나, 키를 가진 채로 DB에게 들은 값(들은 뒤 키 확인)
     → 키를 가진(살아 있는) 실행은 누구도 E로 지목할 수 없다 → 살아 있는 실행을 쫓아내는 승인은 없다
  3. 같은 start_id(=토큰)로 다시 보내면 같은 승인 (응답 유실)
  예전(리뷰 3차) 절차는 DB 시작 → 세대를 담아 키 쓰기(더 큰 세대만 덮어씀)였다. 키를 쓰기 전의 중복 시작이 둘 다 시도를 써서
  총 1회인 실시간 요약이 한 번도 돌지 않고 실패했다. 번호는 지금도 DB의 exec_epoch만 발급한다 (Redis 초기화로 되돌아가지 않음).

소유권 규칙
  - 소유 키 notree:own:{job_id} = "{토큰}|{stream}|{msg_id}", TTL = lease_sec
  - heartbeat 한 번 = Lua 한 번. 다음을 모두 만족할 때만 원본 메시지의 방치 시간을 0으로 하고(XCLAIM JUSTID) 키를 연장한다
      1. 소유 키 값이 내 값과 같다 (만료·다른 실행에게 넘어갔으면 거절)
      2. 원본 메시지가 처리 중 목록(PEL)에 있고 소유 consumer가 나다 (다른 worker가 회수했으면 거절 — 되찾아 오지 않음)
      3. XCLAIM이 그 메시지를 실제로 돌려줬다 (Stream에서 지워진 메시지면 거절 — 키만 연장하지 않음)
  - 시간 관계: heartbeat_sec < lease_sec(키 TTL) < claim_idle(회수 기준)
  - 키 해제도 값 비교. Redis에 TTL보다 오래 닿지 못하면 잃은 것으로 보고 작업을 취소한다
    (오류뿐 아니라 응답이 멈춘 경우도: heartbeat 한 번은 남은 TTL까지만 기다린다)
  - 최종 쓰기는 DB의 exec_epoch(permit)로 막는다

남는 비용 (안전 문제는 아님): Redis가 초기화·재시작되면 키가 사라져 진짜 실행자도 heartbeat에서 잃고 멈춘다 → 복구가 다시 보낸
메시지로 새 세대가 이어서 실행한다 (재실행). 승인이 확정된 뒤 죽거나 키를 잃은 실행은 시도 1회로 센다 (정책, 변경 문서 참고).

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
        self.token = f"{consumer}:{uuid.uuid4().hex}"   # 선점마다 새로. job_start의 start_id로도 쓴다
        self.value: str | None = None                # 선점에 성공하면 정해진다
        self.ttl_ms = int(cfg.lease_sec * 1000)
        self.holder: str | None = None               # 선점에 실패했을 때 그 키의 값 (기록용)
        self.last_ok: float | None = None            # 키가 내 것임을 마지막으로 확인한 때 (monotonic, 요청을 보내기 전 시각)
        self._beat = redis.register_script(HEARTBEAT_LUA)
        self._release = redis.register_script(RELEASE_LUA)

    async def claim(self) -> bool:
        """소유 키 선점 (SET NX PX). 이미 있으면 False — 다른 실행이 있거나 다른 worker가 시작 중이다."""
        value, t0 = f"{self.token}|{self.cfg.stream}|{self.msg_id}", time.monotonic()
        if await self.r.set(self.key, value, nx=True, px=self.ttl_ms):
            self.value, self.last_ok = value, t0
            return True
        self.holder = await self.r.get(self.key)
        return False

    async def heartbeat(self) -> int:
        """1 = 갱신, 0·-1·-2 = 소유권 없음 (BEAT_RESULT)."""
        if self.value is None:
            return 0
        t0 = time.monotonic()
        res = int(await self._beat(keys=[self.key, self.cfg.stream],
                                   args=[self.value, S.GROUP, self.consumer, self.msg_id, self.ttl_ms]))
        if res == 1:
            self.last_ok = t0
        return res

    async def release(self) -> bool:
        if self.value is None:
            return False
        return bool(await self._release(keys=[self.key], args=[self.value]))


class RedisLease(Lease):
    """Lease와 같은 모양(실행권을 잃으면 작업을 취소하고 LeaseLost). 갱신은 Redis Lua 한 번, DB는 부르지 않는다.
    시작 단계(job_start를 기다리는 동안)와 실행 단계에 각각 쓴다. 남은 TTL은 own.last_ok(선점·마지막 갱신)부터 잰다."""

    def __init__(self, cfg: StreamConfig, own: Ownership) -> None:
        super().__init__(None)
        self.cfg = cfg
        self.own = own

    async def _beat(self) -> None:
        c = self.cfg
        if getattr(self.own, "last_ok", None) is None:
            self.own.last_ok = time.monotonic()
        while True:
            await asyncio.sleep(c.heartbeat_sec)
            last_ok, t0 = self.own.last_ok, time.monotonic()
            left = c.lease_sec - (t0 - last_ok)
            try:                                          # 응답이 멈춰도 기다리는 건 키의 남은 TTL까지 (리뷰 6차 ⑤)
                res = await asyncio.wait_for(self.own.heartbeat(), timeout=max(left, 0.01))
            except Exception as e:                        # Redis 일시 장애·무응답: TTL 안이면 다음 박동에 다시
                if time.monotonic() - last_ok < c.lease_sec:
                    log("lease", f"heartbeat 실패({type(e).__name__}) → 다음 주기에 재시도")
                    continue
                res, why = 0, f"Redis에 {c.lease_sec:g}초 넘게 닿지 못함"
            else:
                why = BEAT_RESULT.get(res, str(res))
            if res == 1:
                self.own.last_ok = t0                     # 요청을 보내기 전 시각부터 TTL을 잰다 (보수적)
                continue
            self.lost = True
            log("lease", f"소유권을 잃음({why}) → 작업 취소")
            self._owner.cancel()
            return
