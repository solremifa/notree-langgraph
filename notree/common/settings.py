"""설정. 모든 값은 환경변수로 덮어쓸 수 있다.

값은 팀 결정(docs/team-decisions.md, 2026-10-15 확정)을 따른다.
규칙: heartbeat < lease < 재배송 기준(claim_idle).  실행 제한(job_timeout)은 사용자 약속 시간 이하.
"""
import os
from dataclasses import dataclass


def _f(name: str, default: float) -> float:
    return float(os.getenv(name, default))


REDIS_URL = os.getenv("NOTREE_REDIS_URL", "redis://localhost:6379/0")
MCP_URL = os.getenv("NOTREE_MCP_URL", "http://127.0.0.1:8765/mcp")
GROUP = os.getenv("NOTREE_GROUP", "workers")          # Redis consumer group 이름 (worker 종류별 Stream 공통)
SHUTDOWN_GRACE_SEC = _f("NOTREE_SHUTDOWN_GRACE_SEC", 30)  # 종료 신호 후 하던 작업을 기다려 주는 시간
# 전달 세대(gen)가 없는 메시지 허용 여부. 운영 worker는 기본 거절 (시작 안 하고 ACK → 재전송이 세대를 붙여 다시 보냄).
# 세대 도입 전 메시지가 남은 전환 기간이나, 가짜 MCP로 도는 개발 도구(lane_bench)에서만 1로 켠다.
ALLOW_LEGACY_MESSAGES = os.getenv("NOTREE_ALLOW_LEGACY_MESSAGES", "0") == "1"


@dataclass(frozen=True)
class StreamConfig:
    key: str               # "qa" | "rt" | "batch"
    stream: str            # Redis Stream 이름
    lease_sec: float       # 이 시간 동안 heartbeat가 없으면 '죽었다'고 본다
    heartbeat_sec: float   # lease 연장 주기
    claim_idle_ms: int     # ACK 없이 이만큼 방치된 메시지는 다른 worker가 회수
    max_attempts: int      # 이 횟수만큼 실패하면 '실패 확정'
    job_timeout_sec: float # 작업 하나의 최대 실행 시간
    concurrency: int       # 프로세스 하나가 동시에 돌리는 작업 수


def _cfg(key: str, stream: str, lease: float, claim_idle: float, attempts: int, timeout: float, conc: int):
    P = f"NOTREE_{key.upper()}_"
    lease = _f(P + "LEASE_SEC", lease)
    return StreamConfig(
        key=key, stream=os.getenv(P + "STREAM", stream),
        lease_sec=lease, heartbeat_sec=_f(P + "HEARTBEAT_SEC", lease / 3),
        claim_idle_ms=int(_f(P + "CLAIM_IDLE_SEC", claim_idle) * 1000),
        max_attempts=int(_f(P + "MAX_ATTEMPTS", attempts)),
        job_timeout_sec=_f(P + "JOB_TIMEOUT_SEC", timeout),
        concurrency=int(_f(P + "CONCURRENCY", conc)),
    )


# 레인 = Stream + 그 Stream만 읽는 worker 슬롯. 레인끼리는 서로의 대기열에 막히지 않는다.
#   qa    : 사용자가 답을 기다리는 작업 (Q&A 90초 약속·채점). 실시간 요약에 밀리지 않게 따로 둔다
#   rt    : 실시간 요약 (녹음 중 계속 생김. 늦어도 다음 요약이 덮음)
#   batch : 길어도 되는 작업 (최종 노트·과목 노트)
#                key      stream          lease  claim  시도  제한   동시
QA = _cfg("qa", "notree:qa", 15, 20, 2, 90, 4)
RT = _cfg("rt", "notree:rt", 15, 20, 2, 90, 4)
BATCH = _cfg("batch", "notree:batch", 60, 90, 3, 900, 1)
STREAMS = {"qa": QA, "rt": RT, "batch": BATCH}

# 빌려 쓰기: 레인 → (빌려 올 레인, 빌려 쓸 수 있는 최대 슬롯 수).
# 자기 대기열이 비었을 때만 남의 작업을 가져오고, 빌려 쓰는 슬롯은 이 수를 넘지 않는다.
# → 나머지 (동시 수 - 빌린 수)개는 언제나 자기 레인 몫으로 비어 있다. 0이면 고정 (기본).
BORROW: dict[str, tuple[str, int]] = {
    "qa": ("rt", int(_f("NOTREE_QA_BORROW_SLOTS", 0))),
}

# 배정기: 레인별로 따로 고른다 (한 레인이 밀려도 다른 레인의 의도가 그 뒤에 줄 서지 않게).
# 한 바퀴에 레인마다 최대 몫만큼, 앞 레인부터. 어느 레인이든 몫을 다 채웠으면 쉬지 않고 다음 바퀴.
# 한 주기에 ALLOC_MAX_PER_TICK개를 넘기면 쉬었다 간다 (MCP·DB를 한 번에 몰아치지 않게).
ALLOC_LANES: tuple[tuple[str, int], ...] = (           # (레인, 한 바퀴 몫). 순서 = 우선순위
    ("qa", int(_f("NOTREE_ALLOC_QA", 10))),
    ("rt", int(_f("NOTREE_ALLOC_RT", 10))),
    ("batch", int(_f("NOTREE_ALLOC_BATCH", 5))),
)
ALLOC_INTERVAL_SEC = _f("NOTREE_ALLOC_INTERVAL_SEC", 0.5)
ALLOC_MAX_PER_TICK = int(_f("NOTREE_ALLOC_MAX_PER_TICK", 200))

# 작업 종류별 실행 정책 (팀 결정 2·3번, 2026-10-17 확정). 시도 수는 모두 "최초 포함 총 N회".
#   qa·grading     : 요청부터 전체 90초 안에서 최대 3회 (마감 = 요청 시각 + 90초, 시도당 제한 없이 남은 시간까지)
#   final_note     : 시도당 5분, 총 3회
#   course_update  : 시도당 10분, 총 3회
#   live_summary   : 1회 (곧 다음 요약이 오므로 재시도 가치가 낮다)
# 없으면 Stream 기본값 (rt 2회·90초, batch 3회·15분).
KIND_MAX_ATTEMPTS: dict[str, int] = {
    "qa": 3, "grading": 3,
    "final_note": 3, "course_update": 3,
    "live_summary": 1,
}
KIND_JOB_TIMEOUT_SEC: dict[str, float] = {      # 시도당 제한. Stream 제한보다 길게는 못 늘린다 (작은 쪽)
    "final_note": 5 * 60,
    "course_update": 10 * 60,
}
KIND_DEADLINE_SEC: dict[str, float] = {         # 요청부터 전체 마감. job_create·job_retry가 payload에 deadline_at으로 넣는다
    "qa": 90, "grading": 90,
}


def max_attempts_for(kind: str, cfg: StreamConfig) -> int:
    return KIND_MAX_ATTEMPTS.get(kind, cfg.max_attempts)


def job_timeout_for(kind: str, cfg: StreamConfig) -> float:
    return min(cfg.job_timeout_sec, KIND_JOB_TIMEOUT_SEC.get(kind, cfg.job_timeout_sec))


# scheduler 주기 작업 (팀 결정 6·9번)
RESEND_INTERVAL_SEC = _f("NOTREE_RESEND_INTERVAL_SEC", 10)        # 누락 복구 주기
RESEND_RESERVED_SEC = _f("NOTREE_RESEND_RESERVED_SEC", 30)        # 허가 후 이만큼 전송 완료가 안 되면 다시 보냄
RESEND_SENT_SEC = _f("NOTREE_RESEND_SENT_SEC", 120)               # 전송 후 이만큼 아무도 시작 안 하면 다시 보냄
CLEANUP_INTERVAL_SEC = _f("NOTREE_CLEANUP_INTERVAL_SEC", 3600)     # 체크포인트 정리 주기
CHECKPOINT_KEEP_SEC = _f("NOTREE_CHECKPOINT_KEEP_SEC", 7 * 24 * 3600)  # 완료·실패 후 보관 기간
