"""설정. 모든 값은 환경변수로 덮어쓸 수 있다.

시간 값은 **팀이 정해야 할 제안값**이다 (docs/team-decisions.md 참고).
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


@dataclass(frozen=True)
class StreamConfig:
    key: str               # "rt" | "batch"
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


#                key      stream          lease  claim  시도  제한   동시
RT = _cfg("rt", "notree:rt", 15, 20, 2, 90, 4)          # 화면 앞에서 기다리는 작업: Q&A(90초 약속)·실시간 요약·채점
BATCH = _cfg("batch", "notree:batch", 60, 90, 3, 900, 1)  # 길어도 되는 작업: 최종 노트·과목 노트
STREAMS = {"rt": RT, "batch": BATCH}
