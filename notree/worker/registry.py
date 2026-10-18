"""핸들러 규약.

작업 종류(kind)마다 핸들러 함수 하나:

    @handler("live_summary")
    async def live_summary(ctx: JobContext) -> None: ...

핸들러가 지킬 것
  1. 입력은 ctx.payload(= DB의 작업 입력)와 MCP 읽기 도구로만 얻는다.
  2. 결과는 MCP 발행 도구로만 저장하고, 멱등키는 ctx.job_id를 쓴다 → 두 번 실행돼도 결과가 같다.
  3. LLM·네트워크 호출은 비동기로 한다 (동기로 오래 막으면 heartbeat가 멈춰 작업을 빼앗긴다).
  4. 실패는 둘 중 하나로 던진다.
       RetryableError : 잠시 후 다시 하면 될 수 있음 (OpenAI 429·타임아웃·일시 장애)
       FatalError     : 다시 해도 안 됨 (입력 깨짐, 출처 삭제됨, 권한 없음)
     그 밖의 예외는 RetryableError처럼 취급되고, 시도 한도를 넘으면 실패 확정된다.
     MCP 도구의 거절은 ctx.tool()이 서버가 준 retryable 값을 보고 알아서 둘 중 하나로 바꿔 던진다.
  5. ACK·완료 표시·lease는 신경 쓰지 않는다 (루프가 한다).
  6. payload에 deadline_at(unix 초)이 있으면 루프가 남은 시간만큼만 실행한다 (Q&A 90초 약속, 팀 결정 2번).
     핸들러가 더 잘게 쓰고 싶으면 ctx.time_left()를 본다.
"""
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from notree.common.settings import StreamConfig
from notree.contracts.envelope import ToolError
from notree.worker.mcp_client import McpClient


class RetryableError(Exception):
    """다시 하면 될 수 있는 실패."""


class FatalError(Exception):
    """다시 해도 안 되는 실패."""


class SimulatedDeath(BaseException):
    """시험·데모 전용: worker 프로세스가 그 자리에서 죽은 것처럼 만든다.
    루프는 ACK·완료 표시·lease 반납·heartbeat를 모두 멈추고 종료한다 → 다른 worker가 재배송으로 이어받는다.
    (BaseException이라 핸들러의 except Exception에 걸리지 않음)"""


class ToolUnavailable(RetryableError):
    """MCP 도구가 retryable=true로 거절 (locked, unavailable)."""

    def __init__(self, e: ToolError) -> None:
        super().__init__(str(e))
        self.code = e.code


class ToolRejected(FatalError):
    """MCP 도구가 retryable=false로 거절 (invalid_args, permit_invalid, not_found, generation_mismatch)."""

    def __init__(self, e: ToolError) -> None:
        super().__init__(str(e))
        self.code = e.code


@dataclass
class JobContext:
    job_id: str
    kind: str
    payload: dict
    attempt: int            # 1부터. 재시도·재배송 때마다 증가
    permit: str             # job_start가 발급한 서명 토큰
    worker: str
    cfg: StreamConfig
    mcp: McpClient
    max_attempts: int = 1
    deadline_at: float | None = None                         # 사용자에게 약속한 마감 (unix 초)
    extras: dict[str, Any] = field(default_factory=dict)   # 공용 자원(OpenAI 클라이언트 등)

    async def tool(self, name: str, **args: Any) -> dict:
        """permit을 자동으로 붙여 MCP 도구를 부르고 data만 돌려준다.
        거절되면 서버가 준 retryable 값에 따라 ToolUnavailable(RetryableError) / ToolRejected(FatalError)."""
        try:
            return await self.mcp.tool(name, permit=self.permit, **args)
        except ToolError as e:
            raise (ToolUnavailable(e) if e.retryable else ToolRejected(e)) from e

    def time_left(self) -> float | None:
        return None if self.deadline_at is None else self.deadline_at - time.time()


Handler = Callable[[JobContext], Awaitable[None]]
HANDLERS: dict[str, Handler] = {}


def handler(kind: str) -> Callable[[Handler], Handler]:
    def deco(fn: Handler) -> Handler:
        if kind in HANDLERS:
            raise ValueError(f"핸들러 중복: {kind}")
        HANDLERS[kind] = fn
        return fn
    return deco


def load_handlers() -> dict[str, Handler]:
    """handlers 패키지를 import하면 각 모듈의 @handler가 등록된다."""
    import notree.worker.handlers  # noqa: F401
    return HANDLERS
