"""모든 MCP 도구의 공통 응답 형식 (팀 결정 8번).

    성공       {"ok": true,  "data": {...}}
    중복 저장   {"ok": true,  "data": {...}, "duplicate": true}     ← 같은 idem_key로 이미 저장됨. 성공으로 본다
    거절       {"ok": false, "code": "...", "message": "...", "retryable": bool}

retryable은 서버가 정한다. worker의 ctx.tool()은 이 값을 보고 RetryableError / FatalError로 바꿔 던진다.
'결과의 한 종류'(예: job_start의 "다른 worker가 하는 중")는 오류가 아니라 data 안의 status로 돌려준다.
"""
from enum import StrEnum
from typing import Any

from pydantic import BaseModel


class ErrorCode(StrEnum):
    INVALID_ARGS = "invalid_args"                # 인자 형식 오류
    PERMIT_INVALID = "permit_invalid"            # permit 없음·서명 불일치·만료·다른 작업의 permit
    NOT_FOUND = "not_found"                      # 대상이 없음
    GENERATION_MISMATCH = "generation_mismatch"  # 읽은 뒤 삭제·취소됨 (발행 게이트)
    LOCKED = "locked"                            # 편집 잠금 등으로 지금은 못 함
    UNAVAILABLE = "unavailable"                  # DB 일시 장애 등


RETRYABLE: frozenset[ErrorCode] = frozenset({ErrorCode.LOCKED, ErrorCode.UNAVAILABLE})


def ok(data: BaseModel | dict[str, Any] | None = None, *, duplicate: bool = False) -> dict:
    body = data.model_dump() if isinstance(data, BaseModel) else (data or {})
    env: dict[str, Any] = {"ok": True, "data": body}
    if duplicate:
        env["duplicate"] = True
    return env


def err(code: ErrorCode, message: str) -> dict:
    return {"ok": False, "code": str(code), "message": message, "retryable": code in RETRYABLE}


class ToolError(Exception):
    """MCP 도구가 ok=false를 돌려줌."""

    def __init__(self, tool: str, code: str, message: str, retryable: bool) -> None:
        super().__init__(f"{tool}: {code} ({message})")
        self.tool, self.code, self.message, self.retryable = tool, code, message, retryable


def unwrap(tool: str, env: dict) -> dict:
    """envelope → data. 거절이면 ToolError. 중복 저장이면 data에 duplicate=True를 얹어 준다."""
    if env.get("ok") is True:
        data = dict(env.get("data") or {})
        if env.get("duplicate"):
            data["duplicate"] = True
        return data
    if env.get("ok") is False:
        raise ToolError(tool, env.get("code", "unknown"), env.get("message", ""), bool(env.get("retryable")))
    raise ToolError(tool, "invalid_response", f"envelope 형식이 아님: {str(env)[:200]}", False)
