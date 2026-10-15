"""루프·재시도·종료 처리를 확인하기 위한 시험용 핸들러.

payload
  text   : 출력할 문자열
  sleep  : 걸리는 시간(초)
  fail   : "fatal"     → 매번 FatalError
           "retry:N"   → 처음 N번 시도는 RetryableError, 그 뒤 성공
           "always"    → 매번 RetryableError (시도 한도 확인용)
"""
import asyncio

from notree.common.log import log
from notree.worker.registry import FatalError, JobContext, RetryableError, handler


@handler("echo")
async def echo(ctx: JobContext) -> None:
    p = ctx.payload
    await asyncio.sleep(float(p.get("sleep", 0)))
    fail = p.get("fail", "")
    if fail == "fatal":
        raise FatalError("입력이 잘못됨 (시험)")
    if fail == "always" or (fail.startswith("retry:") and ctx.attempt <= int(fail.split(":")[1])):
        raise RetryableError("일시적 오류 (시험)")
    log("echo", f"{p.get('text', '')!r}")
