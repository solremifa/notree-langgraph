"""실시간 요약 (rt). 설계서 4절: 확정 전사 100자 이상·20초 대기·세션 종료 중 하나면 실행.

TODO (구현 순서)
  1. seg = await ctx.tool("transcript.range", lecture_id=..., ...)  → 롤링 요약 + 새 구간 + 세대(generation)
  2. text = await summarize(llm, seg["rolling_summary"], seg["new_text"])   # OpenAI 1회, 비동기, 트랜잭션 밖
     - 429·타임아웃 → RetryableError
  3. await ctx.tool("summary.publish", idem_key=ctx.job_id, generation=seg["generation"], text=text)
     - 서버가 세대 불일치(강의 삭제·취소)로 거절하면 FatalError
  4. 발행 도구가 outbox를 같이 쓰므로 화면 갱신(NOTIFY → gateway → WS)은 worker가 신경 쓰지 않는다.
"""
from notree.worker.registry import FatalError, JobContext, handler


@handler("live_summary")
async def live_summary(ctx: JobContext) -> None:
    raise FatalError("미구현: live_summary")
