"""복습 채점 (rt). 설계서 8절: 요소별 0·0.5·1점, 중대 오개념 표시, 모호하면 보류.

TODO
  1. item = await ctx.tool("grade.context", answer_id=...)   → 문제·루브릭 버전·근거·제출 답안
  2. OpenAI 루브릭 채점 (비동기, 트랜잭션 밖)
  3. await ctx.tool("grade.save", idem_key=ctx.job_id, scores=..., held=모호하면 True)
"""
from notree.worker.registry import FatalError, JobContext, handler


@handler("grading")
async def grading(ctx: JobContext) -> None:
    raise FatalError("미구현: grading")
