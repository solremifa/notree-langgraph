"""Q&A (rt). 설계서 7절: 16k 컨텍스트, 도구 호출 최대 3회, 90초 이내.

TODO
  1. ctx_pack = await ctx.tool("qa.context", question_id=...)   → 질문·지정 자료·검색·최근 대화·요약 (16k 안)
  2. OpenAI tool calling 루프 (최대 3회). 모델이 고른 도구는 ctx.tool("notes.search" / "web.search" ...)로 실행
  3. await ctx.tool("qa.complete", idem_key=ctx.job_id, answer=..., sources=[...])
     - 근거가 바뀌었으면 서버가 거절 → FatalError가 아니라 '근거 변경' 상태로 저장할지 팀 결정 필요
  * 90초는 job_timeout(rt 기본 90초)으로 걸린다. 시간이 넘으면 재시도할지(한도 2회) 바로 실패로 볼지 결정 필요.
"""
from notree.worker.registry import FatalError, JobContext, handler


@handler("qa")
async def qa(ctx: JobContext) -> None:
    raise FatalError("미구현: qa")
