"""강의 최종 노트 (batch). 설계서 4절: 구간 요약 계층 병합, 부분 완료 시 누락 구간 표시.

TODO
  1. parts = await ctx.tool("summary.segments", lecture_id=...)
  2. 묶음 단위로 OpenAI 병합 → 최종 노트 초안
  3. await ctx.tool("note.publish", idem_key=ctx.job_id, status="완료" | "부분 완료", missing=[...])
  * 이어하기가 필요해지면(120초 목표를 넘기는 경우) course_update처럼 LangGraph로 옮긴다.
"""
from notree.worker.registry import FatalError, JobContext, handler


@handler("final_note")
async def final_note(ctx: JobContext) -> None:
    raise FatalError("미구현: final_note")
