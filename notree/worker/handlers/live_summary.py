"""실시간 요약 (rt). PoC: 요약은 가짜 LLM (OpenAI 연결은 나중에).

    payload = {"lecture_id": ...}      (transcript_commit 또는 summary_recover가 등록. 강의당 대기 1개)

한 번의 실행
  1. transcript_range  → 롤링 요약 + 요약 위치 이후 전사 전부 (그 사이 들어온 커밋이 한 번에 합쳐짐), 세대
     녹음이 끝났거나 새 전사가 없으면 아무것도 안 하고 완료
  2. 요약 (LLM 1회, 트랜잭션 밖)
  3. summary_publish  → 요약 저장 + 위치 이동 (한 트랜잭션). 세대가 바뀌었으면(녹음 종료·삭제) generation_mismatch → FatalError
     위치가 그 사이 움직였으면 stale → 저장 없이 완료 (다른 요약이 이미 덮음)
시도는 총 1회 (settings.KIND_MAX_ATTEMPTS). 실패한 구간은 다음 요약이 처리 위치부터 다시 읽으므로 함께 처리된다.
"""
import asyncio

from notree.common.log import log
from notree.worker.registry import JobContext, RetryableError, handler

FAKE_DELAY = 0.0          # 시험용: 요약 LLM 한 번에 걸리는 시간
CALLS: list[tuple[int, int]] = []


async def summarize(rolling: str, new_text: str, from_seq: int, last_seq: int) -> str:
    CALLS.append((from_seq, last_seq))
    if FAKE_DELAY:
        await asyncio.sleep(FAKE_DELAY)
    if "FAIL_SUMMARY" in new_text:
        raise RetryableError("요약 LLM 실패 (시험)")
    words = new_text.split()
    return (rolling + " " if rolling else "") + f"[{from_seq}-{last_seq}] " + " ".join(words[:3])


@handler("live_summary")
async def live_summary(ctx: JobContext) -> None:
    lid = ctx.payload["lecture_id"]
    seg = await ctx.tool("transcript_range", lecture_id=lid)
    if seg["state"] != "recording" or seg["last_seq"] < seg["from_seq"]:
        log("summary", f"{lid}: 할 일 없음 (상태 {seg['state']}, 범위 {seg['from_seq']}-{seg['last_seq']})")
        return
    text = await summarize(seg["rolling_summary"], seg["new_text"], seg["from_seq"], seg["last_seq"])
    res = await ctx.tool("summary_publish", idem_key=ctx.job_id, lecture_id=lid, expected_generation=seg["generation"],
                         from_seq=seg["from_seq"], upto_seq=seg["last_seq"], text=text)
    log("summary", f"{lid}: [{seg['from_seq']}-{seg['last_seq']}] {res['status']} → 요약 위치 {res['processed_until']}")
