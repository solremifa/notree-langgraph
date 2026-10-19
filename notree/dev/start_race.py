"""중복 전달 경쟁의 **자연 발생** 빈도 측정 (순서를 강제하지 않음). 강제 순서 시험은 tests/test_start_protocol.py.

    docker run -d --name notree-bench-redis -p 127.0.0.1:16398:6379 redis:7-alpine redis-server --save "" --appendonly no
    uv run python -m notree.dev.start_race --redis redis://127.0.0.1:16398/0 --mode redis --trials 200

한 번의 시도: 강의 하나·전사 3개 → 실시간 요약 작업(총 1회) 1건 → 같은 작업의 메시지 2개를 worker A·B가 하나씩 받음
→ 두 worker가 **동시에** 처리 (asyncio.gather, 각자 MCP 연결 = 각자 stdio 서버 프로세스, DB 파일은 하나).
지연·응답 유실은 넣지 않는다. 결과: (상태, 업무 시도, 요약 함수 호출, 저장) 조합별 횟수와 job_start 호출 수.

주의: 같은 프로세스·같은 이벤트 루프에서 도는 두 worker라 실제 배치(다른 호스트·네트워크 지연)보다 경쟁 창이 좁거나 넓을 수 있다.
빈도는 이 환경의 관측값이지 운영 빈도의 추정이 아니다. Redis는 FLUSHALL하므로 **측정 전용** Redis를 줄 것.
"""
import argparse
import asyncio
import collections
import dataclasses
import json
import os
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

import redis.asyncio as aioredis

from notree.common import log as L
from notree.common import settings as S
from notree.worker.handlers import live_summary as LS
from notree.worker.loop import Worker
from notree.worker.mcp_client import McpClient

RT = dataclasses.replace(S.RT, lease_sec=3.0, heartbeat_sec=1.0, claim_idle_ms=4000, job_timeout_sec=20)


async def trial(r, api, a: Worker, b: Worker, db: Path, i: int) -> tuple:
    lec = f"race-{i}"
    await api.tool("lecture_start", lecture_id=lec, user_id="u")
    j = (await api.tool("transcript_commit", lecture_id=lec, user_id="u",
                        segments=[{"seq": k, "text": f"말{k}"} for k in (1, 2, 3)]))["enqueued_job_id"]
    gen = (await api.tool("job_reserve_pending", limit=5, stream=RT.stream))["jobs"][0]["gen"]
    for _ in range(2):
        await r.xadd(RT.stream, {"job_id": j, "gen": str(gen)})
    ma, mb = await a._read(RT, None), await b._read(RT, None)
    before = len(LS.CALLS)
    await asyncio.gather(a._guarded(*ma), b._guarded(*mb))
    for _ in range(50):                                        # 손대지 않고 넘긴 쪽만 있으면 끝난 상태가 아닐 수 있음
        row = await api.tool("job_get", job_id=j)
        if row["state"] in ("done", "failed"):
            break
        await asyncio.sleep(0.1)
    con = sqlite3.connect(db)
    saved = con.execute("SELECT COUNT(*) FROM summaries WHERE idem_key=?", (j,)).fetchone()[0]
    starts = con.execute("SELECT COUNT(*) FROM tool_log WHERE tool='job_start' AND thread_id=?", (j,)).fetchone()[0]
    con.close()
    return row["state"], row["attempts"], len(LS.CALLS) - before, saved, starts


async def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--redis", required=True, help="측정 전용 Redis (FLUSHALL함)")
    p.add_argument("--mode", choices=("redis", "db"), default=S.LIVENESS)
    p.add_argument("--trials", type=int, default=200)
    p.add_argument("--out", help="결과를 덧붙일 jsonl")
    p.add_argument("--label", default="")
    a = p.parse_args()
    S.LIVENESS = a.mode
    import notree.worker.lease as lease_mod
    import notree.worker.loop as loop_mod
    import notree.worker.ownership as own_mod
    for mod in (L, loop_mod, own_mod, lease_mod, LS):           # 측정 중 로그 끔
        mod.log = lambda *x, **k: None
    tmp = Path(tempfile.mkdtemp(prefix="notree-race-"))
    db = tmp / "race.db"
    env = {**os.environ, "NOTREE_DB": str(db), "NOTREE_REQUIRE_PERMIT": "1"}
    r = aioredis.from_url(a.redis, decode_responses=True)
    await r.flushall()
    await r.xgroup_create(RT.stream, S.GROUP, id="0", mkstream=True)
    outcomes: collections.Counter = collections.Counter()
    t0 = time.time()
    async with McpClient(env=env) as api, McpClient(env=env) as m_a, McpClient(env=env) as m_b:
        wa, wb = Worker("A", RT, r, m_a, block_ms=0), Worker("B", RT, r, m_b, block_ms=0)
        for i in range(a.trials):
            outcomes[await trial(r, api, wa, wb, db, i)] += 1
    await r.flushall()
    await r.aclose()
    res = {"label": a.label, "mode": a.mode, "trials": a.trials, "sec": round(time.time() - t0, 1),
           "outcomes": [{"state": k[0], "attempts": k[1], "summary_calls": k[2], "saved": k[3], "job_start_calls": k[4],
                         "n": n} for k, n in sorted(outcomes.items(), key=lambda x: -x[1])]}
    print(json.dumps(res, ensure_ascii=False, indent=1))
    if a.out:
        with open(a.out, "a", encoding="utf-8") as f:
            f.write(json.dumps(res, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
