"""DB 모드와 Redis 모드의 정상 경로·인계 비교 (같은 작업량·동시성·heartbeat 간격). 실제 LLM 없이 재현 가능한 대기 작업.

    docker run -d --name notree-bench-redis -p 127.0.0.1:16398:6379 redis:7-alpine redis-server --save "" --appendonly no
    uv run python -m notree.dev.liveness_bench --redis redis://127.0.0.1:16398/0 --mode redis --phase normal
    uv run python -m notree.dev.liveness_bench --redis redis://127.0.0.1:16398/0 --mode db --phase takeover

작업: 측정 전용 종류 "bench" — sleep초 기다린 뒤(외부 호출 자리) note_publish로 한 번 저장 (idem_key = job_id, 실행권 확인).
구성: 한 프로세스 안에 배정기 1 + worker N개 (각자 MCP 연결 = 각자 stdio 서버 프로세스, DB는 SQLite 파일 하나), Redis 실물.
  normal   : jobs개를 한꺼번에 등록하고 모두 끝날 때까지
  takeover : worker A만 띄워 concurrency개를 실행 중으로 만든 뒤 A를 '죽임'(정리 없음) → 곧바로 worker B를 띄움
  restart  : 2×concurrency개 실행 중에 측정 전용 Redis를 재시작(--restart-cmd) → 유실 복구 주기 작업(1초)과 함께 끝까지
기록: worker 측 MCP 호출(전체·job_heartbeat·job_start), 업무 함수 실행·저장 횟수, 업무 시도·실행 세대 합, 재배송(회수) 횟수,
     완료 시간, 인계 시간(죽인 때 → 새 worker가 실행 승인을 받을 때까지), 업무 함수가 두 번 이상 들어간 작업 수, 실패 작업 수.

한계: SQLite·한 머신·한 프로세스. MCP 호출 수는 worker가 보낸 요청 수이지 SQL 문 수·PostgreSQL 부하가 아니다.
Redis·SQLite 파일은 측정마다 지우고 새로 만든다 (측정 DB는 임시 폴더, 저장소에 남기지 않음).
"""
import argparse
import asyncio
import collections
import dataclasses
import json
import os
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import redis.asyncio as aioredis

from notree.common import settings as S
from notree.scheduler.allocator import run_allocator
from notree.scheduler.periodic import run_periodic
from notree.worker import registry
from notree.worker.loop import Worker
from notree.worker.mcp_client import McpClient

ENTERED: collections.Counter = collections.Counter()   # job_id → 업무 함수에 들어간 횟수
SAVED: collections.Counter = collections.Counter()     # job_id → 저장 응답을 받은 횟수 (duplicate 포함)


@registry.handler("bench")
async def bench(ctx: registry.JobContext) -> None:
    ENTERED[ctx.job_id] += 1
    await asyncio.sleep(float(ctx.payload["sleep"]))
    await ctx.tool("note_publish", idem_key=ctx.job_id, course="bench", note="x", status="ok")
    SAVED[ctx.job_id] += 1


def _quiet():
    from notree.common import log as L
    import notree.scheduler.allocator as al
    import notree.worker.lease as le
    import notree.worker.loop as lo
    import notree.worker.ownership as ow
    for m in (L, al, le, lo, ow):
        m.log = lambda *x, **k: None


class Counted(Worker):
    reclaimed = 0

    async def _claim(self, cfg):
        m = await super()._claim(cfg)
        if m:
            Counted.reclaimed += 1
        return m


def _kill(w: Worker, run_task: asyncio.Task) -> None:
    """프로세스가 그 자리에서 죽은 것처럼: ACK·완료·반납·키 해제 없이 모든 작업을 멈춘다."""
    w.dead = True
    for t in list(w.running):
        t.cancel()
    run_task.cancel()


async def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--redis", required=True, help="측정 전용 Redis (FLUSHALL함)")
    p.add_argument("--mode", choices=("redis", "db"), required=True)
    p.add_argument("--phase", choices=("normal", "takeover", "restart"), required=True)
    p.add_argument("--restart-cmd", help="restart 단계: 측정 전용 Redis를 재시작하는 명령 (예: docker restart notree-bench-redis)")
    p.add_argument("--jobs", type=int, default=40)
    p.add_argument("--sleep", type=float, default=3.0, help="작업 하나의 대기 시간(초)")
    p.add_argument("--workers", type=int, default=2)
    p.add_argument("--concurrency", type=int, default=4)
    p.add_argument("--lease", type=float, default=3.0)
    p.add_argument("--heartbeat", type=float, default=1.0)
    p.add_argument("--claim-idle", type=float, default=4.0)
    p.add_argument("--out")
    a = p.parse_args()
    S.LIVENESS = a.mode
    _quiet()
    cfg = dataclasses.replace(S.RT, lease_sec=a.lease, heartbeat_sec=a.heartbeat, claim_idle_ms=int(a.claim_idle * 1000),
                              job_timeout_sec=a.sleep * 5, concurrency=a.concurrency)
    tmp = Path(tempfile.mkdtemp(prefix="notree-bench-"))
    db = tmp / "bench.db"
    env = {**os.environ, "NOTREE_DB": str(db), "NOTREE_REQUIRE_PERMIT": "1"}
    r = aioredis.from_url(a.redis, decode_responses=True)
    await r.flushall()
    stop = asyncio.Event()
    n_jobs = {"normal": a.jobs, "takeover": a.concurrency, "restart": a.concurrency * 2}[a.phase]
    n_workers = a.workers if a.phase != "takeover" else 2
    async with McpClient(env=env) as api, McpClient(env=env) as sch, McpClient(env=env) as per:
        clients = [await McpClient(env=env).__aenter__() for _ in range(n_workers)]
        tasks = [asyncio.create_task(run_allocator(r, sch, stop, interval=0.1))]
        if a.phase == "restart":                                      # 유실 복구 주기 작업 (1초마다)
            tasks.append(asyncio.create_task(run_periodic(r, per, stop, resend_every=1.0, sent_sec=60)))
        workers = [Counted(chr(65 + i), cfg, r, clients[i], block_ms=200) for i in range(n_workers)]
        runs: list[asyncio.Task | None] = [None] * n_workers
        first = 1 if a.phase == "takeover" else n_workers
        for i in range(first):
            runs[i] = asyncio.create_task(workers[i].run(stop))
        t0 = time.time()
        jobs = [(await api.tool("job_create", kind="bench", payload={"sleep": a.sleep}, user_id="u",
                                stream=cfg.stream))["job_id"] for _ in range(n_jobs)]

        async def rows():
            return {j: await api.tool("job_get", job_id=j) for j in jobs}

        handover = []
        if a.phase == "takeover":
            while not all(x["state"] == "running" for x in (await rows()).values()):
                await asyncio.sleep(0.05)
            await asyncio.sleep(a.heartbeat * 1.5)                   # heartbeat가 몇 번 돈 뒤
            killed_at = time.time()
            _kill(workers[0], runs[0])
            runs[1] = asyncio.create_task(workers[1].run(stop))
            taken: dict[str, float] = {}
            while len(taken) < len(jobs):
                for j, x in (await rows()).items():
                    if j not in taken and x["lease_owner"] == "rt-B" and x["state"] in ("running", "done", "failed"):
                        taken[j] = time.time() - killed_at
                await asyncio.sleep(0.05)
                if time.time() - killed_at > 60:
                    break
            handover = sorted(taken.values())
        restart_to_done = None
        if a.phase == "restart":
            while not all(x["state"] == "running" for x in (await rows()).values()):
                await asyncio.sleep(0.05)
            await asyncio.sleep(a.heartbeat * 1.5)
            t_restart = time.time()
            subprocess.run(a.restart_cmd, shell=True, check=True, capture_output=True)
        while True:
            rs = await rows()
            if all(x["state"] in ("done", "failed") for x in rs.values()) or time.time() - t0 > 180:
                break
            await asyncio.sleep(0.1)
        took = time.time() - t0
        if a.phase == "restart":
            restart_to_done = round(time.time() - t_restart, 2)
        lat =sorted(x["updated_at"] - x["created"] for x in rs.values())
        stop.set()
        await asyncio.gather(*(t for t in tasks + runs if t), return_exceptions=True)
        calls = collections.Counter(c for cl in clients for c in cl.calls)
        for cl in reversed(clients):
            await cl.__aexit__(None, None, None)
    con = sqlite3.connect(db)
    hb = con.execute("SELECT COUNT(*) FROM tool_log WHERE tool='job_heartbeat'").fetchone()[0]
    saves = con.execute("SELECT COUNT(*) FROM notes").fetchone()[0]
    con.close()
    await r.flushall()
    await r.aclose()
    res = {
        "mode": a.mode, "phase": a.phase, "jobs": n_jobs, "workers": n_workers, "concurrency": a.concurrency,
        "sleep": a.sleep, "lease": a.lease, "heartbeat": a.heartbeat, "claim_idle": a.claim_idle,
        "total_sec": round(took, 2), "latency_avg": round(statistics.mean(lat), 2), "latency_max": round(lat[-1], 2),
        "mcp_calls_worker": sum(calls.values()), "mcp_by_tool": dict(sorted(calls.items())),
        "job_heartbeat_db_rows": hb, "business_entered": sum(ENTERED.values()),
        "jobs_entered_twice": sum(1 for n in ENTERED.values() if n > 1), "saved_rows": saves,
        "save_responses": sum(SAVED.values()),
        "attempts_sum": sum(x["attempts"] for x in rs.values()), "exec_epoch_sum": sum(x["exec_epoch"] for x in rs.values()),
        "reclaimed": Counted.reclaimed, "failed": sum(1 for x in rs.values() if x["state"] == "failed"),
        "handover_sec": [round(x, 2) for x in handover], "restart_to_done_sec": restart_to_done,
    }
    print(json.dumps(res, ensure_ascii=False, indent=1))
    if a.out:
        with open(a.out, "a", encoding="utf-8") as f:
            f.write(json.dumps(res, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
