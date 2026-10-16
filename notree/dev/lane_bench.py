"""레인 비교: 한 대기열 / 고정 레인 / 빌려 쓰기를 실제 worker 코드와 실제 Redis로 잰다.

    redis-server --port 6390 --save '' &
    uv run python -m notree.dev.lane_bench --redis redis://localhost:6390/0 --live-rate 12 --qa-rate 2

DB(MCP)는 가짜(StubMcp)로 바꿔 큐·슬롯 정책만 본다. LLM 시간은 sleep으로 흉내 내고,
--scale만큼 시간을 줄여 돌린다 (기본 0.1 → 요약 6초가 0.6초). 결과는 실제 시간으로 되돌려 보여 준다.

설계 (슬롯 합계는 모두 같다)
  shared : 한 Stream, 모든 작업이 같은 줄 (레인 분리 전)
  fixed  : qa 레인 Q슬롯 + rt 레인 나머지
  borrow : fixed + qa가 비면 rt 작업을 B개까지 빌려 씀
"""
import argparse
import asyncio
import dataclasses
import itertools
import json
import random
import statistics
import time

import redis.asyncio as aioredis

from notree.common import settings as S
from notree.common.log import ACTOR
from notree.worker.loop import Worker

LOOSE = dict(lease_sec=600, heartbeat_sec=300, claim_idle_ms=600_000, job_timeout_sec=600, max_attempts=1)


class StubMcp:
    """job_start·job_finish만 흉내 낸다. 시각을 기록해 대기·완료 시간을 계산한다."""

    def __init__(self, jobs: dict):
        self.jobs = jobs

    async def tool(self, name, job_id=None, **_):
        j = self.jobs[job_id]
        if name == "job_start":
            j["start"] = time.perf_counter()
            return {"status": "started", "kind": "echo", "payload": {"sleep": j["sleep"]},
                    "attempt": 1, "permit": "stub"}
        if name == "job_finish":
            j["end"] = time.perf_counter()
            return {"finished": True}
        return {"renewed": True, "released": True, "failed": True}


def lanes(design: str, total: int, qa_slots: int, borrow: int):
    qa = dataclasses.replace(S.QA, **LOOSE)
    rt = dataclasses.replace(S.RT, **LOOSE)
    if design == "shared":
        return {"qa": rt, "live": rt}, [dict(cfg=rt, concurrency=total)]
    workers = [dict(cfg=qa, concurrency=qa_slots), dict(cfg=rt, concurrency=total - qa_slots)]
    if design == "borrow":
        workers[0].update(borrow=rt, borrow_slots=borrow)
    return {"qa": qa, "live": rt}, workers


async def bench(a, design: str) -> dict:
    r = aioredis.from_url(a.redis, decode_responses=True)
    await r.flushdb()
    jobs: dict[str, dict] = {}
    route, specs = lanes(design, a.slots, a.qa_slots, a.borrow)
    mcp = StubMcp(jobs)
    stop = asyncio.Event()
    ws = [Worker(f"{design}{i}", s.pop("cfg"), r, mcp, block_ms=50, **s) for i, s in enumerate(specs)]
    tasks = [asyncio.create_task(w.run(stop)) for w in ws]
    ids = itertools.count()
    sc = a.scale

    async def produce(kind: str, rate: float, work: float):
        while not stop.is_set():
            await asyncio.sleep(random.expovariate(rate / sc))
            jid = f"{kind}-{next(ids)}"
            jobs[jid] = {"kind": kind, "enq": time.perf_counter(), "sleep": work * sc * random.uniform(0.7, 1.3)}
            await r.xadd(route[kind].stream, {"job_id": jid})

    prod = [asyncio.create_task(produce("qa", a.qa_rate, a.qa_sec)),
            asyncio.create_task(produce("live", a.live_rate, a.live_sec))]
    t0 = time.perf_counter()
    await asyncio.sleep(a.seconds * sc)
    t1 = time.perf_counter()
    stop.set()
    for p in prod:
        p.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await r.aclose()

    warm = t0 + (t1 - t0) * 0.2                        # 앞 20%는 데우는 구간으로 뺀다
    out = {"design": design}
    for kind in ("qa", "live"):
        js = [j for j in jobs.values() if j["kind"] == kind and j["enq"] >= warm]
        done = [j for j in js if "end" in j]
        waits = sorted((j["start"] - j["enq"]) / sc for j in done)
        e2e = sorted((j["end"] - j["enq"]) / sc for j in done)
        q = lambda xs, p: round(xs[min(len(xs) - 1, int(len(xs) * p))], 1) if xs else None
        out[kind] = {"요청": len(js), "완료": len(done), "대기 p50": q(waits, .5), "대기 p95": q(waits, .95),
                     "완료까지 p50": q(e2e, .5), "완료까지 p95": q(e2e, .95),
                     "처리량/s": round(len(done) / ((t1 - warm) / sc), 2),
                     "평균 대기": round(statistics.mean(waits), 1) if waits else None}
    return out


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis", default="redis://localhost:6390/0")
    ap.add_argument("--designs", default="shared,fixed,borrow")
    ap.add_argument("--slots", type=int, default=64)
    ap.add_argument("--qa-slots", type=int, default=16)
    ap.add_argument("--borrow", type=int, default=8)
    ap.add_argument("--live-rate", type=float, default=12, help="실시간 요약 도착 (실제 시간 기준 개/초)")
    ap.add_argument("--qa-rate", type=float, default=2, help="Q&A 도착 (개/초)")
    ap.add_argument("--live-sec", type=float, default=6)
    ap.add_argument("--qa-sec", type=float, default=4)
    ap.add_argument("--seconds", type=float, default=300, help="실제 시간 기준 측정 길이")
    ap.add_argument("--scale", type=float, default=0.1)
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args()
    ACTOR.set("bench")
    import notree.worker.handlers.echo as echo_mod
    import notree.worker.lease as lease_mod
    import notree.worker.loop as loop_mod
    loop_mod.log = lease_mod.log = echo_mod.log = lambda *_: None   # 작업마다 찍히는 로그는 끈다
    for d in a.designs.split(","):
        res = await bench(a, d)
        res.update(live_rate=a.live_rate, qa_rate=a.qa_rate, borrow=a.borrow, qa_slots=a.qa_slots)
        print(json.dumps(res, ensure_ascii=False) if a.json else
              f"{d:7} Q&A 대기 p50 {res['qa']['대기 p50']}s p95 {res['qa']['대기 p95']}s · "
              f"요약 대기 p50 {res['live']['대기 p50']}s p95 {res['live']['대기 p95']}s · "
              f"요약 처리 {res['live']['처리량/s']}/s (도착 {a.live_rate}/s)")


if __name__ == "__main__":
    asyncio.run(main())
