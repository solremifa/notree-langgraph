"""레인 비교: 한 대기열 / 고정 레인 / 빌려 쓰기를 실제 worker·배정기 코드와 실제 Redis로 잰다.

    redis-server --port 6390 --save '' &
    uv run python -m notree.dev.lane_bench --live-rate 12 --qa-rate 2
    uv run python -m notree.dev.lane_bench --alloc mixed,lanes --burst 2000 --designs borrow   # 배정 단계까지

DB(MCP)는 가짜(StubMcp)로 바꿔 큐·슬롯·배정 정책만 본다. LLM 시간은 sleep으로 흉내 내고,
--scale만큼 시간을 줄여 돌린다 (기본 0.1 → 요약 6초가 0.6초). 결과는 실제 시간으로 되돌려 보여 준다.

설계 (슬롯 합계는 모두 같다)
  shared : 한 Stream, 모든 작업이 같은 줄 (레인 분리 전)
  fixed  : qa 레인 Q슬롯 + rt 레인 나머지
  borrow : fixed + qa가 비면 rt 작업을 B개까지 빌려 씀
배정 (--alloc)
  none   : 작업을 Redis에 바로 넣음 (배정 단계 없음)
  mixed  : 대기 의도 → 실제 배정기 코드, 레인 구분 없이 생성 순서 (레인 분리 전 배정)
  lanes  : 대기 의도 → 실제 배정기 코드, 레인별 몫 (settings.ALLOC_LANES)
--burst N : 측정 시작 때 요약 의도 N개가 이미 쌓여 있는 상황 (배정기가 멈췄다 돌아온 뒤 등)

집계 (측정 구간 = 앞 20%를 뺀 나머지)
  처리량    : 측정 구간 **안에서** 끝난 작업 수 / 구간 길이 (종료 후 마저 끝난 작업은 넣지 않음)
  대기      : 구간 안에 들어온 작업의 등록 → 시작. 끝까지 시작 못 한 작업은 (종료 시각 - 등록)으로 넣은 하한값
  남은 대기 : 종료 시각에 아직 시작 못 한 작업 수 (대기열에 남은 양)
"""
import argparse
import asyncio
import dataclasses
import itertools
import json
import random
import time

import redis.asyncio as aioredis

from notree.common import settings as S
from notree.common.log import ACTOR
from notree.scheduler.allocator import run_allocator
from notree.worker.loop import Worker

LOOSE = dict(lease_sec=600, heartbeat_sec=300, claim_idle_ms=600_000, job_timeout_sec=600, max_attempts=1)


class StubMcp:
    """배정기·worker가 부르는 도구만 흉내 낸다. 시각을 기록해 대기·완료 시간을 계산한다.
    job_reserve_pending은 서버와 같은 규칙(대기 중, stream이 있으면 그 레인만, 생성 순서)."""

    def __init__(self, jobs: dict):
        self.jobs = jobs
        self.pending: list[str] = []          # 생성 순서

    async def tool(self, name, job_id=None, limit=10, stream=None, **_):
        if name == "job_reserve_pending":
            picked = [i for i in self.pending if stream is None or self.jobs[i]["stream"] == stream][:limit]
            gone = set(picked)
            self.pending = [i for i in self.pending if i not in gone]
            return {"jobs": [{"job_id": i, "kind": self.jobs[i]["kind"], "stream": self.jobs[i]["stream"]}
                             for i in picked]}
        if name == "job_mark_sent":
            return {"marked": True}
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


def pct(xs, p):
    return round(xs[min(len(xs) - 1, int(len(xs) * p))], 1) if xs else None


async def bench(a, design: str, alloc: str) -> dict:
    r = aioredis.from_url(a.redis, decode_responses=True)
    await r.flushdb()
    jobs: dict[str, dict] = {}
    route, specs = lanes(design, a.slots, a.qa_slots, a.borrow)
    mcp = StubMcp(jobs)
    stop = asyncio.Event()
    sc = a.scale
    ws = [Worker(f"{design}{i}", s.pop("cfg"), r, mcp, block_ms=50, shutdown_grace_sec=0.1, allow_legacy=True, **s)   # 가짜 MCP: 전달 세대 없음
          for i, s in enumerate(specs)]
    tasks = [asyncio.create_task(w.run(stop)) for w in ws]
    if alloc != "none":
        tasks.append(asyncio.create_task(run_allocator(r, mcp, stop, interval=S.ALLOC_INTERVAL_SEC * sc,
                                                       lanes=() if alloc == "mixed" else S.ALLOC_LANES)))
    ids = itertools.count()

    async def submit(kind: str, work: float):
        jid = f"{kind}-{next(ids)}"
        jobs[jid] = {"kind": kind, "stream": route[kind].stream, "enq": time.perf_counter(),
                     "sleep": work * sc * random.uniform(0.7, 1.3)}
        if alloc == "none":
            await r.xadd(route[kind].stream, {"job_id": jid})
        else:
            mcp.pending.append(jid)

    async def produce(kind: str, rate: float, work: float):
        while not stop.is_set():
            await asyncio.sleep(random.expovariate(rate / sc))
            await submit(kind, work)

    t0 = time.perf_counter()
    prod = [asyncio.create_task(produce("qa", a.qa_rate, a.qa_sec)),
            asyncio.create_task(produce("live", a.live_rate, a.live_sec))]
    warm = t0 + a.seconds * sc * 0.2                   # 앞 20%는 데우는 구간으로 뺀다
    if a.burst:
        await asyncio.sleep(warm - time.perf_counter())
        for _ in range(a.burst):                       # 측정 구간 시작에 요약이 한꺼번에 쌓임
            await submit("live", a.live_sec)
    await asyncio.sleep(t0 + a.seconds * sc - time.perf_counter())
    t1 = time.perf_counter()
    stop.set()
    for p in prod:
        p.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await r.aclose()

    span = (t1 - warm) / sc
    out = {"design": design, "alloc": alloc}
    for kind in ("qa", "live"):
        mine = [j for j in jobs.values() if j["kind"] == kind]
        js = [j for j in mine if warm <= j["enq"] <= t1]
        started = lambda j: "start" in j and j["start"] <= t1
        finished = lambda j: "end" in j and j["end"] <= t1
        waits = sorted(((j["start"] if started(j) else t1) - j["enq"]) / sc for j in js)
        e2e = sorted((j["end"] - j["enq"]) / sc for j in js if finished(j))
        out[kind] = {
            "요청": len(js),
            "구간 안 완료": sum(map(finished, js)),
            "남은 대기": sum(1 for j in mine if j["enq"] <= t1 and not started(j)),
            "시작 못 함(구간 요청)": sum(1 for j in js if not started(j)),
            "대기 p50": pct(waits, .5), "대기 p95": pct(waits, .95), "대기 최대": pct(waits, 1),
            "완료까지 p50(완료분)": pct(e2e, .5),
            "처리량/s": round(sum(1 for j in mine if "end" in j and warm <= j["end"] <= t1) / span, 2),
        }
    return out


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis", default="redis://localhost:6390/0")
    ap.add_argument("--designs", default="shared,fixed,borrow")
    ap.add_argument("--alloc", default="none", help="none,mixed,lanes 중 쉼표로 여러 개")
    ap.add_argument("--burst", type=int, default=0)
    ap.add_argument("--slots", type=int, default=64)
    ap.add_argument("--qa-slots", type=int, default=16)
    ap.add_argument("--borrow", type=int, default=8)
    ap.add_argument("--live-rate", type=float, default=12, help="실시간 요약 도착 (실제 시간 기준 개/초)")
    ap.add_argument("--qa-rate", type=float, default=2, help="Q&A 도착 (개/초)")
    ap.add_argument("--live-sec", type=float, default=6)
    ap.add_argument("--qa-sec", type=float, default=4)
    ap.add_argument("--seconds", type=float, default=300, help="실제 시간 기준 측정 길이")
    ap.add_argument("--scale", type=float, default=0.1)
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args()
    if a.seed is not None:
        random.seed(a.seed)
    ACTOR.set("bench")
    import notree.scheduler.allocator as alloc_mod
    import notree.worker.handlers.echo as echo_mod
    import notree.worker.lease as lease_mod
    import notree.worker.loop as loop_mod
    for m in (loop_mod, lease_mod, echo_mod, alloc_mod):
        m.log = lambda *_: None                        # 작업마다 찍히는 로그는 끈다
    for alloc in a.alloc.split(","):
        for d in a.designs.split(","):
            res = await bench(a, d, alloc)
            res.update(live_rate=a.live_rate, qa_rate=a.qa_rate, borrow=a.borrow, qa_slots=a.qa_slots,
                       burst=a.burst, seed=a.seed)
            q, v = res["qa"], res["live"]
            print(json.dumps(res, ensure_ascii=False) if a.json else
                  f"{alloc:5} {d:7} Q&A 대기 p50 {q['대기 p50']}s p95 {q['대기 p95']}s 최대 {q['대기 최대']}s · "
                  f"요약 대기 p50 {v['대기 p50']}s p95 {v['대기 p95']}s · 요약 처리 {v['처리량/s']}/s "
                  f"(도착 {a.live_rate}/s) · 남은 대기 Q&A {q['남은 대기']} 요약 {v['남은 대기']}")


if __name__ == "__main__":
    asyncio.run(main())
