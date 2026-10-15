"""죽은 worker의 작업을 다른 worker가 이어받는 데모.

    uv run python -m notree.dev.takeover                      # worker-A가 extract 도중 죽음 → worker-B가 이어감
    uv run python -m notree.dev.takeover --die-at publish     # 발행 직후 죽음 → 중복 발행 없이 마무리
    uv run python -m notree.dev.takeover --redis redis://localhost:6379/0

죽음은 SimulatedDeath로 흉내 낸다: ACK·완료 표시·lease 반납·heartbeat를 모두 멈추고 worker가 끝난다.
진짜로 죽여 보려면 worker를 별도 창으로 띄우고(`python -m notree.worker`) 작업 중에 창을 닫으면 된다.
"""
import argparse
import asyncio
import dataclasses
import shutil
import time
from pathlib import Path

from notree.common import settings as S
from notree.common.log import ACTOR, log
from notree.dev.devenv import mcp_server, redis_client
from notree.scheduler.allocator import run_allocator
from notree.worker.loop import Worker
from notree.worker.mcp_client import McpClient

OUT = Path("out")


async def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--redis", default=None)
    p.add_argument("--die-at", default="extract:world", help="extract:<강의> | publish")
    p.add_argument("--lectures", default="hello,big,world")
    a = p.parse_args()
    batch = dataclasses.replace(S.BATCH, lease_sec=3, heartbeat_sec=1, claim_idle_ms=4000)

    shutil.rmtree(OUT, ignore_errors=True); OUT.mkdir()
    with mcp_server(OUT / "takeover.db") as url:
        r, block = redis_client(a.redis)
        await r.delete(batch.stream)
        print(f"lease {batch.lease_sec:g}초 · 재배송 기준 {batch.claim_idle_ms / 1000:g}초", flush=True)
        async with McpClient(url=url) as m_api, McpClient(url=url) as m_s, McpClient(url=url) as m_a, \
                McpClient(url=url) as m_b:
            stop = asyncio.Event()
            sched = asyncio.create_task(run_allocator(r, m_s, stop, interval=0.2))
            wa = asyncio.create_task(Worker("A", batch, r, m_a, block_ms=block).run(stop))
            await asyncio.sleep(0.3)

            ACTOR.set("api")
            j = (await m_api.tool("job_create", kind="course_update", user_id="u-demo",
                                  payload={"sources": a.lectures.split(","), "die_at": a.die_at}))["job_id"]
            log("request", f"과목 노트 업데이트 → {j}")
            ACTOR.set("")
            while (await m_api.tool("job_get", job_id=j))["state"] not in ("running", "done"):
                await asyncio.sleep(0.1)
            print("\n── worker-B 기동 (A가 죽으면 lease 만료 + 재배송 기준이 지난 뒤 이어받음)", flush=True)
            wb = asyncio.create_task(Worker("B", batch, r, m_b, block_ms=block).run(stop))

            end = time.time() + 30
            while time.time() < end and (await m_api.tool("job_get", job_id=j))["state"] != "done":
                await asyncio.sleep(0.2)
            stop.set()
            results = await asyncio.gather(sched, wa, wb)

            row = await m_api.tool("job_get", job_id=j)
            note = await m_api.tool("note_get", idem_key=j)
            stats = (await m_api.tool("tool_stats", thread_id=j))["counts"]
            print("\n── 결과", flush=True)
            print(f"  worker-A: {results[1]} · worker-B: {results[2]}")
            print(f"  job: {row['state']} · 시도 {row['attempts']}회 · 마지막 담당 {row['lease_owner']}")
            print(f"  노트: {note.get('note')!r} · note_publish 호출 {stats.get('note_publish', 0)}회 (저장은 1회)")
            print(f"  ACK 안 된 메시지 {(await r.xpending(batch.stream, S.GROUP))['pending']}개")


if __name__ == "__main__":
    asyncio.run(main())
