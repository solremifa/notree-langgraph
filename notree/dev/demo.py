"""worker 뼈대 데모: rt·batch worker 하나씩 + 배정기를 띄우고 여러 종류의 작업을 넣어 본다.

    uv run python -m notree.dev.demo                 # Redis 없이 (fakeredis)
    uv run python -m notree.dev.demo --redis redis://localhost:6379/0

넣는 작업
  echo 성공 / echo FatalError / echo 1번 실패 후 성공 / echo 항상 실패(한도 초과) / 미구현 핸들러(qa)
  course_update 첫 시도에 extract 도중 실패 → 재시도 때 체크포인트에서 이어감
마지막에 종료 신호를 보내 진행 중인 작업을 기다렸다 끝나는 것까지 보여 준다.
"""
import argparse
import asyncio
import dataclasses
import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

from notree.common import settings as S
from notree.common.log import ACTOR, log
from notree.scheduler.allocator import run_allocator
from notree.worker.loop import Worker
from notree.worker.mcp_client import SERVER, McpClient

OUT = Path("out")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--redis", default=None)
    a = p.parse_args()

    # 데모용으로 시간 값을 짧게 (실제 제안값은 settings.py)
    rt = dataclasses.replace(S.RT, lease_sec=2, heartbeat_sec=0.6, claim_idle_ms=2500, job_timeout_sec=10)
    batch = dataclasses.replace(S.BATCH, lease_sec=2, heartbeat_sec=0.6, claim_idle_ms=2500, job_timeout_sec=30)

    shutil.rmtree(OUT, ignore_errors=True); OUT.mkdir()
    port = free_port()
    url = f"http://127.0.0.1:{port}/mcp"
    server = subprocess.Popen([sys.executable, str(SERVER), "--http"], stdout=subprocess.DEVNULL,
                              stderr=subprocess.DEVNULL,
                              env={**os.environ, "NOTREE_MCP_PORT": str(port), "NOTREE_DB": str(OUT / "skel.db"),
                                   "NOTREE_REQUIRE_PERMIT": "1"})
    try:
        for _ in range(50):
            try:
                socket.create_connection(("127.0.0.1", port), 0.2).close(); break
            except OSError:
                time.sleep(0.1)
        if a.redis:
            import redis.asyncio as aioredis
            r, block = aioredis.from_url(a.redis, decode_responses=True), 1000
        else:
            import fakeredis
            r, block = fakeredis.FakeAsyncRedis(decode_responses=True), 0
        await r.delete(rt.stream, batch.stream)

        async with McpClient(url=url) as m_api, McpClient(url=url) as m_s, McpClient(url=url) as m_rt, \
                McpClient(url=url) as m_b:
            stop = asyncio.Event()
            tasks = [asyncio.create_task(run_allocator(r, m_s, stop, interval=0.2)),
                     asyncio.create_task(Worker("1", rt, r, m_rt, block_ms=block).run(stop)),
                     asyncio.create_task(Worker("1", batch, r, m_b, block_ms=block).run(stop))]
            await asyncio.sleep(0.5)

            ACTOR.set("api")
            jobs = {}
            for label, kind, payload, stream in [
                ("echo 성공", "echo", {"text": "hello", "sleep": 0.5}, rt.stream),
                ("echo Fatal", "echo", {"fail": "fatal"}, rt.stream),
                ("echo 1번 실패 후 성공", "echo", {"text": "second try", "fail": "retry:1"}, rt.stream),
                ("echo 항상 실패", "echo", {"fail": "always"}, rt.stream),
                ("미구현 qa", "qa", {}, None),
                ("course_update", "course_update",
                 {"sources": ["hello", "big", "world"], "crash_at": "extract:world"}, None),
            ]:
                jobs[label] = (await m_api.call("job_create", kind=kind, payload=payload, stream=stream))["job_id"]
                log("request", f"{label} → {jobs[label]}")
            ACTOR.set("")

            async def all_settled():
                states = [(await m_api.call("job_get", job_id=j))["state"] for j in jobs.values()]
                return all(s in ("done", "failed") for s in states)

            end = time.time() + 40
            while time.time() < end and not await all_settled():
                await asyncio.sleep(0.3)

            print("\n── 종료 신호: 오래 걸리는 echo 하나를 넣고 바로 stop", flush=True)
            ACTOR.set("api")
            slow = (await m_api.call("job_create", kind="echo", payload={"text": "끝까지 기다려 줌", "sleep": 1.5},
                                     stream=rt.stream))["job_id"]
            ACTOR.set("")
            await asyncio.sleep(1.0)
            stop.set()
            await asyncio.gather(*tasks)
            jobs["종료 중 진행 echo"] = slow

            print("\n── 결과", flush=True)
            for label, j in jobs.items():
                row = await m_api.call("job_get", job_id=j)
                print(f"  {label:<20} state={row['state']:<6} 시도={row['attempts']}  "
                      f"{('마지막 오류=' + row['fail_reason']) if row.get('fail_reason') else ''}")
            for s in (rt.stream, batch.stream):
                print(f"  {s}: ACK 안 된 메시지 {(await r.xpending(s, S.GROUP))['pending']}개")
    finally:
        server.terminate()


if __name__ == "__main__":
    asyncio.run(main())
