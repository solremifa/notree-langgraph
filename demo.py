"""한 번에 보는 데모: api → scheduler → worker-A(죽음) → worker-B(이어받음).

    uv run demo.py                         # Redis 없이 (fakeredis, 한 프로세스 안에서 역할별 코루틴)
    uv run demo.py --redis redis://localhost:6379/0   # 진짜 Redis로
    uv run demo.py --die-at publish        # 발행 직후 죽는 경우
    uv run demo.py --die-at none           # 아무도 안 죽는 경우

MCP 서버는 HTTP로 하나만 띄우고(compose의 mcp 컨테이너 역할), 각 역할이 따로 연결한다.
"""
import argparse
import asyncio
import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

import settings as S
from api import request_course_update
from graph import ACTOR
from mcp_saver import McpClient
from scheduler import run_allocator
from worker import run_worker

OUT = Path("out")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def banner(text: str) -> None:
    print(f"\n── {text} " + "─" * max(0, 60 - len(text)), flush=True)


async def wait_until(pred, timeout: float, every: float = 0.2) -> bool:
    end = time.time() + timeout
    while time.time() < end:
        if await pred():
            return True
        await asyncio.sleep(every)
    return False


async def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--redis", default=None, help="Redis URL. 생략하면 fakeredis")
    p.add_argument("--lectures", default="hello,big,world")
    p.add_argument("--die-at", default="extract:world", help="extract:<강의> | publish | none")
    p.add_argument("--llm", choices=["fake", "openai"], default="fake")
    a = p.parse_args()
    die_at = None if a.die_at == "none" else a.die_at

    # MCP 서버 (DB 소유자) 기동
    shutil.rmtree(OUT, ignore_errors=True); OUT.mkdir()
    port = free_port()
    url = f"http://127.0.0.1:{port}/mcp"
    server = subprocess.Popen([sys.executable, str(Path(__file__).with_name("mcp_server.py")), "--http"],
                              env={**os.environ, "NOTREE_MCP_PORT": str(port), "NOTREE_DB": str(OUT / "demo.db")},
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        for _ in range(50):                                  # 서버가 뜰 때까지
            try:
                socket.create_connection(("127.0.0.1", port), 0.2).close(); break
            except OSError:
                time.sleep(0.1)

        if a.redis:
            import redis.asyncio as aioredis
            r = aioredis.from_url(a.redis, decode_responses=True)
        else:
            import fakeredis
            r = fakeredis.FakeAsyncRedis(decode_responses=True)
        await r.delete(S.BATCH_STREAM)
        block = 1000 if a.redis else 0

        print(f"MCP 서버 {url}  ·  Redis {'fakeredis' if not a.redis else a.redis}  ·  "
              f"lease {S.LEASE_SEC:g}초 · 재배송 기준 {S.CLAIM_IDLE_MS/1000:g}초 방치")

        async with McpClient(url=url) as m_api, McpClient(url=url) as m_sched, \
                   McpClient(url=url) as m_a, McpClient(url=url) as m_b, McpClient(url=url) as m_obs:
            stop = asyncio.Event()
            sched = asyncio.create_task(run_allocator(r, m_sched, stop))

            banner("1. worker-A 기동")
            wa = asyncio.create_task(run_worker("A", r, m_a, die_at=die_at, stop=stop, block_ms=block))
            await asyncio.sleep(0.3)

            banner("2. 사용자 요청 → api가 의도 기록 → scheduler가 큐에 넣음 → worker-A가 받음")
            job_id = await request_course_update(m_api, a.lectures.split(","), a.llm)
            ACTOR.set("")

            async def job_state():
                return (await m_obs.call("job_get", job_id=job_id))["state"]

            await wait_until(lambda: _done_or(wa, job_state), 30)
            if wa.done() and wa.result() == "died":
                banner(f"3. worker-A 죽음 → worker-B 기동. ACK 없는 메시지가 {S.CLAIM_IDLE_MS/1000:g}초 방치되길 기다림")
                wb = asyncio.create_task(run_worker("B", r, m_b, stop=stop, block_ms=block))
                ok = await wait_until(lambda: _is(job_state, "done"), 30)
            else:
                wb = None
                ok = await job_state() == "done"

            stop.set()
            await asyncio.gather(sched, wa, *( [wb] if wb else [] ), return_exceptions=True)

            banner("4. 결과")
            job = await m_obs.call("job_get", job_id=job_id)
            note = await m_obs.call("note_get", idem_key=job_id)
            pending = await r.xpending(S.BATCH_STREAM, S.GROUP)
            stats = await m_obs.call("tool_stats", thread_id=job_id)
            print(f"  job      : state={job['state']}  시도={job['attempts']}회  마지막 담당={job['lease_owner']}")
            print(f"  노트     : {note.get('note')!r} [{note.get('status')}]")
            print(f"  Redis    : ACK 안 된 메시지 {pending['pending']}개")
            print(f"  MCP 호출 : {stats}")
            print("  ✔ 완료" if ok else "  ✖ 시간 안에 끝나지 않음")
    finally:
        server.terminate()


async def _done_or(task, job_state) -> bool:
    return task.done() or await job_state() == "done"


async def _is(job_state, want) -> bool:
    return await job_state() == want


if __name__ == "__main__":
    asyncio.run(main())
