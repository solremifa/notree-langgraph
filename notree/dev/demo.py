"""worker 뼈대 데모: rt·batch worker 하나씩 + scheduler(배정기·주기 작업)를 띄우고 여러 종류의 작업을 넣어 본다.

    uv run python -m notree.dev.demo                 # Redis 없이 (fakeredis)
    uv run python -m notree.dev.demo --redis redis://localhost:6379/0

넣는 작업
  echo 성공 / echo FatalError / echo 1번 실패 후 성공 / echo 항상 실패(한도 초과) / 미구현 핸들러(qa)
  echo 마감 시각 초과 (Q&A 90초 약속 흉내)
  course_update 첫 실행에서 extract 도중 실패 → 재시도 때 체크포인트에서 이어감
  배정기가 허가만 하고 죽은 의도 → 주기 작업이 다시 보냄 (누락 복구)
  echo 실패 확정 → 사용자의 '다시 시도'(job_retry)로 되살림
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
from notree.scheduler.periodic import run_periodic
from notree.worker.loop import Worker
from notree.worker.mcp_client import SERVER, McpClient

OUT = Path("out")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def banner(text: str) -> None:
    print(f"\n── {text}", flush=True)


async def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--redis", default=None)
    a = p.parse_args()

    # 데모용으로 시간 값을 짧게 (실제 값은 settings.py)
    rt = dataclasses.replace(S.RT, lease_sec=2, heartbeat_sec=0.6, claim_idle_ms=2500, job_timeout_sec=10)
    batch = dataclasses.replace(S.BATCH, lease_sec=2, heartbeat_sec=0.6, claim_idle_ms=2500, job_timeout_sec=30)

    shutil.rmtree(OUT, ignore_errors=True); OUT.mkdir()
    flag = OUT / "not-ready.flag"          # 되살리기 시나리오: 이 파일이 있는 동안 echo가 실패
    flag.write_text("x")
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

        async with McpClient(url=url) as m_api, McpClient(url=url) as m_s, McpClient(url=url) as m_p, \
                McpClient(url=url) as m_rt, McpClient(url=url) as m_b:

            async def create(label, kind, payload, stream=None):
                ACTOR.set("api")
                j = (await m_api.tool("job_create", kind=kind, payload=payload, user_id="u-demo",
                                      stream=stream))["job_id"]
                log("request", f"{label} → {j}")
                ACTOR.set("")
                return j

            async def state(j):
                return (await m_api.tool("job_get", job_id=j))["state"]

            async def settle(ids, timeout=40):
                end = time.time() + timeout
                while time.time() < end:
                    if all([await state(j) in ("done", "failed") for j in ids]):
                        return
                    await asyncio.sleep(0.3)

            # 누락 복구 시나리오: 배정기가 허가만 하고 XADD 전에 죽은 상황을 먼저 만들어 둔다
            lost = await create("허가 후 배정기 죽음", "echo", {"text": "다시 보내져서 실행됨"}, rt.stream)
            await m_api.tool("job_reserve_pending")            # reserved로만 바꾸고 XADD는 하지 않음
            log("demo", f"{lost}: reserved 상태로 방치 (배정기가 XADD 전에 죽었다고 가정)")

            stop = asyncio.Event()
            tasks = [asyncio.create_task(run_allocator(r, m_s, stop, interval=0.2)),
                     asyncio.create_task(run_periodic(r, m_p, stop, resend_every=1, reserved_sec=3, sent_sec=30)),
                     asyncio.create_task(Worker("1", rt, r, m_rt, block_ms=block).run(stop)),
                     asyncio.create_task(Worker("1", batch, r, m_b, block_ms=block).run(stop))]
            await asyncio.sleep(0.5)

            banner("1. 여러 결과")
            jobs = {"허가 후 배정기 죽음": lost}
            for label, kind, payload, stream in [
                ("echo 성공", "echo", {"text": "hello", "sleep": 0.5}, rt.stream),
                ("echo Fatal", "echo", {"fail": "fatal"}, rt.stream),
                ("echo 1번 실패 후 성공", "echo", {"text": "second try", "fail": "retry:1"}, rt.stream),
                ("echo 항상 실패", "echo", {"fail": "always"}, rt.stream),
                ("echo 마감 초과", "echo", {"sleep": 5, "deadline_at": time.time() + 1.5}, rt.stream),
                ("미구현 qa", "qa", {}, None),
                ("course_update", "course_update",
                 {"sources": ["hello", "big", "world"], "crash_at": "extract:world"}, None),
                ("echo 되살리기", "echo", {"text": "되살아남", "fail_if_exists": str(flag)}, rt.stream),
            ]:
                jobs[label] = await create(label, kind, payload, stream)
            await settle(list(jobs.values()))

            banner("2. 사용자의 '다시 시도': 원인을 고친 뒤(파일 삭제) job_retry")
            flag.unlink()
            ACTOR.set("api")
            res = await m_api.tool("job_retry", job_id=jobs["echo 되살리기"])
            log("retry", f"{jobs['echo 되살리기']} → revived={res['revived']}")
            res = await m_api.tool("job_retry", job_id=jobs["echo Fatal"])
            log("retry", f"{jobs['echo Fatal']} (영구 실패) → revived={res['revived']} · {res['reason']}")
            ACTOR.set("")
            await settle([jobs["echo 되살리기"]])

            banner("3. 종료 신호: 오래 걸리는 echo 하나를 넣고 바로 stop")
            jobs["종료 중 진행 echo"] = await create("종료 중 진행 echo", "echo",
                                                {"text": "끝까지 기다려 줌", "sleep": 1.5}, rt.stream)
            await asyncio.sleep(1.0)
            stop.set()
            await asyncio.gather(*tasks)

            banner("결과")
            for label, j in jobs.items():
                row = await m_api.tool("job_get", job_id=j)
                extra = ""
                if row["state"] == "failed":
                    extra = f"사유={row['fail_reason']}  다시 시도 {'가능' if row['user_retryable'] else '불가'}"
                elif row.get("fail_reason"):
                    extra = f"(마지막 오류: {row['fail_reason']})"
                print(f"  {label:<18} {row['state']:<6} 시도={row['attempts']}  {extra}")
            note = await m_api.tool("note_get", idem_key=jobs["course_update"])
            print(f"  과목 노트: {note['note']!r}  user_id={note['user_id']} (permit에서 꺼냄)")
            for s in (rt.stream, batch.stream):
                print(f"  {s}: ACK 안 된 메시지 {(await r.xpending(s, S.GROUP))['pending']}개")
    finally:
        server.terminate()


if __name__ == "__main__":
    asyncio.run(main())
