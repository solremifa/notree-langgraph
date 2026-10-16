"""worker 진입점.

    uv run python -m notree.worker --stream batch --name A
    uv run python -m notree.worker --stream rt --name A --concurrency 8
    uv run python -m notree.worker --stream qa --name A --concurrency 16 --borrow-slots 4   # qa가 비면 rt 작업을 4개까지

compose에서는 같은 이미지를 command만 바꿔 worker-qa / worker-rt / worker-batch 세 서비스로 띄운다.
Ctrl+C(또는 SIGTERM) 한 번: 새 작업을 멈추고 진행 중인 작업을 기다렸다가 종료. 두 번: 즉시 종료.
"""
import argparse
import asyncio
import os
import signal
import socket

import redis.asyncio as aioredis

from notree.common import settings as S
from notree.worker.loop import Worker
from notree.worker.mcp_client import McpClient


def install_stop_signals(stop: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()

    def on_signal(*_):
        if stop.is_set():
            os._exit(130)                                  # 두 번째 신호: 즉시 종료
        loop.call_soon_threadsafe(stop.set)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, on_signal)         # Linux / macOS (컨테이너)
        except (NotImplementedError, RuntimeError):
            signal.signal(sig, on_signal)                   # Windows


async def main() -> None:
    p = argparse.ArgumentParser(description="Notree worker")
    p.add_argument("--stream", choices=sorted(S.STREAMS), required=True)
    p.add_argument("--name", default=socket.gethostname(), help="consumer 이름 (컨테이너마다 달라야 함)")
    p.add_argument("--concurrency", type=int, default=None)
    p.add_argument("--borrow-slots", type=int, default=None,
                   help="자기 레인이 비었을 때 다른 레인 작업을 빌려 쓸 최대 슬롯 수 (기본: 설정 BORROW, 0이면 고정)")
    a = p.parse_args()
    lent, slots = S.BORROW.get(a.stream, (None, 0))
    slots = slots if a.borrow_slots is None else a.borrow_slots

    stop = asyncio.Event()
    install_stop_signals(stop)
    r = aioredis.from_url(S.REDIS_URL, decode_responses=True)
    try:
        async with McpClient(url=S.MCP_URL) as mcp:
            await Worker(a.name, S.STREAMS[a.stream], r, mcp, concurrency=a.concurrency,
                         borrow=S.STREAMS[lent] if lent and slots else None, borrow_slots=slots).run(stop)
    finally:
        await r.aclose()


def run() -> None:
    asyncio.run(main())
