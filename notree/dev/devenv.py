"""데모용 공통 준비: MCP 서버(HTTP)를 하위 프로세스로 띄우고, Redis(진짜 또는 fakeredis)를 연다."""
import os
import socket
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path

from notree.worker.mcp_client import SERVER


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@contextmanager
def mcp_server(db: Path, require_permit: bool = True):
    """MCP 서버를 HTTP로 띄우고 URL을 돌려준다. 블록을 나가면 종료."""
    port = free_port()
    proc = subprocess.Popen([sys.executable, str(SERVER), "--http"], stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL,
                            env={**os.environ, "NOTREE_MCP_PORT": str(port), "NOTREE_DB": str(db),
                                 "NOTREE_REQUIRE_PERMIT": "1" if require_permit else "0"})
    try:
        for _ in range(100):
            try:
                socket.create_connection(("127.0.0.1", port), 0.2).close()
                break
            except OSError:
                time.sleep(0.1)
        yield f"http://127.0.0.1:{port}/mcp"
    finally:
        proc.terminate()


def redis_client(url: str | None):
    """(redis 클라이언트, worker block_ms). url이 없으면 fakeredis (BLOCK이 이벤트 루프를 멈추므로 폴링)."""
    if url:
        import redis.asyncio as aioredis
        return aioredis.from_url(url, decode_responses=True), 1000
    import fakeredis
    return fakeredis.FakeAsyncRedis(decode_responses=True), 0
