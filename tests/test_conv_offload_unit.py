"""대화 Redis I/O 스레드 분리(conversation.run)의 상한·취소·종료, async 도구 래퍼(guarded). 같은 프로세스 안에서 확인한다."""
import asyncio
import os
import re
import sys
import threading
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from conv_support import FakeRedis, Proxy  # noqa: E402
from notree.common import settings as S  # noqa: E402
from notree.mcp_server import conversation as CONV  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def pool(monkeypatch):
    """스레드 2개, 대기 상한 3, 시한 1초. 시험 뒤 스레드·연결을 닫고 다음 시험이 새로 만들게."""
    CONV.shutdown(wait=True)
    monkeypatch.setattr(S, "CONV_REDIS_THREADS", 2)
    monkeypatch.setattr(S, "CONV_REDIS_MAX_PENDING", 3)
    monkeypatch.setattr(S, "CONV_REDIS_OP_DEADLINE_SEC", 1.0)
    monkeypatch.setattr(S, "CONV_REDIS_TIMEOUT_SEC", 0.3)
    yield
    CONV.shutdown(wait=True)


def conv_threads() -> list[threading.Thread]:
    return [t for t in threading.enumerate() if t.name.startswith("conv-redis")]


def test_run_does_not_block_the_event_loop(pool):
    async def t():
        ticks = 0

        async def ticker():
            nonlocal ticks
            while True:
                await asyncio.sleep(0.01)
                ticks += 1
        tk = asyncio.create_task(ticker())
        assert await CONV.run(lambda: time.sleep(0.5) or "ok") == "ok"
        tk.cancel()
        assert ticks >= 20, f"루프가 멈춤 (tick {ticks})"
    asyncio.run(t())


def test_threads_and_pending_are_bounded(pool):
    gate = threading.Event()
    running = []

    def blocked(i):
        running.append(i)
        gate.wait(5)
        return i

    async def t():
        tasks = [asyncio.create_task(CONV.run(blocked, i)) for i in range(3)]
        await asyncio.sleep(0.2)
        assert CONV.stats() == {"inflight": 3, "threads": 2} and len(running) == 2     # 2개 실행, 1개 대기
        t0 = time.perf_counter()
        with pytest.raises(CONV.MemoryUnavailable, match="밀림"):                        # 상한 초과는 기다리지 않음
            await CONV.run(blocked, 99)
        assert time.perf_counter() - t0 < 0.05
        gate.set()
        assert sorted(await asyncio.gather(*tasks)) == [0, 1, 2]
        assert CONV.stats()["inflight"] == 0
        assert len(conv_threads()) <= 2
    asyncio.run(t())


def test_queued_job_past_deadline_never_touches_redis(pool, monkeypatch):
    monkeypatch.setattr(S, "CONV_REDIS_OP_DEADLINE_SEC", 0.2)
    gate, touched = threading.Event(), []

    async def t():
        busy = [asyncio.create_task(CONV.run(gate.wait, 1)) for _ in range(2)]          # 스레드 2개 다 잡힘 (1초)
        await asyncio.sleep(0.05)
        with pytest.raises(CONV.MemoryUnavailable, match="시간 초과"):
            await CONV.run(touched.append, "x")
        await asyncio.gather(*busy)
    asyncio.run(t())
    assert touched == []


def test_cancel_keeps_slot_until_the_thread_really_finishes(pool):
    """기다리던 쪽이 취소돼도 이미 시작한 동기 I/O는 계속 돈다. 슬롯은 그것이 끝난 뒤에 돌아온다."""
    gate, finished = threading.Event(), threading.Event()

    def io():
        gate.wait(5)
        finished.set()

    async def t():
        task = asyncio.create_task(CONV.run(io))
        await asyncio.sleep(0.1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not finished.is_set() and CONV.stats()["inflight"] == 1                   # 취소 ≠ I/O 중단
        gate.set()
        for _ in range(100):
            if CONV.stats()["inflight"] == 0:
                break
            await asyncio.sleep(0.01)
        assert finished.is_set() and CONV.stats()["inflight"] == 0
    asyncio.run(t())


def test_cancel_before_start_releases_slot(pool):
    gate = threading.Event()

    async def t():
        busy = [asyncio.create_task(CONV.run(gate.wait, 5)) for _ in range(2)]
        await asyncio.sleep(0.05)
        queued = asyncio.create_task(CONV.run(lambda: "never"))
        await asyncio.sleep(0.05)
        queued.cancel()
        with pytest.raises(asyncio.CancelledError):
            await queued
        assert CONV.stats()["inflight"] == 2                                              # 대기 작업은 바로 반환
        gate.set()
        await asyncio.gather(*busy)
    asyncio.run(t())


def test_usable_from_different_event_loops_and_after_shutdown(pool):
    """클라이언트·스레드는 이벤트 루프에 묶이지 않는다: 루프를 바꿔도, shutdown 뒤에도 다시 쓸 수 있다."""
    assert asyncio.run(CONV.run(lambda: 1)) == 1
    assert asyncio.run(CONV.run(lambda: 2)) == 2
    CONV.shutdown(wait=True)
    assert CONV.stats() == {"inflight": 0, "threads": 0} and conv_threads() == []
    assert asyncio.run(CONV.run(lambda: 3)) == 3


def test_unresponsive_redis_is_bounded_and_shutdown_closes_connections(pool, monkeypatch):
    """실제 TCP: 응답 없는 Redis에 create·touch·read·apply·remove·drop이 시한 안에 MemoryUnavailable. 연결은 스레드 수 이하,
    shutdown 뒤 중계기에 남은 연결이 없다."""
    srv = FakeRedis()
    proxy = Proxy(srv.port)
    try:
        monkeypatch.setattr(S, "CONV_REDIS_URL", proxy.url)
        CONV.shutdown(wait=True)
        assert asyncio.run(CONV.run(CONV.create, "cv", 1, "u", "c")) > 0
        proxy.stall()
        calls = [(CONV.create, "cv", 1, "u", "c"), (CONV.touch, "cv", 1, "u", "c"), (CONV.read, "cv", 1, "u", "c"),
                 (CONV.apply_turns, "cv", 1, "u", "c", []), (CONV.remove_question, "cv", 1, "u", "c", "q"),
                 (CONV.drop, "cv", 1)]

        async def t():
            t0 = time.perf_counter()
            res = await asyncio.gather(*(CONV.run(*c) for c in calls), return_exceptions=True)
            return res, time.perf_counter() - t0
        res, took = asyncio.run(t())
        assert all(isinstance(r, CONV.MemoryUnavailable) for r in res), res
        assert took < 2 * (1.0 + 2 * 0.6) + 0.5, took                    # 스레드 2개 → 세 바퀴까지는 안 감 (상한 3)
        assert proxy.peak <= 2
        CONV.shutdown(wait=True)
        for _ in range(100):
            if proxy.active == 0:
                break
            time.sleep(0.02)
        assert proxy.active == 0 and conv_threads() == []
    finally:
        proxy.close()
        srv.close()


def test_guarded_awaits_async_tools_and_maps_errors():
    os.environ.setdefault("NOTREE_DB", str(ROOT / "out" / "offload-unit.db"))
    from notree.contracts.envelope import ErrorCode
    from notree.mcp_server import server

    async def ok_tool(x: int) -> dict:
        await asyncio.sleep(0)
        return {"ok": True, "data": x}

    async def rejecting() -> dict:
        await asyncio.sleep(0)
        raise server.Reject(ErrorCode.NOT_FOUND, "없음")

    async def bad() -> dict:
        raise ValueError("x")

    async def boom() -> dict:
        raise RuntimeError("예상 못 한 오류")

    g = server.guarded(ok_tool)
    assert asyncio.iscoroutinefunction(g) and g.__name__ == "ok_tool"
    assert asyncio.run(g(3)) == {"ok": True, "data": 3}
    assert asyncio.run(server.guarded(rejecting)())["code"] == "not_found"
    assert asyncio.run(server.guarded(bad)())["code"] == "invalid_args"
    with pytest.raises(RuntimeError):                                    # 모르는 오류는 숨기지 않는다 (기존과 같음)
        asyncio.run(server.guarded(boom)())
    tools = {t.name: t for t in asyncio.run(server.mcp.list_tools())}
    assert set(tools["qa_ask"].inputSchema["properties"]) == {"user_id", "course_id", "question", "conversation_id"}


def test_server_never_calls_conversation_io_directly():
    """서버 코드에서 대화 Redis 함수는 CONV.run(...)으로만 부른다 (이벤트 루프에서 동기 Redis 금지)."""
    src = (ROOT / "notree" / "mcp_server" / "server.py").read_text(encoding="utf-8")
    io = ("create", "touch", "read", "apply_turns", "remove_question", "drop", "client")
    direct = re.findall(rf"CONV\.({'|'.join(io)})\(", src)
    assert direct == [], f"동기 Redis 직접 호출: {direct}"
    for name in ("qa_conversation_start", "qa_conversation_close", "qa_conversation_get", "qa_ask", "qa_context",
                 "qa_complete", "qa_delete", "_sync_memory", "_context_turns", "_end_conversation",
                 "_qa_ask_in_conversation"):
        assert re.search(rf"^async def {name}\(", src, re.M), f"{name}이 async가 아님"
