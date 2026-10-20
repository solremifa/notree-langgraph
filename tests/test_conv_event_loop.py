"""대화 메모리 Redis I/O가 MCP 이벤트 루프를 막지 않는지 (2026-10-20 후속, docs/changes/2026-10-20-qa-conversation-memory.md 9절).

MCP 서버 **하나**에 동시 요청을 보낸다. 대화 도구는 Redis 응답을 기다리고(시험용 TCP 중계기 Proxy가 요청 바이트를
늦게 넘기거나 아예 넘기지 않음 — 운영 Redis가 아니라 시험 전용 fakeredis/실제 Redis 앞), 그동안 무관한 짧은 업무 도구
(job_heartbeat)가 제때 답하는지 잰다. 기존 통합 시험은 api·worker가 각자 MCP 하위 프로세스를 써서 이 간섭을 못 본다.

수정 전(동기 도구 안에서 동기 Redis 호출)에는 heartbeat가 Redis 지연만큼(≈1.5초) 밀렸다.
"""
import asyncio
import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from conv_support import FakeRedis, Proxy, RealRedis, real_redis_unavailable  # noqa: E402
from notree.contracts.envelope import ToolError  # noqa: E402
from notree.worker.mcp_client import SERVER, McpClient  # noqa: E402

USER, COURSE = "u-loop", "c-loop"
_REAL_SKIP = real_redis_unavailable()


@pytest.fixture(scope="module")
def fake_redis():
    r = FakeRedis()
    yield r
    r.close()


@pytest.fixture(scope="module")
def real_redis():
    if _REAL_SKIP:
        pytest.skip(f"실제 Redis 시험 건너뜀: {_REAL_SKIP}")
    r = RealRedis()
    yield r
    r.close()


@pytest.fixture(params=["fake", "real"])
def redis(request):
    r = request.getfixturevalue(f"{request.param}_redis")
    r.reset()
    return r


@pytest.fixture
def proxy(redis):
    p = Proxy(redis.port)
    yield p
    p.close()


def server_env(tmp_path, url, **settings) -> dict:
    return {**os.environ, "NOTREE_DB": str(tmp_path / "loop.db"), "NOTREE_REQUIRE_PERMIT": "0",
            "NOTREE_CONV_REDIS_URL": url, **{k: str(v) for k, v in settings.items()}}


async def prepare(m) -> str:
    """과목 하나 + 실행 중 작업 하나 (heartbeat 대상)."""
    await m.tool("qa_seed", user_id=USER, course_id=COURSE, course_name="DB", docs=[])
    job = (await m.tool("job_create", kind="course_update", payload={}, user_id=USER))["job_id"]
    assert (await m.tool("job_start", job_id=job, worker="w", lease_sec=30))["status"] == "started"
    return job


async def timed(coro):
    t0 = time.perf_counter()
    try:
        return await coro, time.perf_counter() - t0
    except ToolError as e:
        return e, time.perf_counter() - t0


async def heartbeat_during(m, job, slow, delay=0.05):
    """slow(코루틴)를 띄우고 delay 뒤 같은 서버에 heartbeat. → (slow 결과, slow 시간, heartbeat 시간)"""
    task = asyncio.create_task(timed(slow))
    await asyncio.sleep(delay)
    hb, hb_sec = await timed(m.tool("job_heartbeat", job_id=job, worker="w", lease_sec=30))
    assert hb == {"renewed": True}
    res, slow_sec = await task
    return res, slow_sec, hb_sec


def test_slow_redis_does_not_delay_other_tools_on_same_server(tmp_path, proxy):
    """Redis가 1.5초 늦게 답하는 동안 같은 MCP 서버의 job_heartbeat가 바로 답한다. 대화 시작 자체는 성공한다."""
    async def t():
        env = server_env(tmp_path, proxy.url, NOTREE_CONV_REDIS_TIMEOUT_SEC=4, NOTREE_CONV_REDIS_OP_DEADLINE_SEC=8)
        async with McpClient(env=env) as m:
            job = await prepare(m)
            await m.tool("qa_conversation_start", user_id=USER, course_id=COURSE)      # 연결을 미리 맺어 둔다
            proxy.stall(1.5)
            res, slow_sec, hb_sec = await heartbeat_during(
                m, job, m.tool("qa_conversation_start", user_id=USER, course_id=COURSE))
            proxy.restore()
            assert "conversation_id" in res and slow_sec >= 1.4, (res, slow_sec)
            assert hb_sec < 0.5, f"heartbeat가 대화 Redis 대기에 막힘: {hb_sec:.2f}초 (대화 시작 {slow_sec:.2f}초)"
    asyncio.run(t())


def test_unresponsive_redis_fails_within_bound_on_every_conversation_path(tmp_path, proxy):
    """연결은 되는데 응답이 없는 Redis: 대화 경로마다 제한 시간 안에 끝나고(거절·unavailable), 그동안 heartbeat는 바로 답한다.
    DB 답변 저장(qa_complete)은 Redis 반영이 실패해도 성공으로 남는다 (memory=failed)."""
    async def t():
        env = server_env(tmp_path, proxy.url, NOTREE_CONV_REDIS_TIMEOUT_SEC=0.3, NOTREE_CONV_REDIS_OP_DEADLINE_SEC=1)
        bound = 1 + 2 * (0.3 + 0.3) + 1.0                     # 작업 시한 + 명령 하나 최악(재시도 1회) + 여유
        async with McpClient(env=env) as m:
            job = await prepare(m)
            cid = (await m.tool("qa_conversation_start", user_id=USER, course_id=COURSE))["conversation_id"]
            q = await m.tool("qa_ask", user_id=USER, course_id=COURSE, question="외래키?", conversation_id=cid)
            proxy.stall()
            calls = {
                "start": m.tool("qa_conversation_start", user_id=USER, course_id=COURSE),
                "ask": m.tool("qa_ask", user_id=USER, course_id=COURSE, question="또?", conversation_id=cid),
                "get": m.tool("qa_conversation_get", conversation_id=cid, user_id=USER),
                "context": m.tool("qa_context", question_id=q["question_id"]),
                "complete": m.tool("qa_complete", idem_key=q["job_id"], question_id=q["question_id"],
                                   expected_generation=1, status="completed", label="일반 지식", answer="참조 열",
                                   sentences=[{"text": "참조 열", "citations": []}], sources=[]),
            }
            out = {}
            for name, call in calls.items():
                out[name] = await heartbeat_during(m, job, call)
            out["close"] = await heartbeat_during(m, job, m.tool("qa_conversation_close", conversation_id=cid,
                                                                  user_id=USER))
            out["delete"] = await heartbeat_during(m, job, m.tool("qa_delete", question_id=q["question_id"],
                                                                   user_id=USER))
            proxy.restore()
            for name, (res, slow_sec, hb_sec) in out.items():
                assert slow_sec < bound, f"{name}: {slow_sec:.2f}초 (상한 {bound})"
                assert hb_sec < 0.5, f"{name} 중 heartbeat {hb_sec:.2f}초"
            for name in ("start", "ask"):
                assert isinstance(out[name][0], ToolError) and out[name][0].code == "unavailable", out[name][0]
            assert out["get"][0]["memory_state"] == "unavailable"
            assert out["context"][0]["memory_state"] == "unavailable"
            assert out["complete"][0]["memory"] == "failed"                               # 저장은 성공
            assert out["close"][0]["closed"] is True and out["close"][0]["memory_cleared"] is False
            assert out["delete"][0]["deleted"] is True
            assert (await m.tool("qa_get", question_id=q["question_id"], user_id=USER))["state"] == "deleted"
    asyncio.run(t())


def test_many_stuck_conversation_requests_are_bounded(tmp_path, proxy):
    """응답 없는 Redis에 대화 요청 20개가 한꺼번에 와도: Redis 연결은 스레드 수(2)를 넘지 않고, 대기 상한(4)을 넘는 요청은
    바로 unavailable, 나머지도 시한 안에 끝난다. 그동안 heartbeat는 바로 답한다."""
    async def t():
        env = server_env(tmp_path, proxy.url, NOTREE_CONV_REDIS_TIMEOUT_SEC=0.3, NOTREE_CONV_REDIS_OP_DEADLINE_SEC=1,
                         NOTREE_CONV_REDIS_THREADS=2, NOTREE_CONV_REDIS_MAX_PENDING=4)
        async with McpClient(env=env) as m:
            job = await prepare(m)
            cid = (await m.tool("qa_conversation_start", user_id=USER, course_id=COURSE))["conversation_id"]
            proxy.stall()
            t0 = time.perf_counter()
            tasks = [asyncio.create_task(timed(m.tool("qa_conversation_get", conversation_id=cid, user_id=USER)))
                     for _ in range(20)]
            await asyncio.sleep(0.05)
            _, hb_sec = await timed(m.tool("job_heartbeat", job_id=job, worker="w", lease_sec=30))
            done = await asyncio.gather(*tasks)
            total = time.perf_counter() - t0
            peak = proxy.peak
            proxy.restore()
            assert all(r["memory_state"] == "unavailable" for r, _ in done)
            fast = sum(1 for _, sec in done if sec < 0.3)
            assert fast >= 20 - 4, f"대기 상한을 넘은 요청이 바로 거절되지 않음: 빠른 거절 {fast}개"
            assert total < 2 * (1 + 2 * 0.6) + 1, f"전체 {total:.2f}초"            # 스레드 2개 × 대기 4개 → 최대 두 바퀴
            assert peak <= 2, f"Redis 동시 연결 {peak}개 (스레드 2개 상한)"
            assert hb_sec < 0.5, f"heartbeat {hb_sec:.2f}초"
    asyncio.run(t())


def _rpc(proc, msg: dict) -> None:
    proc.stdin.write((json.dumps(msg) + "\n").encode())
    proc.stdin.flush()


def test_server_exits_promptly_when_stdin_closes_during_stuck_redis_call(tmp_path, proxy):
    """Redis 무응답 중인 대화 요청이 남아 있어도 stdin을 닫으면 서버 프로세스가 시한 안에 스스로 끝난다
    (수명 종료 때 대화 메모리 스레드·연결 정리, 남은 스레드는 소켓 시간 제한으로 빠짐). 종료 코드도 확인한다."""
    env = server_env(tmp_path, proxy.url, NOTREE_CONV_REDIS_TIMEOUT_SEC=0.3, NOTREE_CONV_REDIS_OP_DEADLINE_SEC=1)
    proc = subprocess.Popen([sys.executable, str(SERVER)], env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    try:
        lines: list[dict] = []
        reader = threading.Thread(target=lambda: lines.extend(json.loads(x) for x in proc.stdout), daemon=True)
        reader.start()
        _rpc(proc, {"jsonrpc": "2.0", "id": 1, "method": "initialize",
                    "params": {"protocolVersion": "2025-06-18", "capabilities": {},
                               "clientInfo": {"name": "t", "version": "0"}}})
        _rpc(proc, {"jsonrpc": "2.0", "method": "notifications/initialized"})
        call = {"jsonrpc": "2.0", "method": "tools/call"}
        _rpc(proc, {**call, "id": 2, "params": {"name": "qa_seed", "arguments": {
            "user_id": USER, "course_id": COURSE, "course_name": "DB", "docs": []}}})
        end = time.time() + 10
        while len(lines) < 2 and time.time() < end:
            time.sleep(0.05)
        assert len(lines) >= 2, lines
        proxy.stall()
        _rpc(proc, {**call, "id": 3, "params": {"name": "qa_conversation_start", "arguments": {
            "user_id": USER, "course_id": COURSE}}})
        time.sleep(0.2)                                     # 요청이 Redis 대기에 들어가게
        t0 = time.time()
        proc.stdin.close()
        code = proc.wait(timeout=10)
        took = time.time() - t0
        assert took < 3.5, f"종료에 {took:.1f}초"         # 남은 요청 하나의 최악(시한 1 + 2×0.6) + 기동 여유
        assert code == 0, (code, proc.stderr.read().decode(errors="replace")[-2000:])
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(5)
        proxy.restore()
