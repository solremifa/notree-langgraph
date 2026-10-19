"""실제 Redis가 필요한 시험의 공통 준비.

redis-server가 PATH에 있으면 시험마다 비영속(--save "" --appendonly no) 프로세스를 띄운다.
없으면(Windows 등) NOTREE_TEST_REDIS_URL로 **시험 전용** Redis를 줄 수 있다 — 시험이 FLUSHALL을 하므로 운영·개발 Redis를 주면 안 된다.
    docker run -d --name notree-test-redis -p 127.0.0.1:16399:6379 redis:7-alpine redis-server --save "" --appendonly no
    NOTREE_TEST_REDIS_URL=redis://127.0.0.1:16399/0 uv run pytest -q
프로세스를 직접 띄워야 하는 시험(스냅숏 복원·재시작·설정 확인)은 redis-server가 있어야 돈다 (EXTERNAL로는 건너뜀).
"""
import os
import shutil
import signal
import socket
import subprocess
import time

import pytest

EXTERNAL = os.getenv("NOTREE_TEST_REDIS_URL")
HAVE_SERVER = shutil.which("redis-server") is not None
HAVE_REDIS = HAVE_SERVER or bool(EXTERNAL)
needs_redis = pytest.mark.skipif(not HAVE_REDIS, reason="redis-server 없음 (NOTREE_TEST_REDIS_URL도 없음)")
needs_server = pytest.mark.skipif(not HAVE_SERVER, reason="redis-server 프로세스가 필요한 시험")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def kill_proc(p: subprocess.Popen) -> None:
    """강제 종료 (POSIX SIGKILL / Windows TerminateProcess — 자손까지). 정리 코드가 돌지 않는다."""
    if os.name == "nt":
        for pid in reversed(_win_tree(p.pid)):                       # 실제 인터프리터(자식)부터
            try:
                os.kill(pid, signal.SIGTERM)                          # Windows에서는 TerminateProcess
            except OSError:
                pass
    else:
        p.kill()
    p.wait(5)


def _win_tree(pid: int) -> list[int]:
    """pid와 그 자손 (Windows). venv의 python.exe는 실제 인터프리터를 자식으로 띄우는 실행기라 자식까지 멈춰야 한다."""
    import ctypes
    from ctypes import wintypes

    class PE(ctypes.Structure):
        _fields_ = [("dwSize", wintypes.DWORD), ("cntUsage", wintypes.DWORD), ("th32ProcessID", wintypes.DWORD),
                    ("th32DefaultHeapID", ctypes.c_void_p), ("th32ModuleID", wintypes.DWORD),
                    ("cntThreads", wintypes.DWORD), ("th32ParentProcessID", wintypes.DWORD),
                    ("pcPriClassBase", ctypes.c_long), ("dwFlags", wintypes.DWORD), ("szExeFile", ctypes.c_char * 260)]
    k = ctypes.windll.kernel32
    k.CreateToolhelp32Snapshot.restype = wintypes.HANDLE
    snap = k.CreateToolhelp32Snapshot(0x2, 0)                          # TH32CS_SNAPPROCESS
    parent: dict[int, int] = {}
    e = PE()
    e.dwSize = ctypes.sizeof(PE)
    ok = k.Process32First(snap, ctypes.byref(e))
    while ok:
        parent[e.th32ProcessID] = e.th32ParentProcessID
        ok = k.Process32Next(snap, ctypes.byref(e))
    k.CloseHandle(snap)
    tree, grew = {pid}, True
    while grew:
        grew = False
        for c, par in parent.items():
            if par in tree and c not in tree:
                tree.add(c)
                grew = True
    return sorted(tree)


def _nt(p: subprocess.Popen, fn: str) -> None:
    import ctypes
    k = ctypes.windll.kernel32
    k.OpenProcess.restype = ctypes.c_void_p
    for pid in _win_tree(p.pid):
        h = k.OpenProcess(0x0800, False, pid)                          # PROCESS_SUSPEND_RESUME
        if h:
            getattr(ctypes.windll.ntdll, fn)(ctypes.c_void_p(h))
            k.CloseHandle(ctypes.c_void_p(h))


def pause_proc(p: subprocess.Popen) -> None:
    """프로세스 전체 정지 (긴 GC·스왑·VM 정지 흉내). POSIX SIGSTOP / Windows NtSuspendProcess."""
    if os.name == "nt":
        _nt(p, "NtSuspendProcess")
    else:
        p.send_signal(signal.SIGSTOP)


def resume_proc(p: subprocess.Popen) -> None:
    if p.poll() is not None:
        return
    if os.name == "nt":
        _nt(p, "NtResumeProcess")
    else:
        p.send_signal(signal.SIGCONT)


_shared: dict[str, object] = {}


def shared_redis_url() -> str:
    """세션에 하나: 외부 시험 전용 Redis, 아니면 처음 부를 때 redis-server를 하나 띄움 (끝날 때 종료). 둘 다 없으면 건너뜀.
    LIVENESS=redis(기본)로 도는 일반 worker 시험(tests/test_skeleton.Env)이 쓴다."""
    if EXTERNAL:
        return EXTERNAL
    if not HAVE_SERVER:
        pytest.skip("LIVENESS=redis 시험에는 실제 Redis가 필요 (redis-server 또는 NOTREE_TEST_REDIS_URL). "
                    "DB 모드로 돌리려면 NOTREE_LIVENESS=db")
    if "url" not in _shared:
        import atexit
        port = free_port()
        p = subprocess.Popen(["redis-server", "--port", str(port), "--save", "", "--appendonly", "no"],
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(50):
            try:
                socket.create_connection(("127.0.0.1", port), timeout=0.1).close()
                break
            except OSError:
                time.sleep(0.1)
        atexit.register(p.terminate)
        _shared["url"] = f"redis://127.0.0.1:{port}/0"
    return _shared["url"]


def redis_url_fixture():
    """module 범위 fixture 본문: redis-server를 띄우거나, 없으면 시험 전용 외부 Redis."""
    if HAVE_SERVER:
        port = free_port()
        p = subprocess.Popen(["redis-server", "--port", str(port), "--save", "", "--appendonly", "no"],
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(50):
            try:
                socket.create_connection(("127.0.0.1", port), timeout=0.1).close()
                break
            except OSError:
                time.sleep(0.1)
        yield f"redis://127.0.0.1:{port}/0"
        p.terminate()
        p.wait(5)
    elif EXTERNAL:
        yield EXTERNAL
    else:
        pytest.skip("redis-server 없음 (NOTREE_TEST_REDIS_URL도 없음)")
