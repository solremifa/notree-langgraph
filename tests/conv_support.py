"""대화 메모리 시험용 Redis 준비.

- FakeRedis: fakeredis TCP 서버 (MCP 서버가 하위 프로세스라 메모리 속 가짜는 공유가 안 됨). 재시작 대신 초기화(FLUSHALL)만
- RealRedis: **시험 전용** 실제 Redis. PATH에 redis-server가 있으면 그것을, 없으면 docker `redis:7-alpine` 컨테이너.
  기존 정책대로 RDB/AOF를 끈다 (`--save "" --appendonly no`). 재시작·초기화는 이 Redis에만 한다
- Proxy: MCP 서버 ↔ Redis 사이 TCP 중계. cut()이면 연결을 끊고 새 연결을 바로 닫는다 (Redis 장애 흉내)
"""
import shutil
import socket
import subprocess
import threading
import time

import redis


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_ping(url: str, timeout: float = 20) -> None:
    end = time.time() + timeout
    while time.time() < end:
        try:
            with redis.Redis.from_url(url, socket_timeout=1, socket_connect_timeout=1) as c:
                if c.ping():
                    return
        except redis.RedisError:
            time.sleep(0.2)
    raise RuntimeError(f"Redis가 뜨지 않음: {url}")


class FakeRedis:
    kind = "fake"
    can_restart = False

    def __init__(self) -> None:
        from fakeredis import TcpFakeServer
        self.port = free_port()
        self.srv = TcpFakeServer(("127.0.0.1", self.port), server_type="redis")
        # 연결마다 생기는 처리 스레드도 데몬으로: 아니면 pytest 종료 때 닫히지 않은 연결의 스레드를 기다리며 멈춘다
        # (2026-10-20 Linux 실행에서 재현: 주 스레드 futex 대기 + 같은 프로세스 안 loopback 연결)
        self.srv.daemon_threads = True
        self.srv.block_on_close = False
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()   # shutdown()은 멈추므로 데몬으로 둔다
        self.url = f"redis://127.0.0.1:{self.port}/0"
        _wait_ping(self.url)

    def reset(self) -> None:
        with redis.Redis.from_url(self.url) as c:
            c.flushall()

    def close(self) -> None:
        pass


def docker_ok() -> bool:
    if not shutil.which("docker"):
        return False
    try:
        return subprocess.run(["docker", "info"], capture_output=True, timeout=20).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


def real_redis_unavailable() -> str | None:
    """실제 Redis 시험을 건너뛸 이유 (없으면 None)."""
    if shutil.which("redis-server") or docker_ok():
        return None
    return "redis-server도 docker도 없음"


class RealRedis:
    kind = "real"
    can_restart = True
    ARGS = ["--save", "", "--appendonly", "no"]

    def __init__(self) -> None:
        self.port = free_port()
        self.url = f"redis://127.0.0.1:{self.port}/0"
        self.binary = shutil.which("redis-server")
        self.proc = None
        self.name = f"notree-test-redis-{self.port}"
        if self.binary:
            self._start_binary()
        else:
            subprocess.run(["docker", "run", "-d", "--name", self.name, "-p", f"127.0.0.1:{self.port}:6379",
                            "redis:7-alpine", "redis-server", *self.ARGS], check=True, capture_output=True)
        _wait_ping(self.url)

    def _start_binary(self) -> None:
        self.proc = subprocess.Popen([self.binary, "--port", str(self.port), *self.ARGS],
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def reset(self) -> None:
        with redis.Redis.from_url(self.url) as c:
            c.flushall()

    def restart(self) -> None:
        """프로세스(컨테이너) 재시작. 영속화가 꺼져 있어 데이터가 모두 사라진다."""
        if self.binary:
            self.proc.terminate()
            self.proc.wait(5)
            self._start_binary()
        else:
            subprocess.run(["docker", "restart", "-t", "0", self.name], check=True, capture_output=True)
        _wait_ping(self.url)

    def close(self) -> None:
        if self.binary:
            self.proc.terminate()
            self.proc.wait(5)
        else:
            subprocess.run(["docker", "rm", "-f", self.name], capture_output=True)


class Proxy:
    """MCP 서버 → Redis 중계. cut(): 기존 연결을 끊고 새 연결은 받자마자 닫음. restore(): 다시 이어 줌."""

    def __init__(self, upstream_port: int) -> None:
        self.up, self.down, self.conns = upstream_port, False, []
        self.lsock = socket.socket()
        self.lsock.bind(("127.0.0.1", 0))
        self.lsock.listen(64)
        self.port = self.lsock.getsockname()[1]
        self.url = f"redis://127.0.0.1:{self.port}/0"
        self.lock = threading.Lock()
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self) -> None:
        while True:
            try:
                c, _ = self.lsock.accept()
            except OSError:
                return
            if self.down:
                c.close()
                continue
            try:
                u = socket.create_connection(("127.0.0.1", self.up))
            except OSError:
                c.close()
                continue
            with self.lock:
                self.conns += [c, u]
            threading.Thread(target=self._pipe, args=(c, u), daemon=True).start()
            threading.Thread(target=self._pipe, args=(u, c), daemon=True).start()

    @staticmethod
    def _pipe(a: socket.socket, b: socket.socket) -> None:
        try:
            while data := a.recv(65536):
                b.sendall(data)
        except OSError:
            pass
        finally:
            for s in (a, b):
                try:
                    s.close()
                except OSError:
                    pass

    def cut(self) -> None:
        self.down = True
        with self.lock:
            for s in self.conns:
                try:
                    s.shutdown(socket.SHUT_RDWR)
                    s.close()
                except OSError:
                    pass
            self.conns = []

    def restore(self) -> None:
        self.down = False

    def close(self) -> None:
        self.cut()
        self.lsock.close()
