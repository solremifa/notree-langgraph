"""대화 메모리 시험용 Redis 준비.

- FakeRedis: fakeredis TCP 서버 (MCP 서버가 하위 프로세스라 메모리 속 가짜는 공유가 안 됨). 재시작 대신 초기화(FLUSHALL)만
- RealRedis: **시험 전용** 실제 Redis. PATH에 redis-server가 있으면 그것을, 없으면 docker `redis:7-alpine` 컨테이너.
  기존 정책대로 RDB/AOF를 끈다 (`--save "" --appendonly no`). 재시작·초기화는 이 Redis에만 한다
- Proxy: MCP 서버 ↔ Redis 사이 TCP 중계. cut()이면 연결을 끊고 새 연결을 바로 닫는다 (Redis 장애 흉내).
  stall(초)이면 요청 바이트를 그만큼 늦게 넘기고, stall()(무한)이면 넘기지 않는다 (연결은 되는데 응답이 없는 Redis)
"""
import math
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
        self.thread = threading.Thread(target=self.srv.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"redis://127.0.0.1:{self.port}/0"
        _wait_ping(self.url)

    def reset(self) -> None:
        with redis.Redis.from_url(self.url) as c:
            c.flushall()

    def close(self, timeout: float = 5) -> None:
        """서버를 실제로 내린다: shutdown()은 처리 스레드들에 종료 신호(_shutdown_event)를 주고 serve_forever 루프가 끝나길
        기다린다. 그 뒤 듣기 소켓을 닫고, 연결 처리 스레드가 모두 빠졌는지(clients가 비었는지) 시간 제한 안에서 확인한다.
        데몬 설정은 안전망일 뿐 — 남은 연결이 있으면 여기서 오류로 드러낸다."""
        stopper = threading.Thread(target=self.srv.shutdown, daemon=True)
        stopper.start()
        stopper.join(timeout)
        self.srv.server_close()
        self.thread.join(timeout)
        end = time.time() + timeout
        while self.srv.clients and time.time() < end:
            time.sleep(0.05)
        left = len(self.srv.clients)
        if stopper.is_alive() or self.thread.is_alive() or left:
            raise RuntimeError(f"fakeredis 서버 종료 실패: shutdown={not stopper.is_alive()} "
                               f"serve={not self.thread.is_alive()} 남은 연결={left}")


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
        self.delay = 0.0
        self.active = self.peak = self.accepted = 0      # 동시 연결 수 (자원 상한 확인용)
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
                self.accepted += 1
                self.active += 1
                self.peak = max(self.peak, self.active)
            threading.Thread(target=self._pipe, args=(c, u, True), daemon=True).start()
            threading.Thread(target=self._pipe, args=(u, c, False), daemon=True).start()

    def _pipe(self, a: socket.socket, b: socket.socket, upstream: bool) -> None:
        try:
            while data := a.recv(65536):
                if upstream and self.delay:
                    if math.isinf(self.delay):
                        continue    # 무응답: 버린다 (클라이언트는 시간 초과로 이 연결을 버리고, 닫히면 recv가 끝나 집계에서 빠짐)
                    else:
                        time.sleep(self.delay)
                b.sendall(data)
        except OSError:
            pass
        finally:
            if upstream:
                with self.lock:
                    self.active -= 1
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

    def stall(self, seconds: float = math.inf) -> None:
        """요청을 Redis로 늦게 넘긴다 (seconds초). 무한이면 넘기지 않는다: 연결은 되지만 응답이 오지 않는 Redis."""
        self.delay = seconds

    def restore(self) -> None:
        self.down = False
        self.delay = 0.0

    def close(self) -> None:
        self.cut()
        self.lsock.close()
