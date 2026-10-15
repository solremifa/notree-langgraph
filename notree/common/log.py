"""역할(worker-A, scheduler …)이 붙는 한 줄 로그. 여러 프로세스 로그를 시간순으로 비교하려고 벽시계를 쓴다."""
import contextvars
import sys
import time

ACTOR = contextvars.ContextVar("actor", default="")   # 지금 이 코드를 돌리는 역할. asyncio 태스크·스레드로 전파됨


def log(topic: str, msg: str) -> None:
    now = time.time()
    stamp = time.strftime("%H:%M:%S", time.localtime(now)) + f".{int(now % 1 * 1000):03d}"
    actor = ACTOR.get()
    who = f"{actor:<10} " if actor else ""
    # 병렬 작업이 동시에 찍어도 줄이 섞이지 않게 한 번에 쓴다
    sys.stdout.write(f"  [{stamp}] {who}{topic:<13} {msg}\n")
    sys.stdout.flush()
