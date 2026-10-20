"""시험을 끝없이 기다리지 않고 돌린다. 전체 시험 한 번 = 이 스크립트 한 번 (중복 실행하지 않는다).

  uv run python -m notree.dev.pytest_guard                       # 전체 (tests/)
  uv run python -m notree.dev.pytest_guard tests/test_x.py -k y  # pytest 인자 그대로
  --total 1800   전체 시간 제한(초). 넘으면 Python 스택을 남기고 하위 프로세스 상태를 적은 뒤 강제 종료 → 종료 코드 124
  --grace 60     pytest 요약이 찍힌 뒤 프로세스가 실제로 끝나기를 기다리는 시간. 넘으면 '종료 정체' → 종료 코드 125
  --per-test 300 시험 하나가 이만큼 넘게 걸리면 (그 뒤로도 이 간격마다) 모든 스레드 스택을 남긴다 (실행은 계속)

기록 (out/test-logs/<시각>/, 지우지 않는다)
  pytest.log   pytest 출력 전체        timing.log  시험별 START/END·결과·소요 시간 (진행 중 시험 확인용)
  stacks.log   정체 때 Python 스택     procs.log   시간 초과·종료 정체 때 하위 프로세스 목록
  result.txt   요약 줄·종료 코드·요약 본 여부·프로세스 종료 여부

종료 코드: pytest 코드 그대로 (0 통과, 1 실패, …). 시간 초과 124, 요약 뒤 종료 정체 125.
"""
import argparse
import faulthandler
import os
import re
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

SUMMARY = re.compile(r"^=*\s*(\d+ \w+.*) in [\d.]+s")


# ── pytest 안에서 도는 쪽 (-p notree.dev.pytest_guard) ─────────────
# faulthandler 타이머는 프로세스에 하나뿐이라 (pytest의 faulthandler_timeout을 쓰면 아래 전체 타이머가 지워짐) 여기서 직접 건다:
# 시험 중에는 min(시험 한도, 남은 전체 시간)마다 반복, 시험 사이에는 남은 전체 시간.
_timing = _stacks = None
_deadline = _per_test = 0.0


def pytest_configure(config):
    global _timing, _stacks, _deadline, _per_test
    log_dir = os.environ.get("NOTREE_GUARD_DIR")
    if not log_dir:
        return
    _timing = open(Path(log_dir) / "timing.log", "a", encoding="utf-8", buffering=1)
    _stacks = open(Path(log_dir) / "stacks.log", "a", encoding="utf-8", buffering=1)
    _deadline = time.monotonic() + float(os.environ["NOTREE_GUARD_TOTAL"]) - 20     # 바깥이 죽이기 20초 전
    _per_test = float(os.environ["NOTREE_GUARD_PER_TEST"])
    _arm(_deadline - time.monotonic())


def _arm(seconds: float, repeat: bool = False) -> None:
    if _stacks:
        faulthandler.dump_traceback_later(max(seconds, 1), repeat=repeat, file=_stacks, exit=False)


def _stamp(*parts) -> None:
    if _timing:
        _timing.write(f"{time.strftime('%H:%M:%S')} {time.monotonic():.3f} " + " ".join(map(str, parts)) + "\n")


def pytest_runtest_logstart(nodeid, location):
    _stamp("START", nodeid)
    _arm(min(_per_test, _deadline - time.monotonic()), repeat=True)


def pytest_runtest_logfinish(nodeid, location):
    _arm(_deadline - time.monotonic())


def pytest_runtest_logreport(report):
    if report.when == "call" or report.outcome != "passed":
        _stamp("END", report.nodeid, report.when, report.outcome, f"{report.duration:.2f}s")


def pytest_unconfigure(config):
    _stamp("SESSION_END")
    if _stacks:                                      # 요약 뒤에도 프로세스가 안 끝나면 무엇을 기다리는지 남긴다
        grace = float(os.environ.get("NOTREE_GUARD_GRACE", "60"))
        _stacks.write(f"--- pytest 종료 {time.strftime('%H:%M:%S')}; {grace / 2:.0f}초 뒤에도 살아 있으면 아래에 스택 ---\n")
        _arm(grace / 2)


# ── 바깥 감시 쪽 ────────────────────────────────────────────────────
def _children(pid: int) -> str:
    if os.name == "nt":
        cmd = ["powershell", "-NoProfile", "-Command",
               "Get-CimInstance Win32_Process | Select-Object ProcessId,ParentProcessId,Name,CommandLine | "
               "Format-Table -AutoSize -Wrap | Out-String -Width 400"]
    else:
        cmd = ["ps", "-eo", "pid,ppid,stat,etime,args"]
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=30).stdout
    except (OSError, subprocess.TimeoutExpired) as e:
        return f"프로세스 목록 실패: {e}"
    return f"pytest pid={pid}\n{out}"


def _kill_tree(proc: subprocess.Popen) -> None:
    if os.name == "nt":
        subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)], capture_output=True)
    else:
        try:
            os.killpg(proc.pid, 9)
        except OSError:
            proc.kill()
    try:
        proc.wait(30)
    except subprocess.TimeoutExpired:
        pass


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--total", type=float, default=1800)
    ap.add_argument("--grace", type=float, default=60)
    ap.add_argument("--per-test", type=float, default=300)
    ap.add_argument("--logs", default="out/test-logs")
    args, rest = ap.parse_known_args()
    log_dir = Path(args.logs) / datetime.now().strftime("%Y%m%d-%H%M%S")
    log_dir.mkdir(parents=True, exist_ok=True)
    env = {**os.environ, "NOTREE_GUARD_DIR": str(log_dir), "NOTREE_GUARD_TOTAL": str(args.total),
           "NOTREE_GUARD_GRACE": str(args.grace), "NOTREE_GUARD_PER_TEST": str(args.per_test),
           "PYTHONUTF8": "1", "PYTHONIOENCODING": "utf-8"}
    cmd = [sys.executable, "-m", "pytest", "-p", "notree.dev.pytest_guard", "-rs", "--durations=25", *(rest or ["-q"])]
    print(f"[guard] 기록: {log_dir}\n[guard] {' '.join(cmd)}", flush=True)
    t0 = time.monotonic()
    proc = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            start_new_session=os.name != "nt")
    summary: dict = {}

    def pump():
        with open(log_dir / "pytest.log", "w", encoding="utf-8") as f:
            for raw in proc.stdout:
                line = raw.decode("utf-8", errors="replace")
                f.write(line)
                f.flush()
                sys.stdout.write(line)
                if m := SUMMARY.match(line.strip()):
                    summary.update(line=m.group(1), at=time.monotonic())
    reader = threading.Thread(target=pump, daemon=True)
    reader.start()

    status, code = "exited", None
    while code is None:
        try:
            code = proc.wait(timeout=1)
        except subprocess.TimeoutExpired:
            now = time.monotonic()
            if "at" in summary and now - summary["at"] > args.grace:
                status = "hang_after_summary"
            elif now - t0 > args.total:
                status = "timeout"
            else:
                continue
            (log_dir / "procs.log").write_text(_children(proc.pid), encoding="utf-8")
            _kill_tree(proc)
            code = 125 if status == "hang_after_summary" else 124
    reader.join(10)
    took = time.monotonic() - t0
    result = (f"status={status}\nexit_code={code}\nsummary_seen={'line' in summary}\n"
              f"summary={summary.get('line', '-')}\nprocess_exited={proc.poll() is not None}\nseconds={took:.1f}\n")
    (log_dir / "result.txt").write_text(result, encoding="utf-8")
    print(f"[guard] {result.replace(chr(10), '  ')}", flush=True)
    return code


if __name__ == "__main__":
    sys.exit(main())
