"""내부 MCP 서버.

이 프로세스만 DB(out/notree.db)를 연다. worker는 아래 도구를 MCP로 호출할 뿐이다 (A안).
도구 이름·입출력·응답 형식은 notree/contracts/ 가 기준이다 (팀 결정 8번).

  - 응답: {"ok": true, "data": ...} / {"ok": true, "data": ..., "duplicate": true} / {"ok": false, "code", "message", "retryable"}
  - worker용 도구는 permit을 받는다. 서버는 permit에서 job_id·worker·사용자 ID를 꺼낸다.
    사용자 ID는 인자로 받지 않는다 (실제 구현에서는 이 값으로 SET LOCAL app.user_id → RLS).
  - 체크포인트는 LangGraph가 직렬화한 바이트를 base64로 받아 그대로 저장한다 (서버는 LangGraph를 몰라도 됨).

실행
  stdio : 테스트·notree.dev.graph_run이 하위 프로세스로 자동으로 띄운다
  HTTP  : uv run python -m notree.mcp_server --http   (compose의 내부 mcp 컨테이너 역할, 기본 127.0.0.1:8765/mcp)
  확인  : uv run mcp dev notree/mcp_server/server.py

환경변수
  NOTREE_DB                  DB 파일 (기본 out/notree.db)
  NOTREE_REQUIRE_PERMIT=1    permit 없는 worker 도구 호출 거절 (기본 0: PoC 스크립트 호환)
  NOTREE_PERMIT_SECRET       permit 서명 키
  NOTREE_PERMIT_TTL_RT / _BATCH   permit 유효 시간 (기본 300 / 1800초, 팀 결정 4번). qa 레인은 RT 값을 쓴다
"""
import base64
import functools
import hashlib
import hmac
import json
import os
import sqlite3
import sys
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

from mcp.server.fastmcp import FastMCP

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))   # 저장소 루트 (파일 경로로 직접 띄워질 때 notree import)
from notree.common.settings import KIND_DEADLINE_SEC  # noqa: E402
from notree.contracts import tools as C  # noqa: E402
from notree.contracts.envelope import ErrorCode, err, ok  # noqa: E402

DB = Path(os.getenv("NOTREE_DB", "out/notree.db"))
DB.parent.mkdir(parents=True, exist_ok=True)

mcp = FastMCP("notree-internal", log_level="WARNING",
              host=os.getenv("NOTREE_MCP_HOST", "127.0.0.1"), port=int(os.getenv("NOTREE_MCP_PORT", "8765")))

# 작업 종류 → Stream(레인). qa = 사용자가 답을 기다리는 작업, rt = 실시간 요약, batch = 길어도 되는 작업
STREAM_OF = {
    "qa": "notree:qa", "grading": "notree:qa", "live_summary": "notree:rt",
    "final_note": "notree:batch", "course_update": "notree:batch",
}

# ── DB ───────────────────────────────────────────────────────────
SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs(
  job_id TEXT PRIMARY KEY, kind TEXT, payload TEXT, stream TEXT,
  state TEXT, lease_owner TEXT, lease_until REAL, attempts INTEGER DEFAULT 0,
  created REAL, fail_reason TEXT);
CREATE TABLE IF NOT EXISTS checkpoints(
  thread_id TEXT, ns TEXT, checkpoint_id TEXT, parent_id TEXT,
  ck_type TEXT, ck_b64 TEXT, meta_type TEXT, meta_b64 TEXT,
  PRIMARY KEY(thread_id, ns, checkpoint_id));
CREATE TABLE IF NOT EXISTS writes(
  thread_id TEXT, ns TEXT, checkpoint_id TEXT, task_id TEXT, idx INTEGER,
  channel TEXT, v_type TEXT, v_b64 TEXT, task_path TEXT,
  PRIMARY KEY(thread_id, ns, checkpoint_id, task_id, idx));
CREATE TABLE IF NOT EXISTS notes(
  idem_key TEXT PRIMARY KEY, course TEXT, note TEXT, status TEXT);
CREATE TABLE IF NOT EXISTS tool_log(
  id INTEGER PRIMARY KEY AUTOINCREMENT, tool TEXT, thread_id TEXT);
CREATE INDEX IF NOT EXISTS jobs_pending_lane ON jobs(state, stream, created);
CREATE TABLE IF NOT EXISTS qa_courses(course_id TEXT PRIMARY KEY, user_id TEXT, name TEXT);
CREATE TABLE IF NOT EXISTS qa_docs(
  course_id TEXT, source TEXT, doc_id TEXT, user_id TEXT, lecture_id TEXT, text TEXT, ref TEXT,
  PRIMARY KEY(course_id, source, doc_id));
CREATE TABLE IF NOT EXISTS qa_questions(
  question_id TEXT PRIMARY KEY, user_id TEXT, course_id TEXT, question TEXT, job_id TEXT,
  generation INTEGER DEFAULT 1, state TEXT, answer TEXT, answer_id TEXT, idem_key TEXT, created REAL, updated_at REAL);
"""
# 나중에 추가된 컬럼 (이전 버전 DB 호환)
ADDED_COLUMNS = [
    ("jobs", "user_id TEXT"), ("jobs", "updated_at REAL"), ("jobs", "user_retryable INTEGER"),
    ("jobs", "exec_epoch INTEGER DEFAULT 0"),   # 실행 세대: job_start마다 +1, 절대 되돌리지 않음 (job_retry도)
    ("jobs", "fence_max INTEGER DEFAULT 0"),    # Redis 소유 키 펜싱 번호 중 가장 큰 것 (늦은 job_start 거절)
    ("notes", "user_id TEXT"),
]


def _init_db() -> None:
    """서버 시작 때 한 번. 같은 DB 파일을 여러 서버 프로세스가 동시에 처음 만들 때 잠김이 날 수 있어 재시도한다.
    (테스트에서 stdio 서버 여러 개가 같은 파일을 쓰는 경우. 실제 배치는 MCP 서버 하나 + PG)"""
    for i in range(50):
        try:
            con = sqlite3.connect(DB, timeout=10)
            con.execute("PRAGMA journal_mode=WAL")
            con.executescript(SCHEMA)
            for table, col in ADDED_COLUMNS:
                try:
                    con.execute(f"ALTER TABLE {table} ADD COLUMN {col}")
                except sqlite3.OperationalError:
                    pass
            con.commit()
            con.close()
            return
        except sqlite3.OperationalError:
            time.sleep(0.1 * (i + 1))
    raise RuntimeError(f"DB 초기화 실패: {DB}")


def db() -> sqlite3.Connection:
    con = sqlite3.connect(DB, timeout=10)
    con.row_factory = sqlite3.Row
    return con


@contextmanager
def tx():
    """쓰기 트랜잭션. BEGIN IMMEDIATE로 시작해 '실행권 확인'과 '쓰기' 사이에 다른 쓰기가 끼어들지 못하게 한다.
    (PG로 옮기면 SELECT ... FOR UPDATE 또는 조건부 UPDATE 한 문장으로 같은 효과)"""
    con = sqlite3.connect(DB, timeout=10, isolation_level=None)
    con.row_factory = sqlite3.Row
    con.execute("BEGIN IMMEDIATE")
    try:
        yield con
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()


def _log(con, tool: str, thread_id: str) -> None:
    con.execute("INSERT INTO tool_log(tool, thread_id) VALUES(?,?)", (tool, thread_id))


# ── 오류 처리: 모든 도구를 감싸 공통 응답 형식으로 ──────────────────
class Reject(Exception):
    def __init__(self, code: ErrorCode, message: str) -> None:
        super().__init__(message)
        self.code = code


def guarded(fn):
    """도구 안에서 난 예외를 공통 거절 형식으로 바꾼다. (FastMCP는 functools.wraps로 원래 시그니처를 읽는다)"""
    @functools.wraps(fn)
    def wrapper(*a, **k):
        try:
            return fn(*a, **k)
        except Reject as e:
            return err(e.code, str(e))
        except sqlite3.OperationalError as e:
            return err(ErrorCode.UNAVAILABLE, f"DB: {e}")
        except (ValueError, KeyError, TypeError) as e:
            return err(ErrorCode.INVALID_ARGS, f"{type(e).__name__}: {e}")
    return wrapper


def tool(fn):
    """계약(notree.contracts.tools.TOOLS)에 등록된 이름만 도구로 공개한다."""
    if fn.__name__ not in C.TOOLS:
        raise RuntimeError(f"계약에 없는 도구: {fn.__name__}")
    return mcp.tool(description=C.TOOLS[fn.__name__].summary or None)(guarded(fn))


# ── permit (팀 결정 4번) ─────────────────────────────────────────
# 형식: base64url({"j": job_id, "w": worker, "a": 시도 번호, "x": 실행 세대, "u": user_id, "e": 만료}) + "." + HMAC 앞 32자
# 시도 번호(attempts)는 재시도 한도용이라 사용자 재시도(job_retry) 때 0으로 돌아간다. 실행권 확인은 되돌아가지 않는 실행 세대(x)로 한다.
# 서명은 '위조되지 않았다'만 증명한다. '지금도 실행권이 있다'는 쓰기 트랜잭션 안에서 _require_live()로 DB와 대조한다.
PERMIT_SECRET = os.getenv("NOTREE_PERMIT_SECRET", "dev-only-secret").encode()
PERMIT_TTL = {"notree:qa": float(os.getenv("NOTREE_PERMIT_TTL_RT", "300")),
              "notree:rt": float(os.getenv("NOTREE_PERMIT_TTL_RT", "300")),
              "notree:batch": float(os.getenv("NOTREE_PERMIT_TTL_BATCH", "1800"))}
REQUIRE_PERMIT = os.getenv("NOTREE_REQUIRE_PERMIT", "0") == "1"


def _sign(body: str) -> str:
    return hmac.new(PERMIT_SECRET, body.encode(), hashlib.sha256).hexdigest()[:32]


def _issue_permit(job_id: str, worker: str, attempt: int, epoch: int, user_id: str, stream: str) -> str:
    claims = {"j": job_id, "w": worker, "a": attempt, "x": epoch, "u": user_id,
              "e": int(time.time() + PERMIT_TTL.get(stream, 1800))}
    body = base64.urlsafe_b64encode(json.dumps(claims, separators=(",", ":")).encode()).decode().rstrip("=")
    return f"{body}.{_sign(body)}"


def _claims(permit: str | None, job_id: str) -> dict | None:
    """permit을 확인하고 claims를 돌려준다. permit이 없으면 None (REQUIRE_PERMIT=1이면 거절)."""
    if permit is None:
        if REQUIRE_PERMIT:
            raise Reject(ErrorCode.PERMIT_INVALID, "permit 필요")
        return None
    try:
        body, sig = permit.rsplit(".", 1)
        claims = json.loads(base64.urlsafe_b64decode(body + "=" * (-len(body) % 4)))
    except ValueError:
        raise Reject(ErrorCode.PERMIT_INVALID, "permit 형식 오류") from None
    if not hmac.compare_digest(sig, _sign(body)):
        raise Reject(ErrorCode.PERMIT_INVALID, "permit 서명 불일치")
    if claims.get("j") != job_id:
        raise Reject(ErrorCode.PERMIT_INVALID, "다른 작업의 permit")
    if claims.get("e", 0) < time.time():
        raise Reject(ErrorCode.PERMIT_INVALID, "permit 만료")
    return claims


def _is_live(con, claims: dict | None, job_id: str) -> bool:
    """permit의 실행자가 지금도 이 작업의 실행권을 가졌는지: running이고, 소유자와 실행 세대(exec_epoch)가 같아야 한다.
    permit 없이 부르는 경우(REQUIRE_PERMIT=0, 작업 밖에서 그래프만 돌릴 때)는 검사하지 않는다."""
    if claims is None:
        return True
    r = con.execute("SELECT state, lease_owner, exec_epoch FROM jobs WHERE job_id=?", (job_id,)).fetchone()
    return (bool(r) and r["state"] == "running" and r["lease_owner"] == claims["w"]
            and claims.get("x") is not None and r["exec_epoch"] == claims["x"])


def _require_live(con, claims: dict | None, job_id: str) -> None:
    """쓰기 도구용: 실행권이 없으면 거절 (lease를 잃은 이전 실행자, 이전 시도, 이미 끝난 작업)."""
    if not _is_live(con, claims, job_id):
        raise Reject(ErrorCode.PERMIT_INVALID, "실행권 없음 (다른 실행으로 넘어갔거나 이미 끝난 작업)")


# ── 체크포인트 (thread_id = job_id) ──────────────────────────────
def _row_to_ck(con, r) -> dict:
    ws = con.execute(
        "SELECT task_id, channel, v_type, v_b64, task_path, idx FROM writes "
        "WHERE thread_id=? AND ns=? AND checkpoint_id=? ORDER BY task_id, idx",
        (r["thread_id"], r["ns"], r["checkpoint_id"]),
    ).fetchall()
    return C.CheckpointItem(
        thread_id=r["thread_id"], ns=r["ns"], checkpoint_id=r["checkpoint_id"], parent_id=r["parent_id"],
        checkpoint=[r["ck_type"], r["ck_b64"]], metadata=[r["meta_type"], r["meta_b64"]],
        writes=[[w["task_id"], w["channel"], [w["v_type"], w["v_b64"]], w["task_path"], w["idx"]] for w in ws],
    ).model_dump()


@tool
def checkpoint_put(thread_id: str, ns: str, checkpoint_id: str, parent_id: str | None,
                   checkpoint: list[str], metadata: list[str], permit: str | None = None) -> dict:
    """단계(super-step)가 끝날 때 상태 전체를 저장한다."""
    claims = _claims(permit, thread_id)
    with tx() as con:
        _require_live(con, claims, thread_id)
        con.execute("INSERT OR REPLACE INTO checkpoints VALUES(?,?,?,?,?,?,?,?)",
                    (thread_id, ns, checkpoint_id, parent_id, *checkpoint, *metadata))
        _log(con, "checkpoint_put", thread_id)
    return ok(C.Empty())


@tool
def checkpoint_put_writes(thread_id: str, ns: str, checkpoint_id: str, task_id: str, task_path: str,
                          writes: list[list], permit: str | None = None) -> dict:
    """노드 하나의 결과(pending write) 저장. idx<0(특수 채널)은 덮어쓰고, idx>=0은 이미 있으면 유지."""
    claims = _claims(permit, thread_id)
    with tx() as con:
        _require_live(con, claims, thread_id)
        for idx, channel, v_type, v_b64 in writes:
            verb = "INSERT OR REPLACE" if idx < 0 else "INSERT OR IGNORE"
            con.execute(f"{verb} INTO writes VALUES(?,?,?,?,?,?,?,?,?)",
                        (thread_id, ns, checkpoint_id, task_id, idx, channel, v_type, v_b64, task_path))
        _log(con, "checkpoint_put_writes", thread_id)
    return ok(C.Empty())


@tool
def checkpoint_get(thread_id: str, ns: str = "", checkpoint_id: str | None = None,
                   permit: str | None = None) -> dict:
    """checkpoint_id가 없으면 최신. 없으면 found=false."""
    _claims(permit, thread_id)
    with db() as con:
        if checkpoint_id:
            r = con.execute("SELECT * FROM checkpoints WHERE thread_id=? AND ns=? AND checkpoint_id=?",
                            (thread_id, ns, checkpoint_id)).fetchone()
        else:
            r = con.execute("SELECT * FROM checkpoints WHERE thread_id=? AND ns=? "
                            "ORDER BY checkpoint_id DESC LIMIT 1", (thread_id, ns)).fetchone()
        _log(con, "checkpoint_get", thread_id)
        return ok(C.CheckpointGetOut(found=bool(r), item=_row_to_ck(con, r) if r else None))


@tool
def checkpoint_list(thread_id: str, ns: str | None = None, before: str | None = None, limit: int | None = None,
                    permit: str | None = None) -> dict:
    """최신 → 과거 순서."""
    _claims(permit, thread_id)
    q, args = "SELECT * FROM checkpoints WHERE thread_id=?", [thread_id]
    if ns is not None:
        q += " AND ns=?"; args.append(ns)
    if before:
        q += " AND checkpoint_id<?"; args.append(before)
    q += " ORDER BY checkpoint_id DESC"
    if limit:
        q += " LIMIT ?"; args.append(limit)
    with db() as con:
        rows = con.execute(q, args).fetchall()
        _log(con, "checkpoint_list", thread_id)
        return ok(C.CheckpointListOut(items=[_row_to_ck(con, r) for r in rows]))


@tool
def checkpoint_delete_thread(thread_id: str, permit: str | None = None) -> dict:
    """thread 하나의 체크포인트를 지운다."""
    claims = _claims(permit, thread_id)
    with tx() as con:
        _require_live(con, claims, thread_id)
        for t in ("checkpoints", "writes"):
            con.execute(f"DELETE FROM {t} WHERE thread_id=?", (thread_id,))
        _log(con, "checkpoint_delete_thread", thread_id)
    return ok(C.Empty())


@tool
def checkpoint_cleanup(older_than_sec: float = 7 * 24 * 3600) -> dict:
    """[scheduler] 완료·실패 후 older_than_sec 넘게 지난 작업의 체크포인트를 지운다 (팀 결정 9번)."""
    cutoff = time.time() - older_than_sec
    with db() as con:
        ids = [r["job_id"] for r in con.execute(
            "SELECT job_id FROM jobs WHERE state IN ('done','failed') AND updated_at < ?", (cutoff,))]
        n = 0
        for j in ids:
            if con.execute("SELECT 1 FROM checkpoints WHERE thread_id=? LIMIT 1", (j,)).fetchone():
                n += 1
            con.execute("DELETE FROM checkpoints WHERE thread_id=?", (j,))
            con.execute("DELETE FROM writes WHERE thread_id=?", (j,))
    return ok(C.CheckpointCleanupOut(deleted_threads=n))


# ── 작업 · 실행 허가 · lease ─────────────────────────────────────
# 실제 설계에서 job_create·job_retry는 api가, job_reserve_pending·job_mark_sent·job_resend_stale는 scheduler가
# PG에 직접 하는 일이다. 이 PoC는 DB를 MCP 서버 하나에 모아 두어 같은 서버의 도구로 흉내 낸다.
def _set(con, job_id: str, **cols) -> int:
    cols["updated_at"] = time.time()
    sets = ", ".join(f"{k}=?" for k in cols)
    return con.execute(f"UPDATE jobs SET {sets} WHERE job_id=?", (*cols.values(), job_id)).rowcount


@tool
def job_create(kind: str, payload: dict, user_id: str, stream: str | None = None) -> dict:
    """[api] 업무 데이터와 같은 트랜잭션에 '의도(intent)'를 남긴다. 큐에는 아직 안 간다."""
    with db() as con:
        job_id = _insert_job(con, kind, payload, user_id, stream)
    return ok(C.JobCreateOut(job_id=job_id))


def _insert_job(con, kind: str, payload: dict, user_id: str, stream: str | None = None) -> str:
    """작업 의도 한 행. 업무 데이터를 쓰는 트랜잭션 안에서 부른다 (qa_ask처럼)."""
    job_id = f"op-{uuid.uuid4().hex[:8]}"
    now = time.time()
    if kind in KIND_DEADLINE_SEC and payload.get("deadline_at") is None:   # Q&A·채점: 요청부터 전체 90초
        payload = {**payload, "deadline_at": now + KIND_DEADLINE_SEC[kind]}
    con.execute("INSERT INTO jobs(job_id, kind, payload, stream, state, created, updated_at, user_id) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (job_id, kind, json.dumps(payload, ensure_ascii=False),
                 stream or STREAM_OF.get(kind, "notree:batch"), "pending", now, now, user_id))
    _log(con, "job_create", job_id)
    return job_id


@tool
def job_reserve_pending(limit: int = 10, stream: str | None = None) -> dict:
    """[scheduler 배정기] 대기 중인 의도 → 실행 허가(reserved). 동시 상한·예산 검사가 들어갈 자리.
    stream을 주면 그 레인만 고른다. 어떤 레인을 얼마나 먼저 고를지는 배정기(애플리케이션)가 정한다."""
    with db() as con:
        if stream is None:
            rows = con.execute("SELECT job_id, kind, stream FROM jobs WHERE state='pending' ORDER BY created LIMIT ?",
                               (limit,)).fetchall()
        else:
            rows = con.execute("SELECT job_id, kind, stream FROM jobs WHERE state='pending' AND stream=? "
                               "ORDER BY created LIMIT ?", (stream, limit)).fetchall()
        for r in rows:
            _set(con, r["job_id"], state="reserved")
            _log(con, "job_reserve_pending", r["job_id"])
        return ok(C.JobListOut(jobs=[C.ReservedJob(**dict(r)) for r in rows]))


@tool
def job_mark_sent(job_id: str) -> dict:
    """[scheduler 배정기] XADD 후 '전송 완료'로 표시."""
    with db() as con:
        cur = con.execute("UPDATE jobs SET state='sent', updated_at=? WHERE job_id=? AND state='reserved'",
                          (time.time(), job_id))
        _log(con, "job_mark_sent", job_id)
        return ok(C.JobMarkSentOut(marked=cur.rowcount == 1))


@tool
def job_resend_stale(reserved_older_than_sec: float = 30, sent_older_than_sec: float = 120, limit: int = 50) -> dict:
    """[scheduler] 허가 후 XADD 전에 죽었거나, 메시지가 유실돼 멈춘 의도를 다시 보낼 목록 (팀 결정 6번).
    돌려준 작업은 updated_at을 갱신해 다음 주기에 또 나오지 않게 한다. 중복 메시지는 job_start가 걸러 낸다."""
    now = time.time()
    with db() as con:
        rows = con.execute(
            "SELECT job_id, kind, stream FROM jobs WHERE (state='reserved' AND updated_at < ?) "
            "OR (state='sent' AND updated_at < ?) ORDER BY updated_at LIMIT ?",
            (now - reserved_older_than_sec, now - sent_older_than_sec, limit)).fetchall()
        for r in rows:
            _set(con, r["job_id"], state="sent")
            _log(con, "job_resend_stale", r["job_id"])
        return ok(C.JobListOut(jobs=[C.ReservedJob(**dict(r)) for r in rows]))


@tool
def job_start(job_id: str, worker: str, lease_sec: float, fence: int | None = None) -> dict:
    """[worker] 실행 시작. 결과는 data.status로: started(성공, permit 발급) / done / failed / leased / unknown.
    lease가 살아 있으면 **같은 worker라도** leased로 거절한다 (자기 메시지를 다시 회수해 중복 실행하는 것 방지).
    lease_sec=0 (LIVENESS=redis): 실행 중 판단은 worker가 Redis 소유 키로 이미 했다. 여기서는 실행 세대만 올려
    이전 실행자의 permit을 무효로 만든다 (lease_until=지금 → 다음 job_start가 leased로 막지 않음)."""
    now = time.time()
    with tx() as con:
        r = con.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
        _log(con, "job_start", job_id)
        if not r:
            return ok(C.JobStartOut(status="unknown"))
        if r["state"] in ("done", "failed"):
            return ok(C.JobStartOut(status=r["state"]))
        if fence is not None and fence <= (r["fence_max"] or 0):   # 소유권을 잃은 뒤 늦게 온 시작 요청
            return ok(C.JobStartOut(status="stale_fence", fence_max=r["fence_max"]))
        if r["state"] == "running" and (r["lease_until"] or 0) > now:
            return ok(C.JobStartOut(status="leased", owner=r["lease_owner"],
                                    lease_left_sec=round(r["lease_until"] - now, 1)))
        epoch = (r["exec_epoch"] or 0) + 1
        _set(con, job_id, state="running", lease_owner=worker, lease_until=now + lease_sec,
             attempts=r["attempts"] + 1, exec_epoch=epoch,
             **({"fence_max": fence} if fence is not None else {}))
        return ok(C.JobStartOut(
            status="started", kind=r["kind"], payload=json.loads(r["payload"]), attempt=r["attempts"] + 1,
            previous_owner=r["lease_owner"],
            permit=_issue_permit(job_id, worker, r["attempts"] + 1, epoch, r["user_id"] or "anonymous", r["stream"])))


@tool
def job_heartbeat(job_id: str, worker: str, lease_sec: float, permit: str | None = None) -> dict:
    """[worker] lease 연장. renewed=false면 실행권을 잃은 것 (다른 worker·다른 시도로 넘어감)."""
    claims = _claims(permit, job_id)
    with tx() as con:
        _log(con, "job_heartbeat", job_id)        # 호출 집계에 빠져 있던 것 (DB를 두드리는데 계측에 0으로 보였음)
        if not _is_live(con, claims, job_id):
            return ok(C.JobHeartbeatOut(renewed=False))
        cur = con.execute("UPDATE jobs SET lease_until=? WHERE job_id=? AND lease_owner=? AND state='running'",
                          (time.time() + lease_sec, job_id, worker))
        return ok(C.JobHeartbeatOut(renewed=cur.rowcount == 1))


@tool
def job_finish(job_id: str, worker: str, permit: str | None = None) -> dict:
    """[worker] 완료 표시. finished=true를 확인한 뒤에만 XACK한다. 실행권이 없으면 finished=false."""
    claims = _claims(permit, job_id)
    with tx() as con:
        if not _is_live(con, claims, job_id):
            return ok(C.JobFinishOut(finished=False))
        cur = con.execute("UPDATE jobs SET state='done', lease_until=NULL, updated_at=? "
                          "WHERE job_id=? AND lease_owner=? AND state='running'", (time.time(), job_id, worker))
        _log(con, "job_finish", job_id)
        return ok(C.JobFinishOut(finished=cur.rowcount == 1))


@tool
def job_fail(job_id: str, worker: str, reason: str, user_retryable: bool, permit: str | None = None) -> dict:
    """[worker] 실패 확정. failed=true를 확인한 뒤에만 XACK. user_retryable은 화면의 '다시 시도' 버튼 근거."""
    claims = _claims(permit, job_id)
    with tx() as con:
        if not _is_live(con, claims, job_id):
            return ok(C.JobFailOut(failed=False))
        cur = con.execute("UPDATE jobs SET state='failed', lease_until=NULL, fail_reason=?, user_retryable=?, "
                          "updated_at=? WHERE job_id=? AND lease_owner=? AND state='running'",
                          (reason, int(user_retryable), time.time(), job_id, worker))
        _log(con, "job_fail", job_id)
        return ok(C.JobFailOut(failed=cur.rowcount == 1))


@tool
def job_release(job_id: str, worker: str, reason: str, permit: str | None = None) -> dict:
    """[worker] 일시적 실패. lease를 바로 반납해 재배송 때 누구든 곧바로 잡을 수 있게 한다. ACK는 안 함."""
    claims = _claims(permit, job_id)
    with tx() as con:
        if not _is_live(con, claims, job_id):
            return ok(C.JobReleaseOut(released=False))
        cur = con.execute("UPDATE jobs SET state='sent', lease_until=NULL, fail_reason=?, updated_at=? "
                          "WHERE job_id=? AND lease_owner=? AND state='running'",
                          (reason, time.time(), job_id, worker))
        _log(con, "job_release", job_id)
        return ok(C.JobReleaseOut(released=cur.rowcount == 1))


@tool
def job_retry(job_id: str, deadline_at: float | None = None) -> dict:
    """[api] 사용자의 '다시 시도'. 다시 해도 되는 실패만 pending으로 되살린다. 체크포인트는 그대로라 이어서 실행된다.
    마감이 있던 작업은 deadline_at으로 새 마감을 준다. 안 주면 Q&A·채점은 지금부터 정책 마감(90초),
    그 밖의 작업은 지난 마감 그대로라 바로 다시 실패한다."""
    with db() as con:
        r = con.execute("SELECT state, user_retryable, payload, kind FROM jobs WHERE job_id=?", (job_id,)).fetchone()
        if not r:
            raise Reject(ErrorCode.NOT_FOUND, job_id)
        if r["state"] != "failed":
            return ok(C.JobRetryOut(revived=False, reason=f"failed 상태가 아님 ({r['state']})"))
        if not r["user_retryable"]:
            return ok(C.JobRetryOut(revived=False, reason="영구 실패라 다시 시도할 수 없음"))
        payload = json.loads(r["payload"])
        if deadline_at is None and r["kind"] in KIND_DEADLINE_SEC:
            deadline_at = time.time() + KIND_DEADLINE_SEC[r["kind"]]
        if deadline_at is not None:
            payload["deadline_at"] = deadline_at
        _set(con, job_id, state="pending", attempts=0, lease_owner=None, lease_until=None, fail_reason=None,
             user_retryable=None, payload=json.dumps(payload, ensure_ascii=False))
        _log(con, "job_retry", job_id)
        return ok(C.JobRetryOut(revived=True))


@tool
def job_get(job_id: str) -> dict:
    """[dev/api] 작업 상태 조회."""
    with db() as con:
        r = con.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
        if not r:
            return ok(C.JobRow(found=False))
        d = dict(r)
        d["payload"] = json.loads(d["payload"])
        d["user_retryable"] = None if d["user_retryable"] is None else bool(d["user_retryable"])
        return ok(C.JobRow(found=True, **d))


# ── 발행 ─────────────────────────────────────────────────────────
@tool
def note_publish(idem_key: str, course: str, note: str, status: str, permit: str | None = None) -> dict:
    """멱등키로 한 번만 발행. 두 번째부터는 duplicate=true (성공). 사용자 ID는 permit에서 꺼낸다.
    실제 설계의 발행 게이트(세대·출처 확인)는 여기서 한다."""
    claims = _claims(permit, idem_key)
    user = claims["u"] if claims else "anonymous"
    with tx() as con:
        _require_live(con, claims, idem_key)          # lease를 잃은 이전 실행자의 발행을 막는다
        cur = con.execute("INSERT OR IGNORE INTO notes(idem_key, course, note, status, user_id) VALUES(?,?,?,?,?)",
                          (idem_key, course, note, status, user))
        _log(con, "note_publish", idem_key)
        return ok(C.NotePublishOut(idem_key=idem_key), duplicate=cur.rowcount == 0)


@tool
def note_get(idem_key: str) -> dict:
    """[dev] 발행된 노트 조회."""
    with db() as con:
        r = con.execute("SELECT * FROM notes WHERE idem_key=?", (idem_key,)).fetchone()
        return ok(C.NoteGetOut(found=True, **{k: r[k] for k in ("user_id", "course", "note", "status")})
                  if r else C.NoteGetOut(found=False))


# ── Q&A (PoC: SQLite + 키워드 일치 점수. 실제는 PG + pg_trgm, 웹은 외부 공급자) ─────────
# 사용자 경계: worker 도구는 permit의 사용자(u)와 질문의 사용자가 같아야 한다 (RLS 흉내). 다르면 not_found.
def _qa_question(con, question_id: str, permit: str | None) -> tuple[sqlite3.Row, dict | None]:
    q = con.execute("SELECT * FROM qa_questions WHERE question_id=?", (question_id,)).fetchone()
    if not q or q["state"] == "deleted":
        raise Reject(ErrorCode.NOT_FOUND, f"질문 없음: {question_id}")
    claims = _claims(permit, q["job_id"])
    if claims is not None and claims.get("u") != q["user_id"]:
        raise Reject(ErrorCode.NOT_FOUND, f"질문 없음: {question_id}")      # 남의 질문은 '없음'과 구별하지 않는다
    return q, claims


def _score(text: str, keywords: list[str]) -> float:
    if not keywords:
        return 0.0
    return sum(1 for k in keywords if k.lower() in text.lower()) / len(keywords)


@tool
def qa_seed(user_id: str, course_id: str, course_name: str, docs: list[dict]) -> dict:
    """[dev] 검색용 문서 넣기. 같은 (과목, 출처, id)는 덮어쓴다."""
    with tx() as con:
        con.execute("INSERT OR REPLACE INTO qa_courses VALUES(?,?,?)", (course_id, user_id, course_name))
        for d in docs:
            if d["source"] not in ("note", "transcript", "web"):
                raise ValueError(f"source: {d['source']}")
            con.execute("INSERT OR REPLACE INTO qa_docs VALUES(?,?,?,?,?,?,?)",
                        (course_id, d["source"], d["id"], user_id, d.get("lecture_id"), d["text"], d.get("ref") or d["id"]))
    return ok(C.QaSeedOut(inserted=len(docs)))


@tool
def qa_ask(user_id: str, course_id: str, question: str) -> dict:
    """[api] 질문 행과 qa 작업 의도를 한 트랜잭션에. 마감은 job_create와 같은 규칙(요청 + 90초)."""
    question_id = f"q-{uuid.uuid4().hex[:8]}"
    now = time.time()
    with tx() as con:
        c = con.execute("SELECT user_id FROM qa_courses WHERE course_id=?", (course_id,)).fetchone()
        if not c or c["user_id"] != user_id:
            raise Reject(ErrorCode.NOT_FOUND, f"과목 없음: {course_id}")
        job_id = _insert_job(con, "qa", {"question_id": question_id}, user_id)
        con.execute("INSERT INTO qa_questions(question_id, user_id, course_id, question, job_id, state, created, updated_at) "
                    "VALUES(?,?,?,?,?,'pending',?,?)", (question_id, user_id, course_id, question, job_id, now, now))
    return ok(C.QaAskOut(question_id=question_id, job_id=job_id))


@tool
def qa_context(question_id: str, permit: str | None = None) -> dict:
    """[worker] 질문·과목·강의 목록·세대."""
    with db() as con:
        q, _ = _qa_question(con, question_id, permit)
        _log(con, "qa_context", q["job_id"])
        c = con.execute("SELECT name FROM qa_courses WHERE course_id=?", (q["course_id"],)).fetchone()
        lectures = [r["lecture_id"] for r in con.execute(
            "SELECT DISTINCT lecture_id FROM qa_docs WHERE course_id=? AND lecture_id IS NOT NULL ORDER BY lecture_id",
            (q["course_id"],))]
        return ok(C.QaContextOut(question=q["question"], course_id=q["course_id"], course_name=c["name"] if c else "",
                                 lecture_ids=lectures, generation=q["generation"],
                                 answered=q["state"] == "answered" and q["idem_key"] == q["job_id"]))


@tool
def qa_search(question_id: str, source: str, keywords: list[str], limit: int = 3,
              lecture_ids: list[str] | None = None, permit: str | None = None) -> dict:
    """[worker] 노트·전사 근거 검색 (읽기). 범위는 질문의 사용자·과목."""
    if source not in ("note", "transcript"):
        raise ValueError(f"source: {source} (웹은 web_search)")
    with db() as con:
        q, _ = _qa_question(con, question_id, permit)
        _log(con, "qa_search", q["job_id"])
        sql, args = "SELECT * FROM qa_docs WHERE course_id=? AND user_id=? AND source=?", [q["course_id"], q["user_id"], source]
        if lecture_ids:
            sql += f" AND lecture_id IN ({','.join('?' * len(lecture_ids))})"
            args += lecture_ids
        hits = [C.QaHit(id=r["doc_id"], source=source, text=r["text"], score=round(_score(r["text"], keywords), 3),
                        ref=r["ref"], lecture_id=r["lecture_id"]) for r in con.execute(sql, args)]
        hits = sorted((h for h in hits if h.score > 0), key=lambda h: -h.score)[:limit]
        return ok(C.QaSearchOut(hits=hits))


@tool
def web_search(question_id: str, query: str, limit: int = 3, permit: str | None = None) -> dict:
    """[worker] 웹 검색 1회. PoC는 그 과목에 시드한 web 문서에서 찾는 가짜."""
    words = [w for w in query.split() if w]
    with db() as con:
        q, _ = _qa_question(con, question_id, permit)
        _log(con, "web_search", q["job_id"])
        rows = con.execute("SELECT * FROM qa_docs WHERE course_id=? AND source='web'", (q["course_id"],)).fetchall()
        res = [{"id": r["doc_id"], "title": r["doc_id"], "url": r["ref"], "snippet": r["text"],
                "score": round(_score(r["text"], words), 3), "fetched_at": time.time()} for r in rows]
        res = sorted((x for x in res if x["score"] > 0), key=lambda x: -x["score"])[:limit]
        return ok(C.WebSearchOut(results=res))


@tool
def qa_complete(idem_key: str, question_id: str, expected_generation: int, status: str, label: str, answer: str,
                sentences: list[dict], sources: list[dict], notice_code: str | None = None,
                permit: str | None = None) -> dict:
    """[worker] 답변 저장. 같은 idem_key는 처음 결과 그대로 duplicate=true. 실행권과 질문 세대를 확인한다."""
    C.QaCompleteIn(idem_key=idem_key, question_id=question_id, expected_generation=expected_generation, status=status,
                   label=label, answer=answer, sentences=sentences, sources=sources, notice_code=notice_code)  # 값 검사
    citations = list(dict.fromkeys(c for s in sentences for c in s["citations"]))   # 화면용 전체 인용 (순서, 중복 없음)
    missing = [c for c in citations if c not in {s["id"] for s in sources}]
    if missing:
        raise ValueError(f"출처에 없는 인용: {missing}")
    with tx() as con:
        q = con.execute("SELECT * FROM qa_questions WHERE question_id=?", (question_id,)).fetchone()
        if not q:
            raise Reject(ErrorCode.NOT_FOUND, f"질문 없음: {question_id}")
        claims = _claims(permit, q["job_id"])
        if claims is not None and claims.get("u") != q["user_id"]:
            raise Reject(ErrorCode.NOT_FOUND, f"질문 없음: {question_id}")
        if q["idem_key"] == idem_key:                              # 이미 저장됨 (응답을 못 받은 재시도 등)
            return ok(C.QaCompleteOut(answer_id=q["answer_id"]), duplicate=True)
        if q["state"] == "answered":                               # 다른 키로 이미 답함 → 덮어쓰지 않는다
            raise Reject(ErrorCode.GENERATION_MISMATCH, "이미 다른 실행이 답을 저장함")
        _require_live(con, claims, q["job_id"])
        if q["generation"] != expected_generation:                 # 삭제(state=deleted)도 세대가 올라가 여기서 걸린다
            raise Reject(ErrorCode.GENERATION_MISMATCH, f"질문이 바뀜 (세대 {q['generation']} ≠ {expected_generation})")
        answer_id = f"a-{uuid.uuid4().hex[:8]}"
        body = {"status": status, "label": label, "answer": answer, "sentences": sentences, "citations": citations,
                "sources": sources, "notice_code": notice_code}
        con.execute("UPDATE qa_questions SET state='answered', answer=?, answer_id=?, idem_key=?, updated_at=? "
                    "WHERE question_id=?", (json.dumps(body, ensure_ascii=False), answer_id, idem_key, time.time(), question_id))
        _log(con, "qa_complete", q["job_id"])
        return ok(C.QaCompleteOut(answer_id=answer_id))


@tool
def qa_get(question_id: str, user_id: str) -> dict:
    """[api] 화면 조회."""
    with db() as con:
        q = con.execute("SELECT * FROM qa_questions WHERE question_id=? AND user_id=?", (question_id, user_id)).fetchone()
        if not q:
            return ok(C.QaGetOut(found=False))
        if q["state"] == "deleted":                                # 본문은 돌려주지 않는다 (복사본도 이미 지움)
            return ok(C.QaGetOut(found=True, state="deleted", job_id=q["job_id"]))
        return ok(C.QaGetOut(found=True, question=q["question"], state=q["state"], job_id=q["job_id"],
                             answer=json.loads(q["answer"]) if q["answer"] else None))


@tool
def qa_delete(question_id: str, user_id: str) -> dict:
    """[api] 질문 삭제. 질문 원문·답변·출처 복사본을 지우고(보관 안 함), 세대를 올려 진행 중인 답변 저장을 막는다.
    행은 남긴다: 진행 중 작업의 저장이 generation_mismatch로 거절되고, 조회는 'deleted'로 답하게."""
    with tx() as con:
        cur = con.execute("UPDATE qa_questions SET state='deleted', generation=generation+1, question=NULL, answer=NULL, "
                          "updated_at=? WHERE question_id=? AND user_id=? AND state!='deleted'",
                          (time.time(), question_id, user_id))
        return ok(C.QaDeleteOut(deleted=cur.rowcount == 1))


@tool
def tool_stats(thread_id: str) -> dict:
    """[dev] 이 thread(=job)에 대해 각 도구가 몇 번 불렸는지."""
    with db() as con:
        rows = con.execute("SELECT tool, COUNT(*) n FROM tool_log WHERE thread_id=? GROUP BY tool",
                           (thread_id,)).fetchall()
        return ok(C.ToolStatsOut(counts={r["tool"]: r["n"] for r in rows}))


_init_db()


def main() -> None:
    mcp.run("streamable-http" if "--http" in sys.argv else "stdio")


if __name__ == "__main__":
    main()
