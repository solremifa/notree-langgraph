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
from contextlib import closing, contextmanager
from pathlib import Path

from mcp.server.fastmcp import FastMCP

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))   # 저장소 루트 (파일 경로로 직접 띄워질 때 notree import)
from notree.common import settings as S  # noqa: E402
from notree.common.settings import KIND_DEADLINE_SEC, STREAMS, max_attempts_for  # noqa: E402
from notree.contracts import tools as C  # noqa: E402
from notree.contracts.envelope import ErrorCode, err, ok  # noqa: E402
from notree.mcp_server import conversation as CONV  # noqa: E402

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
CREATE TABLE IF NOT EXISTS qa_conversations(
  conversation_id TEXT PRIMARY KEY, user_id TEXT, course_id TEXT, state TEXT, epoch INTEGER DEFAULT 1,
  next_seq INTEGER DEFAULT 1, active_question_id TEXT, last_user_at REAL, created REAL, ended_at REAL, end_reason TEXT);
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS lectures(
  lecture_id TEXT PRIMARY KEY, user_id TEXT, state TEXT, generation INTEGER DEFAULT 1,
  saved_until INTEGER DEFAULT 0, processed_until INTEGER DEFAULT 0, attempted_until INTEGER DEFAULT 0,
  rolling_summary TEXT DEFAULT '', summary_version INTEGER DEFAULT 0, updated_at REAL);
CREATE TABLE IF NOT EXISTS transcripts(lecture_id TEXT, seq INTEGER, text TEXT, PRIMARY KEY(lecture_id, seq));
CREATE TABLE IF NOT EXISTS summaries(
  idem_key TEXT PRIMARY KEY, lecture_id TEXT, version INTEGER, from_seq INTEGER, upto_seq INTEGER,
  generation INTEGER, text TEXT, created REAL);
"""
# 나중에 추가된 컬럼 (이전 버전 DB 호환)
# 실시간 요약: 강의당 '대기 중(pending)' 요약은 하나뿐 (DB 제약). 동시 커밋이 각각 "없다"고 보고 둘을 넣는 경쟁을 막는다.
# 실행 중(reserved·sent·running) 요약과 대기 1개는 함께 있을 수 있다 → 대기 1개가 그사이 들어온 전사를 모두 합친다.
AFTER_COLUMNS = """
CREATE UNIQUE INDEX IF NOT EXISTS jobs_one_pending_summary ON jobs(lecture_id)
  WHERE kind='live_summary' AND state='pending';
CREATE INDEX IF NOT EXISTS jobs_lecture ON jobs(lecture_id, kind, state);
"""
SUMMARY_IN_FLIGHT = ("reserved", "sent", "running")

ADDED_COLUMNS = [
    ("jobs", "user_id TEXT"), ("jobs", "updated_at REAL"), ("jobs", "user_retryable INTEGER"),
    ("jobs", "exec_epoch INTEGER DEFAULT 0"),   # 실행 세대: job_start마다 +1, 절대 되돌리지 않음 (job_retry도)
    ("jobs", "lecture_id TEXT"),                 # 실시간 요약 합치기용 (강의당 대기 1개)
    ("jobs", "dispatch_gen INTEGER"),            # 마지막으로 Redis에 보낸 전달 세대 (Redis 유실 복구 판정)
    ("notes", "user_id TEXT"),
    # Q&A 대화 (2026-10-20): 질문이 속한 대화, 대화 안 차례, 메시지 ID (등록 때 정해 재시도에도 같음)
    ("qa_questions", "conversation_id TEXT"), ("qa_questions", "turn_seq INTEGER"),
    ("qa_questions", "user_message_id TEXT"), ("qa_questions", "assistant_message_id TEXT"),
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
            con.executescript(AFTER_COLUMNS)
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


def _claims(permit: str | None, job_id: str | None) -> dict | None:
    """permit을 확인하고 claims를 돌려준다. permit이 없으면 None (REQUIRE_PERMIT=1이면 거절).
    job_id=None이면 어느 작업의 permit인지는 보지 않는다 (서명·만료만. 읽기 도구가 사용자 범위를 확인할 때)."""
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
    if job_id is not None and claims.get("j") != job_id:
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
def _gen(con) -> int:
    """현재 전달 세대. Redis 유실을 발견할 때마다(job_recover_orphans start) 1씩 오른다. 메시지와 jobs.dispatch_gen에 기록."""
    r = con.execute("SELECT value FROM meta WHERE key='dispatch_gen'").fetchone()
    return int(r["value"]) if r else 1


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


def _insert_job(con, kind: str, payload: dict, user_id: str, stream: str | None = None,
                lecture_id: str | None = None) -> str:
    """작업 의도 한 행. 업무 데이터를 쓰는 트랜잭션 안에서 부른다 (qa_ask처럼).
    실시간 요약은 lecture_id를 준다 → 대기 중 요약이 이미 있으면 sqlite3.IntegrityError (jobs_one_pending_summary)."""
    job_id = f"op-{uuid.uuid4().hex[:8]}"
    now = time.time()
    if kind in KIND_DEADLINE_SEC and payload.get("deadline_at") is None:   # Q&A·채점: 요청부터 전체 90초
        payload = {**payload, "deadline_at": now + KIND_DEADLINE_SEC[kind]}
    con.execute("INSERT INTO jobs(job_id, kind, payload, stream, state, created, updated_at, user_id, lecture_id) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (job_id, kind, json.dumps(payload, ensure_ascii=False),
                 stream or STREAM_OF.get(kind, "notree:batch"), "pending", now, now, user_id, lecture_id))
    _log(con, "job_create", job_id)
    return job_id


@tool
def job_reserve_pending(limit: int = 10, stream: str | None = None) -> dict:
    """[scheduler 배정기] 대기 중인 의도 → 실행 허가(reserved). 동시 상한·예산 검사가 들어갈 자리.
    stream을 주면 그 레인만 고른다. 어떤 레인을 얼마나 먼저 고를지는 배정기(애플리케이션)가 정한다."""
    # 실시간 요약은 같은 강의의 요약이 실행 중(reserved·sent·running)이면 허가하지 않는다 → 강의당 동시 실행 1개.
    # 대기 1개는 앞 요약이 끝난 뒤 허가되고, 시작할 때 그때까지 쌓인 전사를 한꺼번에 읽는다 (업무 규칙은 서비스에서)
    gate = (" AND NOT (kind='live_summary' AND EXISTS (SELECT 1 FROM jobs j2 WHERE j2.kind='live_summary' "
            "AND j2.lecture_id=jobs.lecture_id AND j2.state IN ('reserved','sent','running')))")
    # 조회와 허가를 한 쓰기 트랜잭션에, 갱신은 state='pending'일 때만 → 배정기가 둘 이상이어도 같은 작업을 두 번
    # 허가하지 않고, 늦게 도착한 갱신이 끝난 작업을 되돌리지 못한다. 실제로 허가한 것만 돌려준다.
    # (PG: UPDATE ... WHERE state='pending' RETURNING 또는 SELECT ... FOR UPDATE SKIP LOCKED)
    with tx() as con:
        if stream is None:
            rows = con.execute("SELECT job_id, kind, stream FROM jobs WHERE state='pending'" + gate +
                               " ORDER BY created LIMIT ?", (limit,)).fetchall()
        else:
            rows = con.execute("SELECT job_id, kind, stream FROM jobs WHERE state='pending' AND stream=?" + gate +
                               " ORDER BY created LIMIT ?", (stream, limit)).fetchall()
        got, gen = [], _gen(con)
        for r in rows:
            if con.execute("UPDATE jobs SET state='reserved', dispatch_gen=?, updated_at=? WHERE job_id=? AND state='pending'",
                           (gen, time.time(), r["job_id"])).rowcount == 1:
                got.append(r)
                _log(con, "job_reserve_pending", r["job_id"])
        return ok(C.JobListOut(jobs=[C.ReservedJob(**dict(r), gen=gen) for r in got]))


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
    with tx() as con:
        rows = con.execute(
            "SELECT job_id, kind, stream FROM jobs WHERE (state='reserved' AND updated_at < ?) "
            "OR (state='sent' AND updated_at < ?) ORDER BY updated_at LIMIT ?",
            (now - reserved_older_than_sec, now - sent_older_than_sec, limit)).fetchall()
        gen = _gen(con)
        for r in rows:
            _set(con, r["job_id"], state="sent", dispatch_gen=gen)
            _log(con, "job_resend_stale", r["job_id"])
        return ok(C.JobListOut(jobs=[C.ReservedJob(**dict(r), gen=gen) for r in rows]))


@tool
def job_start(job_id: str, worker: str, lease_sec: float, dispatch_gen: int | None = None) -> dict:
    """[worker] 실행 시작. 결과는 data.status로: started(성공, permit 발급) / done / failed / leased / unknown.
    lease가 살아 있으면 **같은 worker라도** leased로 거절한다 (자기 메시지를 다시 회수해 중복 실행하는 것 방지).
    lease_sec=0 (LIVENESS=redis): DB lease로 실행 중을 판단하지 않는다. 실행 세대만 올려 이전 실행자의 permit을 무효로 만들고,
    worker는 받은 세대(exec_epoch)를 Redis 소유 키에 담는다 — 키는 더 큰 세대만 덮어쓸 수 있다 (ownership.py)."""
    now = time.time()
    with tx() as con:
        r = con.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
        _log(con, "job_start", job_id)
        if not r:
            return ok(C.JobStartOut(status="unknown"))
        if r["state"] in ("done", "failed"):
            return ok(C.JobStartOut(status=r["state"]))
        if dispatch_gen is not None and dispatch_gen < _gen(con):   # Redis 유실 전 메시지의 늦은 시작 → 복구가 새로 보낸다
            return ok(C.JobStartOut(status="stale_dispatch"))
        if r["state"] == "running" and (r["lease_until"] or 0) > now:
            return ok(C.JobStartOut(status="leased", owner=r["lease_owner"],
                                    lease_left_sec=round(r["lease_until"] - now, 1)))
        epoch = (r["exec_epoch"] or 0) + 1
        _set(con, job_id, state="running", lease_owner=worker, lease_until=now + lease_sec,
             attempts=r["attempts"] + 1, exec_epoch=epoch)
        return ok(C.JobStartOut(
            status="started", kind=r["kind"], payload=json.loads(r["payload"]), attempt=r["attempts"] + 1, exec_epoch=epoch,
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
def job_recover_orphans(start: bool = False, after: str | None = None, limit: int = 500) -> dict:
    """[scheduler] Redis 통째 유실 뒤 전달 상태를 DB에서 다시 만든다. '발견'과 '완료'를 나눈다.
      start=True : Redis 초기화를 발견함 → 전달 세대(dispatch_gen)를 1 올린다 (영속)
      대상       : 마지막 전달이 **이전 세대**인 reserved·sent·running — 그 메시지는 사라진 Redis에 있었다.
                   updated_at(최근에 DB에서 바뀜)은 새 Redis에 메시지가 있다는 증거가 아니라 쓰지 않는다
                   running은 lease가 끝났으면 다시 보내고, 살아 있으면 보류 → 다음 주기에 다시 확인
                   (DB 모드: lease가 살아 있으면 worker가 실행 중. Redis 소유 키 모드: lease_sec=0이라 바로 대상)
      늦은 시작  : 이전 세대 메시지의 job_start는 stale_dispatch로 거절된다 → 상태가 안 바뀌어 대상에 그대로 남는다
      다시 보내기 전: 마감이 지났으면 실패 확정(다시 시도 가능), running인데 시도 한도를 다 썼으면 실패 확정
      쪽 나누기  : job_id 순서로 limit개씩, next_after로 이어서 끝까지
      완료       : 대상이 하나도 남지 않으면 open=false
    다시 보낼 작업은 sent·dispatch_gen=지금 세대로 표시 → 대상에서 빠진다."""
    now = time.time()
    cfg_of = {c.stream: c for c in STREAMS.values()}
    with tx() as con:
        if start:
            con.execute("INSERT OR REPLACE INTO meta VALUES('dispatch_gen', ?)", (str(_gen(con) + 1),))
        gen = _gen(con)
        cond = "state IN ('reserved','sent','running') AND COALESCE(dispatch_gen, 0) < ?"
        rows = con.execute(f"SELECT * FROM jobs WHERE {cond} AND job_id > ? ORDER BY job_id LIMIT ?",
                           (gen, after or "", limit)).fetchall()
        resend, failed, held = [], [], 0
        for r in rows:
            if r["state"] == "running" and (r["lease_until"] or 0) >= now:
                held += 1                                     # 살아 있는 lease → 다음 주기에 다시 확인
                continue
            deadline = json.loads(r["payload"] or "{}").get("deadline_at")
            cfg = cfg_of.get(r["stream"])
            limit_n = max_attempts_for(r["kind"], cfg) if cfg else 3
            reason = ("마감 시각 지남 (Redis 유실 복구)" if deadline is not None and deadline <= now else
                      "시도 한도 초과 (Redis 유실 복구)" if r["state"] == "running" and r["attempts"] >= limit_n else None)
            if reason:
                _set(con, r["job_id"], state="failed", lease_until=None, fail_reason=reason, user_retryable=1)
                failed.append(r["job_id"])
            else:
                _set(con, r["job_id"], state="sent", lease_until=None, dispatch_gen=gen)
                resend.append(r)
            _log(con, "job_recover_orphans", r["job_id"])
        nxt = rows[-1]["job_id"] if len(rows) == limit else None
        remaining = con.execute(f"SELECT COUNT(*) FROM jobs WHERE {cond}", (gen,)).fetchone()[0]
        return ok(C.JobRecoverOut(jobs=[C.ReservedJob(job_id=r["job_id"], kind=r["kind"], stream=r["stream"], gen=gen)
                                        for r in resend], failed=failed, held=held, open=remaining > 0,
                                  next_after=nxt))


@tool
def job_retry(job_id: str, deadline_at: float | None = None) -> dict:
    """[api] 사용자의 '다시 시도'. 다시 해도 되는 실패만 pending으로 되살린다. 체크포인트는 그대로라 이어서 실행된다.
    마감이 있던 작업은 deadline_at으로 새 마감을 준다. 안 주면 Q&A·채점은 지금부터 정책 마감(90초),
    그 밖의 작업은 지난 마감 그대로라 바로 다시 실패한다."""
    # 검사와 변경을 한 쓰기 트랜잭션에 (BEGIN IMMEDIATE) → 검사 뒤 커밋이 후속 요약을 끼워 넣는 경쟁이 없다
    with tx() as con:
        r = con.execute("SELECT state, user_retryable, payload, kind, lecture_id FROM jobs WHERE job_id=?",
                        (job_id,)).fetchone()
        if not r:
            raise Reject(ErrorCode.NOT_FOUND, job_id)
        if r["state"] != "failed":
            return ok(C.JobRetryOut(revived=False, reason=f"failed 상태가 아님 ({r['state']})"))
        if not r["user_retryable"]:
            return ok(C.JobRetryOut(revived=False, reason="영구 실패라 다시 시도할 수 없음"))
        if r["kind"] == "live_summary":                      # 실시간 요약: 강의당 대기 1개·동시 실행 1개 규칙을 지킨다
            lec = con.execute("SELECT state FROM lectures WHERE lecture_id=?", (r["lecture_id"],)).fetchone()
            if not lec or lec["state"] != "recording":
                return ok(C.JobRetryOut(revived=False, reason="녹음이 끝난 강의의 실시간 요약은 되살리지 않음"))
            if con.execute("SELECT 1 FROM jobs WHERE kind='live_summary' AND lecture_id=? AND job_id<>? "
                           "AND state IN ('pending','reserved','sent','running')", (r["lecture_id"], job_id)).fetchone():
                return ok(C.JobRetryOut(revived=False,
                                        reason="후속 요약이 이미 있음 → 그 요약이 실패 구간부터 함께 처리함"))
        payload = json.loads(r["payload"])
        if r["kind"] == "qa":                                # 대화 속 질문: 대화당 진행 중 1개·차례 순서를 지킨다
            why = _qa_retry_blocked(con, payload.get("question_id"))
            if why:
                return ok(C.JobRetryOut(revived=False, reason=why))
        if deadline_at is None and r["kind"] in KIND_DEADLINE_SEC:
            deadline_at = time.time() + KIND_DEADLINE_SEC[r["kind"]]
        if deadline_at is not None:
            payload["deadline_at"] = deadline_at
        try:                                                 # failed일 때만 (다른 경로가 먼저 바꿨으면 0행)
            n = con.execute("UPDATE jobs SET state='pending', attempts=0, lease_owner=NULL, lease_until=NULL, "
                            "fail_reason=NULL, user_retryable=NULL, payload=?, updated_at=? WHERE job_id=? AND state='failed'",
                            (json.dumps(payload, ensure_ascii=False), time.time(), job_id)).rowcount
        except sqlite3.IntegrityError:                       # 마지막 방어: 강의당 대기 요약 1개 (jobs_one_pending_summary)
            return ok(C.JobRetryOut(revived=False, reason="후속 요약이 이미 있음 → 그 요약이 실패 구간부터 함께 처리함"))
        if n != 1:
            return ok(C.JobRetryOut(revived=False, reason="그사이 상태가 바뀜"))
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


# ── 실시간 요약 (PoC: SQLite, 실제는 PG) ─────────────────────────────
# 위치 세 개 (강의 행): saved_until(저장된 확정 전사 끝) ≥ attempted_until(요약이 읽어 간 끝) ≥ processed_until(요약 끝)
# 합치기: 강의당 대기 요약 1개(유일 인덱스) + 실행 중 1개(배정 조건). 대기 요약은 시작할 때 그때까지의 전사를 모두 읽는다.
def _lecture(con, lecture_id: str, user_id: str | None = None) -> sqlite3.Row:
    r = con.execute("SELECT * FROM lectures WHERE lecture_id=?", (lecture_id,)).fetchone()
    if not r or (user_id is not None and r["user_id"] != user_id):
        raise Reject(ErrorCode.NOT_FOUND, f"강의 없음: {lecture_id}")
    return r


def _lecture_out(r) -> dict:
    return ok(C.LectureOut(**{k: r[k] for k in C.LectureOut.model_fields}))


def _require_summary_job(con, claims: dict | None, lec: sqlite3.Row, *, live: bool = True) -> None:
    """permit의 작업이 이 강의를 요약하도록 허가된 작업인지: 종류 live_summary · 같은 lecture_id · 작업 사용자 = 강의 주인.
    live=True면 지금도 실행권(소유자·실행 세대)이 있어야 한다. permit 없이 부르는 dev 경로(REQUIRE_PERMIT=0)는 검사 안 함."""
    if claims is None:
        return
    j = con.execute("SELECT kind, lecture_id, user_id FROM jobs WHERE job_id=?", (claims["j"],)).fetchone()
    if not j or j["kind"] != "live_summary" or j["lecture_id"] != lec["lecture_id"] or j["user_id"] != lec["user_id"]:
        raise Reject(ErrorCode.PERMIT_INVALID, "이 강의를 요약하는 작업의 permit이 아님")
    if live:
        _require_live(con, claims, claims["j"])


def _enqueue_summary(con, lec: sqlite3.Row) -> str | None:
    """대기 요약이 없으면 1건 등록하고 job_id, 이미 있으면 None (합쳐짐)."""
    try:
        return _insert_job(con, "live_summary", {"lecture_id": lec["lecture_id"]}, lec["user_id"],
                           lecture_id=lec["lecture_id"])
    except sqlite3.IntegrityError:
        return None


@tool
def lecture_start(lecture_id: str, user_id: str) -> dict:
    """[api] 녹음 시작."""
    with tx() as con:
        con.execute("INSERT OR IGNORE INTO lectures(lecture_id, user_id, state, updated_at) VALUES(?,?,'recording',?)",
                    (lecture_id, user_id, time.time()))
        return _lecture_out(_lecture(con, lecture_id, user_id))


@tool
def transcript_commit(lecture_id: str, user_id: str, segments: list[dict]) -> dict:
    """[api·gateway] 확정 전사 묶음 + (대기 요약이 없으면) 요약 작업 의도를 한 트랜잭션에."""
    with tx() as con:
        lec = _lecture(con, lecture_id, user_id)
        if lec["state"] != "recording":
            raise Reject(ErrorCode.GENERATION_MISMATCH, f"녹음 중이 아님 ({lec['state']})")
        saved = lec["saved_until"]
        for seg in sorted(segments, key=lambda x: int(x["seq"])):
            seq = int(seg["seq"])
            if seq <= saved:
                continue                                     # 다시 보낸 묶음 (멱등)
            if seq != saved + 1:
                raise ValueError(f"전사 번호에 빈틈: {saved} 다음이 {seq}")
            con.execute("INSERT INTO transcripts VALUES(?,?,?)", (lecture_id, seq, seg["text"]))
            saved = seq
        added = saved - lec["saved_until"]
        _log(con, "transcript_commit", lecture_id)
        if not added:                                        # 재전송·빈 묶음: 요약을 등록하지 않는다 (실패 구간 재시도 우회 방지)
            return ok(C.TranscriptCommitOut(saved_until=saved, added=0, coalesced=False))
        con.execute("UPDATE lectures SET saved_until=?, updated_at=? WHERE lecture_id=?", (saved, time.time(), lecture_id))
        job_id = _enqueue_summary(con, lec)
        return ok(C.TranscriptCommitOut(saved_until=saved, added=added, enqueued_job_id=job_id, coalesced=job_id is None))


@tool
def transcript_range(lecture_id: str, after_seq: int | None = None, permit: str | None = None) -> dict:
    """[worker] 롤링 요약 + 요약 위치 이후 확정 전사. 상태 변경: 읽은 끝 번호를 attempted_until로 남긴다
    → 이 강의를 요약하는 작업의 **현재** 실행권이 있어야 한다 (실행권을 잃은 이전 실행자는 못 바꿈)."""
    claims = _claims(permit, None)
    with tx() as con:
        lec = _lecture(con, lecture_id)
        _require_summary_job(con, claims, lec)
        mine = claims and con.execute("SELECT 1 FROM summaries WHERE idem_key=?", (claims["j"],)).fetchone()
        start = (lec["processed_until"] if after_seq is None else after_seq) + 1
        rows = con.execute("SELECT seq, text FROM transcripts WHERE lecture_id=? AND seq BETWEEN ? AND ? ORDER BY seq",
                           (lecture_id, start, lec["saved_until"])).fetchall()
        con.execute("UPDATE lectures SET attempted_until=MAX(attempted_until, ?) WHERE lecture_id=?",
                    (lec["saved_until"], lecture_id))
        _log(con, "transcript_range", lecture_id)
        return ok(C.TranscriptRangeOut(state=lec["state"], rolling_summary=lec["rolling_summary"],
                                       new_text="\n".join(r["text"] for r in rows), from_seq=start,
                                       last_seq=lec["saved_until"], generation=lec["generation"],
                                       already_published=bool(mine)))


@tool
def summary_publish(idem_key: str, lecture_id: str, expected_generation: int, from_seq: int, upto_seq: int, text: str,
                    permit: str | None = None) -> dict:
    """[worker] 요약 저장 + 요약 위치 이동 (한 트랜잭션). 멱등, 세대·실행권·위치 확인. 위치는 앞으로만 움직인다."""
    claims = _claims(permit, idem_key)
    with tx() as con:
        lec = _lecture(con, lecture_id)
        _require_summary_job(con, claims, lec, live=False)    # 작업↔강의 연결은 duplicate 경로에도
        done = con.execute("SELECT version, lecture_id FROM summaries WHERE idem_key=?", (idem_key,)).fetchone()
        if done:
            if done["lecture_id"] != lecture_id:
                raise Reject(ErrorCode.PERMIT_INVALID, "다른 강의에 저장된 결과")
            return ok(C.SummaryPublishOut(status="duplicate", summary_version=done["version"],
                                          processed_until=lec["processed_until"]), duplicate=True)
        _require_live(con, claims, idem_key)
        if lec["generation"] != expected_generation or lec["state"] != "recording":
            raise Reject(ErrorCode.GENERATION_MISMATCH, f"강의 세대·상태가 바뀜 ({lec['state']}, 세대 {lec['generation']})")
        if lec["processed_until"] != from_seq - 1 or upto_seq > lec["saved_until"] or upto_seq < from_seq:
            return ok(C.SummaryPublishOut(status="stale", summary_version=lec["summary_version"],
                                          processed_until=lec["processed_until"]))
        version = lec["summary_version"] + 1
        con.execute("INSERT INTO summaries VALUES(?,?,?,?,?,?,?,?)",
                    (idem_key, lecture_id, version, from_seq, upto_seq, lec["generation"], text, time.time()))
        con.execute("UPDATE lectures SET rolling_summary=?, processed_until=?, summary_version=?, updated_at=? "
                    "WHERE lecture_id=?", (text, upto_seq, version, time.time(), lecture_id))
        _log(con, "summary_publish", idem_key)
        return ok(C.SummaryPublishOut(status="published", summary_version=version, processed_until=upto_seq))


@tool
def lecture_end(lecture_id: str, user_id: str) -> dict:
    """[api] 녹음 종료: ended, 세대 +1. 진행 중이던 실시간 요약의 발행은 generation_mismatch로 막힌다.
    (최종 노트 등록은 이 PoC에서 아직 안 함)"""
    with tx() as con:
        lec = _lecture(con, lecture_id, user_id)
        if lec["state"] == "recording":
            con.execute("UPDATE lectures SET state='ended', generation=generation+1, updated_at=? WHERE lecture_id=?",
                        (time.time(), lecture_id))
        return _lecture_out(_lecture(con, lecture_id))


@tool
def lecture_get(lecture_id: str, user_id: str) -> dict:
    """[dev] 강의 상태·요약 위치."""
    with closing(db()) as con:
        return _lecture_out(_lecture(con, lecture_id, user_id))


@tool
def summary_recover(limit: int = 50) -> dict:
    """[scheduler] 누락 복구. 다음을 모두 만족하는 강의에 요약 1건 등록:
      녹음 중 · 요약 안 된 전사가 있음(saved > processed) · 아무 요약도 읽어 가지 않은 전사가 있음(saved > attempted)
      · 대기·실행 중 요약 작업이 없음
    'saved > attempted' 조건 때문에 실패한 요약의 범위를 혼자 계속 다시 등록하지 않는다 — 새 전사가 오면
    커밋이 등록하고, 그 요약이 실패 구간까지 함께 읽는다. 비용 한도 검사는 비용 원장이 생기면 여기에 넣는다."""
    with tx() as con:
        rows = con.execute(
            "SELECT * FROM lectures l WHERE state='recording' AND saved_until > processed_until "
            "AND saved_until > attempted_until AND NOT EXISTS (SELECT 1 FROM jobs j WHERE j.kind='live_summary' "
            "AND j.lecture_id=l.lecture_id AND j.state IN ('pending','reserved','sent','running')) LIMIT ?",
            (limit,)).fetchall()
        got = [j for lec in rows if (j := _enqueue_summary(con, lec))]
        return ok(C.SummaryRecoverOut(registered=got))


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


# ── Q&A 대화 세션 (2026-10-20, docs/changes/2026-10-20-qa-conversation-memory.md) ─────────
# DB가 세션의 정체(주인·상태·세대·차례·진행 중 질문)를, Redis(conversation.py)가 최근 문답을 가진다.
# 둘은 한 번에 원자적으로 쓸 수 없으므로 경계마다 순서를 정했다 (문서 3절):
#   시작: Redis 키 → DB 행 / 질문: DB 확인 → Redis 연장(있을 때만) → DB 슬롯·질문 / 답: DB 저장 → Redis 반영(실패해도 저장 성공)
#   종료·만료: DB 세대 +1 → Redis 삭제. 종료·만료된 대화를 DB 이력으로 다시 만들지 않는다.
def _conv_row(con, conversation_id: str, user_id: str, course_id: str | None = None) -> sqlite3.Row:
    r = con.execute("SELECT * FROM qa_conversations WHERE conversation_id=?", (conversation_id,)).fetchone()
    if not r or r["user_id"] != user_id or (course_id is not None and r["course_id"] != course_id):
        raise Reject(ErrorCode.NOT_FOUND, f"대화 없음: {conversation_id}")      # 남의 대화는 '없음'과 구별하지 않는다
    return r


def _timed_out(r) -> bool:
    """DB 기준 미사용 만료 (마지막 사용자 활동 + TTL). Redis TTL과 따로 본다: 둘 중 하나라도 지나면 만료."""
    return time.time() - (r["last_user_at"] or r["created"]) > S.QA_CONV_TTL_SEC


def _end_conversation(conversation_id: str, epoch: int, state: str, reason: str) -> bool:
    """열린 대화를 끝낸다 (closed·expired). 세대를 올려 이전 세대 키로 가는 늦은 쓰기를 무력화하고 단기 문맥을 지운다.
    DB는 조건부(state='open' AND epoch=?)라 여러 번 불려도 한 번만 바뀐다. → Redis 삭제 성공 여부."""
    with tx() as con:
        con.execute("UPDATE qa_conversations SET state=?, epoch=epoch+1, ended_at=?, end_reason=? "
                    "WHERE conversation_id=? AND state='open' AND epoch=?",
                    (state, time.time(), reason, conversation_id, epoch))
    try:
        CONV.drop(conversation_id, epoch)
        return True
    except CONV.MemoryUnavailable:
        return False                                  # 이전 세대 키는 아무도 읽지 않고 TTL로 사라진다


def _release_if_finished(con, r) -> None:
    """진행 중 슬롯의 질문이 이미 끝났으면 비운다. 작업 실패(failed)는 qa_complete 없이 끝나므로 다음 질문 등록 때 여기서 비운다."""
    q = con.execute("SELECT q.state qs, j.state js FROM qa_questions q LEFT JOIN jobs j ON j.job_id=q.job_id "
                    "WHERE q.question_id=?", (r["active_question_id"],)).fetchone()
    if not q or q["qs"] != "pending" or q["js"] in ("done", "failed"):
        con.execute("UPDATE qa_conversations SET active_question_id=NULL WHERE conversation_id=? AND active_question_id=?",
                    (r["conversation_id"], r["active_question_id"]))


def _assistant_text(a: dict) -> str:
    """이력에 넣는 답: 인용 표시 없는 문장만 (이전 답의 근거 ID가 다음 답의 인용으로 새지 않게). 확인 질문은 그 문장."""
    if a.get("clarification"):
        return a["clarification"]["question"]
    return " ".join(s["text"] for s in a.get("sentences") or []) or a.get("answer") or ""


def _memory_turns(con, conversation_id: str) -> list[dict]:
    """DB에 저장된 최근 답 (이력의 원본). 남기는 쌍 수만큼만 본다."""
    rows = con.execute("SELECT question_id, turn_seq, user_message_id, assistant_message_id, question, answer "
                       "FROM qa_questions WHERE conversation_id=? AND state='answered' AND answer IS NOT NULL "
                       "ORDER BY turn_seq DESC LIMIT ?", (conversation_id, S.QA_CONV_MAX_TURNS)).fetchall()
    out = []
    for r in rows:
        a = json.loads(r["answer"])
        out.append({"turn_seq": r["turn_seq"], "question_id": r["question_id"], "question": r["question"],
                    "user_message_id": r["user_message_id"], "assistant_message_id": r["assistant_message_id"],
                    "answer": _assistant_text(a), "status": a.get("status")})
    return out


def _sync_memory(conversation_id: str) -> tuple[str, int]:
    """DB에 저장됐는데 Redis 이력에 아직 없는 답을 반영한다 (mark 비교라 몇 번 불려도 중복 없음).
    열린 대화의 현재 세대 키가 있을 때만 — 재시도 중인 현재 대화의 결과 확인이지, 끝난 대화의 복원이 아니다.
    → (applied | noop | skipped | failed, 반영 뒤 mark)"""
    with db() as con:
        r = con.execute("SELECT * FROM qa_conversations WHERE conversation_id=?", (conversation_id,)).fetchone()
        if not r or r["state"] != "open":
            return "skipped", -1
        turns = _memory_turns(con, conversation_id)
    try:
        res, mark = CONV.apply_turns(conversation_id, r["epoch"], r["user_id"], r["course_id"], turns)
    except CONV.MemoryUnavailable:
        return "failed", -1
    return ("skipped" if res == "missing" else res), mark


def _context_turns(q) -> tuple[list[dict], str]:
    """qa_context용 이전 문답과 memory_state. 현재 질문은 빼고(차례·ID 둘 다), DB에서 지운 질문도 뺀다."""
    cid = q["conversation_id"]
    sync, _ = _sync_memory(cid)
    with db() as con:
        r = con.execute("SELECT * FROM qa_conversations WHERE conversation_id=?", (cid,)).fetchone()
        if r["state"] != "open":
            return [], "ended"
        answered = {x["question_id"]: x["turn_seq"] for x in con.execute(
            "SELECT question_id, turn_seq FROM qa_questions WHERE conversation_id=? AND state='answered' AND turn_seq<?",
            (cid, q["turn_seq"]))}
    try:
        m = CONV.read(cid, r["epoch"], r["user_id"], r["course_id"])
    except CONV.MemoryUnavailable:
        return [], "unavailable"
    if m is None:
        return [], "lost"
    msgs = [x for x in m["messages"] if x["question_id"] in answered]
    turns = CONV.window(msgs, q["turn_seq"], q["question_id"])
    missing = max(answered.values(), default=0) > m["mark"]      # 저장된 최근 답이 아직 이력에 없음
    return turns, ("incomplete" if sync == "failed" or missing else "ok")


@tool
def qa_conversation_start(user_id: str, course_id: str) -> dict:
    """[api] 새 대화. Redis 키를 먼저 만들고 DB 행을 넣는다 (DB가 실패하면 고아 키는 아무도 읽지 않고 TTL로 사라짐)."""
    with db() as con:
        c = con.execute("SELECT user_id FROM qa_courses WHERE course_id=?", (course_id,)).fetchone()
    if not c or c["user_id"] != user_id:
        raise Reject(ErrorCode.NOT_FOUND, f"과목 없음: {course_id}")
    cid = f"cv-{uuid.uuid4().hex[:12]}"
    try:
        exp = CONV.create(cid, 1, user_id, course_id)
    except CONV.MemoryUnavailable as e:
        raise Reject(ErrorCode.UNAVAILABLE, f"대화 메모리: {e}") from None
    now = time.time()
    with tx() as con:
        con.execute("INSERT INTO qa_conversations(conversation_id, user_id, course_id, state, epoch, next_seq, "
                    "last_user_at, created) VALUES(?,?,?,'open',1,1,?,?)", (cid, user_id, course_id, now, now))
    return ok(C.QaConversationStartOut(conversation_id=cid, expires_at=exp / 1000))


@tool
def qa_conversation_close(conversation_id: str, user_id: str) -> dict:
    """[api] 대화 종료. 새 질문을 거절하고 단기 문맥을 지운다. 진행 중 질문은 계속 돌아 DB에 답을 저장하지만,
    세대가 바뀌어 이력에는 들어가지 않는다. DB의 질문·답변은 지우지 않는다 (보관 정책 그대로)."""
    with db() as con:
        r = _conv_row(con, conversation_id, user_id)
        active = None
        if r["active_question_id"]:
            q = con.execute("SELECT state FROM qa_questions WHERE question_id=?", (r["active_question_id"],)).fetchone()
            active = r["active_question_id"] if q and q["state"] == "pending" else None
    if r["state"] != "open":
        return ok(C.QaConversationCloseOut(closed=False, state=r["state"], memory_cleared=False))
    with tx() as con:
        n = con.execute("UPDATE qa_conversations SET state='closed', epoch=epoch+1, ended_at=?, end_reason='closed' "
                        "WHERE conversation_id=? AND state='open' AND epoch=?",
                        (time.time(), conversation_id, r["epoch"])).rowcount
        now_state = con.execute("SELECT state FROM qa_conversations WHERE conversation_id=?",
                                (conversation_id,)).fetchone()["state"]
    if n != 1:                                          # 그사이 만료·종료됨
        return ok(C.QaConversationCloseOut(closed=False, state=now_state, memory_cleared=False))
    try:
        CONV.drop(conversation_id, r["epoch"])
        cleared = True
    except CONV.MemoryUnavailable:
        cleared = False
    return ok(C.QaConversationCloseOut(closed=True, state="closed", memory_cleared=cleared, active_question_id=active))


@tool
def qa_conversation_get(conversation_id: str, user_id: str, include_messages: bool = False) -> dict:
    """[api·dev] 대화 상태. 만료는 다음 질문 때 DB에 확정되지만, 여기서는 시간·Redis 키를 보고 계산해 돌려준다."""
    with db() as con:
        r = con.execute("SELECT * FROM qa_conversations WHERE conversation_id=?", (conversation_id,)).fetchone()
        if not r or r["user_id"] != user_id:
            return ok(C.QaConversationGetOut(found=False))
        answered = {x[0] for x in con.execute(
            "SELECT question_id FROM qa_questions WHERE conversation_id=? AND state='answered'", (conversation_id,))}
    state, exp, mstate, msgs = r["state"], None, "ended", []
    if r["state"] == "open":
        try:
            m = CONV.read(conversation_id, r["epoch"], r["user_id"], r["course_id"])
            mstate = "lost" if m is None else "ok"
            if m is not None:
                exp = m["expires_at"] / 1000
                msgs = [x for x in m["messages"] if x["question_id"] in answered] if include_messages else []
        except CONV.MemoryUnavailable:
            mstate = "unavailable"
        if _timed_out(r) or mstate == "lost":
            state = "expired"
    return ok(C.QaConversationGetOut(found=True, state=state, course_id=r["course_id"], turns=r["next_seq"] - 1,
                                     active_question_id=r["active_question_id"], expires_at=exp, memory_state=mstate,
                                     messages=msgs))


def _ended(r) -> dict:
    return ok(C.QaAskOut(status="closed" if r["state"] == "closed" else "expired",
                         conversation_id=r["conversation_id"], expire_reason=r["end_reason"]))


def _qa_ask_in_conversation(user_id: str, course_id: str, question: str, conversation_id: str) -> dict:
    with db() as con:
        r = _conv_row(con, conversation_id, user_id, course_id)
    if r["state"] != "open":
        return _ended(r)
    epoch = r["epoch"]
    if _timed_out(r):
        _end_conversation(conversation_id, epoch, "expired", "ttl")
        return ok(C.QaAskOut(status="expired", conversation_id=conversation_id, expire_reason="ttl"))
    try:
        alive = CONV.touch(conversation_id, epoch, user_id, course_id)     # 사용자 활동: 있는 키만 연장 (없으면 만들지 않음)
    except CONV.MemoryUnavailable as e:
        raise Reject(ErrorCode.UNAVAILABLE, f"대화 메모리: {e}") from None
    if alive is None:                         # DB는 열려 있는데 키가 없음 → Redis 유실(또는 막 만료). 같은 세션을 되살리지 않는다
        _end_conversation(conversation_id, epoch, "expired", "context_lost")
        return ok(C.QaAskOut(status="expired", conversation_id=conversation_id, expire_reason="context_lost"))
    question_id, now = f"q-{uuid.uuid4().hex[:8]}", time.time()
    with tx() as con:
        r = con.execute("SELECT * FROM qa_conversations WHERE conversation_id=?", (conversation_id,)).fetchone()
        if r["state"] != "open" or r["epoch"] != epoch:                  # 그사이 종료·만료
            return _ended(r)
        if r["active_question_id"]:
            _release_if_finished(con, r)
        # 대화당 진행 중 질문 1개: 조건부 갱신 한 문장 (PG에서도 같은 문장 + RETURNING next_seq로 경쟁 없이)
        n = con.execute("UPDATE qa_conversations SET active_question_id=?, next_seq=next_seq+1, last_user_at=? "
                        "WHERE conversation_id=? AND state='open' AND epoch=? AND active_question_id IS NULL",
                        (question_id, now, conversation_id, epoch)).rowcount
        if n != 1:
            con.execute("UPDATE qa_conversations SET last_user_at=? WHERE conversation_id=?", (now, conversation_id))
            active = con.execute("SELECT active_question_id FROM qa_conversations WHERE conversation_id=?",
                                 (conversation_id,)).fetchone()[0]
            return ok(C.QaAskOut(status="busy", conversation_id=conversation_id, active_question_id=active))
        turn = r["next_seq"]                          # 같은 트랜잭션(BEGIN IMMEDIATE) 안이라 위 갱신 전 값 = 이 질문의 차례
        job_id = _insert_job(con, "qa", {"question_id": question_id}, user_id)
        con.execute("INSERT INTO qa_questions(question_id, user_id, course_id, question, job_id, state, created, updated_at, "
                    "conversation_id, turn_seq, user_message_id, assistant_message_id) "
                    "VALUES(?,?,?,?,?,'pending',?,?,?,?,?,?)",
                    (question_id, user_id, course_id, question, job_id, now, now, conversation_id, turn,
                     f"m-{uuid.uuid4().hex[:12]}", f"m-{uuid.uuid4().hex[:12]}"))
    return ok(C.QaAskOut(question_id=question_id, job_id=job_id, conversation_id=conversation_id, turn_seq=turn))


def _qa_retry_blocked(con, question_id: str | None) -> str | None:
    """대화 속 질문의 '다시 시도': 대화가 열려 있고, 그 뒤 새 질문이 없고, 진행 중 슬롯을 다시 잡을 수 있을 때만."""
    q = con.execute("SELECT * FROM qa_questions WHERE question_id=?", (question_id,)).fetchone() if question_id else None
    if not q or not q["conversation_id"]:
        return None
    r = con.execute("SELECT * FROM qa_conversations WHERE conversation_id=?", (q["conversation_id"],)).fetchone()
    if r["state"] != "open" or _timed_out(r):
        return "대화가 끝나 다시 시도할 수 없음 → 새 대화에서 다시 질문"
    if q["turn_seq"] != r["next_seq"] - 1:
        return "그 뒤에 새 질문이 있어 다시 시도할 수 없음 → 다시 질문"
    if r["active_question_id"] and r["active_question_id"] != question_id:
        _release_if_finished(con, r)
    n = con.execute("UPDATE qa_conversations SET active_question_id=? WHERE conversation_id=? AND state='open' "
                    "AND epoch=? AND (active_question_id IS NULL OR active_question_id=?)",
                    (question_id, r["conversation_id"], r["epoch"], question_id)).rowcount
    return None if n == 1 else "이 대화에 진행 중인 질문이 있음"


@tool
def qa_ask(user_id: str, course_id: str, question: str, conversation_id: str | None = None) -> dict:
    """[api] 질문 행과 qa 작업 의도를 한 트랜잭션에. 마감은 job_create와 같은 규칙(요청 + 90초).
    conversation_id가 없으면 이전과 같은 단일 질문 (문맥 없음, Redis를 건드리지 않음)."""
    if conversation_id is not None:
        return _qa_ask_in_conversation(user_id, course_id, question, conversation_id)
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
    """[worker] 질문·과목·강의 목록·세대 + 대화의 이전 문답. 이 작업의 답이 이미 저장됐으면 answered (이때도 이력 반영을 재확인)."""
    with db() as con:
        q, _ = _qa_question(con, question_id, permit)
        _log(con, "qa_context", q["job_id"])
        c = con.execute("SELECT name FROM qa_courses WHERE course_id=?", (q["course_id"],)).fetchone()
        lectures = [r["lecture_id"] for r in con.execute(
            "SELECT DISTINCT lecture_id FROM qa_docs WHERE course_id=? AND lecture_id IS NOT NULL ORDER BY lecture_id",
            (q["course_id"],))]
        answered = q["state"] == "answered" and q["idem_key"] == q["job_id"]
    turns, memory_state = _context_turns(q) if q["conversation_id"] else ([], "none")
    return ok(C.QaContextOut(question=q["question"], course_id=q["course_id"], course_name=c["name"] if c else "",
                             lecture_ids=lectures, generation=q["generation"], conversation_id=q["conversation_id"],
                             recent_turns=turns, memory_state=memory_state, answered=answered))


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
                clarification: dict | None = None, permit: str | None = None) -> dict:
    """[worker] 답변 저장. 같은 idem_key는 처음 결과 그대로 duplicate=true. 실행권과 질문 세대를 확인한다.
    대화 속 질문이면 같은 트랜잭션에서 진행 중 슬롯을 비우고, 커밋 뒤 이력에 반영한다 (실패해도 저장은 성공 → memory=failed)."""
    C.QaCompleteIn(idem_key=idem_key, question_id=question_id, expected_generation=expected_generation, status=status,
                   label=label, answer=answer, sentences=sentences, sources=sources, notice_code=notice_code,
                   clarification=clarification)  # 값 검사
    if (status == "확인 필요") != (clarification is not None) or (status == "확인 필요") != (label == "확인 질문"):
        raise ValueError("확인 필요 ↔ label 확인 질문 ↔ clarification은 함께")
    if clarification is not None and sources:
        raise ValueError("확인 질문에는 출처가 없다")
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
        duplicate = q["idem_key"] == idem_key                      # 이미 저장됨 (응답을 못 받은 재시도 등)
        if duplicate:
            answer_id = q["answer_id"]
        else:
            if q["state"] == "answered":                           # 다른 키로 이미 답함 → 덮어쓰지 않는다
                raise Reject(ErrorCode.GENERATION_MISMATCH, "이미 다른 실행이 답을 저장함")
            _require_live(con, claims, q["job_id"])
            if q["generation"] != expected_generation:             # 삭제(state=deleted)도 세대가 올라가 여기서 걸린다
                raise Reject(ErrorCode.GENERATION_MISMATCH, f"질문이 바뀜 (세대 {q['generation']} ≠ {expected_generation})")
            answer_id = f"a-{uuid.uuid4().hex[:8]}"
            body = {"status": status, "label": label, "answer": answer, "sentences": sentences, "citations": citations,
                    "sources": sources, "notice_code": notice_code}
            if clarification is not None:
                body["clarification"] = clarification
            con.execute("UPDATE qa_questions SET state='answered', answer=?, answer_id=?, idem_key=?, updated_at=? "
                        "WHERE question_id=?",
                        (json.dumps(body, ensure_ascii=False), answer_id, idem_key, time.time(), question_id))
            if q["conversation_id"]:
                con.execute("UPDATE qa_conversations SET active_question_id=NULL "
                            "WHERE conversation_id=? AND active_question_id=?", (q["conversation_id"], question_id))
            _log(con, "qa_complete", q["job_id"])
    # DB 커밋 뒤: 이력 반영. 같은 차례는 mark로 한 번만 들어가므로 duplicate(재시도)에서도 그대로 다시 확인한다
    memory = _sync_memory(q["conversation_id"])[0] if q["conversation_id"] else "none"
    return ok(C.QaCompleteOut(answer_id=answer_id, memory=memory), duplicate=duplicate)


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
    행은 남긴다: 진행 중 작업의 저장이 generation_mismatch로 거절되고, 조회는 'deleted'로 답하게.
    대화 속 질문이면 단기 문맥에서도 뺀다. Redis 장애로 못 빼도 qa_context가 지운 질문의 메시지는 걸러 낸다."""
    with tx() as con:
        cur = con.execute("UPDATE qa_questions SET state='deleted', generation=generation+1, question=NULL, answer=NULL, "
                          "updated_at=? WHERE question_id=? AND user_id=? AND state!='deleted'",
                          (time.time(), question_id, user_id))
        deleted = cur.rowcount == 1
        conv = con.execute("SELECT c.* FROM qa_questions q JOIN qa_conversations c ON c.conversation_id=q.conversation_id "
                           "WHERE q.question_id=?", (question_id,)).fetchone() if deleted else None
    if conv is not None and conv["state"] == "open":
        try:
            CONV.remove_question(conv["conversation_id"], conv["epoch"], conv["user_id"], conv["course_id"], question_id)
        except CONV.MemoryUnavailable:
            pass
    return ok(C.QaDeleteOut(deleted=deleted))


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
