"""내부 MCP 서버 (A안).

이 프로세스만 DB(out/notree.db)를 연다. worker(그래프)는 아래 도구를 MCP로 호출할 뿐이다.
실제 설계의 checkpoint.put/get, note.publish에 해당한다. (MCP 도구 이름은 점 대신 밑줄)

체크포인트 내용은 LangGraph가 직렬화한 바이트를 base64로 받아 그대로 저장한다.
서버는 LangGraph를 몰라도 된다(불투명 blob 저장소).

실행
  stdio : run.py가 하위 프로세스로 자동으로 띄운다
  HTTP  : uv run mcp_server.py --http   (compose의 내부 mcp 컨테이너 역할, 기본 127.0.0.1:8765/mcp)
  확인  : uv run mcp dev mcp_server.py
"""
import hashlib
import hmac
import json
import os
import sqlite3
import sys
import time
import uuid
from pathlib import Path

from mcp.server.fastmcp import FastMCP

DB = Path(os.getenv("NOTREE_DB", "out/notree.db"))
DB.parent.mkdir(parents=True, exist_ok=True)

mcp = FastMCP("notree-internal", log_level="WARNING",
              host=os.getenv("NOTREE_MCP_HOST", "127.0.0.1"), port=int(os.getenv("NOTREE_MCP_PORT", "8765")))


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
"""


def _init_db() -> None:
    """서버 시작 때 한 번. 같은 DB 파일을 여러 서버 프로세스가 동시에 처음 만들 때 잠김이 날 수 있어 재시도한다.
    (테스트에서 stdio 서버 여러 개가 같은 파일을 쓰는 경우. 실제 배치는 MCP 서버 하나 + PG)"""
    for i in range(50):
        try:
            con = sqlite3.connect(DB, timeout=10)
            con.execute("PRAGMA journal_mode=WAL")
            con.executescript(SCHEMA)
            try:  # 이전 버전에서 만든 DB 호환
                con.execute("ALTER TABLE jobs ADD COLUMN fail_reason TEXT")
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


def _log(con, tool: str, thread_id: str) -> None:
    con.execute("INSERT INTO tool_log(tool, thread_id) VALUES(?,?)", (tool, thread_id))


def _row_to_ck(con, r) -> dict:
    ws = con.execute(
        "SELECT task_id, channel, v_type, v_b64, task_path, idx FROM writes "
        "WHERE thread_id=? AND ns=? AND checkpoint_id=? ORDER BY task_id, idx",
        (r["thread_id"], r["ns"], r["checkpoint_id"]),
    ).fetchall()
    return {
        "thread_id": r["thread_id"], "ns": r["ns"], "checkpoint_id": r["checkpoint_id"],
        "parent_id": r["parent_id"],
        "checkpoint": [r["ck_type"], r["ck_b64"]], "metadata": [r["meta_type"], r["meta_b64"]],
        "writes": [[w["task_id"], w["channel"], [w["v_type"], w["v_b64"]], w["task_path"], w["idx"]] for w in ws],
    }


# ── 체크포인트 도구 ───────────────────────────────────────────────
@mcp.tool()
def checkpoint_put(thread_id: str, ns: str, checkpoint_id: str, parent_id: str | None,
                   checkpoint: list[str], metadata: list[str]) -> dict:
    """단계(super-step)가 끝날 때 상태 전체를 저장한다."""
    with db() as con:
        con.execute(
            "INSERT OR REPLACE INTO checkpoints VALUES(?,?,?,?,?,?,?,?)",
            (thread_id, ns, checkpoint_id, parent_id, *checkpoint, *metadata),
        )
        _log(con, "checkpoint_put", thread_id)
    return {"ok": True}


@mcp.tool()
def checkpoint_put_writes(thread_id: str, ns: str, checkpoint_id: str, task_id: str,
                          task_path: str, writes: list[list]) -> dict:
    """노드 하나가 끝날 때 그 결과(pending write)를 저장한다. 병렬 노드 중 먼저 끝난 것이 여기서 살아남는다.
    writes: [[idx, channel, v_type, v_b64], ...]  idx<0(특수 채널)은 덮어쓰고, idx>=0은 이미 있으면 유지."""
    with db() as con:
        for idx, channel, v_type, v_b64 in writes:
            verb = "INSERT OR REPLACE" if idx < 0 else "INSERT OR IGNORE"
            con.execute(
                f"{verb} INTO writes VALUES(?,?,?,?,?,?,?,?,?)",
                (thread_id, ns, checkpoint_id, task_id, idx, channel, v_type, v_b64, task_path),
            )
        _log(con, "checkpoint_put_writes", thread_id)
    return {"ok": True}


@mcp.tool()
def checkpoint_get(thread_id: str, ns: str = "", checkpoint_id: str | None = None) -> dict:
    """checkpoint_id가 없으면 최신 체크포인트. 없으면 {"found": false}."""
    with db() as con:
        if checkpoint_id:
            r = con.execute("SELECT * FROM checkpoints WHERE thread_id=? AND ns=? AND checkpoint_id=?",
                            (thread_id, ns, checkpoint_id)).fetchone()
        else:
            r = con.execute("SELECT * FROM checkpoints WHERE thread_id=? AND ns=? "
                            "ORDER BY checkpoint_id DESC LIMIT 1", (thread_id, ns)).fetchone()
        _log(con, "checkpoint_get", thread_id)
        return {"found": True, **_row_to_ck(con, r)} if r else {"found": False}


@mcp.tool()
def checkpoint_list(thread_id: str, ns: str | None = None, before: str | None = None,
                    limit: int | None = None) -> dict:
    """최신 → 과거 순서."""
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
        return {"items": [_row_to_ck(con, r) for r in rows]}


@mcp.tool()
def checkpoint_delete_thread(thread_id: str) -> dict:
    """끝난 작업의 체크포인트 정리 (실제로는 scheduler 주기 작업)."""
    with db() as con:
        for t in ("checkpoints", "writes"):
            con.execute(f"DELETE FROM {t} WHERE thread_id=?", (thread_id,))
        _log(con, "checkpoint_delete_thread", thread_id)
    return {"ok": True}


# ── 작업(job) · 실행 허가(permit) · lease ────────────────────────
# 실제 설계에서 job_create는 api가, job_reserve_pending/job_mark_sent는 scheduler가 PG에 직접 하는 일이다.
# 이 PoC는 DB를 MCP 서버 하나에 모아 두었기 때문에 같은 서버의 도구로 흉내 낸다.
# worker가 쓰는 것은 job_start / job_heartbeat / job_finish 뿐이다.
STREAM_OF = {  # 작업 종류 → Stream. rt = 사용자가 화면 앞에서 기다리는 짧은 작업, batch = 길어도 되는 작업
    "live_summary": "notree:rt", "qa": "notree:rt", "grading": "notree:rt",
    "final_note": "notree:batch", "course_update": "notree:batch",
}

# ── permit 토큰 ─────────────────────────────────────────────────
# job_start가 발급하고, worker는 이후 모든 도구 호출에 첨부한다. 서버는 서명·만료·job_id 일치를 확인한다.
# 형식: "<job_id>.<worker>.<만료 unix초>.<HMAC-SHA256 앞 32자>"   (실제로는 비밀키를 환경변수/시크릿으로)
PERMIT_SECRET = os.getenv("NOTREE_PERMIT_SECRET", "dev-only-secret").encode()
PERMIT_TTL = float(os.getenv("NOTREE_PERMIT_TTL", "3600"))
REQUIRE_PERMIT = os.getenv("NOTREE_REQUIRE_PERMIT", "0") == "1"   # 1이면 permit 없는 worker 도구 호출 거절


def _sign(body: str) -> str:
    return hmac.new(PERMIT_SECRET, body.encode(), hashlib.sha256).hexdigest()[:32]


def _issue_permit(job_id: str, worker: str) -> str:
    body = f"{job_id}.{worker}.{int(time.time() + PERMIT_TTL)}"
    return f"{body}.{_sign(body)}"


def _check_permit(permit: str | None, job_id: str) -> str | None:
    """문제가 없으면 None, 있으면 거절 사유."""
    if permit is None:
        return "permit 필요" if REQUIRE_PERMIT else None
    try:
        body, sig = permit.rsplit(".", 1)
        p_job, _worker, exp = body.rsplit(".", 2)
    except ValueError:
        return "permit 형식 오류"
    if not hmac.compare_digest(sig, _sign(body)):
        return "permit 서명 불일치"
    if p_job != job_id:
        return "다른 작업의 permit"
    if float(exp) < time.time():
        return "permit 만료"
    return None


@mcp.tool()
def job_create(kind: str, payload: dict, stream: str | None = None) -> dict:
    """[api 역할] 업무 데이터와 같은 트랜잭션에 '의도(intent)'를 남긴다. 큐에는 아직 안 간다."""
    job_id = f"op-{uuid.uuid4().hex[:8]}"
    with db() as con:
        con.execute("INSERT INTO jobs(job_id, kind, payload, stream, state, created) VALUES(?,?,?,?,?,?)",
                    (job_id, kind, json.dumps(payload, ensure_ascii=False),
                     stream or STREAM_OF.get(kind, "notree:batch"), "pending", time.time()))
        _log(con, "job_create", job_id)
    return {"job_id": job_id}


@mcp.tool()
def job_reserve_pending(limit: int = 10) -> dict:
    """[scheduler 배정기] 대기 중인 의도를 골라 실행 허가(reserved)를 기록. 동시 상한·예산 검사가 들어갈 자리."""
    with db() as con:
        rows = con.execute("SELECT job_id, kind, stream FROM jobs WHERE state='pending' ORDER BY created LIMIT ?",
                           (limit,)).fetchall()
        for r in rows:
            con.execute("UPDATE jobs SET state='reserved' WHERE job_id=?", (r["job_id"],))
            _log(con, "job_reserve_pending", r["job_id"])
        return {"jobs": [dict(r) for r in rows]}


@mcp.tool()
def job_mark_sent(job_id: str) -> dict:
    """[scheduler 배정기] Redis에 XADD한 뒤 '전송 완료'로 표시."""
    with db() as con:
        con.execute("UPDATE jobs SET state='sent' WHERE job_id=? AND state='reserved'", (job_id,))
        _log(con, "job_mark_sent", job_id)
    return {"ok": True}


@mcp.tool()
def job_start(job_id: str, worker: str, lease_sec: float) -> dict:
    """[worker] 실행 시작. 다른 worker의 lease가 살아 있으면 거절, 이미 끝났거나 실패 확정이면 그 사실을 알려 준다.
    성공하면 permit 토큰을 발급한다."""
    now = time.time()
    with db() as con:
        r = con.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
        _log(con, "job_start", job_id)
        if not r:
            return {"ok": False, "reason": "unknown"}
        if r["state"] in ("done", "failed"):
            return {"ok": False, "reason": r["state"]}
        if r["state"] == "running" and r["lease_owner"] != worker and r["lease_until"] > now:
            return {"ok": False, "reason": "leased", "owner": r["lease_owner"],
                    "left": round(r["lease_until"] - now, 1)}
        con.execute("UPDATE jobs SET state='running', lease_owner=?, lease_until=?, attempts=attempts+1 "
                    "WHERE job_id=?", (worker, now + lease_sec, job_id))
        return {"ok": True, "kind": r["kind"], "payload": json.loads(r["payload"]),
                "attempt": r["attempts"] + 1, "previous_owner": r["lease_owner"],
                "permit": _issue_permit(job_id, worker)}


@mcp.tool()
def job_heartbeat(job_id: str, worker: str, lease_sec: float, permit: str | None = None) -> dict:
    """[worker] lease 연장. 다른 worker에게 넘어갔으면 ok=false → 하던 일을 멈춰야 한다."""
    if err := _check_permit(permit, job_id):
        return {"ok": False, "reason": err}
    with db() as con:
        cur = con.execute("UPDATE jobs SET lease_until=? WHERE job_id=? AND lease_owner=? AND state='running'",
                          (time.time() + lease_sec, job_id, worker))
        return {"ok": cur.rowcount == 1}


@mcp.tool()
def job_finish(job_id: str, worker: str, permit: str | None = None) -> dict:
    """[worker] 완료 표시. 이 뒤에 XACK."""
    if err := _check_permit(permit, job_id):
        return {"ok": False, "reason": err}
    with db() as con:
        cur = con.execute("UPDATE jobs SET state='done', lease_until=NULL WHERE job_id=? AND lease_owner=?",
                          (job_id, worker))
        _log(con, "job_finish", job_id)
        return {"ok": cur.rowcount == 1}


@mcp.tool()
def job_fail(job_id: str, worker: str, reason: str, permit: str | None = None) -> dict:
    """[worker] 다시 해도 안 되는 실패로 확정. 이 뒤에 XACK. (화면에 '실패'를 보여 줄 근거)"""
    if err := _check_permit(permit, job_id):
        return {"ok": False, "reason": err}
    with db() as con:
        cur = con.execute("UPDATE jobs SET state='failed', lease_until=NULL, fail_reason=? "
                          "WHERE job_id=? AND lease_owner=?", (reason, job_id, worker))
        _log(con, "job_fail", job_id)
        return {"ok": cur.rowcount == 1}


@mcp.tool()
def job_release(job_id: str, worker: str, reason: str, permit: str | None = None) -> dict:
    """[worker] 일시적 실패. lease를 바로 반납해 재배송 때 누구든 곧바로 다시 잡을 수 있게 한다. ACK는 안 함."""
    if err := _check_permit(permit, job_id):
        return {"ok": False, "reason": err}
    with db() as con:
        cur = con.execute("UPDATE jobs SET state='sent', lease_until=NULL, fail_reason=? "
                          "WHERE job_id=? AND lease_owner=? AND state='running'", (reason, job_id, worker))
        _log(con, "job_release", job_id)
        return {"ok": cur.rowcount == 1}


@mcp.tool()
def job_get(job_id: str) -> dict:
    with db() as con:
        r = con.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
        return {**dict(r), "payload": json.loads(r["payload"])} if r else {}


# ── 발행 도구 ─────────────────────────────────────────────────────
@mcp.tool()
def note_publish(idem_key: str, course: str, note: str, status: str) -> dict:
    """멱등키로 한 번만 발행. 실제 설계의 발행 게이트(세대·출처 확인)는 여기서 하게 된다."""
    with db() as con:
        cur = con.execute("INSERT OR IGNORE INTO notes VALUES(?,?,?,?)", (idem_key, course, note, status))
        _log(con, "note_publish", idem_key)
        return {"published": cur.rowcount == 1}


@mcp.tool()
def note_get(idem_key: str) -> dict:
    with db() as con:
        r = con.execute("SELECT * FROM notes WHERE idem_key=?", (idem_key,)).fetchone()
        return dict(r) if r else {}


@mcp.tool()
def tool_stats(thread_id: str) -> dict:
    """이 thread에 대해 각 도구가 몇 번 불렸는지 (실행 흐름 확인용)."""
    with db() as con:
        rows = con.execute("SELECT tool, COUNT(*) n FROM tool_log WHERE thread_id=? GROUP BY tool",
                           (thread_id,)).fetchall()
        return {r["tool"]: r["n"] for r in rows}


_init_db()

if __name__ == "__main__":
    mcp.run("streamable-http" if "--http" in sys.argv else "stdio")
