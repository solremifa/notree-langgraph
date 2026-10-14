"""내부 MCP 서버 (A안).

이 프로세스만 DB(out/notree.db)를 연다. worker(그래프)는 아래 도구를 MCP로 호출할 뿐이다.
실제 설계의 checkpoint.put/get, note.publish에 해당한다. (MCP 도구 이름은 점 대신 밑줄)

체크포인트 내용은 LangGraph가 직렬화한 바이트를 base64로 받아 그대로 저장한다.
서버는 LangGraph를 몰라도 된다(불투명 blob 저장소).

실행: worker가 stdio로 자동으로 띄운다.  직접 확인:  mcp dev mcp_server.py
"""
import os
import sqlite3
from pathlib import Path

from mcp.server.fastmcp import FastMCP

DB = Path(os.getenv("NOTREE_DB", "out/notree.db"))
DB.parent.mkdir(parents=True, exist_ok=True)

mcp = FastMCP("notree-internal", log_level="WARNING")


def db() -> sqlite3.Connection:
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    con.executescript(
        """
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
    )
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


if __name__ == "__main__":
    mcp.run()  # stdio
