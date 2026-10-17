"""작업 종류별 실행 정책 (팀 결정 2·3번, 2026-10-17 확정).

  qa·grading    : 요청부터 전체 90초 안에서 최초 포함 최대 3회
  final_note    : 시도당 5분, 최초 포함 총 3회
  course_update : 시도당 10분, 최초 포함 총 3회
"""
import dataclasses
import json
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from notree.common import settings as S  # noqa: E402
from test_skeleton import Env, run  # noqa: E402


def test_attempts_and_timeouts_per_kind():
    assert S.max_attempts_for("qa", S.QA) == 3
    assert S.max_attempts_for("grading", S.QA) == 3
    assert S.max_attempts_for("final_note", S.BATCH) == 3
    assert S.max_attempts_for("course_update", S.BATCH) == 3
    assert S.max_attempts_for("live_summary", S.RT) == 1
    assert S.job_timeout_for("final_note", S.BATCH) == 300
    assert S.job_timeout_for("course_update", S.BATCH) == 600
    assert S.job_timeout_for("qa", S.QA) == 90                  # 시도당 제한 없음 → 레인 제한(90초)과 마감 중 짧은 쪽
    short = dataclasses.replace(S.BATCH, job_timeout_sec=20)
    assert S.job_timeout_for("course_update", short) == 20     # 레인 제한보다 길게 늘리지 않는다


def test_qa_and_grading_get_a_90s_deadline_from_request(tmp_path):
    async def t():
        async with Env(tmp_path, allocator=False) as e:
            for kind in ("qa", "grading"):
                t0 = time.time()
                jid = (await e.api.tool("job_create", kind=kind, payload={}, user_id="u"))["job_id"]
                dl = (await e.row(jid))["payload"]["deadline_at"]
                assert t0 + 89 <= dl <= time.time() + 90
            given = time.time() + 30
            jid = (await e.api.tool("job_create", kind="qa", payload={"deadline_at": given}, user_id="u"))["job_id"]
            assert (await e.row(jid))["payload"]["deadline_at"] == given      # api가 준 값은 그대로
            jid = (await e.api.tool("job_create", kind="final_note", payload={}, user_id="u"))["job_id"]
            assert "deadline_at" not in (await e.row(jid))["payload"]
    run(t())


def test_user_retry_of_qa_gets_a_fresh_deadline(tmp_path):
    async def t():
        async with Env(tmp_path, allocator=False) as e:
            jid = (await e.api.tool("job_create", kind="qa", payload={}, user_id="u"))["job_id"]
            con = sqlite3.connect(e.db, timeout=10)                       # 마감 초과로 실패한 상태를 만든다
            con.execute("UPDATE jobs SET state='failed', user_retryable=1, payload=? WHERE job_id=?",
                        (json.dumps({"deadline_at": time.time() - 5}), jid))
            con.commit()
            con.close()
            assert (await e.api.tool("job_retry", job_id=jid))["revived"]
            row = await e.row(jid)
            assert row["state"] == "pending" and row["payload"]["deadline_at"] > time.time() + 85
    run(t())
