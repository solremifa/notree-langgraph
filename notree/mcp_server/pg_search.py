"""Q&A 근거 검색의 PostgreSQL(pg_trgm) 구현. MCP 서버의 qa_search·qa_seed가 NOTREE_QA_SEARCH=pg일 때 쓴다.

    NOTREE_QA_SEARCH=pg  NOTREE_PG_QA_DSN=postgresql://postgres@127.0.0.1:55432/notree_qa

DB는 CRUD만 (팀 결정): 여기서 하는 일은 "범위 안 문서를 점수와 함께 읽기"뿐이다. 어느 강의를 먼저 찾을지,
언제 넓힐지, 무엇을 LLM에 넘길지는 그래프(애플리케이션)가 정한다.

점수 kw_mean = 검색어마다 word_similarity(검색어, 본문)의 평균 (0~1).
  2026-10-17 실제 강의 7개·질문 33개 평가에서 가장 좋았던 방식 (docs/measurements/2026-10-17-pg-search). 잠정값.
범위: 사용자 + 과목 + 출처(note | transcript) + (선택) 강의 목록, 삭제된 문서(deleted_at)는 제외.
지금은 범위 안 문서마다 점수를 계산한다 (문서 1,100개에서 질문당 약 100ms). 큰 규모에서는 trgm 인덱스로 후보를 먼저
줄여야 하고, 그때 정답 후보가 잘리지 않는지 이 전체 계산과 비교해야 한다.
질문·과목 행은 PoC에서 아직 SQLite(작업 DB)에 있다. 여기는 검색 문서만.
"""
import os

import psycopg

DSN = os.getenv("NOTREE_PG_QA_DSN", "postgresql://postgres@127.0.0.1:55432/notree_qa")
# 시간 제한 (잠정). 그래프의 검색 대기(SEARCH_TIMEOUT_SEC 8초)보다 짧게 둬서, 그래프가 기다리기를 그만둔 뒤에도
# 서버 쪽 연결·쿼리가 계속 도는 일이 없게 한다. statement_timeout은 잠금 대기도 포함한다.
CONNECT_TIMEOUT_SEC = int(os.getenv("NOTREE_PG_QA_CONNECT_TIMEOUT_SEC", "2"))       # libpq 최소 2초
STATEMENT_TIMEOUT_MS = int(os.getenv("NOTREE_PG_QA_STATEMENT_TIMEOUT_MS", "5000"))

SCHEMA = """
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE TABLE IF NOT EXISTS qa_search_docs(
  doc_id text PRIMARY KEY, user_id text NOT NULL, course_id text NOT NULL, lecture_id text,
  source text NOT NULL CHECK (source IN ('note', 'transcript')), kind text, ref text, text text NOT NULL,
  deleted_at timestamptz);
CREATE INDEX IF NOT EXISTS qa_search_docs_scope ON qa_search_docs(user_id, course_id, source, lecture_id);
CREATE INDEX IF NOT EXISTS qa_search_docs_trgm ON qa_search_docs USING gin (text gin_trgm_ops);
"""

KW_MEAN = "(SELECT avg(word_similarity(k, text))::float8 FROM unnest(%(kw)s::text[]) k)"


def connect():
    return psycopg.connect(DSN, autocommit=True, connect_timeout=CONNECT_TIMEOUT_SEC,
                           options=f"-c statement_timeout={STATEMENT_TIMEOUT_MS}")


def ensure_schema(con) -> None:
    con.execute(SCHEMA)


def upsert_docs(user_id: str, course_id: str, docs: list[dict]) -> int:
    """노트·전사 문서를 넣는다 (같은 doc_id는 덮어씀, 삭제 표시는 풀림). 웹 문서는 여기 넣지 않는다."""
    rows = [(d["id"], user_id, course_id, d.get("lecture_id"), d["source"], d.get("kind"), d.get("ref") or d["id"], d["text"])
            for d in docs if d["source"] in ("note", "transcript")]
    if not rows:
        return 0
    with connect() as con:
        ensure_schema(con)
        with con.cursor() as cur:
            cur.executemany(
                "INSERT INTO qa_search_docs(doc_id, user_id, course_id, lecture_id, source, kind, ref, text) "
                "VALUES(%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (doc_id) DO UPDATE SET user_id=EXCLUDED.user_id, "
                "course_id=EXCLUDED.course_id, lecture_id=EXCLUDED.lecture_id, source=EXCLUDED.source, kind=EXCLUDED.kind, "
                "ref=EXCLUDED.ref, text=EXCLUDED.text, deleted_at=NULL", rows)
    return len(rows)


def search(user_id: str, course_id: str, source: str, keywords: list[str], limit: int,
           lecture_ids: list[str] | None = None) -> list[dict]:
    """범위 안 문서를 kw_mean 점수순으로. 점수 0인 문서는 빼고, 출처 ID·강의 ID를 그대로 돌려준다."""
    if not keywords:
        return []
    sql = (f"SELECT doc_id, lecture_id, ref, text, {KW_MEAN} AS score FROM qa_search_docs "
           "WHERE user_id=%(u)s AND course_id=%(c)s AND source=%(s)s AND deleted_at IS NULL"
           + (" AND lecture_id = ANY(%(lec)s::text[])" if lecture_ids else "")
           + " ORDER BY score DESC LIMIT %(n)s")
    with connect() as con:
        rows = con.execute(sql, {"kw": keywords, "u": user_id, "c": course_id, "s": source,
                                 "lec": lecture_ids, "n": limit}).fetchall()
    return [{"id": r[0], "lecture_id": r[1], "ref": r[2], "text": r[3], "score": round(r[4] or 0.0, 3)}
            for r in rows if (r[4] or 0) > 0]
