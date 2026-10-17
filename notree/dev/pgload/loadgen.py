"""Notree DB 부하 측정기.

설계 문서·데이터 흐름 페이지의 DB 호출을 사용자 수만큼 흉내 낸다. 실제 PostgreSQL 16에 붙는다.

    uv run python -m notree.dev.pgload.loadgen --users 200 --design naive --seconds 60

design
  naive : 설계 그대로
          - 확정 전사 1초 커밋마다 요약 작업 등록 (강의당 대기 1개는 고유 인덱스로 막음 → 요약이 쉬지 않고 돎)
          - 배정기 0.5초 폴링, 전송 표시는 작업마다
          - heartbeat 5초마다 작업 행 UPDATE
          - API 요청마다 세션 조회 + 마지막 접속 시각 UPDATE
          - 답안 2초마다 자동 저장
  fixed : 대책 적용
          - 요약 작업은 강의당 30초에 한 번만 등록 (+ 고유 인덱스)
          - 배정기는 NOTIFY로 깨우고 10초 대비 폴링, 전송 표시는 한 문장으로
          - heartbeat는 Redis로 옮겼다고 보고 DB에 쓰지 않음 (Redis 비용은 측정 밖)
          - 세션은 30초 캐시, 접속 시각은 5분에 한 번
          - 답안은 10초마다, 바뀌었을 때만(절반) 저장
공통: Q&A는 학습 중 사용자마다 2분에 한 번 (노트·전사 pg_trgm 검색 2번), 결과는 outbox + NOTIFY → gateway가 읽음.
"""
import argparse
import asyncio
import json
import os
import random
import resource
import statistics
import time
from collections import defaultdict

import asyncpg

DSN = os.getenv("NOTREE_PG_DSN", "postgresql://notree@127.0.0.1:55432/postgres")          # RLS가 걸리는 앱 계정
ADMIN = os.getenv("NOTREE_PG_ADMIN_DSN", "postgresql://postgres@127.0.0.1:55432/postgres")  # 통계 조회용
VOCAB = ("스택 큐 힙 트리 그래프 정렬 탐색 재귀 오버플로 메모리 포인터 배열 연결 리스트 해시 충돌 노드 간선 "
         "최단 경로 동적 계획법 분할 정복 알고리즘 시간 복잡도 공간 이진 균형 회전 삽입 삭제 순회 교수님이 "
         "설명했다 예를 들어 따라서 중요하다 시험에 나온다 구조 연산 호출 반환 조건 반복 함수").split()
QUESTIONS = ["스택 오버플로 재귀", "힙 정렬 시간 복잡도", "이진 트리 순회 순서", "해시 충돌 해결", "최단 경로 알고리즘"]

LAT = defaultdict(list)      # 작업 이름 → 지연(ms), 연결 대기 포함
COUNT = defaultdict(int)
JOB_E2E = defaultdict(list)  # 작업 종류 → 등록부터 완료까지(초)
STOP = asyncio.Event()
MEASURING = False
QA_QUEUE = None


def sentence(n=10):
    return " ".join(random.choice(VOCAB) for _ in range(n))


class Op:
    def __init__(self, name):
        self.name = name

    async def __aenter__(self):
        self.t = time.perf_counter()

    async def __aexit__(self, *exc):
        if MEASURING and exc[0] is None:
            LAT[self.name].append((time.perf_counter() - self.t) * 1000)
            COUNT[self.name] += 1


def data_user(u):
    return (u - 1) % 200 + 1


def lecture_of(u):
    return 10_000_000 + u


def course_of(u):
    return data_user(u) * 10 + 1


async def tick(period, jitter=True):
    await asyncio.sleep(period * (random.random() if jitter else 1))


# ── 녹음 중인 사용자 ──────────────────────────────────────────────
async def recording_user(u, api, design):
    lec, last_intent, seq, n = lecture_of(u), 0.0, 0, 0
    await tick(1.0)
    while not STOP.is_set():
        t0 = time.perf_counter()
        seq += 3
        enqueue = design == "naive" or (time.monotonic() - last_intent >= 30)
        async with Op("전사 커밋" + (" + 요약 등록" if enqueue else "")):
            async with api.acquire() as c, c.transaction():
                await c.execute("SELECT set_config('app.user_id', $1, true)", str(data_user(u)))
                await c.execute(
                    "INSERT INTO transcript_segments(lecture_id, course_id, user_id, seq, text) "
                    "SELECT $1, $2, $3, s, t FROM unnest($4::int[], $5::text[]) AS x(s, t)",
                    lec, course_of(u), data_user(u), [seq - 2, seq - 1, seq], [sentence(), sentence(), sentence()])
                if enqueue:
                    r = await c.fetchval(
                        "INSERT INTO jobs(kind, user_id, lecture_id, stream, payload) VALUES('live_summary', $1, $2, 'rt', '{}') "
                        "ON CONFLICT DO NOTHING RETURNING job_id", u, lec)
                    if design == "fixed":
                        last_intent = time.monotonic()
                        if r:
                            await c.execute("SELECT pg_notify('jobs', '')")
        n += 1
        if n % 10 == 0:
            async with Op("음성 manifest"):
                async with api.acquire() as c:
                    await c.execute("INSERT INTO audio_segments(lecture_id, seq, uri, bytes, sha) VALUES($1,$2,$3,$4,$5)",
                                    lec, n // 10, f"s3://audio/{lec}/{n}", 160000, "x" * 64)
        await asyncio.sleep(max(0, 1.0 - (time.perf_counter() - t0)))


# ── 학습 중인 사용자 ──────────────────────────────────────────────
async def studying_user(u, api, design):
    sess, cache_until, seen_at, last_save, qa_at = f"s{(u - 1) % 1000 + 1}", 0.0, 0.0, 0.0, time.monotonic() + random.random() * 120
    next_req = time.monotonic() + random.random() * 3
    await tick(1.0)
    while not STOP.is_set():
        now = time.monotonic()
        # API 요청 (3초마다): 세션 확인
        req = now >= next_req
        if req:
            next_req = now + 3
        if req and (design == "naive" or now >= cache_until):
            async with Op("세션 조회"):
                async with api.acquire() as c:
                    await c.fetchrow("SELECT user_id, revoked FROM sessions WHERE id=$1", sess)
            cache_until = now + 30
        if req and (design == "naive" or now - seen_at >= 300):
            async with Op("접속 시각 갱신"):
                async with api.acquire() as c:
                    await c.execute("UPDATE sessions SET last_seen=now() WHERE id=$1", sess)
            seen_at = now
        # 답안 자동 저장
        period = 2 if design == "naive" else 10
        if now - last_save >= period:
            last_save = now
            if design == "naive" or random.random() < 0.5:
                async with Op("답안 자동 저장"):
                    async with api.acquire() as c:
                        await c.execute(
                            "INSERT INTO answer_drafts(user_id, question_id, body, updated) VALUES($1, 1, $2, now()) "
                            "ON CONFLICT (user_id, question_id) DO UPDATE SET body=excluded.body, updated=now()",
                            u, sentence(60))
        # Q&A (2분에 한 번)
        if now >= qa_at:
            qa_at = now + 120
            async with Op("Q&A 등록"):
                async with api.acquire() as c, c.transaction():
                    r = await c.fetchval("INSERT INTO jobs(kind, user_id, stream, payload) VALUES('qa', $1, 'rt', $2) RETURNING job_id",
                                         u, json.dumps({"q": random.choice(QUESTIONS)}))
                    if design == "fixed":
                        await c.execute("SELECT pg_notify('jobs', '')")
        await asyncio.sleep(1.0)


# ── 배정기 ───────────────────────────────────────────────────────
async def allocator(admin_conn_factory, queue, design):
    conn = await admin_conn_factory()
    wake = asyncio.Event()
    if design == "fixed":
        await conn.add_listener("jobs", lambda *a: wake.set())
    work = await admin_conn_factory()
    last_resend = time.monotonic()
    while not STOP.is_set():
        if design == "naive":
            await asyncio.sleep(0.5)
        else:
            try:
                await asyncio.wait_for(wake.wait(), 10)
            except asyncio.TimeoutError:
                pass
            wake.clear()
            await asyncio.sleep(0.05)            # 몰려오는 알림을 한 번에 처리
        async with Op("배정기 확인"):
            rows = await work.fetch(
                "WITH c AS (SELECT job_id FROM jobs WHERE state='pending' ORDER BY job_id LIMIT 100 FOR UPDATE SKIP LOCKED) "
                "UPDATE jobs j SET state='reserved', updated_at=now() FROM c WHERE j.job_id=c.job_id "
                "RETURNING j.job_id, j.kind, j.user_id, j.lecture_id, j.created, j.payload")
        ids = [r["job_id"] for r in rows]
        for r in rows:
            (QA_QUEUE if (QA_QUEUE is not None and r["kind"] == "qa") else queue).put_nowait(dict(r))   # Redis XADD 대신
        if ids:
            if design == "naive":
                for i in ids:
                    async with Op("전송 표시"):
                        await work.execute("UPDATE jobs SET state='sent', updated_at=now() WHERE job_id=$1 AND state='reserved'", i)
            else:
                async with Op("전송 표시"):
                    await work.execute("UPDATE jobs SET state='sent', updated_at=now() WHERE job_id = ANY($1) AND state='reserved'", ids)
        if time.monotonic() - last_resend >= 10:
            last_resend = time.monotonic()
            async with Op("누락 복구 확인"):
                await work.fetch("SELECT job_id FROM jobs WHERE (state='reserved' AND updated_at < now()-interval '30 s') "
                                 "OR (state='sent' AND updated_at < now()-interval '120 s') OR (state='running' AND lease_until < now()) LIMIT 100")
    await conn.close(); await work.close()


# ── worker (MCP 서버 연결 풀을 거친다고 보고 mcp 풀 사용) ─────────
async def worker_slot(wid, queue, mcp, design, llm_sec):
    while not STOP.is_set():
        try:
            job = await asyncio.wait_for(queue.get(), 0.5)
        except asyncio.TimeoutError:
            continue
        jid, kind, u = job["job_id"], job["kind"], job["user_id"]
        async with Op("작업 시작"):
            async with mcp.acquire() as c:
                st = await c.fetchrow(
                    "UPDATE jobs SET state='running', lease_owner=$2, lease_until=now()+interval '15 s', attempts=attempts+1, "
                    "exec_epoch=exec_epoch+1, updated_at=now() WHERE job_id=$1 AND state IN ('sent','reserved') RETURNING exec_epoch", jid, wid)
        if not st:
            continue
        epoch = st["exec_epoch"]
        if kind == "live_summary":
            async with Op("최근 전사 읽기"):
                async with mcp.acquire() as c, c.transaction():
                    await c.execute("SELECT set_config('app.user_id', $1, true)", str(data_user(u)))
                    await c.fetch("SELECT text FROM transcript_segments WHERE lecture_id=$1 ORDER BY seq DESC LIMIT 200", job["lecture_id"])
        else:
            q = json.loads(job["payload"])["q"]
            async with Op("Q&A 검색 (노트+전사)"):
                async with mcp.acquire() as c, c.transaction():
                    await c.execute("SELECT set_config('app.user_id', $1, true)", str(data_user(u)))
                    await c.fetch("SELECT id FROM note_blocks WHERE course_id=$1 AND $2 <% text "
                                  "ORDER BY word_similarity($2, text) DESC LIMIT 5", course_of(u), q)
                    await c.fetch("SELECT id FROM transcript_segments WHERE course_id=$1 AND $2 <% text "
                                  "ORDER BY word_similarity($2, text) DESC LIMIT 5", course_of(u), q)
        # LLM 호출 시간 동안 heartbeat
        t_end = time.monotonic() + llm_sec[kind]
        while time.monotonic() < t_end:
            await asyncio.sleep(min(5, t_end - time.monotonic()))
            if design == "naive" and time.monotonic() < t_end:
                async with Op("heartbeat"):
                    async with mcp.acquire() as c:
                        await c.execute("UPDATE jobs SET lease_until=now()+interval '15 s', updated_at=now() "
                                        "WHERE job_id=$1 AND lease_owner=$2 AND exec_epoch=$3", jid, wid, epoch)
        async with Op("결과 발행"):
            async with mcp.acquire() as c, c.transaction():
                live = await c.fetchval("SELECT 1 FROM jobs WHERE job_id=$1 AND state='running' AND lease_owner=$2 AND exec_epoch=$3 FOR UPDATE",
                                        jid, wid, epoch)
                if live:
                    if kind == "live_summary":
                        await c.execute("INSERT INTO summaries(lecture_id, user_id, body) VALUES($1,$2,$3) ON CONFLICT (lecture_id) "
                                        "DO UPDATE SET body=excluded.body, version=summaries.version+1, updated=now()",
                                        job["lecture_id"], u, sentence(300))
                    else:
                        await c.execute("INSERT INTO qa(user_id, course_id, question, answer) VALUES($1,$2,$3,$4)", u, course_of(u), "q", sentence(120))
                    oid = await c.fetchval("INSERT INTO outbox(user_id, topic, payload) VALUES($1,$2,'{}') RETURNING id", u, kind)
                    await c.execute("SELECT pg_notify('outbox', $1)", f"{oid}:{kind}:{u}:{job['lecture_id'] or 0}")
        async with Op("작업 완료"):
            async with mcp.acquire() as c:
                await c.execute("UPDATE jobs SET state='done', lease_owner=NULL, lease_until=NULL, updated_at=now() "
                                "WHERE job_id=$1 AND exec_epoch=$2", jid, epoch)
        if MEASURING:
            JOB_E2E[kind].append(time.time() - job["created"].timestamp())


# ── gateway: NOTIFY를 받아 화면에 보낼 내용을 읽는다 ──────────────
async def gateway(admin_conn_factory, api):
    conn = await admin_conn_factory()
    q = asyncio.Queue()
    await conn.add_listener("outbox", lambda c, pid, ch, payload: q.put_nowait(payload))

    async def reader():
        while not STOP.is_set():
            try:
                p = await asyncio.wait_for(q.get(), 0.5)
            except asyncio.TimeoutError:
                continue
            _, kind, u, lec = p.split(":")
            async with Op("화면 갱신 읽기"):
                async with api.acquire() as c:
                    if kind == "live_summary":
                        await c.fetchrow("SELECT body, version FROM summaries WHERE lecture_id=$1", int(lec))
                    else:
                        await c.fetchrow("SELECT answer FROM qa WHERE user_id=$1 ORDER BY id DESC LIMIT 1", int(u))
    readers = [asyncio.create_task(reader()) for _ in range(4)]
    await STOP.wait()
    await asyncio.gather(*readers)
    await conn.close()


# ── 측정 ─────────────────────────────────────────────────────────
def pg_cpu_seconds():
    tot = 0.0
    tick_hz = os.sysconf("SC_CLK_TCK")
    for pid in os.listdir("/proc"):
        if not pid.isdigit():
            continue
        try:
            with open(f"/proc/{pid}/stat") as f:
                s = f.read()
            if "(postgres)" not in s:
                continue
            parts = s.rsplit(")", 1)[1].split()
            tot += (int(parts[11]) + int(parts[12])) / tick_hz
        except (FileNotFoundError, ProcessLookupError, IndexError, PermissionError):
            pass
    return tot


async def snapshot(admin):
    r = await admin.fetchrow("SELECT xact_commit, xact_rollback, tup_inserted, tup_updated, tup_fetched, blks_read "
                             "FROM pg_stat_database WHERE datname='postgres'")
    wal = await admin.fetchval("SELECT pg_current_wal_lsn() - '0/0'::pg_lsn")
    return dict(r), int(wal)


async def lock_sampler(admin, out):
    while not STOP.is_set():
        if MEASURING:
            n = await admin.fetchval("SELECT count(*) FROM pg_stat_activity WHERE wait_event_type='Lock'")
            act = await admin.fetchval("SELECT count(*) FROM pg_stat_activity WHERE state='active' AND backend_type='client backend'")
            out["lock"].append(n); out["active"].append(act)
            b = await admin.fetchval("SELECT count(*) FROM app.jobs WHERE state IN ('pending','reserved','sent')")
            out["backlog"].append(b)
        await asyncio.sleep(1)


async def reset(admin):
    await admin.execute("TRUNCATE app.jobs, app.outbox, app.summaries, app.answer_drafts, app.qa, app.audio_segments RESTART IDENTITY")
    await admin.execute("DELETE FROM app.transcript_segments WHERE lecture_id >= 10000000")
    await admin.execute("UPDATE app.sessions SET last_seen=now()")
    await admin.execute("VACUUM app.jobs, app.sessions, app.answer_drafts, app.transcript_segments")
    await admin.execute("SELECT pg_stat_reset(); SELECT pg_stat_statements_reset();")


def pct(xs, p):
    if not xs:
        return float("nan")
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * p))]


async def main():
    global MEASURING
    ap = argparse.ArgumentParser()
    ap.add_argument("--users", type=int, default=100)
    ap.add_argument("--design", choices=["naive", "fixed"], default="naive")
    ap.add_argument("--seconds", type=int, default=60)
    ap.add_argument("--warmup", type=int, default=10)
    ap.add_argument("--rec", type=float, default=0.7, help="녹음 중인 사용자 비율")
    ap.add_argument("--api-pool", type=int, default=40)
    ap.add_argument("--mcp-pool", type=int, default=20)
    ap.add_argument("--worker-slots", type=int, default=64)
    ap.add_argument("--json", default="")
    ap.add_argument("--qa-lane", type=int, default=0, help="Q&A 전용 worker 슬롯 수 (0이면 공용 큐)")
    a = ap.parse_args()

    admin = await asyncpg.connect(ADMIN)
    await reset(admin)
    init = lambda c: c.execute("SET search_path=app,public")
    api = await asyncpg.create_pool(DSN, min_size=a.api_pool, max_size=a.api_pool, init=init)
    mcp = await asyncpg.create_pool(DSN, min_size=a.mcp_pool, max_size=a.mcp_pool, init=init)

    async def conn_factory():
        c = await asyncpg.connect(DSN); await c.execute("SET search_path=app,public"); return c

    global QA_QUEUE
    queue = asyncio.Queue()
    if a.qa_lane:
        QA_QUEUE = asyncio.Queue()
    llm = {"live_summary": 6.0, "qa": 4.0}
    n_rec = int(a.users * a.rec)
    tasks = [asyncio.create_task(recording_user(u, api, a.design)) for u in range(1, n_rec + 1)]
    tasks += [asyncio.create_task(studying_user(u, api, a.design)) for u in range(n_rec + 1, a.users + 1)]
    tasks += [asyncio.create_task(allocator(conn_factory, queue, a.design)), asyncio.create_task(gateway(conn_factory, api))]
    tasks += [asyncio.create_task(worker_slot(f"w{i}", QA_QUEUE if i < a.qa_lane else queue, mcp, a.design, llm))
              for i in range(a.worker_slots)]
    samples = {"lock": [], "active": [], "backlog": []}
    tasks.append(asyncio.create_task(lock_sampler(admin, samples)))

    await asyncio.sleep(a.warmup)
    s0, w0, c0, u0 = *(await snapshot(admin)), pg_cpu_seconds(), resource.getrusage(resource.RUSAGE_SELF)
    MEASURING = True
    t0 = time.time()
    await asyncio.sleep(a.seconds)
    MEASURING = False
    dt = time.time() - t0
    s1, w1, c1, u1 = *(await snapshot(admin)), pg_cpu_seconds(), resource.getrusage(resource.RUSAGE_SELF)
    STOP.set()
    await asyncio.gather(*tasks, return_exceptions=True)
    dead = {r["relname"]: (r["n_live_tup"], r["n_dead_tup"]) for r in await admin.fetch(
        "SELECT relname, n_live_tup, n_dead_tup FROM pg_stat_user_tables WHERE relname IN ('jobs','sessions','answer_drafts','summaries')")}
    top = await admin.fetch("SELECT calls, round(total_exec_time::numeric,0) ms, round(mean_exec_time::numeric,2) mean, left(regexp_replace(query,'\\s+',' ','g'),70) q "
                            "FROM pg_stat_statements WHERE query NOT ILIKE '%pg_stat%' ORDER BY total_exec_time DESC LIMIT 6")

    res = {
        "users": a.users, "design": a.design + (f"+qa{a.qa_lane}" if a.qa_lane else ""), "seconds": round(dt, 1),
        "tps": round((s1["xact_commit"] - s0["xact_commit"]) / dt, 1),
        "rows_ins_s": round((s1["tup_inserted"] - s0["tup_inserted"]) / dt, 1),
        "rows_upd_s": round((s1["tup_updated"] - s0["tup_updated"]) / dt, 1),
        "wal_kb_s": round((w1 - w0) / dt / 1024, 1),
        "pg_cpu_pct_of_2": round((c1 - c0) / dt / 2 * 100, 1),
        "client_cpu_pct_of_2": round(((u1.ru_utime + u1.ru_stime) - (u0.ru_utime + u0.ru_stime)) / dt / 2 * 100, 1),
        "lock_wait_avg": round(statistics.mean(samples["lock"]), 2) if samples["lock"] else 0,
        "active_avg": round(statistics.mean(samples["active"]), 1) if samples["active"] else 0,
        "backlog_max": max(samples["backlog"]) if samples["backlog"] else 0,
        "backlog_end": samples["backlog"][-1] if samples["backlog"] else 0,
        "ops": {k: {"n_s": round(COUNT[k] / dt, 2), "p50": round(pct(v, .5), 1), "p95": round(pct(v, .95), 1), "p99": round(pct(v, .99), 1)}
                for k, v in sorted(LAT.items())},
        "job_e2e": {k: {"n": len(v), "p50": round(pct(v, .5), 1), "p95": round(pct(v, .95), 1)} for k, v in JOB_E2E.items()},
        "dead": dead,
        "top": [dict(r) for r in top],
    }
    print(json.dumps(res, ensure_ascii=False, default=str))
    if a.json:
        with open(a.json, "a") as f:
            f.write(json.dumps(res, ensure_ascii=False, default=str) + "\n")
    await api.close(); await mcp.close(); await admin.close()


if __name__ == "__main__":
    asyncio.run(main())
