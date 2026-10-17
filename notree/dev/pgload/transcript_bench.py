"""확정 전사 저장 방식 비교.

    per_session : 녹음마다 1초에 한 번 트랜잭션 (현행 설계)
    grouped     : gateway가 모든 녹음의 전사를 250ms마다 모아 한 트랜잭션으로 저장 (개선안)
"""
import argparse, asyncio, os, os, random, time
import asyncpg

DSN = os.getenv("NOTREE_PG_DSN", "postgresql://notree@127.0.0.1:55432/postgres")          # RLS가 걸리는 앱 계정
ADMIN = os.getenv("NOTREE_PG_ADMIN_DSN", "postgresql://postgres@127.0.0.1:55432/postgres")  # 통계 조회용
W = "스택 큐 힙 트리 정렬 탐색 재귀 메모리 배열 해시 노드 경로 시간 복잡도 교수님이 설명했다".split()
LAT = []


def s():
    return " ".join(random.choice(W) for _ in range(10))


def pg_cpu():
    t = 0.0
    for p in os.listdir("/proc"):
        if p.isdigit():
            try:
                x = open(f"/proc/{p}/stat").read()
                if "(postgres)" in x:
                    f = x.rsplit(")", 1)[1].split(); t += (int(f[11]) + int(f[12])) / os.sysconf("SC_CLK_TCK")
            except Exception:
                pass
    return t


async def per_session(pool, u, stop):
    seq = 0
    await asyncio.sleep(random.random())
    while not stop.is_set():
        t = time.perf_counter(); seq += 3
        async with pool.acquire() as c, c.transaction():
            await c.execute("SELECT set_config('app.user_id', $1, true)", str((u - 1) % 200 + 1))
            await c.execute("INSERT INTO transcript_segments(lecture_id, course_id, user_id, seq, text) "
                            "SELECT $1,$2,$3,x,y FROM unnest($4::int[],$5::text[]) AS z(x,y)",
                            20_000_000 + u, ((u - 1) % 200 + 1) * 10 + 1, (u - 1) % 200 + 1, [seq - 2, seq - 1, seq], [s(), s(), s()])
        LAT.append((time.perf_counter() - t) * 1000)
        await asyncio.sleep(max(0, 1 - (time.perf_counter() - t)))


async def grouped(pool, n, stop):
    buf, seqs = [], [0] * (n + 1)

    async def producer(u):
        await asyncio.sleep(random.random())
        while not stop.is_set():
            seqs[u] += 3
            for k in range(3):
                buf.append((20_000_000 + u, ((u - 1) % 200 + 1) * 10 + 1, (u - 1) % 200 + 1, seqs[u] - 2 + k, s(), time.perf_counter()))
            await asyncio.sleep(1)

    async def flusher():
        while not stop.is_set():
            await asyncio.sleep(0.25)
            if not buf:
                continue
            rows = buf[:]; buf.clear()
            async with pool.acquire() as c, c.transaction():
                # gateway는 서비스 계정으로 쓰고, 행마다 user_id를 직접 넣는다 (RLS는 읽기 쪽에서)
                await c.execute("SELECT set_config('app.user_id', '0', true)")
                await c.execute("INSERT INTO transcript_segments(lecture_id, course_id, user_id, seq, text) "
                                "SELECT * FROM unnest($1::bigint[],$2::bigint[],$3::bigint[],$4::int[],$5::text[])",
                                [r[0] for r in rows], [r[1] for r in rows], [r[2] for r in rows], [r[3] for r in rows], [r[4] for r in rows])
            done = time.perf_counter()
            LAT.extend((done - r[5]) * 1000 for r in rows[::3])

    await asyncio.gather(flusher(), *(producer(u) for u in range(1, n + 1)))


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=800)
    ap.add_argument("--mode", choices=["per_session", "grouped"], default="per_session")
    ap.add_argument("--seconds", type=int, default=30)
    a = ap.parse_args()
    admin = await asyncpg.connect(ADMIN)
    await admin.execute("DELETE FROM app.transcript_segments WHERE lecture_id >= 20000000")
    await admin.execute("ALTER TABLE app.transcript_segments DISABLE ROW LEVEL SECURITY") if a.mode == "grouped" else None
    pool = await asyncpg.create_pool(DSN, min_size=40, max_size=40, init=lambda c: c.execute("SET search_path=app,public"))
    stop = asyncio.Event()
    async def all_sessions():
        await asyncio.gather(*(per_session(pool, u, stop) for u in range(1, a.n + 1)))
    task = asyncio.create_task(grouped(pool, a.n, stop) if a.mode == "grouped" else all_sessions())
    await asyncio.sleep(5); LAT.clear()
    x0 = await admin.fetchval("SELECT xact_commit FROM pg_stat_database WHERE datname='postgres'")
    w0 = await admin.fetchval("SELECT pg_current_wal_lsn() - '0/0'::pg_lsn"); c0 = pg_cpu(); t0 = time.time()
    await asyncio.sleep(a.seconds)
    dt = time.time() - t0
    x1 = await admin.fetchval("SELECT xact_commit FROM pg_stat_database WHERE datname='postgres'")
    w1 = await admin.fetchval("SELECT pg_current_wal_lsn() - '0/0'::pg_lsn"); c1 = pg_cpu()
    stop.set(); await asyncio.gather(task, return_exceptions=True)
    await admin.execute("ALTER TABLE app.transcript_segments ENABLE ROW LEVEL SECURITY")
    L = sorted(LAT); p = lambda q: L[min(len(L) - 1, int(len(L) * q))]
    print(f"{a.mode:11} 녹음 {a.n:5}개 → 트랜잭션 {(x1 - x0) / dt:7.1f}/s · PG CPU {(c1 - c0) / dt / 2 * 100:5.1f}% (2코어 중) · "
          f"WAL {(int(w1) - int(w0)) / dt / 1024:7.1f}KB/s · 저장까지 p50 {p(.5):6.1f}ms p99 {p(.99):6.1f}ms")
    await pool.close(); await admin.close()

if __name__ == "__main__":
    asyncio.run(main())
