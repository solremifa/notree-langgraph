"""수업 직후 Q&A가 한꺼번에 몰릴 때 검색 쿼리 부하.

    uv run python -m notree.dev.pgload.qa_burst --n 200 --mode course     # 과목 전체 전사 검색 (현행 설계)
    uv run python -m notree.dev.pgload.qa_burst --n 200 --mode lecture    # 노트에서 찾은 강의 2개 안에서만 전사 검색 (개선안)
n개의 질문을 동시에 던지고(연결 풀 20 = MCP 서버 풀), 끝날 때까지 걸린 시간과 질문별 지연을 잰다.
"""
import argparse, asyncio, os, random, time
import asyncpg

DSN = os.getenv("NOTREE_PG_DSN", "postgresql://notree@127.0.0.1:55432/postgres")          # RLS가 걸리는 앱 계정
QS = ["스택 오버플로 재귀", "힙 정렬 시간 복잡도", "이진 트리 순회 순서", "해시 충돌 해결", "최단 경로 알고리즘"]


async def one(pool, u, mode):
    q = random.choice(QS)
    course = ((u - 1) % 200 + 1) * 10 + 1
    t = time.perf_counter()
    async with pool.acquire() as c, c.transaction():
        await c.execute("SELECT set_config('app.user_id', $1, true)", str((u - 1) % 200 + 1))
        notes = await c.fetch("SELECT id, lecture_id FROM note_blocks WHERE course_id=$1 AND $2 <% text "
                              "ORDER BY word_similarity($2, text) DESC LIMIT 5", course, q)
        if mode == "course":
            await c.fetch("SELECT id FROM transcript_segments WHERE course_id=$1 AND $2 <% text "
                          "ORDER BY word_similarity($2, text) DESC LIMIT 5", course, q)
        else:
            lecs = list(dict.fromkeys(r["lecture_id"] for r in notes))[:2] or [course * 100 + 1]
            await c.fetch("SELECT id FROM transcript_segments WHERE lecture_id = ANY($1) AND $2 <% text "
                          "ORDER BY word_similarity($2, text) DESC LIMIT 5", lecs, q)
    return (time.perf_counter() - t) * 1000


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=100)
    ap.add_argument("--mode", choices=["course", "lecture"], default="course")
    ap.add_argument("--pool", type=int, default=20)
    a = ap.parse_args()
    pool = await asyncpg.create_pool(DSN, min_size=a.pool, max_size=a.pool, init=lambda c: c.execute("SET search_path=app,public"))
    await asyncio.gather(*(one(pool, u, a.mode) for u in range(1, 21)))      # 데우기
    t = time.perf_counter()
    lat = sorted(await asyncio.gather(*(one(pool, u, a.mode) for u in range(1, a.n + 1))))
    wall = time.perf_counter() - t
    p = lambda x: lat[min(len(lat) - 1, int(len(lat) * x))]
    print(f"{a.mode:8} 질문 {a.n:4}개 동시 → 전부 끝날 때까지 {wall:5.1f}초 · 처리량 {a.n / wall:5.1f}개/초 · "
          f"지연 p50 {p(.5):6.0f}ms p95 {p(.95):6.0f}ms 최대 {lat[-1]:6.0f}ms")
    await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
