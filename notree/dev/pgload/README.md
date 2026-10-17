# PostgreSQL 부하 측정 (2026-10-16)

아키텍처 검토 문서 "실측: 사용자가 몰렸을 때"의 재현 도구입니다. 결과 원본은 `docs/measurements/2026-10-16-pg/`.
작업 처리 PoC(SQLite·MCP)와는 별개로, 설계서·데이터 흐름의 DB 호출을 PostgreSQL에 직접 흉내 냅니다.

| 파일 | 하는 일 |
|---|---|
| `schema.sql` | 측정용 최소 스키마 (`app` 스키마, RLS, `pg_trgm` 인덱스, 앱 계정 `notree`) |
| `data.sql` | 합성 데이터: 사용자 200 × 과목 2 × 강의 8 × 전사 800문장 = 256만 행, 노트 블록 강의당 40개. 어휘 약 110개 |
| `loadgen.py` | 사용자 N명(녹음 70%·학습 30%)을 60초 흉내. `--design naive`(설계 그대로) / `fixed`(대책 적용), `--qa-lane N` |
| `qa_burst.py` | 질문 N개를 동시에 던져 Q&A 검색 쿼리만 잼. `--mode course`(과목 전체 전사) / `lecture`(노트로 고른 강의 2개 우선) |
| `transcript_bench.py` | 확정 전사 저장 방식 비교. `--mode per_session`(녹음마다 1초 커밋) / `grouped`(250ms 묶음 커밋) |
| `explain_qa_search.sql` | 과목 전체 검색 쿼리의 실행 계획 확인 |

## 재현

PostgreSQL 16이 55432 포트에 있어야 합니다. Docker라면:

```bash
docker run -d --name notree-pg -p 55432:5432 -e POSTGRES_HOST_AUTH_METHOD=trust postgres:16
psql -h 127.0.0.1 -p 55432 -U postgres -f notree/dev/pgload/schema.sql
psql -h 127.0.0.1 -p 55432 -U postgres -f notree/dev/pgload/data.sql      # 몇 분 걸림
psql -h 127.0.0.1 -p 55432 -U postgres -c "VACUUM ANALYZE"
```

다른 주소면 `NOTREE_PG_DSN`(앱 계정, RLS 적용)과 `NOTREE_PG_ADMIN_DSN`(통계 조회용 superuser)을 지정합니다.

```bash
# 사용자 수별 (원본: sweep.jsonl). 한 번에 약 75초
for n in 50 100 200 400 800; do for d in naive fixed; do
  uv run python -m notree.dev.pgload.loadgen --users $n --design $d --seconds 60 --warmup 10 --json sweep.jsonl
done; done
uv run python -m notree.dev.pgload.loadgen --users 800 --design fixed --qa-lane 16 --json lanes.jsonl   # Q&A 전용 슬롯

uv run python -m notree.dev.pgload.qa_burst --n 500 --mode course        # 원본: qa_burst.txt
uv run python -m notree.dev.pgload.qa_burst --n 500 --mode lecture
uv run python -m notree.dev.pgload.transcript_bench --n 2000 --mode per_session --seconds 30   # 원본: transcript_bench.txt
uv run python -m notree.dev.pgload.transcript_bench --n 2000 --mode grouped --seconds 30
```

- PG CPU는 `/proc`에서 postgres 프로세스 시간을 읽으므로 **PG와 같은 Linux 머신**에서 돌려야 합니다 (`loadgen.py`는 `resource` 모듈 때문에 Windows에서 안 돎).
- LLM 호출은 sleep으로 흉내 냅니다 (요약 6초, Q&A 4초). Redis·네트워크 비용은 넣지 않았습니다.
