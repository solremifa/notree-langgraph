# 2026-10-19 시작 절차·Redis 기본값 측정

변경: [changes/2026-10-19-start-protocol.md](../../changes/2026-10-19-start-protocol.md) · 브랜치 `feat/redis-liveness-default` (기준 `fix/review-6` `969ba03`)

환경: Windows 11, Python 3.13, Redis 7 (`redis:7-alpine` 컨테이너, `--save "" --appendonly no`, 측정·시험 전용), SQLite(MCP 서버 stdio 하위 프로세스).
worker·배정기는 한 프로세스 안 asyncio, MCP 연결마다 서버 프로세스 하나, DB 파일은 하나. 가짜 LLM/대기 작업 — 실제 OpenAI 호출 없음.

**한계**: SQLite·한 머신·한 프로세스. MCP 호출 수는 worker가 보낸 요청 수이고 SQL 문 수·PostgreSQL 부하가 아니다 (PG 성능 개선으로 읽으면 안 됨).
자연 발생 빈도는 이 환경의 관측값이며 운영 빈도의 추정이 아니다. 측정 DB는 임시 폴더에 만들고 저장소에 남기지 않았다.

## 1. 중복 전달 경쟁 — 강제 순서 (시험)

`tests/test_start_protocol.py::test_duplicate_delivery_with_delayed_start_response_does_not_burn_the_only_attempt`
— A의 `job_start`를 DB에 반영시킨 뒤 응답을 쥐고, 그동안 B가 같은 작업의 중복 메시지를 끝까지 처리, 그 뒤 A를 풂.

| | 상태 | 업무 시도 | 요약 함수 | 저장 |
|---|---|---|---|---|
| DB 모드 (수정 전·후) | done | 1 | 1 | 1 |
| Redis 모드 수정 전 `969ba03` | **failed** | **2** | **0** | **0** |
| Redis 모드 수정 후 | done | 1 | 1 | 1 |

## 2. 중복 전달 경쟁 — 자연 발생 빈도 (강제하지 않음)

`notree/dev/start_race.py` 200회씩: 같은 실시간 요약 작업의 메시지 2개를 A·B가 동시에 처리 (지연·유실 주입 없음). 원본 `start_race.jsonl`.

| 코드 | 모드 | done·시도1 | done·시도2 | failed·요약 0회 | failed·요약 1회(저장 거절) | 작업당 job_start |
|---|---|---|---|---|---|---|
| 수정 전 `969ba03` | redis | 124 | 9 | **39** | **28** | 2 |
| 수정 전 `969ba03` | db | 200 | 0 | 0 | 0 | 2 |
| 수정 후 | redis | **200** | 0 | 0 | 0 | **1** |
| 수정 후 | db | 200 | 0 | 0 | 0 | 2 |

수정 전 Redis 모드는 이 환경에서 67/200(33.5%)이 실패 확정, 76/200이 시도 2회를 썼다. "failed·요약 1회"는 먼저 실행한 쪽이
늦게 도착한 다른 쪽의 시작에 실행 세대를 빼앗겨 저장이 거절된 경우다 (살아 있는 실행을 쫓아냄). 수정 후에는 진 쪽이 소유 키 선점에서
멈춰 DB를 부르지 않는다 (작업당 job_start 1회).

## 3. 정상 경로 DB/Redis 비교 (같은 작업량·동시성·heartbeat 간격)

`notree/dev/liveness_bench.py --phase normal`: 측정용 작업 40개 × 3초 대기 + 저장 1회(`note_publish`), worker 2개 × 동시 4,
lease(키 TTL) 3초 · heartbeat 1초 · 회수 기준 4초. 3회 반복, 원본 `liveness_bench.jsonl`.

| | 완료 시간(초) | worker MCP 호출 | 그중 job_heartbeat | job_start | 업무 함수 실행 | 저장 | 시도 합 | 세대 합 | 회수 | 실패 |
|---|---|---|---|---|---|---|---|---|---|---|
| db | 15.43 · 15.46 · 15.48 | 204 · 208 · 209 | 84 · 88 · 89 | 40 | 40 | 40 | 40 | 40 | 0 | 0 |
| redis | 15.38 · 15.41 · 16.23 | 120 · 120 · 120 | 0 | 40 | 40 | 40 | 40 | 40 | 0 | 0 |

작업당 worker MCP 호출이 약 5.1 → 3.0 (start·publish·finish). 줄어든 것은 heartbeat 요청이며, 작업 시간이 길수록 차이가 커진다
(heartbeat 주기마다 1건). 대신 Redis 쪽 명령이 늘어난다(선점 1 + heartbeat Lua 1/주기 + 해제 1) — Redis 명령 수는 따로 세지 않았다.

## 4. worker 강제 종료 뒤 인계

`--phase takeover`: worker A가 작업 4개를 실행 중(heartbeat 1.5주기 뒤) 정리 없이 죽음 → 곧바로 worker B. 3회 반복.

| | 인계 시간(초, 죽인 때 → B가 승인) | 업무 함수 실행 | 두 번 들어간 작업 | 저장 | 시도 합 | 세대 합 | job_start | 실패 |
|---|---|---|---|---|---|---|---|---|
| db | 3.61 – 3.69 | 8 | 4 | 4 | 8 | 8 | 8 | 0 |
| redis | 3.57 – 3.66 | 8 | 4 | 4 | 8 | 8 | 12 | 0 |

- 인계 시간은 두 모드가 같다 (회수 기준 4초가 결정. 키 TTL·DB lease 3초는 그 안에 끝남).
- **중복 외부 호출 가능성**: 죽은 A가 이미 업무 함수(외부 호출 자리)에 들어간 작업 4개는 B가 다시 들어간다 — 두 모드 모두. 저장은 idem_key로 1회.
- 죽기 전 승인도 시도 1회로 센다 (정책). Redis 모드 job_start 12 = A 4 + B 8 (superseded 4 + 승인 4).

## 5. Redis 재시작 (실행 중)

`--phase restart --sleep 4`: worker 2개 × 동시 4로 8개 실행 중 `docker restart`(측정 전용 컨테이너) → 유실 복구 주기 1초. 1회. 원본 `redis_restart.jsonl`.

| | 재시작 → 모두 끝(초) | 업무 함수 실행 | 두 번 들어간 작업 | 저장 | 시도 합 | 실패 |
|---|---|---|---|---|---|---|
| db | 2.58 | 8 | 0 | 8 | 8 | 0 |
| redis | 8.77 | 16 | **8** | 8 | **16** | 0 |

**Redis 모드의 남는 비용**: 재시작·초기화로 소유 키가 사라지면 살아 있던 실행도 heartbeat에서 소유권을 잃고 멈춘다 → 복구가 새 전달 세대로
다시 보내 처음(또는 체크포인트)부터 다시 실행한다. 그만큼 외부 호출이 다시 일어날 수 있고 시도를 하나 더 쓴다.
DB 모드는 DB lease가 살아 있어 그대로 끝난다. 실시간 요약(총 1회)은 Redis 모드에서 이 경우 실패 확정된다(결과가 저장돼 있으면 완료) —
`tests/test_combined_faults.py`가 같은 결과를 확인한다. 이번 결정은 이 비용을 받아들이고 Redis를 기본으로 하는 것이다 (비영속 Redis 전제).

## 재현

```powershell
docker run -d --name notree-bench-redis -p 127.0.0.1:16398:6379 redis:7-alpine redis-server --save "" --appendonly no
uv run python -m notree.dev.start_race --redis redis://127.0.0.1:16398/0 --mode redis --trials 200
uv run python -m notree.dev.liveness_bench --redis redis://127.0.0.1:16398/0 --mode db --phase normal
uv run python -m notree.dev.liveness_bench --redis redis://127.0.0.1:16398/0 --mode redis --phase takeover
uv run python -m notree.dev.liveness_bench --redis redis://127.0.0.1:16398/0 --mode redis --phase restart --sleep 4 --restart-cmd "docker restart notree-bench-redis"
```
수정 전 측정은 `git worktree add ../base 969ba03` 후 `start_race.py`를 복사해 같은 명령으로 돌렸다.
