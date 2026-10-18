# 2026-10-18 변경 정리: 배정·회수·Redis 유실 복구 (`fix/dispatch-recovery`)

리뷰 2차의 공통 경로 문제(1·4·5)입니다. 요약 기능 전부터 있던 경로라 `feat/summary-coalesce` 위에 따로 브랜치를 뒀습니다.
네 문제를 모두 시험으로 먼저 재현(고치기 전 실패)한 뒤 고쳤습니다. 4·5는 **실제 redis-server**로 시험합니다.

| # | 문제 | 고친 뒤 |
|---|---|---|
| 1 | `job_reserve_pending`이 조회 뒤 상태를 다시 확인하지 않고 reserved로 바꿈 → 배정기가 둘이면 같은 작업을 두 번 허가, 늦은 갱신이 `done → reserved`로 되돌림. **SQLite에서 MCP 서버 3개로 동시 배정해 중복 허가를 재현** | 조회·허가를 한 쓰기 트랜잭션(BEGIN IMMEDIATE)에, 갱신은 `WHERE state='pending'`, 실제로 허가한 것만 반환. PG에서는 `UPDATE … WHERE state='pending' RETURNING` 또는 `FOR UPDATE SKIP LOCKED` |
| 4 | `XAUTOCLAIM`을 늘 `0-0`부터, 돌려준 다음 위치를 버림 → Redis는 한 번에 PEL을 COUNT×10개만 봐서, 앞쪽에 살아 있는 메시지가 10개 넘으면 뒤의 죽은 메시지에 영영 닿지 못함. **실제 Redis로 재현 (앞 12개 살아 있음, 뒤 3개 죽음)** | Stream별로 다음 탐색 위치를 보관해 이어 찾음. 끝까지 보면 Redis가 `0-0`을 줘서 처음부터 |
| 5a | worker가 시작할 때만 consumer group을 만듦 → Redis 초기화 뒤 `NOGROUP`만 반복 | `NOGROUP`이면 group을 다시 만들고 계속 |
| 5b | Redis와 worker가 같이 사라지면 DB의 running·sent가 다시 보내지지 않음 (재전송은 reserved·sent만, 요약 복구·후속 배정은 running이 막음) | scheduler가 표시 키 `notree:redis-alive`가 없으면 Redis 유실로 보고 `job_recover_orphans` → reserved·sent·lease가 끝난 running을 다시 XADD. **마감이 지났거나 시도 한도를 다 쓴 작업은 다시 보내지 않고 실패 확정**. DB 모드에서 lease가 살아 있는 running은 건드리지 않음 |

## 남는 것

- 처음 배포할 때도 표시 키가 없으니 한 번 돈다 (새 Redis와 지워진 Redis를 구분할 수 없음). 중복 메시지는 `job_start`가 걸러 낸다.
- Redis 소유 키 모드(`feat/redis-heartbeat`)는 `lease_sec=0`이라 running이 모두 대상 → 실행 중이던 작업도 다시 계산될 수 있음 (설계 v0.2 15절과 같음, 늦은 쓰기는 `exec_epoch`가 막음).
- 메시지만 지워지고 Redis는 살아 있는 경우(XDEL 등)의 running은 이 경로로 복구되지 않음. 그 경우는 지금 운영 절차에 없음.
- 녹음 종료된 강의의 고립된 실시간 요약은 다시 보내지면 핸들러가 "할 일 없음"으로 끝낸다.

## 시험 (`tests/test_dispatch_recovery.py`, 4개, 3회 반복 통과)

| 시험 | 결과 |
|---|---|
| MCP 서버 3개 × 3번 동시 배정 × 4바퀴 → 두 번 허가된 작업 없음 | 통과 |
| (실제 Redis) 앞 12개 살아 있음 + 뒤 3개 죽음 → 3번 안에 죽은 메시지 회수 | 통과 |
| (실제 Redis) 처리 중 `FLUSHALL` → worker가 group을 다시 만들고 다음 작업 처리 | 통과 |
| (실제 Redis) worker 죽음 + `FLUSHALL` → running·sent 다시 보냄, 마감 지난 작업은 실패 확정, 두 번째 복구는 0건, 다시 보낸 작업 완료 | 통과 |
