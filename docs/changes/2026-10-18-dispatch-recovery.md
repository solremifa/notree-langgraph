# 2026-10-18 변경 정리: 배정·회수·Redis 유실 복구 (`fix/dispatch-recovery`)

리뷰 2차의 공통 경로 문제(1·4·5)입니다. 요약 기능 전부터 있던 경로라 `feat/summary-coalesce` 위에 따로 브랜치를 뒀습니다.
네 문제를 모두 시험으로 먼저 재현(고치기 전 실패)한 뒤 고쳤습니다. 4·5는 **실제 redis-server**로 시험합니다.

| # | 문제 | 고친 뒤 |
|---|---|---|
| 1 | `job_reserve_pending`이 조회 뒤 상태를 다시 확인하지 않고 reserved로 바꿈 → 배정기가 둘이면 같은 작업을 두 번 허가, 늦은 갱신이 `done → reserved`로 되돌림. **SQLite에서 MCP 서버 3개로 동시 배정해 중복 허가를 재현** | 조회·허가를 한 쓰기 트랜잭션(BEGIN IMMEDIATE)에, 갱신은 `WHERE state='pending'`, 실제로 허가한 것만 반환. PG에서는 `UPDATE … WHERE state='pending' RETURNING` 또는 `FOR UPDATE SKIP LOCKED` |
| 4 | `XAUTOCLAIM`을 늘 `0-0`부터, 돌려준 다음 위치를 버림 → Redis는 한 번에 PEL을 COUNT×10개만 봐서, 앞쪽에 살아 있는 메시지가 10개 넘으면 뒤의 죽은 메시지에 영영 닿지 못함. **실제 Redis로 재현 (앞 12개 살아 있음, 뒤 3개 죽음)** | Stream별로 다음 탐색 위치를 보관해 이어 찾음. 끝까지 보면 Redis가 `0-0`을 줘서 처음부터 |
| 5a | worker가 시작할 때만 consumer group을 만듦 → Redis 초기화 뒤 `NOGROUP`만 반복 | `NOGROUP`이면 group을 다시 만들고 계속 |
| 5b | Redis와 worker가 같이 사라지면 DB의 running·sent가 다시 보내지지 않음 (재전송은 reserved·sent만, 요약 복구·후속 배정은 running이 막음) | scheduler가 표시 키 `notree:redis-alive`가 없으면 Redis 유실로 보고 `job_recover_orphans` → reserved·sent·lease가 끝난 running을 다시 XADD. **마감이 지났거나 시도 한도를 다 쓴 작업은 다시 보내지 않고 실패 확정**. DB 모드에서 lease가 살아 있는 running은 건드리지 않음 |

## 리뷰 3차: 복구 '발견'과 '완료'를 나눔

문제: 표시 키를 "복구 끝"으로 썼다. 첫 복구 때 lease가 아직 살아 있던 running(죽은 worker)은 제외되고, 표시 키가 생긴 뒤로는 다시 보지 않아 영영 running에 남았다. 한 번에 500건만 봐서 그 뒤도 빠질 수 있었다.

고친 뒤:
- 표시 키 = "Redis 초기화를 **발견**했다". 발견하면 DB에 복구 기준 시각(`meta.recovery_since`)을 남긴다.
- 복구가 **끝났는지**는 DB가 안다: 기준 시각 전에 마지막으로 바뀐 reserved·sent·running이 남아 있으면 열림. scheduler는 열려 있는 동안 매 주기 다시 확인한다.
- lease가 살아 있는 running은 보류했다가 끝난 뒤 다시 보낸다. 기준 시각 뒤에 시작·전송된 작업은 대상이 아니다 (Redis 소유 키 모드처럼 DB lease로 생존을 알 수 없는 작업도 건드리지 않음).
- job_id 순서로 쪽을 나눠 끝까지 돈다.
- 기준 시각을 DB에 남긴 **뒤에** 표시 키를 둔다 (그 사이에 죽으면 다음 주기에 다시 시작).

시험 (실제 Redis): lease 1.5초가 남은 죽은 작업 → 첫 복구 보류 → 만료 뒤 다시 보냄 → 복구 종료 / 7건을 3개씩 끝까지 / 복구가 열려 있는 동안 새로 시작한 작업(lease_sec=0)은 건드리지 않음.

운영 규칙: 처리 중 메시지를 Stream에서 지우거나 잘라내지 않는다 (XDEL·MAXLEN·XTRIM 금지). 이 경우는 복구 경로가 다루지 않는다.

## 리뷰 4차: '최근 DB에서 시작됨' ≠ '새 Redis에 메시지가 있음' — 전달 세대

문제: 3차의 대상 조건 `updated_at < 복구 기준 시각`은 "복구 뒤에 바뀐 작업은 새 Redis에 있다"를 전제했다. 초기화 전 메시지의 **늦은 job_start**가 복구 시작 뒤에 도착하면 running·updated_at이 기준 뒤로 바뀌어 대상에서 빠지고, 그 worker가 멈추면 메시지도 없이 영영 running에 남았다 (재현).

고친 뒤 — **전달 세대**(`meta.dispatch_gen`, 영속):
- Redis 유실을 발견할 때마다(복구 start) 세대를 1 올린다.
- 배정·재전송·복구가 XADD할 때 메시지에 `gen`을, 작업 행에 `dispatch_gen`을 기록한다.
- worker는 메시지의 `gen`을 `job_start(dispatch_gen=)`에 넘긴다. 지금 세대보다 작으면 `stale_dispatch`로 거절 → worker는 시작하지 않고 ACK.
- 복구 대상 = **마지막 전달이 이전 세대인** reserved·sent·running. 늦은 시작은 거절되므로 상태가 안 바뀌어 대상에 그대로 남는다. 다시 보내면 지금 세대로 기록돼 대상에서 빠진다.
- 늦은 시작이 복구 시작보다 먼저 도착해 running이 된 경우도 이전 세대 전달이라 대상 → lease가 끝나면 다시 보낸다.
- `updated_at`은 판정에 쓰지 않는다.

시험 (실제 Redis): 리뷰 순서(초기화 전 읽은 메시지 → 초기화 → 복구 시작 → 늦은 시작) → `stale_dispatch`, 다시 보내져 완료 / 복구 전에 도착한 늦은 시작 → 보류 → lease 만료 뒤 다시 보냄 / 이전 세대 메시지가 새 Redis에 들어옴 → 시작 안 함, 작업은 새 세대 메시지로 1번 완료.

## 남는 것

- 처음 배포할 때도 표시 키가 없으니 한 번 돈다 (새 Redis와 지워진 Redis를 구분할 수 없음). 중복 메시지는 `job_start`가 걸러 낸다.
- Redis 소유 키 모드(`feat/redis-heartbeat`)는 `lease_sec=0`이라 running이 모두 대상 → 실행 중이던 작업도 다시 계산될 수 있음 (설계 v0.2 15절과 같음, 늦은 쓰기는 `exec_epoch`가 막음).
- 메시지만 지워지고 Redis는 살아 있는 경우(XDEL 등)의 running은 이 경로로 복구되지 않음. 그 경우는 지금 운영 절차에 없음.
- 녹음 종료된 강의의 고립된 실시간 요약은 다시 보내지면 핸들러가 "할 일 없음"으로 끝낸다.

## 시험 (`tests/test_dispatch_recovery.py`, 10개, 반복 통과)

| 시험 | 결과 |
|---|---|
| MCP 서버 3개 × 3번 동시 배정 × 4바퀴 → 두 번 허가된 작업 없음 | 통과 |
| (실제 Redis) 앞 12개 살아 있음 + 뒤 3개 죽음 → 3번 안에 죽은 메시지 회수 | 통과 |
| (실제 Redis) 처리 중 `FLUSHALL` → worker가 group을 다시 만들고 다음 작업 처리 | 통과 |
| (실제 Redis) worker 죽음 + `FLUSHALL` → running·sent 다시 보냄, 마감 지난 작업은 실패 확정, 두 번째 복구는 0건, 다시 보낸 작업 완료 | 통과 |
| (리뷰 3차) 첫 복구 때 lease가 살아 있던 작업 → 보류 → 만료 뒤 다시 보냄 → 복구 종료 | 통과 |
| (리뷰 3차) 7건을 3개씩 끝까지 | 통과 |
| (리뷰 3차) 복구가 열린 동안 새로 시작한 작업은 그대로 | 통과 |
| (리뷰 4차) 초기화 전 메시지의 늦은 시작이 복구 시작 뒤 도착 → 거절, 다시 보내져 완료 | 통과 |
| (리뷰 4차) 늦은 시작이 복구 전에 도착 → 보류 → lease 만료 뒤 다시 보냄 | 통과 |
| (리뷰 4차) 이전 세대 메시지가 새 Redis에 → 시작 안 함, 1번 완료, PEL 비움 | 통과 |
