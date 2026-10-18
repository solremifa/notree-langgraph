# 2026-10-18 통합 시험: 요약 합치기 + 배정·회수 복구 + Redis 소유 키 (`integ/2026-10-18`)

리뷰 2차 권장 순서의 마지막 단계("실제 Redis에서 결합 장애 시험")입니다. 세 브랜치를 합친 상태에서만 볼 수 있는 상호작용을 시험합니다.

| 브랜치 | 내용 |
|---|---|
| `feat/summary-coalesce` | 실시간 요약 합치기 + 리뷰 1·2차 (작업↔강의 연결, 재전송 재등록, 사용자 재시도, 저장 응답 유실) |
| `fix/dispatch-recovery` (위 브랜치 기반) | 조건부 배정, XAUTOCLAIM 탐색 위치, NOGROUP 재생성, Redis 유실 뒤 고립 작업 재전송 |
| `feat/redis-heartbeat` (main 기반) | Redis 소유 키 + Lua heartbeat, 소유 키에 DB 실행 세대를 담아 더 큰 세대만 덮어씀 (`NOTREE_LIVENESS=redis`, 기본은 db) |

합칠 때 충돌은 `jobs` 추가 열(`lecture_id`·`fence_max`), CHANGELOG, README 테스트 수뿐이었고 양쪽을 모두 살렸습니다.

## 결합 장애 시험 (`tests/test_combined_faults.py`, 실제 redis-server, 3회 반복 통과)

| 시나리오 | 확인한 것 |
|---|---|
| Redis 소유 키 모드에서 실시간 요약 실행 중 `FLUSHALL`, 그사이 새 전사 커밋, scheduler 유실 복구 | 실행 중이던 worker는 소유 키가 사라져 즉시 소유권을 잃고 저장하지 않음 · 첫 요약은 총 1회라 다시 보내지 않고 실패 확정 · worker가 consumer group을 다시 만듦 · 후속 요약이 [1-8]을 한 번에 저장 · 멈춘 작업·남은 소유 키 없음 |
| 시도가 남은 작업(echo) 실행 중 `FLUSHALL` → 유실 복구가 다시 보냄 | DB가 세대 2를 발급해 소유 키를 다시 씀 → 완료 · 첫 실행은 키가 사라져 잃음 · 시도 2·실행 세대 2 · 완료 기록 1번 |

전체 143개 통과, 건너뜀 0 (통합 브랜치, 실제 redis-server 포함).

## 리뷰 4차: 전달 세대 (통합에서의 결합)

`job_start`는 이제 `dispatch_gen`(메시지의 전달 세대)을 받고 `exec_epoch`(실행 세대)를 돌려준다. Redis 소유 키 모드의 worker 순서:
소유 키 확인 → `job_start(lease_sec=0, dispatch_gen)` → 이전 세대 메시지면 `stale_dispatch`로 ACK만 (DB 세대·시도를 쓰지 않음) → 아니면 받은 실행 세대를 담아 소유 키 쓰기.

| 결합 시험 | 결과 |
|---|---|
| 소유 키 모드: 초기화 전 전달 → 초기화 → 복구가 새 세대로 다시 보냄 → 이전 세대 메시지가 늦게 도착 | 이전 세대 메시지로는 시작 안 함 · 새 세대 메시지로 1번 실행(시도 1·실행 세대 1) · PEL·소유 키 비움 |

## 리뷰 3차 세 순서의 재검증 (통합 브랜치에서 함께 통과)

| 순서 | 시험 | 저장소 |
|---|---|---|
| 첫 복구 때 lease가 살아 있던 고립 작업 → 만료 뒤 재확인, 500건 넘는 쪽 나누기 | `test_dispatch_recovery.py` (3개) | 실제 Redis |
| 초기화 전에 발급됐지만 DB가 못 본 번호 → 이제 번호는 DB 세대라 해당 없음, 늦은 시작·낡은 키 쓰기·쓴 뒤 초기화 | `test_redis_ownership.py` (4개) | 실제 Redis |
| (4차) 초기화 전 메시지의 늦은 시작이 복구에서 빠짐 → 전달 세대로 거절·재전송 | `test_dispatch_recovery.py` (3개), `test_combined_faults.py` (1개) | 실제 Redis |
| 재시도 확인과 전사 커밋 동시 → 유일 제약 오류 없음 | `test_summary_coalesce.py` (동시 30회) | SQLite (Redis 무관) |

## 남는 것 (리뷰 2차 "추가로 남겨야 할 검증")

| 경계 | 상태 |
|---|---|
| 녹음 종료 순서 (마지막 STT 저장 뒤 종료) | 미구현 — 지금은 종료 뒤 커밋을 거절만 함 |
| 요약 적체 시 입력 범위·토큰 상한 | 미구현 — 대기 요약이 밀린 전사를 모두 읽음 |
| 실제 PG의 동시 배정·종료·재시도 원자성 | 미검증 — SQLite BEGIN IMMEDIATE로만 확인 |
| LLM 호출 전 비용 예약, 저장 뒤 화면 이벤트 | 미구현 |
