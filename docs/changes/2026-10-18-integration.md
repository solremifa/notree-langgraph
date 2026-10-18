# 2026-10-18 통합 시험: 요약 합치기 + 배정·회수 복구 + Redis 소유 키 (`integ/2026-10-18`)

리뷰 2차 권장 순서의 마지막 단계("실제 Redis에서 결합 장애 시험")입니다. 세 브랜치를 합친 상태에서만 볼 수 있는 상호작용을 시험합니다.

| 브랜치 | 내용 |
|---|---|
| `feat/summary-coalesce` | 실시간 요약 합치기 + 리뷰 1·2차 (작업↔강의 연결, 재전송 재등록, 사용자 재시도, 저장 응답 유실) |
| `fix/dispatch-recovery` (위 브랜치 기반) | 조건부 배정, XAUTOCLAIM 탐색 위치, NOGROUP 재생성, Redis 유실 뒤 고립 작업 재전송 |
| `feat/redis-heartbeat` (main 기반) | Redis 소유 키 + Lua heartbeat + 펜싱 번호 (`NOTREE_LIVENESS=redis`, 기본은 db) |

합칠 때 충돌은 `jobs` 추가 열(`lecture_id`·`fence_max`), CHANGELOG, README 테스트 수뿐이었고 양쪽을 모두 살렸습니다.

## 결합 장애 시험 (`tests/test_combined_faults.py`, 실제 redis-server, 3회 반복 통과)

| 시나리오 | 확인한 것 |
|---|---|
| Redis 소유 키 모드에서 실시간 요약 실행 중 `FLUSHALL`, 그사이 새 전사 커밋, scheduler 유실 복구 | 실행 중이던 worker는 소유 키가 사라져 즉시 소유권을 잃고 저장하지 않음 · 첫 요약은 총 1회라 다시 보내지 않고 실패 확정 · worker가 consumer group을 다시 만듦 · 후속 요약이 [1-8]을 한 번에 저장 · 멈춘 작업·남은 소유 키 없음 |
| 시도가 남은 작업(echo) 실행 중 `FLUSHALL` → 유실 복구가 다시 보냄 | 펜싱 카운터가 1로 돌아가 `stale_fence` → 카운터를 올리고 재배송 때 다시 잡아 완료 · 시도 2·실행 세대 2 · 완료 기록 1번 |

전체 134개 통과 (통합 브랜치).

## 남는 것 (리뷰 2차 "추가로 남겨야 할 검증")

| 경계 | 상태 |
|---|---|
| 녹음 종료 순서 (마지막 STT 저장 뒤 종료) | 미구현 — 지금은 종료 뒤 커밋을 거절만 함 |
| 요약 적체 시 입력 범위·토큰 상한 | 미구현 — 대기 요약이 밀린 전사를 모두 읽음 |
| 실제 PG의 동시 배정·종료·재시도 원자성 | 미검증 — SQLite BEGIN IMMEDIATE로만 확인 |
| LLM 호출 전 비용 예약, 저장 뒤 화면 이벤트 | 미구현 |
