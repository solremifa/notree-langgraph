# 2026-10-15 변경 정리: 실행 안정성 4건 수정

외부 분석(기준 커밋 `feac3dc`)에서 지적된 네 가지 문제를 고쳤습니다.
먼저 재현 테스트를 `xfail(strict=True)`로 올리고(`2c85706`), 이번에 고친 뒤 표시를 지워 통과시켰습니다.

## 한눈에

| # | 문제 | 재현 결과 (수정 전) | 고친 방법 | 테스트 |
|---|---|---|---|---|
| 1 | 같은 worker가 자기 실행 중인 메시지를 다시 가져와 중복 실행 | 2.5초 작업이 **3번 시작** (재배송 기준 0.6초, 동시 3) | heartbeat마다 Redis 방치 시간 초기화 + `job_start`가 같은 worker도 거절 + worker 내부 실행 중 목록 | `test_worker_does_not_reclaim_its_own_running_job`, `test_job_start_rejects_live_lease_even_for_same_worker` |
| 2 | 완료 기록 실패 후에도 ACK → 작업 유실 | DB `running`, 미확인 메시지 0개, 복구 대상 0개 | 완료·실패 표시를 재시도하고 **확정 응답일 때만 ACK** | `test_finish_failure_does_not_lose_the_job` |
| 3 | lease를 잃은 실행자의 permit으로 쓰기 성공 | 넘겨받은 뒤 A의 발행·체크포인트 저장이 성공 | permit에 실행 세대, 쓰기 트랜잭션 안에서 DB와 대조 | `test_stale_permit_cannot_write_after_takeover`, `test_previous_attempt_permit_is_rejected_for_same_worker` |
| 4 | 슬롯이 꽉 찬 상태에서 종료 신호를 못 봄 | 유예 0.3초인데 8초 넘게 종료 안 됨 | 슬롯 대기와 종료 신호를 함께 기다림 | `test_shutdown_grace_starts_immediately_when_slots_are_full` |

## 1. 중복 실행

**원인**: heartbeat가 DB의 lease만 연장하고 Redis 메시지의 방치 시간(idle)은 그대로 둬서, 오래 걸리는 작업이 계속
`XAUTOCLAIM` 대상이 됐습니다. `job_start`는 lease 소유자가 **다른** worker일 때만 거절했기 때문에 같은 worker가 다시 가져가면 통과했습니다.

**수정** (세 겹)
1. `Lease`가 heartbeat에 성공할 때마다 `touch()`를 부르고, 루프는 여기서 `XCLAIM <stream> <group> <나> 0 <msg_id> JUSTID`로
   방치 시간을 0으로 되돌립니다. 살아 있는 작업은 아예 재배송 대상에 오르지 않습니다. (지난 문서에 적었던 "느린 worker의 메시지를
   다른 worker가 계속 가져갔다가 거절당하는 로그"도 같이 사라졌습니다)
2. `job_start`: `running`이고 lease가 살아 있으면 **같은 worker라도** `leased`.
3. worker 안의 `active_jobs`: 이미 실행 중인 job_id의 메시지는 `job_start`도 부르지 않고 넘깁니다.

## 2. 완료 확정 전 ACK

**원인**: `_ctl()`이 오류를 로그만 남기고 삼켰고, 호출자는 결과를 보지 않고 ACK했습니다. 누락 복구는 `reserved`·`sent`만 봐서 이 작업을 놓쳤습니다.

**수정**: `_confirm()`이 완료·실패 표시를 0.2초·0.5초 간격으로 최대 3번 시도합니다.
- `finished`/`failed`가 `true` → ACK
- 서버가 `false`(실행권 없음) → ACK하지 않음 (이미 다른 실행의 몫)
- 끝내 응답 없음 → ACK하지 않음. heartbeat가 멈췄으므로 lease가 만료되고 재배송 → 다시 실행(핸들러는 멱등) → 완료

`job_release`처럼 결과가 ACK에 영향을 주지 않는 호출만 기존 `_ctl()`을 씁니다.

## 3. 실행 세대 검증

**원인**: permit 검사가 서명·작업 ID·만료만 봤습니다. 서명은 "위조되지 않았다"일 뿐 "지금도 실행권이 있다"는 아닙니다.

**수정**
- permit claims에 `"a"`(시도 번호 = 실행 세대) 추가. `job_start`가 발급할 때 새 시도 번호를 넣습니다.
- `_is_live()`: 작업이 `running`이고 `lease_owner == permit.w`이고 `attempts == permit.a`인지.
- 쓰기 도구(`checkpoint_put`, `checkpoint_put_writes`, `checkpoint_delete_thread`, `note_publish`)는 **같은 트랜잭션 안에서**
  확인 후 씁니다(`tx()` = `BEGIN IMMEDIATE`). 실행권이 없으면 `permit_invalid`.
- 제어 도구(`job_heartbeat`, `job_finish`, `job_fail`, `job_release`)는 실행권이 없으면 `renewed/finished/failed/released = false`.
  `job_finish`·`job_fail`은 `state='running'` 조건도 추가 (반납된 작업을 끝내지 못하게).
- PG로 옮길 때는 같은 검사를 `SELECT … FOR UPDATE` 또는 조건부 `UPDATE … WHERE lease_owner=… AND attempts=…` 한 문장으로 합니다.
- permit 없이 부르는 경우(`NOTREE_REQUIRE_PERMIT=0`, `graph_run`)는 검사하지 않습니다.

## 4. 슬롯이 꽉 찬 상태의 종료

**원인**: 메인 루프가 `slots.acquire()`에서 기다리는 동안 종료 이벤트를 보지 못해, 진행 중인 작업이 끝나 슬롯이 날 때까지 `_drain()`에 들어가지 못했습니다.

**수정**: `_acquire_or_stop()`이 슬롯 획득과 `stop.wait()`를 함께 기다립니다. 종료 신호가 먼저면 바로 빠져나와 유예 시간을 시작합니다.
둘이 동시에 끝났으면 종료를 우선하고 받은 슬롯은 돌려줍니다.

## 깨지는 변경

| 무엇 | 이전 | 이후 |
|---|---|---|
| permit 형식 | `{j, w, u, e}` | `{j, w, a, u, e}`. 이전 permit은 쓰기 도구에서 `permit_invalid` |
| `job_start` | 같은 worker면 lease가 살아 있어도 `started` | `leased` |
| `job_finish`/`job_fail`/`job_release`/`job_heartbeat` | 소유자만 확인 | 소유자·실행 세대·`running` 확인, 아니면 `false` |
| 쓰기 도구 | permit 서명만 확인 | 실행권 없으면 `permit_invalid` |
| `Lease(ctx)` | `Lease(ctx)` | `Lease(ctx, touch=None)` (기존 호출은 그대로 동작) |

## 확인한 것

- 테스트 37개 통과 (기존 31 + 이번 6). 타이밍에 민감한 테스트 묶음(`test_robustness`, `test_takeover`, `test_skeleton`)을 3번 반복해도 모두 통과
- 로그로 동작 경로 확인: 완료 기록 3번 실패 → ACK 안 함 → 재배송 → 완료 / 종료 신호 0.3초 만에 반납 후 종료 / 중복 실행 테스트에서는 재배송 자체가 일어나지 않음
- `notree.dev.demo`, `notree.dev.takeover`(extract 도중·발행 직후 죽음) 결과 변화 없음

## 남은 것

- 실제 Redis·PostgreSQL·OpenAI 부하 시험은 하지 않았습니다.
- 계획된 도구의 발행 게이트(`expected_generation` = 콘텐츠 세대, 삭제·취소 확인)는 아직 없습니다. 이번에 넣은 것은 **실행 세대**이고, 둘은 다른 검사입니다.
