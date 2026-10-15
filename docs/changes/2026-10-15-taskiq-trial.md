# 2026-10-15 실험: worker 루프를 taskiq로 바꿔 보기 (`feat/taskiq` 브랜치)

> **main에 합치지 않은 실험입니다.** 결론: 지금 루프(`notree/worker/loop.py`)를 유지하는 것을 권합니다.

## 무엇을 비교했나

`loop.py`에서 **메시지 받기·동시 실행 수·종료 대기·방치 메시지 회수** 부분만 taskiq로 바꿨습니다.
작업 하나를 처리하는 규칙(job_start → 핸들러 → 완료 확정 뒤 ACK, lease, permit, 시도 한도, 종료 시 lease 반납)은 그대로 씁니다.
그래서 비교 대상은 "Redis와 직접 대화하는 부분"뿐입니다.

| 버전 | 내용 |
|---|---|
| `loop` | 지금 main의 직접 만든 루프 |
| `taskiq` | taskiq 0.13.0 + taskiq-redis 1.2.4 `RedisStreamBroker` 그대로. 수동 ACK(`AcknowledgeType.MANUAL` + `Context.ack()`), 재시도 미들웨어 끔 |
| `taskiq+reclaim` | 위와 같고, broker의 메시지 읽기(`listen`)만 고친 판 (`ReclaimingStreamBroker`) |

세 버전 모두 **진짜 redis-server 7.0.15**에서, 재배송 기준을 같은 값(1.2초)으로 주고 같은 시나리오를 돌렸습니다 (`tests/test_worker_compare.py`).

## 결과

| 시나리오 | loop | taskiq | taskiq+reclaim |
|---|---|---|---|
| 성공·영구 실패·1번 실패 후 성공·한도 초과·핸들러 없음 | 통과 (1.7초) | **실패**: 재시도 대기 작업이 영영 다시 안 옴 | 통과 (1.7초) |
| 긴 작업(3초)이 worker 2개에서 한 번만 실행 | 통과 | 통과 | 통과 |
| worker가 죽은 뒤 새 작업이 없을 때 인계 | 1.3초 | **12초 안에 안 됨** | 1.3초 |
| worker가 죽은 뒤 새 작업이 들어오면 인계 | 0.0초 | 0.4초 | 0.0초 |
| 완료 기록이 4번 실패해도 작업을 잃지 않음 | 통과 | **실패**: DB는 running, 메시지는 처리 중 목록에 방치 | 통과 |
| 종료 신호: 진행 중 작업을 기다림 | 통과 | 통과 | 통과 |
| 슬롯이 꽉 찬 상태의 종료: 유예 뒤 lease 반납 | 0.3초 | 0.3초 | 0.3초 |
| 4.5초 작업 중 불필요한 재회수 | **0번** | 0번 (회수 자체를 안 함) | **3번** |
| 바뀌는 코드 | (기준) 89줄 | — | 108줄 (보완 broker·배정기 포함) |

### 1. 원본 taskiq-redis는 조용할 때 회수하지 않음 (가장 큰 문제)

`RedisStreamBroker.listen()`은 `XREADGROUP`이 **새 메시지를 돌려줬을 때만** `XAUTOCLAIM`을 합니다 (`if not fetched: continue`).
그래서 새 작업이 들어오지 않는 동안에는 다음이 모두 멈춥니다.

- 죽은 worker가 잡고 있던 작업 인계
- 재시도 대기(lease 반납 후 재배송)
- 완료 기록에 실패해 ACK하지 않은 작업의 재처리

Notree는 사용자가 적어서, 수업이 끝난 뒤 새 작업이 없는 시간대가 깁니다.
그 사이 batch worker가 죽으면 **최종 노트가 다음 요청이 올 때까지 만들어지지 않습니다.** 우리 설계에서 재배송을 worker 쪽 `XAUTOCLAIM`에 맡긴 것(팀 결정 1번)과 정면으로 부딪힙니다.

### 2. 고치려면 broker 내부를 다시 써야 함

`ReclaimingStreamBroker`는 `listen()`을 덮어써서 loop.py처럼 **매번 회수부터** 합니다 (약 20줄). 이렇게 하면 모든 시나리오가 통과합니다.
다만 taskiq-redis의 내부 구현(`_ack_generator`, 메시지 형식)에 기대므로, 버전을 올릴 때마다 다시 확인해야 합니다.

### 3. 고쳐도 살아 있는 작업의 방치 시간은 초기화할 수 없음

taskiq는 작업 함수에 Redis 메시지 ID를 주지 않습니다. 그래서 loop.py가 heartbeat마다 하는 `XCLAIM … JUSTID`(방치 시간 초기화)를 할 수 없습니다.
결과적으로 재배송 기준보다 오래 걸리는 작업은 실행 중에도 계속 다른 worker(또는 자기 자신)에게 다시 배달됩니다.

- 4.5초 작업, 기준 1.2초 → 3번
- 실제 설정(batch 기준 90초, 최대 15분)이면 작업 하나에 최대 약 10번

DB의 lease 검사(`job_start` → `leased`)와 worker 안의 실행 중 목록이 막아 주므로 **중복 실행은 없었습니다.** 대신 그때마다 `job_start` 호출과 로그가 늘어납니다.
재배송 기준을 늘려 줄이면 죽은 worker 인계가 그만큼 늦어집니다.

### 4. 코드가 줄지 않음

taskiq가 대신하는 부분은 loop.py에서 89줄(`run`, `_acquire_or_stop`, `_next`, `_drain` 등)입니다.
taskiq 버전은 108줄입니다. 보완 broker(약 20줄)와, 배정기가 `job_id` 대신 taskiq 형식 메시지를 보내도록 바꾼 부분(약 20줄)이 들어갑니다.
작업 처리 규칙(약 120줄)은 어느 쪽이든 직접 유지해야 합니다.

## 좋았던 점

- 수동 ACK가 깔끔합니다. "완료가 확정된 뒤에만 ACK"를 `Context.ack()`로 그대로 옮길 수 있었습니다.
- 동시 실행 수(`max_async_tasks`)와 종료 대기(`wait_tasks_timeout`)는 우리 요구대로 동작했습니다. 슬롯이 꽉 찬 상태의 종료도 0.3초.
- taskiq CLI(프로세스 관리), FastAPI 연동, 스케줄러 같은 주변 기능은 쓸 수 있습니다. 다만 우리는 배정기·주기 작업을 DB 원장 기준으로 이미 갖고 있습니다.

## 권고

**지금 루프를 유지합니다.**
taskiq를 쓰려면 broker를 고쳐 써야 하고, 고쳐도 불필요한 재회수가 남으며, 코드는 줄지 않습니다.
얻는 것(표준 라이브러리라는 점)에 비해 우리가 이미 테스트로 막아 둔 문제를 다시 관리해야 합니다.

다시 볼 조건: taskiq-redis가 ① 조용한 스트림에서도 회수하고 ② 작업에 메시지 ID를 넘기거나 방치 시간을 늘리는 방법을 제공할 때.
①은 작은 수정이라 upstream에 제안해 볼 만합니다.

## 돌려 보기

```powershell
git checkout feat/taskiq
uv sync
uv run pytest -q tests/test_worker_compare.py -s     # redis-server가 있어야 함 (없으면 건너뜀)
```

테스트가 테스트마다 redis-server를 직접 띄웁니다. Windows에는 redis-server가 없어서 이 테스트는 건너뛰어지니, WSL에서 실행하세요.

## 바뀐 파일 (브랜치)

| 파일 | 내용 |
|---|---|
| `notree/worker_tq/` | `TqWorker`(Worker를 상속, Redis 부분만 taskiq로), `ReclaimingStreamBroker`, `run_allocator_tq` |
| `notree/worker/handlers/echo.py` | 시험용 `die_on` 옵션 (지정한 worker가 실행하면 죽음) |
| `tests/test_worker_compare.py` | 세 버전 비교 테스트 24개. 원본 taskiq의 실패 3개는 알려진 한계로 `xfail(strict=True)` |
| `pyproject.toml`, `uv.lock` | `taskiq`, `taskiq-redis` 추가 |
