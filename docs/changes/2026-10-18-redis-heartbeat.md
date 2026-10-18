# 2026-10-18 변경 정리: heartbeat를 Redis 소유 키로 (`feat/redis-heartbeat`, 시험 구현)

설계 v0.2 검토의 ①(생존 신호 하나·소유권 검사가 들어간 원자적 heartbeat)을 구현하고 실제 Redis로 시험했습니다.
**기본값은 그대로 `db`**입니다. `NOTREE_LIVENESS=redis`일 때만 새 경로를 씁니다 (팀 결정 전).
시험 기록: [measurements/2026-10-18-redis-heartbeat](../measurements/2026-10-18-redis-heartbeat/README.md)

## 무엇이 바뀌나 (`NOTREE_LIVENESS=redis`)

| | db (지금까지) | redis |
|---|---|---|
| 같은 작업 동시 시작 막기 | `job_start`가 DB lease로 `leased` | worker가 소유 키 `notree:own:{job_id}`를 `SET NX PX`로 획득. 실패하면 손대지 않음(ACK도 안 함) |
| heartbeat | MCP `job_heartbeat`(DB 쓰기) → 성공하면 XCLAIM JUSTID | Lua 한 번 (아래 조건). DB 호출 없음 |
| `job_start` | `lease_sec`만큼 DB lease | `lease_sec=0`: DB lease 없이 실행 세대(`exec_epoch`)만 올림 |
| 늦은 쓰기 차단 | permit의 `exec_epoch` | 같음 |
| 끝날 때 | — | 소유 키를 토큰 비교로 해제 (죽은 경우는 TTL로 풀림) |

heartbeat Lua (`notree/worker/ownership.py`) — 모두 만족할 때만 원본 메시지의 방치 시간을 0으로 하고 키를 연장한다.
1. 소유 키 값이 내 토큰과 같다 (만료·다른 worker면 거절)
2. 원본 메시지가 PEL에 있고 consumer가 나다 (다른 worker가 회수했으면 거절 — XCLAIM은 소유권을 옮기는 명령이라 되찾아 오지 않게)
3. XCLAIM이 그 메시지를 실제로 돌려줬다 (Stream에서 지워졌으면 거절 — 키만 연장하지 않음)

시간 관계는 기존 규칙 그대로 `heartbeat < lease(키 TTL) < 재배송 기준`. Redis에 TTL보다 오래 닿지 못하면 소유권을 잃은 것으로 보고 작업을 취소한다.

## 리뷰 2·3차: 소유권 획득 ↔ DB 시작 경쟁 — 번호는 DB만 발급

**2차 문제**: "A가 소유 키를 얻음 → DB 시작이 늦어짐 → 키 만료 → B가 키·DB 시작 → A의 늦은 시작"이면 Redis 소유자 B, DB 실행자 A.
2차 때는 Redis INCR로 펜싱 번호를 받아 DB가 최대값 이하를 거절하게 했다.

**3차 문제**: Redis가 발급했지만 DB가 아직 못 본 번호가 초기화를 건너면 다시 위험하다 —
"Redis가 A에게 2 → A의 DB 시작 지연 → Redis 초기화 → B가 1을 받아 시작(DB 최대 1) → A가 2로 도착 → 허용, B를 덮음".
지금 코드로 그대로 재현했다 (B·A 모두 started, DB 실행자 A, Redis 소유자 B). 카운터를 DB 최대값까지 올리는 것으로는 못 막는다.

**고친 뒤 (설계 변경)**: 번호는 영속·단조인 **DB의 `exec_epoch`만** 발급한다. Redis 펜싱 카운터, `job_start.fence`, `stale_fence`는 없앴다.
1. `held()`: 살아 있는 소유 키가 있으면 건너뜀 (중복 메시지 — DB 세대를 올리지 않게)
2. `job_start(lease_sec=0)`: DB가 세대를 발급 (응답에 `exec_epoch`)
3. `acquire(세대)` (Lua): 키가 없거나 **키에 담긴 세대가 더 작을 때만** `토큰|stream|msg_id|e세대`로 씀. 못 쓰면 낡은 실행 → 시작 안 함
4. heartbeat·해제는 키 값 전체 비교 (세대 포함)

→ **최종 쓰기는 DB의 현재 실행 세대만 허용한다.** 소유 키는 더 큰 세대 쪽으로만 바뀌지만, DB가 세대를 발급한 뒤 키를 쓰기 전까지는 Redis 소유자와 DB 실행자가 잠시 다를 수 있다 (그 사이의 쓰기는 세대로 거절). Redis가 초기화돼도 세대는 되돌아가지 않는다.

**남는 비용 (안전 문제 아님)**: 초기화 직후 키가 비어 있으면 낡은 세대도 키를 얻을 수 있다. 그 실행의 DB 쓰기는 거절되고, 진짜 실행자는 키를 잃어 멈춘다 → 키가 풀리면 새 세대로 다시 실행. 또 `held()`와 DB 시작 사이에 두 worker가 함께 들어오면 진 쪽도 시도 횟수를 하나 쓴다. 이건 처리 비용만이 아니라 **시도 한도를 줄이는 비용**이다 — 예: 실시간 요약(총 1회)·Q&A(총 3회)는 그만큼 실패 확정이 빨라지고, 사용자의 '다시 시도'가 필요해질 수 있다.

시험 (실제 redis-server):
| 시험 | 결과 |
|---|---|
| B가 세대 1로 실행 중 → A의 늦은 DB 시작이 세대 2 → A가 덮어씀 | Redis 소유자·DB 실행자 모두 A, B는 heartbeat에서 잃고 완료 거절 |
| A가 세대 1을 받고 키 쓰기 지연 → B가 세대 2로 씀 | A는 키를 못 얻음 |
| (3차 순서) A가 번호를 받고 지연 → Redis 초기화 → B 시작 → A 도착 | A는 키를 못 얻고 완료도 거절, 소유자·실행자 모두 B |
| B가 키를 쓴 뒤 초기화 → A(세대 1)가 빈 키를 얻음 | A의 완료 거절, B는 잃음 → 해제 뒤 세대 3으로 다시 실행해 완료 |

## 바꾸지 않은 것

- 중복 메시지 처리: 지금의 보수적 규칙(실행도 ACK도 안 함) 그대로. 설계 ②(조건부 즉시 ACK)는 나중에.
- 요약 후속 등록(③), NOTIFY, Redis 전체 유실 재구성.
- 기본 모드(db)의 동작. 기존 시험 101개 그대로 통과.

## 깨지는 변경

없음 (기본값 db). 계약: `job_start.lease_sec`에 0의 의미 추가, `job_start` 응답과 `job_get`에 선택 필드 `exec_epoch` 추가.

## 남은 것

- 팀 결정: 생존 기준을 Redis로 확정할지 (팀 결정 문서의 보류 표)
- 실제 설정값(lease 15·60초 등)과 실제 지연에서 인계 시간 측정, Redis 연결 단절(네트워크 분할) 시험
- Redis 전체 유실 시 running 작업 재구성 (PG `expected_finish_by` 기준)
- redis 모드에서는 DB의 `lease_until`이 실행 중 여부를 뜻하지 않음 → 화면·복구 스캔이 이 값을 쓰면 안 됨
