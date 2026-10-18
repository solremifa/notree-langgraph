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

## 바꾸지 않은 것

- 중복 메시지 처리: 지금의 보수적 규칙(실행도 ACK도 안 함) 그대로. 설계 ②(조건부 즉시 ACK)는 나중에.
- 요약 후속 등록(③), NOTIFY, Redis 전체 유실 재구성.
- 기본 모드(db)의 동작. 기존 시험 101개 그대로 통과.

## 깨지는 변경

없음 (기본값 db). 계약: `job_start.lease_sec`에 0의 의미 추가, `job_get` 응답에 선택 필드 `exec_epoch` 추가.

## 남은 것

- 팀 결정: 생존 기준을 Redis로 확정할지 (팀 결정 문서의 보류 표)
- 실제 설정값(lease 15·60초 등)과 실제 지연에서 인계 시간 측정, Redis 연결 단절(네트워크 분할) 시험
- Redis 전체 유실 시 running 작업 재구성 (PG `expected_finish_by` 기준)
- redis 모드에서는 DB의 `lease_until`이 실행 중 여부를 뜻하지 않음 → 화면·복구 스캔이 이 값을 쓰면 안 됨
