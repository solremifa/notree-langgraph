# 실제 Redis + 프로세스 분리 실행 기록 (2026-10-17)

테스트(가짜 Redis, 한 프로세스)가 아니라 **실제 Redis와 별도 프로세스**(MCP 서버·배정기·worker)로 작업 처리 뼈대를 돌린 기록입니다.
정상 경로 1건(Windows)과 worker 인계(Linux)를 확인했습니다. 성능 측정이 아니라 동작 확인입니다.

## 1. 정상 경로 — Windows (이근욱 PC)

| 항목 | 값 |
|---|---|
| 환경 | Windows, PowerShell, Python 3.13, uv, Docker Desktop의 `redis:7-alpine` |
| 코드 | main `997be43` (레인 분리 전) |
| 특이사항 | 6379 포트가 Windows 예약 구간(`6314~6413`, `netsh interface ipv4 show excludedportrange protocol=tcp`)에 걸려 Docker가 못 엶 → `docker run -d --name notree-redis -p 16379:6379 redis:7-alpine`, 모든 터미널에 `$env:NOTREE_REDIS_URL="redis://localhost:16379/0"` |

```powershell
uv run python -m notree.mcp_server --http
uv run python -m notree.scheduler.allocator
uv run python -m notree.worker --stream rt --name A
uv run python -m notree.worker --stream batch --name A
uv run python -m notree.dev.enqueue course_update sources=hello,big,world
uv run python -m notree.dev.jobs get op-58f4cc42
```

결과: `op-58f4cc42` `state=done`, `lease_owner=batch-A`, `attempts=1`, 등록부터 완료까지 0.55초. batch worker 로그:

```
[14:59:54.030] batch-A    job           신규 op-58f4cc42 [course_update] 시도 1/3
[14:59:54.062] batch-A    snapshot      출처 강의 고정: ['hello', 'big', 'world']
[14:59:54.077] batch-A    extract       'hello' → 'HELLO'   (big, world 같은 시각 — 병렬)
[14:59:54.115] batch-A    merge         1차 병합 → 'HELLO BIG WORLD!'
[14:59:54.127] batch-A    validate      통과
[14:59:54.148] batch-A    publish       새 버전 저장: 'HELLO BIG WORLD!' [완료]
[14:59:54.167] batch-A    done          op-58f4cc42 완료 → XACK
```

## 2. 정상 경로 + worker 인계 — Linux (클라우드 작업 환경)

| 항목 | 값 |
|---|---|
| 환경 | Linux 컨테이너, redis-server 7.0.15 (6379), Python 3.13, uv |
| 코드 | main `0d02ce7` (레인 분리 머지 후) |
| 프로세스 | MCP 서버(`--http`), 배정기, batch worker A·B. 설정은 모두 기본값 (batch lease 60초·재배송 90초·3회) |
| 원본 로그 | `wA.log`, `wB.log`, `alloc.log` |

```bash
uv run python -m notree.mcp_server --http &
uv run python -m notree.scheduler.allocator &
uv run python -m notree.worker --stream batch --name A &
uv run python -m notree.worker --stream batch --name B &
uv run python -m notree.dev.enqueue course_update sources=hello,big,world                         # 정상
uv run python -m notree.dev.enqueue course_update sources=hello,big,world die_at=extract:world   # world 정리 중 worker 죽음
```

| 작업 | 결과 |
|---|---|
| `op-70ebd7e2` (정상) | A가 처리, `done`, 시도 1, 0.2초 |
| `op-db9b2c42` (`die_at=extract:world`) | 16:25:47 A가 world에서 죽음 (ACK·완료 표시·lease 반납 없음) → **16:27:17 B가 재배송으로 회수**(90초 = 재배송 기준) → `체크포인트에서 이어감: 끝난 extract=['hello', 'big']` → world만 다시 → `done`, `lease_owner=batch-B`, 시도 2, 등록부터 90.8초 |

```
[16:25:47.180] batch-A    💥  extract:world 에서 강제 중단 (worker가 죽었다고 가정)
[16:25:47.189] batch-A    💀  worker 프로세스 죽음 (ACK·완료 표시·lease 반납·heartbeat 없음)
[16:27:17.379] batch-B    job    재배송 op-db9b2c42 [course_update] 시도 2/3
[16:27:17.392] batch-B    graph  체크포인트에서 이어감: 남은 노드=['extract'] 끝난 extract=['hello', 'big']
[16:27:17.400] batch-B    extract  'world' → 'WORLD'
[16:27:17.460] batch-B    done   op-db9b2c42 완료 → XACK
```

- 중단 위치 형식은 `extract:<강의>` 또는 노드 이름(`merge`, `validate`, `publish`)입니다. `die_at=big`처럼 강의 이름만 주면 아무 데서도 멈추지 않습니다 (같은 날 한 번 그렇게 돌려 정상 완료된 `op-616cf525`가 로그에 남아 있음).
- `crash_at`을 쓰면 worker는 살아 있고 작업만 실패해 재시도로 이어집니다.

## 확인하지 않은 것

- PostgreSQL (MCP 서버 DB는 SQLite), 실제 업무 데이터, 실제 LLM
- Redis 재시작·장애, MCP 서버 장애 중 실행, 여러 머신에 나눈 배치
- 레인 분리 뒤 `qa` worker 경로 (Q&A 핸들러가 아직 TODO)
