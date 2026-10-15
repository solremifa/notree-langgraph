# worker 구조 (notree/ 패키지)

팀 저장소로 옮길 것을 전제로 만든 뼈대입니다. 저장소 루트의 평면 파일들(`run.py`, `worker.py`, `demo.py` …)은
LangGraph·A안·인계 흐름을 확인하려고 만든 1차 PoC이고, **실제로 옮길 코드는 `notree/`** 입니다.

```
notree/
├─ common/
│   ├─ settings.py        Redis·MCP 주소, rt/batch별 시간 값(StreamConfig) — 전부 환경변수로 덮어쓰기 가능
│   └─ log.py             역할 이름이 붙는 한 줄 로그
├─ scheduler/
│   └─ allocator.py       의도 → 실행 허가 → XADD (작업 종류별 Stream)
├─ worker/
│   ├─ main.py            진입점: python -m notree.worker --stream rt|batch --name A
│   ├─ loop.py            Worker: 메시지 받기·동시 실행·결과별 처리·ACK·종료 처리
│   ├─ lease.py           Lease: heartbeat, lease를 잃으면 핸들러 취소
│   ├─ registry.py        JobContext, RetryableError/FatalError, @handler
│   ├─ mcp_client.py      MCP 연결 (HTTP / 테스트용 stdio)
│   ├─ checkpointer.py    LangGraph 체크포인트를 MCP로 저장 (A안) + 노트 발행 저장소
│   └─ handlers/
│       ├─ echo.py            시험용 (성공·실패·재시도 흉내)
│       ├─ course_update.py   과목 노트 (LangGraph) — 동작함, 그래프는 아직 hello world
│       ├─ live_summary.py    rt  — 스텁 (TODO 주석)
│       ├─ qa.py              rt  — 스텁
│       ├─ grading.py         rt  — 스텁
│       └─ final_note.py      batch — 스텁
└─ dev/
    ├─ enqueue.py         작업 하나 넣기 (api 흉내)
    └─ demo.py            rt·batch worker + 배정기 + 여러 시나리오를 한 번에
```

## 작업 하나가 코드를 지나가는 길

```
allocator.run_allocator     job_reserve_pending → XADD {job_id} → job_mark_sent
        ▼
Worker.run                  빈 슬롯이 있을 때만 _next()
Worker._next                XAUTOCLAIM(방치된 것 먼저) → 없으면 XREADGROUP
Worker.process              job_start ─ done/failed/unknown → XACK
                                      ├ leased → 손대지 않음
                                      └ ok → JobContext 만들고
                            async with Lease(ctx):                     ← heartbeat 시작
                                await wait_for(handler(ctx), job_timeout)
                            ├ 성공            job_finish → XACK
                            ├ FatalError      job_fail   → XACK
                            ├ 그 밖의 예외     시도 < 한도: job_release (ACK 없음 → claim_idle 뒤 재배송)
                            │                  시도 = 한도: job_fail → XACK
                            ├ LeaseLost       아무것도 안 함 (다른 worker 것)
                            └ 종료로 취소      job_release (ACK 없음 → 곧 다른 worker가 잡음)
```

## 핸들러 규약

```python
from notree.worker.registry import FatalError, JobContext, RetryableError, handler

@handler("live_summary")
async def live_summary(ctx: JobContext) -> None:
    seg = await ctx.tool("transcript.range", lecture_id=ctx.payload["lecture_id"])   # permit 자동 첨부
    text = await summarize(ctx.extras["openai"], seg)                                 # 비동기
    await ctx.tool("summary.publish", idem_key=ctx.job_id, generation=seg["generation"], text=text)
```

| 규칙 | 이유 |
|---|---|
| 입력은 `ctx.payload`와 MCP 읽기 도구로만 | worker는 DB를 모른다 (A안) |
| 결과는 MCP 발행 도구로만, 멱등키 = `ctx.job_id` | 재배송·재시도로 두 번 실행돼도 결과가 하나 |
| LLM·네트워크 호출은 비동기 | 동기로 오래 막으면 heartbeat가 멈춰 작업을 빼앗긴다 |
| 일시적 실패는 `RetryableError`, 영구 실패는 `FatalError` | 루프가 재시도할지 실패 확정할지 정한다 |
| ACK·lease·완료 표시는 신경 쓰지 않음 | 루프가 한다 |

`JobContext`에 있는 것: `job_id`, `kind`, `payload`, `attempt`(1부터), `permit`, `worker`, `cfg`(시간 값), `mcp`, `extras`(공용 자원).

## 새 핸들러 추가하기

1. `notree/worker/handlers/<이름>.py`를 만들고 `@handler("<kind>")` 함수를 쓴다
2. `notree/worker/handlers/__init__.py`의 import 목록에 추가
3. `mcp_server.py`의 `STREAM_OF`에 작업 종류 → Stream(rt/batch) 추가 (실제로는 api가 의도를 쓸 때 정함)
4. 쓰는 MCP 도구가 없으면 서버에 추가. **worker용 도구는 모두 `permit` 인자를 받는다**
5. 테스트: `tests/test_skeleton.py`처럼 fakeredis + MCP로 성공·실패 한 번씩

## 돌려 보기

```powershell
uv run python -m notree.dev.demo                 # 한 번에: 성공·Fatal·재시도·한도 초과·미구현·LangGraph 이어하기·종료 처리
uv run pytest -q tests/test_skeleton.py          # 결과별 처리, lease 상실, 종료 대기/반납, 동시 실행, permit 위조

# 진짜 Redis + 프로세스 분리 (docker compose up -d 후, 터미널 여러 개)
uv run mcp_server.py --http
uv run python -m notree.scheduler.allocator
uv run python -m notree.worker --stream rt --name A
uv run python -m notree.worker --stream batch --name A
uv run python -m notree.dev.enqueue echo text=hi sleep=3 --stream rt
uv run python -m notree.dev.enqueue course_update sources=hello,big,world
```

worker 창에서 Ctrl+C 한 번이면 진행 중인 작업을 끝내고 종료, 두 번이면 즉시 종료합니다.

## 팀 저장소로 옮길 때 바뀌는 것

| 이 PoC | 팀 저장소 |
|---|---|
| `mcp_server.py` (SQLite, 도구 몇 개) | FastAPI 코드베이스의 mcp 프로세스. SQLAlchemy + PG + RLS. permit의 사용자 ID로 `SET LOCAL` |
| `allocator`가 MCP 도구로 의도를 읽음 | scheduler가 PG에 직접 (SKIP LOCKED, 동시 상한·예산) |
| `course_update`가 루트의 `graph.py`를 import | 그래프를 `notree/worker/graphs/course_update.py` 같은 곳으로 이동 |
| permit = HMAC 문자열, 사용자 정보 없음 | 형식·비밀키·담긴 정보 결정 필요 (team-decisions.md 4절) |
| 시간 값 = 제안값 | 팀 결정 후 settings 기본값 수정 |

`worker/` 아래(`loop.py`, `lease.py`, `registry.py`, `mcp_client.py`, `checkpointer.py`)는 거의 그대로 옮길 수 있게 만들었습니다.
