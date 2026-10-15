# worker 구조 (notree/ 패키지)

팀 저장소로 옮길 것을 전제로 만든 뼈대입니다. 코드는 전부 `notree/` 패키지 안에 있습니다.
(정리 전 1차 PoC — 루트의 `worker.py`, `demo.py`, `run.py` 등 — 은 `archive/poc-v1` 브랜치에 그대로 남아 있습니다)

```
notree/
├─ common/
│   ├─ settings.py        Redis·MCP 주소, rt/batch별 시간 값(StreamConfig), 종류별 시도 한도, 주기 작업 값
│   └─ log.py             역할 이름이 붙는 한 줄 로그
├─ contracts/             MCP 도구 계약 (팀 결정 8번) — MCP 서버와 worker가 같이 import
│   ├─ envelope.py        공통 응답 형식 {ok, data} / {ok:false, code, message, retryable}, 오류 코드
│   └─ tools.py           도구별 입력·출력 Pydantic 모델, 담당, 모델 노출 여부 (→ docs/tool-contracts.md 자동 생성)
├─ mcp_server/            내부 MCP 서버 (DB를 소유하는 유일한 프로세스). python -m notree.mcp_server --http
│   └─ server.py
├─ graphs/
│   └─ course_update/     과목 노트 그래프 (graph.py 노드·분기, state.py 상태, llm.py 가짜/OpenAI LLM)
├─ scheduler/
│   ├─ allocator.py       의도 → 실행 허가 → XADD (작업 종류별 Stream). 실행하면 periodic도 같이 돎
│   └─ periodic.py        누락 복구(멈춘 reserved/sent 다시 보내기), 체크포인트 정리
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
└─ dev/                  개발·데모용 (운영 코드 아님)
    ├─ demo.py            rt·batch worker + scheduler + 여러 시나리오를 한 번에
    ├─ takeover.py        죽은 worker의 작업을 다른 worker가 이어받는 데모
    ├─ graph_run.py       그래프만 따로 실행 (중단·이어하기·분기·Mermaid)
    ├─ enqueue.py         작업 하나 넣기 (api 흉내). --user, --deadline-in
    ├─ jobs.py            작업 조회 / 되살리기 (get, retry)
    ├─ gen_tool_docs.py   계약 → docs/tool-contracts.md
    └─ devenv.py          데모용 MCP 서버·Redis 준비
```

## 작업 하나가 코드를 지나가는 길

```
allocator.run_allocator     job_reserve_pending → XADD {job_id} → job_mark_sent
        ▼
Worker.run                  빈 슬롯이 생기거나 종료 신호가 올 때까지 대기 → _next()
Worker._next                XAUTOCLAIM(방치된 것 먼저) → 없으면 XREADGROUP
Worker.process              job_start → data.status
                                      ├ done/failed/unknown → XACK
                                      ├ leased → 손대지 않음 (같은 worker라도)
                                      └ started → JobContext 만들고 (permit, 시도 한도, 마감 시각)
                            async with Lease(ctx, touch):              ← heartbeat + Redis 방치 시간 초기화
                                await wait_for(handler(ctx), min(job_timeout, 마감까지 남은 시간))
                            ├ 성공            job_finish 확정 → XACK
                            ├ FatalError      job_fail(다시 시도 불가) 확정 → XACK
                            ├ 마감 지남·초과   job_fail(다시 시도 가능) 확정 → XACK   ※ 재시도 안 함
                            ├ 그 밖의 예외     시도 < 한도: job_release (ACK 없음 → claim_idle 뒤 재배송)
                            │                  시도 = 한도: job_fail(다시 시도 가능) 확정 → XACK
                            │  ※ 확정 응답을 못 받으면 ACK하지 않는다 (재배송으로 다시 처리)
                            ├ LeaseLost       아무것도 안 함 (다른 worker 것)
                            └ 종료로 취소      job_release (ACK 없음 → 곧 다른 worker가 잡음)

periodic.run_periodic       10초마다 job_resend_stale → 다시 XADD,  1시간마다 checkpoint_cleanup
job_retry (api)             failed(다시 시도 가능) → pending → 배정기가 다시 보냄 → 체크포인트에서 이어감
```

## 핸들러 규약

```python
from notree.worker.registry import FatalError, JobContext, RetryableError, handler

@handler("live_summary")
async def live_summary(ctx: JobContext) -> None:
    seg = await ctx.tool("transcript_range", lecture_id=ctx.payload["lecture_id"], after_seq=...)  # permit 자동, data만 옴
    text = await summarize(ctx.extras["openai"], seg)                                              # 비동기
    await ctx.tool("summary_publish", idem_key=ctx.job_id, lecture_id=..., upto_seq=seg["last_seq"],
                   expected_generation=seg["generation"], text=text)
    # 서버가 generation_mismatch로 거절하면 ctx.tool()이 ToolRejected(FatalError)를 던진다 → 루프가 실패 확정
```

| 규칙 | 이유 |
|---|---|
| 입력은 `ctx.payload`와 MCP 읽기 도구로만 | worker는 DB를 모른다 (A안) |
| 결과는 MCP 발행 도구로만, 멱등키 = `ctx.job_id` | 재배송·재시도로 두 번 실행돼도 결과가 하나 |
| LLM·네트워크 호출은 비동기 | 동기로 오래 막으면 heartbeat가 멈춰 작업을 빼앗긴다 |
| 일시적 실패는 `RetryableError`, 영구 실패는 `FatalError` | 루프가 재시도할지 실패 확정할지 정한다 |
| MCP 도구 거절은 직접 판단하지 않음 | `ctx.tool()`이 서버의 `retryable` 값으로 `ToolUnavailable`/`ToolRejected` 변환 |
| 마감이 있으면 `ctx.time_left()` 참고 | 루프가 마감에 맞춰 끊지만, 남은 시간으로 일을 줄일 수 있다 (Q&A 도구 호출 횟수 등) |
| ACK·lease·완료 표시는 신경 쓰지 않음 | 루프가 한다 |

`JobContext`에 있는 것: `job_id`, `kind`, `payload`, `attempt`(1부터), `max_attempts`, `deadline_at`, `permit`, `worker`, `cfg`(시간 값), `mcp`, `extras`(공용 자원).

## 새 핸들러 추가하기

1. `notree/worker/handlers/<이름>.py`를 만들고 `@handler("<kind>")` 함수를 쓴다
2. `notree/worker/handlers/__init__.py`의 import 목록에 추가
3. `notree/mcp_server/server.py`의 `STREAM_OF`에 작업 종류 → Stream(rt/batch) 추가 (실제로는 api가 의도를 쓸 때 정함)
4. 재시도 한도가 기본값과 다르면 `settings.KIND_MAX_ATTEMPTS`에 추가
5. 쓰는 MCP 도구가 없으면 **먼저 `notree/contracts/tools.py`에 계약 추가** → `gen_tool_docs`로 문서 재생성 → 서버에 `@tool`로 구현.
   worker용 도구는 모두 `permit` 인자를 받는다 (테스트가 확인)
6. 테스트: `tests/test_skeleton.py`의 `Env`로 fakeredis + MCP 환경에서 성공·실패 한 번씩

## 돌려 보기

```powershell
uv run python -m notree.dev.demo                 # 성공·Fatal·재시도·한도 초과·마감 초과·미구현·LangGraph 이어하기·누락 복구·되살리기·종료 처리
uv run pytest -q tests/test_skeleton.py tests/test_decisions.py

# 진짜 Redis + 프로세스 분리 (docker compose up -d 후, 터미널 여러 개)
uv run python -m notree.mcp_server --http
uv run python -m notree.scheduler.allocator
uv run python -m notree.worker --stream rt --name A
uv run python -m notree.worker --stream batch --name A
uv run python -m notree.dev.enqueue echo text=hi sleep=3 --stream rt
uv run python -m notree.dev.enqueue echo sleep=5 --stream rt --deadline-in 2      # 마감 초과 → 실패(다시 시도 가능)
uv run python -m notree.dev.enqueue course_update sources=hello,big,world --user u-42
uv run python -m notree.dev.jobs get <job_id>
uv run python -m notree.dev.jobs retry <job_id> --deadline-in 90
```

worker 창에서 Ctrl+C 한 번이면 진행 중인 작업을 끝내고 종료, 두 번이면 즉시 종료합니다.

## 팀 저장소로 옮길 때 바뀌는 것

| 이 PoC | 팀 저장소 |
|---|---|
| `notree/mcp_server/` (SQLite) | FastAPI 코드베이스의 mcp 프로세스. SQLAlchemy + PG + RLS. permit의 사용자 ID로 `SET LOCAL` |
| `allocator`가 MCP 도구로 의도를 읽음 | scheduler가 PG에 직접 (SKIP LOCKED, 동시 상한·예산) |
| `note_publish`가 permit의 사용자 ID로 저장 (RLS 흉내) | MCP 서버가 permit의 사용자 ID로 `SET LOCAL app.user_id` → 진짜 RLS |
| `contracts/tools.py`의 계획된 도구 12개 = 초안 | 담당자 PR로 필드 확정 후 서버 구현 |

`worker/` 아래(`loop.py`, `lease.py`, `registry.py`, `mcp_client.py`, `checkpointer.py`)는 거의 그대로 옮길 수 있게 만들었습니다.
