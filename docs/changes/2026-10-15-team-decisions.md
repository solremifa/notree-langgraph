# 2026-10-15 변경 정리: 팀 결정 확정 반영

worker 관련 결정 9개를 확정하고 코드에 반영했습니다. 이 문서는 **무엇이 바뀌었는지**만 모은 것입니다.
확정된 결정 전체는 [../team-decisions.md](../team-decisions.md), 도구 목록은 [../tool-contracts.md](../tool-contracts.md)에 있습니다.

## 한눈에

- 결정 1~7, 9번은 처음 제안대로 확정했습니다. 다만 구현하면서 세부를 몇 가지 추가했습니다 (아래 1절의 "추가·변경").
- 8번(MCP 도구 계약)은 새로 정했습니다. 공통 응답 형식이 바뀌어서 **도구를 직접 부르는 코드는 모두 수정이 필요**합니다 (3절).
- 테스트는 18개에서 31개로 늘었습니다. 결정마다 확인하는 테스트가 있습니다 (`tests/test_decisions.py`).

---

## 1. 결정 내용: 제안 → 확정

| # | 처음 제안 | 확정 | 추가·변경 |
|---|---|---|---|
| 1 | worker가 `XAUTOCLAIM`, scheduler는 누락 복구 | 그대로 | — |
| 2 | rt 15/5/20초, batch 60/20/90초. Q&A는 마감 시각 방식(b) 제안 | 그대로 | `deadline_at`을 **모든 작업**에 쓸 수 있게 일반화. 마감이 지나면 재시도 안 함. `ctx.time_left()` 추가 |
| 3 | rt 2회, batch 3회, 실시간 요약 1회. 실패는 `failed` + 화면 표시 | 그대로 | 실패에 **`user_retryable`**(다시 시도 버튼 근거)을 같이 저장. 한도·시간 초과는 true, 영구 실패는 false |
| 4 | HMAC 서명 + 사용자 ID, 유효 rt 5분·batch 30분 | 그대로 | permit 형식을 `job_id.worker.만료.서명` 문자열에서 **base64 JSON claims + 서명**으로 변경 (사용자 ID를 담기 위해) |
| 5 | 프로세스당 rt 4, batch 1 | 그대로 | — |
| 6 | 30초 넘게 reserved/sent인 의도 다시 보내기 | 변경 | 기준을 둘로 나눔: **reserved 30초, sent 120초**. sent는 정상적으로 큐에서 기다리는 중일 수 있어 더 길게 |
| 7 | 같은 job 되살림, 영구 실패는 버튼 없음 | 그대로 | 되살릴 때 **새 마감(`deadline_at`)을 줄 수 있게** 함. 안 주면 지난 마감 때문에 바로 다시 실패하기 때문 |
| 8 | (논의 중) | **새로 확정** | 아래 2절 |
| 9 | 완료 후 7일 보관 뒤 삭제 | 그대로 | 실패한 작업도 같은 기간 보관 (되살려서 이어가야 하므로). 1시간마다 실행 |

## 2. 8번 MCP 도구 계약 (새로 확정)

| 항목 | 내용 |
|---|---|
| 이름 | `영역_동사`, 소문자·밑줄. 설계서의 `notes.search` → `notes_search`. 이유: OpenAI 함수 이름이 점을 허용하지 않음 |
| 공통 인자 | worker용 도구는 모두 `permit`. 쓰기는 `idem_key`. 삭제 영향 쓰기는 `expected_generation`. 사용자 ID는 인자로 받지 않음 |
| 공통 응답 | `{ok, data}` / `{ok, data, duplicate}` / `{ok: false, code, message, retryable}` |
| 재시도 판단 | 서버가 `retryable`을 정함 → `ctx.tool()`이 `ToolUnavailable`(재시도) / `ToolRejected`(실패)로 변환 |
| 중복 저장 | 같은 `idem_key`면 `duplicate: true`인 **성공** (오류 아님) |
| 계약 위치 | `notree/contracts/tools.py` (Pydantic). 문서는 여기서 자동 생성 |
| 담당 | 쓰는 핸들러 담당이 정의, 그 테이블 담당이 리뷰. `job_*`·`checkpoint_*`는 worker 담당 |
| 변경 절차 | 계약 파일 PR → 문서 재생성 → 선택 필드 추가는 자유, 나머지는 합의 후 |

계획된 도구 12개(`transcript_range`, `summary_publish`, `notes_search`, `qa_context`, `grade_save` 등)의 입출력 초안도 계약에 넣었습니다.
**담당자가 필드를 확인하고 PR로 확정**하면 됩니다.

---

## 3. 호환성이 깨진 것 (기존 코드를 고쳐야 하는 부분)

| 무엇 | 이전 | 이후 |
|---|---|---|
| 모든 도구 응답 | 도구마다 제각각 (`{"ok": True}`, `{"found": ...}`, `{"published": ...}` 등) | 공통 envelope. `McpClient.tool()`이 풀어서 `data`만 돌려줌 (`call()`은 날것 그대로) |
| `job_create` | `kind, payload, stream` | **`user_id` 필수** 추가 |
| `job_start` 결과 | 실패 시 `{"ok": False, "reason": ...}` | 항상 성공 응답, `data.status` = `started` / `done` / `failed` / `leased` / `unknown`. `left` → `lease_left_sec` |
| `job_heartbeat` / `job_finish` / `job_release` | `{"ok": bool}` | `data.renewed` / `data.finished` / `data.released` |
| `job_fail` | `job_id, worker, reason` | **`user_retryable` 필수** 추가 |
| `checkpoint_get` | `{"found": True, ...필드}` | `data.found`, `data.item` |
| `note_publish` | `{"published": bool}` | 성공 + 두 번째부터 `duplicate: true` |
| permit 없는 호출 거절 | `{"ok": False, "reason": "permit 필요"}` | `{"ok": False, "code": "permit_invalid", "message": "permit 필요", "retryable": False}` |
| permit 형식 | `job_id.worker.만료.서명` | `base64(JSON).서명` (이전 permit은 무효) |
| `McpCheckpointSaver` / `McpNoteStore` | `(client)` | `(client, permit=None)`. worker 안에서는 `ctx.permit`을 넘김 |

---

## 4. 파일별 변경

### 새 파일
| 파일 | 내용 |
|---|---|
| `notree/contracts/envelope.py` | 공통 응답 형식, 오류 코드, `ToolError`, `unwrap()` |
| `notree/contracts/tools.py` | 도구 계약: 입력·출력 Pydantic 모델, 담당, 모델 노출 여부. 구현 20개 + 계획 12개 |
| `notree/scheduler/periodic.py` | 주기 작업: 누락 복구(`resend_stale`), 체크포인트 정리(`cleanup_checkpoints`) |
| `notree/dev/gen_tool_docs.py` | 계약 → `docs/tool-contracts.md` 생성, `--check` |
| `notree/dev/jobs.py` | 작업 조회·되살리기 CLI (`get`, `retry --deadline-in`) |
| `docs/tool-contracts.md` | 도구 계약 문서 (자동 생성) |
| `docs/changes/2026-10-15-team-decisions.md` | 이 문서 |
| `tests/test_decisions.py` | 결정별 테스트 13개 |

### 바뀐 파일
| 파일 | 변경 |
|---|---|
| `mcp_server.py` | 전면 개편. 모든 도구를 공통 응답으로, 계약에 없는 도구 등록 금지, 예외를 오류 코드로 변환. permit에 사용자 ID·stream별 유효 시간. `updated_at`·`user_id`·`user_retryable` 컬럼. 새 도구 `job_resend_stale`, `job_retry`, `checkpoint_cleanup`. 체크포인트·발행 도구도 permit 확인. `note_publish`는 permit의 사용자 ID로 저장 |
| `notree/worker/loop.py` | `job_start`의 `status` 처리, 마감 시각(시작 전 지남 / 실행 중 초과), 종류별 시도 한도, `user_retryable` 기록, 제어 호출 실패가 루프를 멈추지 않게 |
| `notree/worker/registry.py` | `ToolUnavailable`/`ToolRejected`, `ctx.tool()`의 거절 → 예외 변환, `ctx.max_attempts`·`ctx.deadline_at`·`ctx.time_left()` |
| `notree/worker/lease.py` | 새 응답 형식(`renewed`), permit 거절 시 lease 상실로 처리 |
| `notree/worker/mcp_client.py` | `tool()` 추가 (envelope 풀기, permit=None이면 생략) |
| `notree/worker/checkpointer.py` | permit 전달, 새 응답 형식 |
| `notree/worker/handlers/course_update.py` | permit 전달. 시험용 `crash_at`은 첫 시도가 아니라 "체크포인트가 없을 때"만 (되살리기 후 이어가기 확인용) |
| `notree/worker/handlers/echo.py` | `fail_if_exists` (되살리기 시험용) |
| `notree/common/settings.py` | `KIND_MAX_ATTEMPTS`, `max_attempts_for()`, 주기 작업 값 |
| `notree/scheduler/allocator.py` | `mcp.tool()` 사용, 프로세스 실행 시 주기 작업도 같이 |
| `notree/dev/enqueue.py` | `--user`, `--deadline-in` |
| `notree/dev/demo.py` | 마감 초과·누락 복구·되살리기 시나리오 추가 |
| `worker.py`, `scheduler.py`, `api.py`, `demo.py` (루트 PoC) | 새 응답 형식에 맞춤 (동작은 그대로) |
| `tests/test_flow.py`, `test_worker.py`, `test_skeleton.py` | 새 응답 형식에 맞춤 |
| `docs/team-decisions.md` | 제안 문서 → 확정 문서로 다시 씀 |

---

## 5. 확인한 것

- `uv run pytest -q` → 31개 통과 (3회 반복 안정)
- `uv run python -m notree.dev.demo` → 성공, Fatal, 재시도 후 성공, 한도 초과, 마감 초과, 미구현, LangGraph 이어하기, 누락 복구, 되살리기, 종료 대기가 모두 기대대로
- 실제 Redis + 프로세스 분리(MCP 서버 HTTP, scheduler, worker rt·batch)로 작업 넣기 → 조회 → 되살리기 CLI 동작

## 6. 남은 일

| 할 일 | 담당 |
|---|---|
| 계획된 도구 12개의 필드 확정 PR | 각 정의 담당 |
| 첫 실제 핸들러(`live_summary`) 구현 + 화면 갱신까지 연결 | T3·T4 |
| 배정기의 사용자·과목별 동시 상한·예산 검사 | worker 담당 |
| 전체 동시 OpenAI 호출 수 측정 (5번) | 첫 핸들러 붙인 뒤 |
| 탈퇴·과목 삭제 흐름에 체크포인트 테이블 포함 (9번) | 삭제 흐름 담당 |
| 팀 저장소로 옮길 때 MCP 서버를 SQLAlchemy + PG + RLS로 (permit의 사용자 ID로 `SET LOCAL`) | worker 담당 |
