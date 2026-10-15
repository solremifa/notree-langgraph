# worker 관련 팀 결정 사항 (확정)

2026-10-15 확정. 처음 제안에서 무엇이 바뀌었고 코드의 어디를 고쳤는지는
[changes/2026-10-15-team-decisions.md](changes/2026-10-15-team-decisions.md)에 따로 정리했습니다.

| # | 항목 | 확정 내용 | 코드 |
|---|---|---|---|
| 1 | 재배송 주체 | worker가 `XAUTOCLAIM`으로 직접 회수. scheduler는 누락 복구(6번)만 | `notree/worker/loop.py` `_next` |
| 2 | 시간 값 | rt 15/5/20초·제한 90초, batch 60/20/90초·제한 15분. 마감 시각(`deadline_at`) 지원 | `notree/common/settings.py`, `loop.py` |
| 3 | 재시도·실패 | rt 2회, batch 3회, 실시간 요약 1회. 실패는 `failed` + 사유 + 다시 시도 가능 여부 | `settings.KIND_MAX_ATTEMPTS`, `loop.py` |
| 4 | permit | HMAC 서명, 사용자 ID 포함. MCP가 꺼내 씀(인자로 받지 않음). 유효 rt 5분·batch 30분 | `notree/mcp_server/server.py` `_issue_permit` |
| 5 | 동시 실행 | 프로세스당 rt 4, batch 1 | `settings.RT/BATCH.concurrency` |
| 6 | 배정기 누락 복구 | 10초마다, reserved 30초·sent 120초 넘게 멈춘 의도를 다시 XADD | `notree/scheduler/periodic.py` |
| 7 | 다시 시도 | 같은 job을 되살림(체크포인트 유지). 영구 실패는 거절. 마감 있던 작업은 새 마감과 함께 | `job_retry` 도구 |
| 8 | MCP 도구 계약 | 이름 `영역_동사`, 공통 인자·응답 형식, 계약은 Pydantic 코드 하나 | `notree/contracts/` |
| 9 | 체크포인트 정리 | 완료·실패 후 7일 보관 뒤 삭제, 1시간마다 | `checkpoint_cleanup` 도구, `periodic.py` |

---

## 1. 재배송 주체

- worker가 새 메시지를 읽기 전에 매번 `XAUTOCLAIM`으로 재배송 기준보다 오래 방치된 메시지를 먼저 가져갑니다.
- scheduler는 재배송을 하지 않습니다. 대신 "DB에는 있는데 Redis에는 없는" 의도를 복구합니다(6번).
- 설계서의 "scheduler: 오래된 Redis 작업 재배송" 문구는 "worker: 방치된 메시지 회수 / scheduler: 의도-메시지 불일치 복구"로 고칩니다.

## 2. 시간 값

| 값 | rt | batch | 환경변수 (rt 예시) |
|---|---|---|---|
| lease | 15초 | 60초 | `NOTREE_RT_LEASE_SEC` |
| heartbeat | 5초 (lease/3) | 20초 | `NOTREE_RT_HEARTBEAT_SEC` |
| 재배송 기준 | 20초 | 90초 | `NOTREE_RT_CLAIM_IDLE_SEC` |
| 실행 제한 | 90초 | 15분 | `NOTREE_RT_JOB_TIMEOUT_SEC` |

규칙: heartbeat < lease < 재배송 기준. 실행 제한 ≤ 사용자에게 약속한 시간.

**마감 시각**: api가 의도를 만들 때 payload에 `deadline_at`(unix 초)을 넣으면, worker는 실행 제한과 남은 시간 중 짧은 쪽으로 실행합니다.
- 시작할 때 이미 지났으면 실행하지 않고 실패 확정 (`마감 시각 지남`, 다시 시도 가능)
- 실행 중 마감을 넘기면 재시도하지 않고 실패 확정 (`마감 시각 초과`)
- 핸들러는 `ctx.time_left()`로 남은 시간을 볼 수 있습니다 (예: Q&A 도구 호출을 3회 다 쓸지 판단)
- Q&A의 90초 약속은 이걸로 지킵니다. api가 `deadline_at = 요청 시각 + 90`을 넣습니다.

## 3. 재시도 한도와 실패 처리

| 재시도 (`RetryableError`) | 즉시 실패 (`FatalError`) |
|---|---|
| OpenAI 429 / 5xx / 타임아웃 | 입력 형식 오류, 필수 값 없음 |
| MCP 도구 거절 중 `retryable=true` (`locked`, `unavailable`) | MCP 도구 거절 중 `retryable=false` (`not_found`, `generation_mismatch`, `permit_invalid`, `invalid_args`) |
| 실행 제한 초과 (마감 전) | 핸들러 없음 (미구현) |
| 예상 못 한 예외 | |

- 한도: rt 2회, batch 3회, `live_summary` 1회 (`settings.KIND_MAX_ATTEMPTS`에서 종류별로 바꿈)
- 재시도 간격: lease를 반납하고 재배송 기준만큼 기다렸다 다시 (고정 간격). 429가 많으면 측정 후 늘어나는 간격 검토
- 실패 확정 시 DB에 `state=failed`, `fail_reason`, `user_retryable`이 남습니다. 화면은 이 셋을 근거로 표시합니다
  - `user_retryable=true`: 한도 초과, 실행 제한·마감 초과 → "다시 시도" 버튼
  - `user_retryable=false`: 영구 실패 → 버튼 없음
  - 사용자에게는 `fail_reason` 원문 대신 화면용 문구를 보여 줍니다 (문구는 프런트에서 결정)
- 실패 작업 보관함은 따로 만들지 않고 `failed` 상태로 대신합니다

## 4. permit 토큰

- 형식: `base64url({"j": job_id, "w": worker, "a": 시도 번호(실행 세대), "u": user_id, "e": 만료}).HMAC-SHA256 앞 32자`
- `job_start`가 발급하고, worker는 이후 모든 도구 호출에 붙입니다 (`ctx.tool()`이 자동으로).
- 서버는 서명·만료·job_id 일치를 확인하고, **사용자 ID를 permit에서 꺼내 씁니다.** 도구 인자로 사용자 ID를 받지 않습니다.
- **쓰기 도구는 같은 트랜잭션 안에서 실행권을 다시 확인합니다**: 작업이 `running`이고, 소유자와 실행 세대(시도 번호)가 permit과 같아야 합니다.
  서명은 "위조되지 않았다"만 증명하므로, lease를 잃은 이전 실행자나 이전 시도의 permit은 이 검사로 막힙니다 (2026-10-15 추가).
  실제 구현에서는 이 값으로 `SET LOCAL app.user_id = …` → RLS. (PoC에서는 `note_publish`가 permit의 사용자 ID로 저장)
- 유효 시간: rt 5분, batch 30분 (`NOTREE_PERMIT_TTL_RT/_BATCH`). 시도할 때마다 새로 발급됩니다.
- 비밀키: `NOTREE_PERMIT_SECRET` 환경변수. 배포 시 GitHub Actions secret / 서버 `.env`. 키를 바꾸면 진행 중 작업의 permit이 무효가 되므로 worker를 먼저 비우고 바꿉니다.
- 작업 종류별 도구 제한은 하지 않습니다 (도구 목록만 계약에 문서화). 필요해지면 permit claims에 범위를 추가합니다.
- `NOTREE_REQUIRE_PERMIT=1`이면 permit 없는 worker 도구 호출을 거절합니다. 운영은 1, 작업 밖에서 그래프만 돌리는 `notree.dev.graph_run`은 0.

## 5. 동시 실행 수

- 프로세스당 rt 4, batch 1. worker 컨테이너는 rt 1개, batch 1개로 시작.
- 세 가지 제한을 구분합니다: 배정기의 사용자·과목별 상한(독점 방지) / worker 동시 실행 수(프로세스 감당) / OpenAI 계정 한도(외부).
- OpenAI 한도는 앞의 두 개로 간접적으로만 지켜지므로, 첫 핸들러가 붙으면 **batch 안의 병렬 추출까지 포함한 전체 동시 OpenAI 호출 수**를 측정합니다.
  넘치면 `ctx.extras`에 공용 세마포어를 넣어 핸들러들이 같이 씁니다.

## 6. 배정기 누락 복구

| 멈춘 상태 | 원인 | 복구 |
|---|---|---|
| `reserved` 30초 이상 | 허가 후 XADD 전에 배정기가 죽음 | 다시 XADD |
| `sent` 120초 이상 | Redis 재시작 등으로 메시지 유실, 또는 `job_release` 후 메시지 유실 | 다시 XADD |

- scheduler 프로세스의 주기 작업(`periodic.py`, 10초마다). jobs 테이블에 `updated_at` 컬럼을 추가했습니다.
- 다시 보낸 메시지가 원래 메시지와 겹쳐도 worker의 `job_start`가 done/leased로 걸러 내므로 안전합니다.
- 큐가 오래 밀려 120초 넘게 아무도 시작하지 못한 정상 작업도 다시 보내질 수 있습니다(중복 메시지). 해는 없지만, 밀림이 잦으면 기준을 늘립니다.

## 7. 실패한 작업 다시 하기

- `job_retry`: `failed → pending`, 시도 횟수 0으로 초기화. 같은 job_id라서 과목 노트는 **체크포인트에서 이어갑니다** (끝난 추출의 LLM 비용 절약).
- `user_retryable=false`(영구 실패)면 거절합니다.
- 마감이 있던 작업(Q&A)은 `deadline_at`에 새 마감을 함께 줍니다. 안 주면 지난 마감 그대로라 바로 다시 실패합니다.

## 8. MCP 도구 계약

전체 목록과 필드는 [tool-contracts.md](tool-contracts.md) (계약 코드에서 자동 생성).

**이름**: `영역_동사`, 소문자·밑줄. Q&A는 일부 도구를 OpenAI tool calling에 그대로 넘기는데 OpenAI 함수 이름은 점을 허용하지 않기 때문입니다. 설계서의 `notes.search` 등은 `notes_search`로 바꿔 부릅니다.

**공통 인자**
| 인자 | 어디에 | 뜻 |
|---|---|---|
| `permit` | worker가 부르는 모든 도구 (`job_start` 제외) | 누가 어떤 작업으로 부르는지. 사용자 ID를 담음 |
| `idem_key` | 모든 쓰기 도구 | job_id. 같은 키로 두 번 오면 두 번째는 저장하지 않음 |
| `expected_generation` | 삭제·취소 영향을 받는 쓰기 | 읽을 때 받은 세대. 그사이 삭제됐으면 거절 |

**공통 응답**
```json
{"ok": true,  "data": { ... }}
{"ok": true,  "data": { ... }, "duplicate": true}
{"ok": false, "code": "generation_mismatch", "message": "...", "retryable": false}
```
- `retryable`은 서버가 정합니다. `ctx.tool()`이 거절을 받으면 `ToolUnavailable`(RetryableError) / `ToolRejected`(FatalError)로 바꿔 던집니다.
- 같은 `idem_key`로 이미 저장된 경우는 `duplicate: true`인 **성공**입니다 (재배송 때 정상 결과).
- "다른 worker가 하는 중" 같은 결과의 한 종류는 오류가 아니라 `data.status`로 돌려줍니다 (`job_start`).

**오류 코드**: `invalid_args`, `permit_invalid`, `not_found`, `generation_mismatch` (재시도 안 함) / `locked`, `unavailable` (재시도).

**계약은 코드로**: `notree/contracts/tools.py`에 도구마다 입력·출력 Pydantic 모델, 담당, 리뷰 담당, 모델 노출 여부.
- MCP 서버는 계약에 없는 이름의 도구를 등록할 수 없습니다 (`@tool` 데코레이터가 막음).
- 테스트가 서버 도구의 인자와 계약 모델이 같은지, 문서가 계약과 같은지 확인합니다.

**담당**: 도구의 입출력은 그 도구를 쓰는 핸들러 담당이 정의하고, 그 도구가 읽고 쓰는 테이블의 담당이 리뷰합니다.
| 영역 | 정의 | 리뷰 |
|---|---|---|
| `job_*`, `checkpoint_*` | worker 담당 | 전원 |
| `transcript_range`, `summary_*`, `notes_*`, `lecture_note_publish` | T3·T4 | `transcript_range`는 T1·T2, `notes_*`는 T5 |
| `qa_*`, `web_search`, `grade_*`, `question_publish` | T5 | `qa_context`·`question_publish`는 T3·T4 |

**절차**
1. 도구 추가·변경은 `notree/contracts/tools.py`를 고치는 PR로. 정의 담당이 올리고 리뷰 담당이 승인
2. `uv run python -m notree.dev.gen_tool_docs`로 문서 재생성 (안 하면 테스트 실패)
3. 선택 필드 추가는 자유. 삭제·이름 변경·의미 변경은 호출하는 쪽과 합의 후
4. 서버 구현 전에도 핸들러 담당은 계약 모델로 가짜 응답을 만들어 먼저 개발할 수 있습니다

## 9. 체크포인트 정리

- 완료·실패 후 7일(`NOTREE_CHECKPOINT_KEEP_SEC`) 지난 작업의 체크포인트를 1시간마다 삭제 (`checkpoint_cleanup`).
- 실패한 작업도 같은 기간 보관합니다 (7번에서 되살려 이어가야 하므로).
- **탈퇴·과목 삭제 시 지우는 대상(설계서 11절 삭제 원장 흐름)에 체크포인트 테이블을 포함합니다.** 아직 PoC에는 없음.
