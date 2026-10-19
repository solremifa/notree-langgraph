# worker 관련 팀 결정 사항 (확정)

2026-10-15 확정 (2·3번 작업 종류별 값과 11번은 2026-10-17 추가). 처음 제안에서 무엇이 바뀌었고 코드의 어디를 고쳤는지는
[changes/2026-10-15-team-decisions.md](changes/2026-10-15-team-decisions.md)에 따로 정리했습니다.

| # | 항목 | 확정 내용 | 코드 |
|---|---|---|---|
| 1 | 재배송 주체 | worker가 `XAUTOCLAIM`으로 직접 회수. scheduler는 누락 복구(6번)만 | `notree/worker/loop.py` `_next` |
| 2 | 시간 값 | rt 15/5/20초·제한 90초, batch 60/20/90초·제한 15분. Q&A·채점은 요청부터 전체 90초(`deadline_at`), final_note 시도당 5분, course_update 시도당 10분 | `notree/common/settings.py`, `loop.py` |
| 3 | 재시도·실패 | 최초 포함 총 횟수: Q&A·채점 3회(90초 안에서), final_note·course_update 3회, 실시간 요약 1회. 실패는 `failed` + 사유 + 다시 시도 가능 여부 | `settings.KIND_MAX_ATTEMPTS`, `loop.py` |
| 4 | permit | HMAC 서명, 사용자 ID 포함. MCP가 꺼내 씀(인자로 받지 않음). 유효 rt 5분·batch 30분 | `notree/mcp_server/server.py` `_issue_permit` |
| 5 | 동시 실행 | 프로세스당 rt 4, batch 1 | `settings.RT/BATCH.concurrency` |
| 6 | 배정기 누락 복구 | 10초마다, reserved 30초·sent 120초 넘게 멈춘 의도를 다시 XADD | `notree/scheduler/periodic.py` |
| 7 | 다시 시도 | 같은 job을 되살림(체크포인트 유지). 영구 실패는 거절. 마감 있던 작업은 새 마감과 함께 | `job_retry` 도구 |
| 8 | MCP 도구 계약 | 이름 `영역_동사`, 공통 인자·응답 형식, 계약은 Pydantic 코드 하나 | `notree/contracts/` |
| 9 | 체크포인트 정리 | 완료·실패 후 7일 보관 뒤 삭제, 1시간마다 | `checkpoint_cleanup` 도구, `periodic.py` |
| 10 | Q&A 구현 방식 | LangGraph로 구현 (노트 → 전사 → 웹, 인용 확인). 보완 목록 10번 참고 | `notree/graphs/qa/`, `qa` 핸들러 (T5) |
| 11 | 레인 | Q&A·채점(`qa`) / 실시간 요약(`rt`) / batch로 분리, 배정도 레인별. 슬롯은 고정이 기본, 빌려 쓰기는 옵션 | `settings.STREAMS`, `ALLOC_LANES`, `BORROW` |

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

**작업 종류별 값** (2026-10-17 확정, `settings.KIND_*`). 시도 수는 모두 **최초 포함 총 횟수**입니다.

| 작업 | 전체 마감 | 시도당 제한 | 총 시도 |
|---|---|---|---|
| `qa`, `grading` | 요청부터 90초 (`deadline_at`) | 없음 (마감까지 남은 시간) | 3회, 모두 90초 안에서 |
| `final_note` | 없음 | 5분 | 3회 |
| `course_update` | 없음 | 10분 | 3회 |
| `live_summary` | 없음 | rt 기본 90초 | 1회 |

- Q&A·채점은 api가 `deadline_at`을 안 넣으면 서버(`job_create`)가 요청 시각 + 90초로 넣습니다. 사용자 재시도(`job_retry`)는 새 마감을 안 주면 지금 + 90초로 다시 잡습니다.
- 시도당 제한은 레인 제한보다 길게 늘리지 않습니다 (둘 중 짧은 쪽).

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

- 한도 (최초 포함): Q&A·채점 3회, final_note·course_update 3회, `live_summary` 1회. 그 밖은 rt 2회·batch 3회 (`settings.KIND_MAX_ATTEMPTS`)
- 재시도 간격: lease를 반납하고 재배송 기준만큼 기다렸다 다시 (고정 간격). 429가 많으면 측정 후 늘어나는 간격 검토
- 실패 확정 시 DB에 `state=failed`, `fail_reason`, `user_retryable`이 남습니다. 화면은 이 셋을 근거로 표시합니다
  - `user_retryable=true`: 한도 초과, 실행 제한·마감 초과 → "다시 시도" 버튼
  - `user_retryable=false`: 영구 실패 → 버튼 없음
  - 사용자에게는 `fail_reason` 원문 대신 화면용 문구를 보여 줍니다 (문구는 프런트에서 결정)
- 실패 작업 보관함은 따로 만들지 않고 `failed` 상태로 대신합니다

## 4. permit 토큰

- 형식: `base64url({"j": job_id, "w": worker, "a": 시도 번호, "x": 실행 세대, "u": user_id, "e": 만료}).HMAC-SHA256 앞 32자`
- 실행 세대(`exec_epoch`)는 `job_start`마다 1씩 늘고 **절대 되돌리지 않습니다.** 시도 번호는 재시도 한도용이라 사용자 재시도(`job_retry`) 때 0으로 돌아가므로 실행권 확인에 쓰면 안 됩니다 (재시도 전 permit이 새 실행에서 통하던 문제, 2026-10-15 2차 검토).
- `job_start`가 발급하고, worker는 이후 모든 도구 호출에 붙입니다 (`ctx.tool()`이 자동으로).
- 서버는 서명·만료·job_id 일치를 확인하고, **사용자 ID를 permit에서 꺼내 씁니다.** 도구 인자로 사용자 ID를 받지 않습니다.
- **쓰기 도구는 같은 트랜잭션 안에서 실행권을 다시 확인합니다**: 작업이 `running`이고, 소유자와 실행 세대(`exec_epoch`)가 permit과 같아야 합니다.
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

## 10. Q&A 구현 방식

- **Q&A는 LangGraph로 구현합니다** (2026-10-15 재결정. 같은 날 "tool calling으로"라고 했다가 바꿈).
  `feat/qa-graph`에서 만든 그래프를 main에 합쳤습니다: `notree/graphs/qa/`, 데모 `uv run python -m notree.dev.qa_run`.
  설명: [changes/2026-10-15-qa-graph.md](changes/2026-10-15-qa-graph.md)
- 그래프가 정하는 것: 검색 순서(노트 → 전사 → 웹), 충분하면 멈춤, 시간이 부족하면 건너뜀, 웹 최대 1번, 인용 ID 확인 후 1번 다시 답하기, 출처 라벨.
  LLM은 질문 이해와 답변 작성만. 체크포인트는 쓰지 않음 (rt 작업, 죽으면 처음부터).
- 실험·실제 LLM 실행·외부 검토에서 나온 보완 목록 (✅ 그래프에 반영됨 / ⬜ 남음). 6~10번 상세: [changes/2026-10-15-qa-hardening.md](changes/2026-10-15-qa-hardening.md)
  1. ✅ "더 찾을지"(`ENOUGH_SCORE` 0.6)와 "LLM에게 보여줄지"(`EVIDENCE_MIN` 0.3)를 다른 기준으로
  2. ✅ 인용 ID가 받은 근거에 있는지 코드로 확인, 틀리면 1번 다시 답하기
  3. ✅ 출처 라벨은 LLM이 실제로 인용한 근거의 출처로 (`label_for`)
  4. ✅ 웹은 단계도 실제 요청도 1번 (장애가 나도 다시 요청하지 않음. 노트·전사는 1번 다시), 남은 시간 확인은 그래프 구조로 강제
  5. ✅ OpenAI JSON 모드에 'json' 단어 넣기
  6. ✅ 인용 검증이 끝내 실패하면 본문도 정해진 "근거 부족" 안내문으로 교체
  7. ✅ 문장별 `{text, citations}`로 받고 본문의 인용 표시 `[^ID]`는 코드가 생성. LLM이 넣은 표시는 `[^..]`나 이번 근거 ID만 지우고 `a[0]`·`[1, 2]` 같은 코드·수식은 그대로
  8. ✅ 점수가 높아도 LLM이 "답할 수 없음"(`needs_more_evidence`)이면 남은 출처로. LLM 호출은 실행당 4번까지
  9. ✅ 검색 장애 구분: 일시 장애·타임아웃 → 1번 다시, 안 되면 다음 출처 / 권한 없음·삭제(`SearchForbidden`) → `QAFatal`로 중단
  10. ✅ 시간 예산: 검색 1회 최대 8초·답변용 8초 남김, LLM 1회 최대 25초·저장용 2초 남김, 시간이 모자라면 "시간 초과" 안내문 (마감이 지났으면 LLM을 부르지 않음)
  11. 🔶 관련 여부 판단에 최근 대화 넣기 (`feat/qa-conversation-memory`, 2026-10-20, **main 미머지**): 대화 세션별 Redis 단기 메모리,
      understand부터 이전 문답으로 지시 대상을 풀어 검색용 질문을 만들고, 모호하면 확인 질문(`확인 필요`). 가짜 LLM으로만 검증 —
      실제 모델의 해석·확인 질문 품질은 미검증. 지정 자료는 남음. [변경 정리](changes/2026-10-20-qa-conversation-memory.md)
  12. 🔶 `qa_context` 계약 연결 (`feat/qa-worker`, 2026-10-17): 사용자·과목 검색 범위, 질문 세대, 저장용 sources는 연결 (PoC 가짜 도구).
      대화 맥락(`recent_turns` 구조화)과 이력 입력 상한(추정 토큰)은 위 11번 브랜치에서. 실제 토크나이저 기준 예산은 남음
  13. ⬜ 노트·전사·웹 점수는 의미가 다름 (계약상 `notes_search`는 pg_trgm, 웹은 점수 계약 없음) → 기준값은 검색별로 보정
  14. ⬜ 검색어: LLM이 일반적인 단어를 섞음 → 검색 방식에 맞춰 질문 문장을 그대로 넣을지 비교
  15. ⬜ 화면의 출처 표시는 `citations` 목록으로 (API·화면 쪽)
  16. ✅ 상태(얼마나 답했나)를 라벨(어디서 왔나)과 따로: `completed` / `부분 답변`(인용은 있으나 근거가 더 필요하다고 함) / `근거 부족` / `시간 초과`
  17. ✅ 과목 질문인데 인용이 하나도 없으면 LLM 본문을 버리고 안내문만 (근거 없이 지어낸 답 차단). 일반 지식 답을 같이 보여줄지는 아래 '아직 정할 것'
  - 참고: 인용 확인은 "인용 ID 유효성 검사"일 뿐 사실성 검증이 아님 (근거 n1=LIFO를 붙여 "FIFO"라고 답해도 통과)
- **검색 방식 확인 필요**: 도구 계약은 pg_trgm, 기획의 Agent 안은 벡터 DB 인덱싱. 팀에서 하나로 정해야 함
- 아직 정할 것 (기획·API): 과목과 관계없는 질문에 답할지, "근거 부족"일 때 일반 지식 답을 같이 보여줄지, 진행 상황 표시 여부

---

## 11. 레인 (2026-10-17)

- 레인 = Redis Stream + 그 Stream을 읽는 worker 슬롯. `qa`(Q&A·채점), `rt`(실시간 요약), `batch`(최종 노트·과목 노트).
- 배정도 레인별로 합니다. DB 도구는 "이 레인의 대기 의도를 생성 순서로 N개"(`job_reserve_pending stream=`)만 하고, 어느 레인을 얼마나 먼저 고를지는 배정기가 정합니다 (`ALLOC_LANES`, 기본 qa 10 / rt 10 / batch 5, 한 바퀴 단위).
- 슬롯은 **고정이 기본**입니다. 빌려 쓰기(qa가 비면 rt 작업을 가져옴)는 옵션이고, 쓸 때는 **qa 총 슬롯과 함께** 정합니다 (예: "qa 16슬롯 중 최대 8개 빌려 씀"). 빌려 쓰는 수는 qa 총 슬롯 - 1을 넘지 못합니다.
- 근거와 측정: [changes/2026-10-16-lanes.md](changes/2026-10-16-lanes.md)

## 결정 대기 (2026-10-17 정리)

하나로 묶지 말고 따로 정합니다.

| 항목 | 선택지 | 메모 |
|---|---|---|
| heartbeat·lease·진행 상태를 Redis로 | 옮김 / 유지 | 옮겨도 실행권 확인은 DB의 `exec_epoch`로 (`attempts`는 사용자 재시도 때 0으로 돌아가 쓸 수 없음). Redis 쪽 생존 신호는 하나로 (지금 쓰는 Stream 방치 시간 또는 TTL 키) |
| DB에 기록한 작업의 전달 누락 복구 | outbox(영속 작업 의도) + 주기 복구 **유지** | heartbeat 위치와 별개. "DB 저장 성공 → API 종료·Redis 장애 → Redis 등록 실패" 구간은 커밋 직후 XADD로도 남는다 |
| 정상 경로 전달을 즉시 깨우기 | 커밋 직후 XADD 또는 배정기 깨우기(NOTIFY 등) | 복구 장치는 그대로 두고 0.5초 주기 대기만 줄인다. 사용자·비용 한도를 배정기가 판단해 왔다면 그 판단을 어디로 옮길지 함께 정함 |
| Q&A 검색 (`pg_trgm`) | DB 조회(Read)로 봄 | "DB는 CRUD만" = 업무 판단은 애플리케이션. 유사 문자열 후보 조회는 Read. 검색 모듈 분리와 검색 엔진 교체는 별개 결정 |
| 전사 검색 범위 | 우선 검색 2개 + 부족 시 확장 | 노트로 고른 강의 2개를 먼저, 근거가 부족하면 과목 전체로. 속도와 함께 **근거 검색 성공률**을 비교한 뒤 확정 |

바로 할 수 있는 작업의 조건

| 작업 | 조건 |
|---|---|
| 요약 간격 자동 조절 | 처리한 전사 번호를 유지하고, 대기 요청을 병합해 내용이 빠지지 않게 |
| 전사 묶음 저장 | 최대 저장 지연, 종료 시 남은 분량 저장, 장애 복구 기준 합의 |
| 전사 검색 우선 범위 | 우선 탐색으로만 쓰고 근거 부족 시 넓힘 |
| Q&A 그래프 worker 연결 | 실행 마감·permit·오류 변환·멱등 저장 연결 |
| 실제 OpenAI 검증 | 새 JSON 형식과 함께 인용·부분 답변·시간 초과 확인 |
