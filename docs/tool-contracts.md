# MCP 도구 계약

> 이 문서는 `notree/contracts/tools.py`에서 자동 생성됩니다. 직접 고치지 말고 계약 코드를 고친 뒤
> `uv run python -m notree.dev.gen_tool_docs`를 실행하세요. (테스트가 둘이 같은지 확인합니다)

## 공통 규칙

- 이름: `영역_동사`, 소문자·밑줄 (OpenAI 함수 이름은 점을 허용하지 않음)
- worker가 부르는 도구는 모두 `permit`을 받는다 (`job_start` 제외). 사용자 ID 등 permit으로 알 수 있는 값은 인자로 받지 않는다
- 쓰기 도구는 같은 트랜잭션 안에서 permit의 실행권(작업이 running, 소유자·실행 세대 일치)을 확인한다. 없으면 `permit_invalid`
- 쓰기 도구는 `idem_key`(= job_id), 삭제·취소 영향을 받는 쓰기는 `expected_generation`을 받는다
- 응답: `{ok: true, data}` / `{ok: true, data, duplicate: true}` / `{ok: false, code, message, retryable}`
- 바꾸는 규칙: 선택 필드 추가는 자유, 삭제·이름 변경·의미 변경은 호출하는 쪽·리뷰 담당과 합의 후

## 오류 코드

| code | retryable | worker에서 |
|---|---|---|
| `invalid_args` | 아니오 | ToolRejected (FatalError) |
| `permit_invalid` | 아니오 | ToolRejected (FatalError) |
| `not_found` | 아니오 | ToolRejected (FatalError) |
| `generation_mismatch` | 아니오 | ToolRejected (FatalError) |
| `locked` | 예 | ToolUnavailable (RetryableError) |
| `unavailable` | 예 | ToolUnavailable (RetryableError) |

## 목록

| 도구 | 종류 | 호출 | 정의 | 리뷰 | 모델 노출 | 상태 |
|---|---|---|---|---|---|---|
| `job_create` | 작업 제어 | api | worker 담당 |  |  | 구현 |
| `job_reserve_pending` | 작업 제어 | scheduler | worker 담당 |  |  | 구현 |
| `job_mark_sent` | 작업 제어 | scheduler | worker 담당 |  |  | 구현 |
| `job_resend_stale` | 작업 제어 | scheduler | worker 담당 |  |  | 구현 |
| `job_start` | 작업 제어 | worker | worker 담당 |  |  | 구현 |
| `job_heartbeat` | 작업 제어 | worker | worker 담당 |  |  | 구현 |
| `job_finish` | 작업 제어 | worker | worker 담당 |  |  | 구현 |
| `job_fail` | 작업 제어 | worker | worker 담당 |  |  | 구현 |
| `job_release` | 작업 제어 | worker | worker 담당 |  |  | 구현 |
| `job_retry` | 작업 제어 | api | worker 담당 |  |  | 구현 |
| `job_get` | 개발용 | dev | worker 담당 |  |  | 구현 |
| `checkpoint_put` | 쓰기 | worker | worker 담당 |  |  | 구현 |
| `checkpoint_put_writes` | 쓰기 | worker | worker 담당 |  |  | 구현 |
| `checkpoint_get` | 읽기 | worker | worker 담당 |  |  | 구현 |
| `checkpoint_list` | 읽기 | worker | worker 담당 |  |  | 구현 |
| `checkpoint_delete_thread` | 쓰기 | worker | worker 담당 |  |  | 구현 |
| `checkpoint_cleanup` | 쓰기 | scheduler | worker 담당 |  |  | 구현 |
| `note_publish` | 쓰기 | worker | T3·T4 |  |  | 구현 |
| `note_get` | 개발용 | dev | T3·T4 |  |  | 구현 |
| `tool_stats` | 개발용 | dev | worker 담당 |  |  | 구현 |
| `qa_ask` | 쓰기 | api | T5 | T3·T4 |  | 구현 |
| `qa_context` | 읽기 | worker | T5 | T3·T4 |  | 구현 |
| `qa_search` | 읽기 | worker | T5 | T3·T4 |  | 구현 |
| `web_search` | 외부 | worker | T5 |  |  | 구현 |
| `qa_complete` | 쓰기 | worker | T5 | T3·T4 |  | 구현 |
| `qa_get` | 읽기 | api | T5 | T3·T4 |  | 구현 |
| `qa_delete` | 쓰기 | api | T5 | T3·T4 |  | 구현 |
| `qa_seed` | 개발용 | dev | T5 |  |  | 구현 |
| `transcript_range` | 읽기 | worker | T3·T4 | T1·T2 | ○ | 계획 |
| `summary_segments` | 읽기 | worker | T3·T4 |  |  | 계획 |
| `summary_publish` | 쓰기 | worker | T3·T4 |  |  | 계획 |
| `notes_read` | 읽기 | worker | T3·T4 | T5 | ○ | 계획 |
| `notes_search` | 읽기 | worker | T3·T4 | T5 | ○ | 계획 |
| `lecture_note_publish` | 쓰기 | worker | T3·T4 |  |  | 계획 |
| `grade_context` | 읽기 | worker | T5 |  |  | 계획 |
| `grade_save` | 쓰기 | worker | T5 |  |  | 계획 |
| `question_publish` | 쓰기 | worker | T5 | T3·T4 |  | 계획 |

## 구현된 도구

### `job_create`

작업 제어 · 호출: api · 정의: worker 담당

의도(intent) 기록. 실제로는 api가 PG에 직접

입력

| 필드 | 타입 | 설명 |
|---|---|---|
| `kind` | `str` | 작업 종류 (핸들러 이름) |
| `payload` | `dict[str, any]` | 핸들러 입력. Q&A·채점에 deadline_at이 없으면 서버가 요청 시각 + 90초로 넣는다 |
| `user_id` | `str` | 요청한 사용자. permit에 담겨 MCP 서버의 RLS 기준이 된다 |
| `stream` | `str | null` = None | 생략하면 작업 종류별 기본 Stream |

출력 (`data`)

| 필드 | 타입 | 설명 |
|---|---|---|
| `job_id` | `str` |  |

### `job_reserve_pending`

작업 제어 · 호출: scheduler · 정의: worker 담당

대기 의도 → 실행 허가. stream을 주면 그 레인만 (배정기가 레인별로 부름)

입력

| 필드 | 타입 | 설명 |
|---|---|---|
| `limit` | `int` = 10 |  |
| `stream` | `str | null` = None | 이 레인(Stream)의 의도만 고른다. 생략하면 모든 레인을 생성 순서로 (레인 분리 전 방식) |

출력 (`data`)

| 필드 | 타입 | 설명 |
|---|---|---|
| `jobs` | `list[ReservedJob]` |  |

### `job_mark_sent`

작업 제어 · 호출: scheduler · 정의: worker 담당

XADD 후 전송 완료 표시

입력

| 필드 | 타입 | 설명 |
|---|---|---|
| `job_id` | `str` |  |

출력 (`data`)

| 필드 | 타입 | 설명 |
|---|---|---|
| `marked` | `bool` |  |

### `job_resend_stale`

작업 제어 · 호출: scheduler · 정의: worker 담당

오래 멈춘 reserved/sent 의도를 다시 보낼 목록 (결정 6)

입력

| 필드 | 타입 | 설명 |
|---|---|---|
| `reserved_older_than_sec` | `float` = 30 | 허가 후 이만큼 지나도 전송 완료가 아니면 다시 보냄 |
| `sent_older_than_sec` | `float` = 120 | 전송 완료 후 이만큼 아무도 시작하지 않으면 다시 보냄 |
| `limit` | `int` = 50 |  |

출력 (`data`)

| 필드 | 타입 | 설명 |
|---|---|---|
| `jobs` | `list[ReservedJob]` |  |

### `job_start`

작업 제어 · 호출: worker · 정의: worker 담당

실행 시작 + lease + permit(실행 세대 포함) 발급. 살아 있는 lease면 같은 worker라도 leased

입력

| 필드 | 타입 | 설명 |
|---|---|---|
| `job_id` | `str` |  |
| `worker` | `str` |  |
| `lease_sec` | `float` |  |

출력 (`data`)

| 필드 | 타입 | 설명 |
|---|---|---|
| `status` | `"started" | "done" | "failed" | "leased" | "unknown"` |  |
| `kind` | `str | null` = None |  |
| `payload` | `dict[str, any] | null` = None |  |
| `attempt` | `int | null` = None | 1부터. 이번이 몇 번째 시도인지 |
| `previous_owner` | `str | null` = None |  |
| `permit` | `str | null` = None | 이후 모든 도구 호출에 첨부 |
| `owner` | `str | null` = None | status=leased일 때 현재 소유자 |
| `lease_left_sec` | `float | null` = None |  |

### `job_heartbeat`

작업 제어 · 호출: worker · 정의: worker 담당 · permit 필요

lease 연장. 실행권(소유자·실행 세대)이 없으면 renewed=false

입력

| 필드 | 타입 | 설명 |
|---|---|---|
| `job_id` | `str` |  |
| `worker` | `str` |  |
| `lease_sec` | `float` |  |

출력 (`data`)

| 필드 | 타입 | 설명 |
|---|---|---|
| `renewed` | `bool` | false면 lease를 잃은 것 → 하던 일을 멈춘다 |

### `job_finish`

작업 제어 · 호출: worker · 정의: worker 담당 · permit 필요

완료 표시. finished=true를 확인한 뒤에만 XACK

입력

| 필드 | 타입 | 설명 |
|---|---|---|
| `job_id` | `str` |  |
| `worker` | `str` |  |

출력 (`data`)

| 필드 | 타입 | 설명 |
|---|---|---|
| `finished` | `bool` |  |

### `job_fail`

작업 제어 · 호출: worker · 정의: worker 담당 · permit 필요

실패 확정. failed=true를 확인한 뒤에만 XACK

입력

| 필드 | 타입 | 설명 |
|---|---|---|
| `job_id` | `str` |  |
| `worker` | `str` |  |
| `reason` | `str` |  |
| `user_retryable` | `bool` | 사용자에게 '다시 시도' 버튼을 보여 줄지 (영구 실패면 false) |

출력 (`data`)

| 필드 | 타입 | 설명 |
|---|---|---|
| `failed` | `bool` |  |

### `job_release`

작업 제어 · 호출: worker · 정의: worker 담당 · permit 필요

일시적 실패: lease 반납, 재배송 대기

입력

| 필드 | 타입 | 설명 |
|---|---|---|
| `job_id` | `str` |  |
| `worker` | `str` |  |
| `reason` | `str` |  |

출력 (`data`)

| 필드 | 타입 | 설명 |
|---|---|---|
| `released` | `bool` |  |

### `job_retry`

작업 제어 · 호출: api · 정의: worker 담당

사용자의 '다시 시도': failed → pending (결정 7)

입력

| 필드 | 타입 | 설명 |
|---|---|---|
| `job_id` | `str` |  |
| `deadline_at` | `float | null` = None | 새 마감. 없으면 Q&A·채점은 지금 + 90초, 그 밖의 작업은 기존 값 유지 |

출력 (`data`)

| 필드 | 타입 | 설명 |
|---|---|---|
| `revived` | `bool` |  |
| `reason` | `str | null` = None |  |

### `job_get`

개발용 · 호출: dev · 정의: worker 담당

작업 상태 조회

입력

| 필드 | 타입 | 설명 |
|---|---|---|
| `job_id` | `str` |  |

출력 (`data`)

| 필드 | 타입 | 설명 |
|---|---|---|
| `found` | `bool` |  |
| `job_id` | `str | null` = None |  |
| `kind` | `str | null` = None |  |
| `user_id` | `str | null` = None |  |
| `payload` | `dict[str, any] | null` = None |  |
| `stream` | `str | null` = None |  |
| `state` | `str | null` = None |  |
| `lease_owner` | `str | null` = None |  |
| `lease_until` | `float | null` = None |  |
| `attempts` | `int | null` = None |  |
| `fail_reason` | `str | null` = None |  |
| `user_retryable` | `bool | null` = None |  |
| `created` | `float | null` = None |  |
| `updated_at` | `float | null` = None |  |

### `checkpoint_put`

쓰기 · 호출: worker · 정의: worker 담당 · permit 필요

LangGraph 단계 상태 저장

입력

| 필드 | 타입 | 설명 |
|---|---|---|
| `thread_id` | `str` |  |
| `ns` | `str` |  |
| `checkpoint_id` | `str` |  |
| `parent_id` | `str | null` |  |
| `checkpoint` | `list[str]` | [serde 타입, base64] |
| `metadata` | `list[str]` | [serde 타입, base64] |

출력 (`data`)

| 필드 | 타입 | 설명 |
|---|---|---|
| (없음) | | |

### `checkpoint_put_writes`

쓰기 · 호출: worker · 정의: worker 담당 · permit 필요

노드 결과(pending writes) 저장

입력

| 필드 | 타입 | 설명 |
|---|---|---|
| `thread_id` | `str` |  |
| `ns` | `str` |  |
| `checkpoint_id` | `str` |  |
| `task_id` | `str` |  |
| `task_path` | `str` |  |
| `writes` | `list[list[any]]` | [[idx, channel, v_type, v_b64], ...] |

출력 (`data`)

| 필드 | 타입 | 설명 |
|---|---|---|
| (없음) | | |

### `checkpoint_get`

읽기 · 호출: worker · 정의: worker 담당 · permit 필요

체크포인트 조회 (없으면 최신)

입력

| 필드 | 타입 | 설명 |
|---|---|---|
| `thread_id` | `str` |  |
| `ns` | `str` = '' |  |
| `checkpoint_id` | `str | null` = None |  |

출력 (`data`)

| 필드 | 타입 | 설명 |
|---|---|---|
| `found` | `bool` |  |
| `item` | `CheckpointItem | null` = None |  |

### `checkpoint_list`

읽기 · 호출: worker · 정의: worker 담당 · permit 필요

체크포인트 이력

입력

| 필드 | 타입 | 설명 |
|---|---|---|
| `thread_id` | `str` |  |
| `ns` | `str | null` = None |  |
| `before` | `str | null` = None |  |
| `limit` | `int | null` = None |  |

출력 (`data`)

| 필드 | 타입 | 설명 |
|---|---|---|
| `items` | `list[CheckpointItem]` |  |

### `checkpoint_delete_thread`

쓰기 · 호출: worker · 정의: worker 담당 · permit 필요

thread 체크포인트 삭제

입력

| 필드 | 타입 | 설명 |
|---|---|---|
| `thread_id` | `str` |  |

출력 (`data`)

| 필드 | 타입 | 설명 |
|---|---|---|
| (없음) | | |

### `checkpoint_cleanup`

쓰기 · 호출: scheduler · 정의: worker 담당

끝난 작업 체크포인트 정리 (결정 9)

입력

| 필드 | 타입 | 설명 |
|---|---|---|
| `older_than_sec` | `float` = 604800 | 완료·실패 후 이만큼 지난 작업의 체크포인트 삭제 |

출력 (`data`)

| 필드 | 타입 | 설명 |
|---|---|---|
| `deleted_threads` | `int` |  |

### `note_publish`

쓰기 · 호출: worker · 정의: T3·T4 · permit 필요

PoC 과목 노트 발행. 사용자 ID는 permit에서

입력

| 필드 | 타입 | 설명 |
|---|---|---|
| `idem_key` | `str` |  |
| `course` | `str` |  |
| `note` | `str` |  |
| `status` | `str` |  |

출력 (`data`)

| 필드 | 타입 | 설명 |
|---|---|---|
| `idem_key` | `str` |  |

### `note_get`

개발용 · 호출: dev · 정의: T3·T4

발행된 노트 조회

입력

| 필드 | 타입 | 설명 |
|---|---|---|
| `idem_key` | `str` |  |

출력 (`data`)

| 필드 | 타입 | 설명 |
|---|---|---|
| `found` | `bool` |  |
| `user_id` | `str | null` = None |  |
| `course` | `str | null` = None |  |
| `note` | `str | null` = None |  |
| `status` | `str | null` = None |  |

### `tool_stats`

개발용 · 호출: dev · 정의: worker 담당

도구 호출 횟수 (실행 흐름 확인용)

입력

| 필드 | 타입 | 설명 |
|---|---|---|
| `thread_id` | `str` |  |

출력 (`data`)

| 필드 | 타입 | 설명 |
|---|---|---|
| `counts` | `dict[str, int]` |  |

### `qa_ask`

쓰기 · 호출: api · 정의: T5 · 리뷰: T3·T4

질문 저장 + qa 작업 의도를 한 트랜잭션에 (PoC: SQLite)

입력

| 필드 | 타입 | 설명 |
|---|---|---|
| `user_id` | `str` | 질문한 사용자 (실제로는 api가 세션에서 꺼냄) |
| `course_id` | `str` |  |
| `question` | `str` |  |

출력 (`data`)

| 필드 | 타입 | 설명 |
|---|---|---|
| `question_id` | `str` |  |
| `job_id` | `str` | qa 작업. 마감(deadline_at)은 요청 시각 + 90초 |

### `qa_context`

읽기 · 호출: worker · 정의: T5 · 리뷰: T3·T4 · permit 필요

질문·과목·강의 목록·세대. 사용자는 permit에서

입력

| 필드 | 타입 | 설명 |
|---|---|---|
| `question_id` | `str` |  |

출력 (`data`)

| 필드 | 타입 | 설명 |
|---|---|---|
| `question` | `str` |  |
| `course_id` | `str` |  |
| `course_name` | `str` | LLM에 주는 과목 이름 |
| `lecture_ids` | `list[str]` | 과목의 강의 목록 (전사 검색 범위를 좁힐 때) |
| `generation` | `int` | 질문 세대. 삭제·수정되면 바뀐다 → qa_complete의 expected_generation |
| `recent_turns` | `list[str]` = [] | 같은 과목의 최근 대화 (첫 통합에서는 비어 있음) |
| `answered` | `bool` = False | 이 작업의 답이 이미 저장됨 (저장 응답을 못 받고 재시도된 경우) → 그래프를 건너뛰고 완료 |

### `qa_search`

읽기 · 호출: worker · 정의: T5 · 리뷰: T3·T4 · permit 필요

노트·전사 근거 검색. PoC는 키워드 일치 점수, 실제는 pg_trgm(Read)

입력

| 필드 | 타입 | 설명 |
|---|---|---|
| `question_id` | `str` | 검색 범위(사용자·과목)는 질문에서 정한다 |
| `source` | `"note" | "transcript"` |  |
| `keywords` | `list[str]` |  |
| `limit` | `int` = 3 |  |
| `lecture_ids` | `list[str] | null` = None | 전사 검색을 이 강의들로 좁힘. 없으면 과목 전체 |

출력 (`data`)

| 필드 | 타입 | 설명 |
|---|---|---|
| `hits` | `list[QaHit]` |  |

### `web_search`

외부 · 호출: worker · 정의: T5 · permit 필요

웹 검색 1회. PoC는 시드한 web 문서에서 찾는 가짜 (실제 공급자 미정)

입력

| 필드 | 타입 | 설명 |
|---|---|---|
| `question_id` | `str` | 어떤 질문의 검색인지 (사용자별 한도·감사용. PoC는 그 과목에 시드한 web 문서에서 찾음) |
| `query` | `str` |  |
| `limit` | `int` = 3 |  |

출력 (`data`)

| 필드 | 타입 | 설명 |
|---|---|---|
| `results` | `list[dict[str, any]]` | [{id, title, url, snippet, score, fetched_at}] |

### `qa_complete`

쓰기 · 호출: worker · 정의: T5 · 리뷰: T3·T4 · permit 필요

답변 저장. 멱등(idem_key), 질문 세대가 그대로일 때만

입력

| 필드 | 타입 | 설명 |
|---|---|---|
| `idem_key` | `str` | = job_id. 같은 키로 두 번 오면 처음 결과를 돌려주고 duplicate=true |
| `question_id` | `str` |  |
| `expected_generation` | `int` | qa_context에서 받은 세대. 다르면 generation_mismatch (질문이 삭제·수정됨) |
| `status` | `"completed" | "부분 답변" | "근거 부족" | "시간 초과"` |  |
| `label` | `"강의" | "웹" | "일반 지식" | "근거 부족"` |  |
| `answer` | `str` | 화면용 본문. 인용 표시 [^ID] 포함 (코드가 만든 것) |
| `sentences` | `list[QaSentence]` |  |
| `sources` | `list[QaSource]` | 인용된 근거만. 문장의 인용은 모두 여기에 있어야 함 (없으면 invalid_args) |
| `notice_code` | `"not_found" | "insufficient" | "timeout" | null` = None |  |

출력 (`data`)

| 필드 | 타입 | 설명 |
|---|---|---|
| `answer_id` | `str` |  |
| `reflect_suggested` | `bool` = False | 노트 반영 제안 여부 (첫 통합에서는 항상 false) |

### `qa_get`

읽기 · 호출: api · 정의: T5 · 리뷰: T3·T4

화면 조회: 질문 상태와 저장된 답변

입력

| 필드 | 타입 | 설명 |
|---|---|---|
| `question_id` | `str` |  |
| `user_id` | `str` |  |

출력 (`data`)

| 필드 | 타입 | 설명 |
|---|---|---|
| `found` | `bool` |  |
| `question` | `str | null` = None |  |
| `state` | `"pending" | "answered" | "deleted" | null` = None |  |
| `job_id` | `str | null` = None |  |
| `answer` | `dict[str, any] | null` = None | 저장된 결과 (status, label, answer, sentences, citations, sources, notice_code). citations는 서버가 문장별 인용을 순서대로 중복 없이 모은 것. 삭제된 질문은 question·answer 모두 null |

### `qa_delete`

쓰기 · 호출: api · 정의: T5 · 리뷰: T3·T4

질문 삭제 (세대 증가 → 진행 중 답변 저장은 generation_mismatch)

입력

| 필드 | 타입 | 설명 |
|---|---|---|
| `question_id` | `str` |  |
| `user_id` | `str` |  |

출력 (`data`)

| 필드 | 타입 | 설명 |
|---|---|---|
| `deleted` | `bool` |  |

### `qa_seed`

개발용 · 호출: dev · 정의: T5

PoC 검색용 노트·전사·웹 문서 넣기

입력

| 필드 | 타입 | 설명 |
|---|---|---|
| `user_id` | `str` |  |
| `course_id` | `str` |  |
| `course_name` | `str` |  |
| `docs` | `list[dict[str, any]]` | [{source: note|transcript|web, id, text, lecture_id?, ref?}] |

출력 (`data`)

| 필드 | 타입 | 설명 |
|---|---|---|
| `inserted` | `int` |  |

## 계획된 도구

담당자가 PR로 필드를 확정하고 `notree/mcp_server/server.py`에 구현합니다.

### `transcript_range` (계획)

읽기 · 호출: worker · 정의: T3·T4 · 리뷰: T1·T2 · **모델에 노출** · permit 필요

강의의 롤링 요약과 지정 번호 이후 확정 전사를 읽는다

입력

| 필드 | 타입 | 설명 |
|---|---|---|
| `lecture_id` | `str` |  |
| `after_seq` | `int` | 이 번호 이후의 확정 전사만 |

출력 (`data`)

| 필드 | 타입 | 설명 |
|---|---|---|
| `rolling_summary` | `str` |  |
| `new_text` | `str` |  |
| `last_seq` | `int` |  |
| `generation` | `int` | 발행할 때 expected_generation으로 다시 넘긴다 |

### `summary_segments` (계획)

읽기 · 호출: worker · 정의: T3·T4 · permit 필요

최종 노트용 구간 요약 목록

입력

| 필드 | 타입 | 설명 |
|---|---|---|
| `lecture_id` | `str` |  |

출력 (`data`)

| 필드 | 타입 | 설명 |
|---|---|---|
| `segments` | `list[SummarySegment]` |  |
| `missing_ranges` | `list[list[int]]` |  |
| `generation` | `int` |  |

### `summary_publish` (계획)

쓰기 · 호출: worker · 정의: T3·T4 · permit 필요

실시간 요약 저장 + outbox

입력

| 필드 | 타입 | 설명 |
|---|---|---|
| `idem_key` | `str` |  |
| `lecture_id` | `str` |  |
| `expected_generation` | `int` |  |
| `upto_seq` | `int` |  |
| `text` | `str` |  |

출력 (`data`)

| 필드 | 타입 | 설명 |
|---|---|---|
| `summary_version` | `int` |  |

### `notes_read` (계획)

읽기 · 호출: worker · 정의: T3·T4 · 리뷰: T5 · **모델에 노출** · permit 필요

노트 블록을 읽는다

입력

| 필드 | 타입 | 설명 |
|---|---|---|
| `note_id` | `str` |  |
| `block_ids` | `list[str] | null` = None |  |

출력 (`data`)

| 필드 | 타입 | 설명 |
|---|---|---|
| `note_id` | `str` |  |
| `blocks` | `list[NoteBlock]` |  |
| `generation` | `int` |  |

### `notes_search` (계획)

읽기 · 호출: worker · 정의: T3·T4 · 리뷰: T5 · **모델에 노출** · permit 필요

과목 노트에서 질문과 관련된 블록을 찾는다 (pg_trgm)

입력

| 필드 | 타입 | 설명 |
|---|---|---|
| `course_id` | `str` |  |
| `query` | `str` |  |
| `limit` | `int` = 8 |  |

출력 (`data`)

| 필드 | 타입 | 설명 |
|---|---|---|
| `hits` | `list[NotesSearchHit]` |  |

### `lecture_note_publish` (계획)

쓰기 · 호출: worker · 정의: T3·T4 · permit 필요

강의 최종 노트 발행 (완료/부분 완료)

입력

| 필드 | 타입 | 설명 |
|---|---|---|
| `idem_key` | `str` |  |
| `lecture_id` | `str` |  |
| `expected_generation` | `int` |  |
| `status` | `"완료" | "부분 완료"` |  |
| `blocks` | `list[NoteBlock]` |  |
| `missing_ranges` | `list[list[int]]` = [] |  |

출력 (`data`)

| 필드 | 타입 | 설명 |
|---|---|---|
| `note_id` | `str` |  |
| `version` | `int` |  |

### `grade_context` (계획)

읽기 · 호출: worker · 정의: T5 · permit 필요

채점용 문제·루브릭·답안·근거

입력

| 필드 | 타입 | 설명 |
|---|---|---|
| `answer_id` | `str` |  |

출력 (`data`)

| 필드 | 타입 | 설명 |
|---|---|---|
| `question` | `str` |  |
| `rubric` | `list[dict[str, any]]` |  |
| `rubric_version` | `int` |  |
| `answer` | `str` |  |
| `evidence_blocks` | `list[NoteBlock]` |  |

### `grade_save` (계획)

쓰기 · 호출: worker · 정의: T5 · permit 필요

채점 결과 저장, 이해 상태·FSRS 갱신

입력

| 필드 | 타입 | 설명 |
|---|---|---|
| `idem_key` | `str` |  |
| `answer_id` | `str` |  |
| `rubric_version` | `int` |  |
| `scores` | `list[dict[str, any]]` | [{element_id, score: 0|0.5|1, note}] |
| `misconception` | `bool` |  |
| `held` | `bool` | 모호해서 보류 → FSRS에 기록하지 않음 |

출력 (`data`)

| 필드 | 타입 | 설명 |
|---|---|---|
| `saved` | `bool` |  |

### `question_publish` (계획)

쓰기 · 호출: worker · 정의: T5 · 리뷰: T3·T4 · permit 필요

복습 문제 발행 (노트 블록 근거)

입력

| 필드 | 타입 | 설명 |
|---|---|---|
| `idem_key` | `str` |  |
| `course_id` | `str` |  |
| `questions` | `list[dict[str, any]]` | [{concept_id, prompt, rubric, source_block_ids}] |

출력 (`data`)

| 필드 | 타입 | 설명 |
|---|---|---|
| `question_ids` | `list[str]` |  |
