# 업데이트 목록

무엇이 바뀌었는지 최신 순으로 적습니다. 항목마다 **실행 방법이나 코드 사용법이 바뀌는지**를 같이 표시합니다.
큰 변경의 자세한 설명은 `docs/changes/`에 따로 둡니다.

표시
- 🆕 추가 · 🔧 변경 · 🗑 삭제 · 🐛 수정
- ⚠️ **깨지는 변경**: 기존 코드·명령을 고쳐야 함

---

## 2026-10-15

### 2차 검토 반영: 인용 표시 훼손, 최종 상태, 웹 재시도, 실행 세대 · `9a91d80`
자세히: [docs/changes/2026-10-15-qa-hardening.md](docs/changes/2026-10-15-qa-hardening.md) "2차 검토 반영"

- 🐛 인용 표시를 지우는 정규식이 `a[0]`, `[1, 2, 3]` 같은 코드·수식을 지우던 문제 → 인용 표시는 `[^ID]` 문법, 지우는 대상을 좁힘
- 🐛 근거가 더 필요하다는 답이 `completed`로 저장되던 문제 → `부분 답변` 상태 추가
- 🐛 과목 질문에 인용 없는 답(근거 없이 지어냈을 수 있음)이 본문째 저장되던 문제 → 안내문으로 교체
- 🐛 웹 검색이 장애 시 2번 요청되던 문제 → 웹은 다시 요청하지 않음
- 🐛 사용자 재시도 뒤 이전 permit이 새 실행에서 통하던 문제 → 되돌리지 않는 실행 세대 `exec_epoch`
- 🔧 `docs/team-decisions.md` 4번(permit 형식), 10번(보완 목록 4·7번 수정, 16·17번 추가)
- ⚠️ 본문 인용 표시 `[n1]` → `[^n1]`, 상태에 `부분 답변` 추가
- ⚠️ permit에 `x`(실행 세대) 추가 → 이전 permit은 쓰기 도구에서 `permit_invalid`
- 테스트 63 → 69개

### Q&A 그래프 보완 5가지 · `a97c155`
자세히: [docs/changes/2026-10-15-qa-hardening.md](docs/changes/2026-10-15-qa-hardening.md)

- 🐛 인용 검증이 끝내 실패하면 틀린 본문까지 저장되던 문제 → 정해진 "근거 부족" 안내문으로 저장
- 🐛 본문의 `[ID]`와 인용 목록이 어긋날 수 있던 문제 → LLM은 문장별 인용만 내고 `[ID]` 표시는 코드가 생성
- 🆕 점수가 높아도 LLM이 "답할 수 없음"이면 다음 출처로 다시 검색 (LLM 호출 실행당 4번까지)
- 🆕 검색 장애 처리: 일시 장애·타임아웃은 1번 다시 후 다음 출처, 권한 없음·삭제는 `QAFatal`
- 🆕 시간 예산: 검색·LLM 호출별 제한, 답변·저장용 시간 확보, 모자라면 "시간 초과" 안내문
- 🔧 `docs/team-decisions.md` 10번 보완 목록 6~10번 ✅
- ⚠️ LLM `answer()` 반환이 `Answer(sentences, needs_more_evidence)`로 바뀜 (OpenAI 답변 JSON 형식도 변경)
- ⚠️ 상태에 `시간 초과` 추가, 남은 시간이 아주 적으면 검색 없이 답변
- 테스트 53 → 63개

### Q&A 그래프를 main에 합침 (결정 변경: Q&A에 LangGraph 사용) · `e7ab556`
- 🆕 `feat/qa-graph` 병합: `notree/graphs/qa/`, `notree/dev/qa_run.py`, `tests/test_qa_graph.py` (16개)
- 🔧 `docs/team-decisions.md` 10번: "LangGraph 안 씀" → **LangGraph로 구현**. 실험·외부 검토에서 나온 보완 목록은 그대로 두고, 그래프에 이미 반영된 것과 남은 것을 표시
- 기존 코드·명령 변경 없음. 새 명령 `uv run python -m notree.dev.qa_run`
- 테스트 37 → 53개

### 팀 결정 10번 보강: Q&A 외부 검토 반영 · `5fb69ab`
- 🔧 `docs/team-decisions.md` 10번에 Q&A 핸들러에 넣을 것 9가지 추가 (실패 시 본문 교체, 본문·인용 일치, 답변 불가 시 다음 출처, 점수 보정, 대화 맥락, 장애 구분, 시간 예산, `qa_context` 연결)
- 🐛 7번의 "임베딩 검색" 표현 수정: 계약상 `notes_search`는 pg_trgm. 검색 방식은 팀 확인 필요로 표시
- 코드 변경 없음

### Q&A는 LangGraph 없이 가기로 결정 · `470ca8c`
- 🔧 `docs/team-decisions.md` 10번 추가: Q&A는 설계서대로 tool calling. `feat/qa-graph` 브랜치는 합치지 않고 참고용으로 보관
- 🔧 같은 10번에 실험에서 확인한 것 중 구현 방식과 상관없이 Q&A 핸들러에 넣을 것 7가지 (근거 기준 분리, 코드로 인용 확인, 인용 기준 라벨 등)
- 코드 변경 없음
### [실험 브랜치 `feat/qa-graph`] 실제 LLM 실행 결과 반영: 근거 기준 분리 · `35ed1b7`
- 🐛 정답 근거를 찾고도 "근거 부족"이 되던 문제: 멈춤 기준(0.6)이 LLM에게 줄 근거까지 걸렀음.
  이제 `EVIDENCE_MIN`(0.3) 이상인 근거는 모두 LLM에게 주고, 라벨은 **LLM이 실제로 인용한 근거의 출처**로 정함
- 🔧 OpenAI 프롬프트: 검색어는 일반 단어 빼고 2~4개, 근거가 없으면 추측하지 말 것
- 🔧 `FakeQALLM(cite_min=0.6)`: 답이 되는 근거만 인용하는 흉내
- 🆕 테스트 2개 (총 53개)

### [실험 브랜치 `feat/qa-graph`] OpenAI JSON 모드 400 오류 수정 · `77c0325`
- 🐛 `--llm openai`에서 answer 단계가 400으로 실패: JSON 모드는 메시지에 'json'이라는 단어가 있어야 하는데 답변 프롬프트에 없었음.
  모든 요청에 "JSON으로만 답하라"는 system 메시지를 붙임
- 🆕 회귀 테스트 `test_openai_requests_always_mention_json` (가짜 클라이언트, 총 51개)

### [실험 브랜치 `feat/qa-graph`] Q&A 근거 탐색 그래프 · `5302e14`
자세히·merge 기준: [docs/changes/2026-10-15-qa-graph.md](docs/changes/2026-10-15-qa-graph.md). **main에는 아직 합치지 않음.**

- 🆕 `notree/graphs/qa/`: 노트 → 강의 전사 → 웹 순서로 근거를 찾고, 충분하면 멈추고, 마감이 가까우면 비싼 단계를 건너뛰고,
  근거에 없는 인용은 1번 다시 답하게 한 뒤 그래도 틀리면 "근거 부족"으로 저장하는 LangGraph. 체크포인트 없음
- 🆕 `notree/dev/qa_run.py`: 단계별 진행 출력 데모 (`--deadline-in`, `--bad-citation`, `--llm openai`, `--graph`)
- 🆕 `tests/test_qa_graph.py` 13개 (총 50개)
- 기존 코드·명령 변경 없음

### 실행 안정성 4건 수정 · `b6fe820`
자세히: [docs/changes/2026-10-15-execution-safety.md](docs/changes/2026-10-15-execution-safety.md)

- 🐛 같은 worker가 자기 실행 중인 메시지를 다시 가져와 중복 실행하던 문제: heartbeat마다 Redis 방치 시간 초기화(`XCLAIM … JUSTID`),
  `job_start`가 살아 있는 lease면 같은 worker도 거절, worker 내부 실행 중 목록
- 🐛 완료 기록 실패 후에도 ACK해 작업이 버려지던 문제: 완료·실패 표시를 최대 3번 시도하고 **확정 응답일 때만 ACK**
- 🐛 lease를 잃은 실행자의 permit으로 쓰기가 성공하던 문제: permit에 실행 세대(시도 번호), 쓰기 트랜잭션 안에서 소유자·세대를 DB와 대조
- 🐛 슬롯이 꽉 찬 상태에서 종료 신호를 못 보던 문제: 슬롯 대기와 종료 신호를 함께 기다림
- 🔧 `tests/test_robustness.py`의 xfail 표시 제거 → 6개 통과 (총 37개)
- 🔧 문서: `worker-flow.md`(한 바퀴·죽거나 느릴 때·알게 된 것), `worker-structure.md`, `team-decisions.md` 4번, 도구 계약 설명
- ⚠️ permit 형식에 `a`(실행 세대) 추가 → 이전 permit은 쓰기 도구에서 `permit_invalid`
- ⚠️ `job_start`가 같은 worker라도 살아 있는 lease면 `leased`
- ⚠️ `job_finish`·`job_fail`·`job_release`·`job_heartbeat`가 실행권(소유자·세대·running)이 없으면 `false`
- 이제 `notree/worker/`를 팀 저장소로 옮겨도 되는 조건(위 네 가지)이 충족됨

### 실행 안정성 회귀 테스트 추가 (아직 수정 전) · `2c85706`
외부 분석에서 지적된 네 가지 문제를 재현하는 테스트. 지금은 모두 실패하며, `xfail(strict=True)`로 표시해 둠.
고치면 그 테스트의 표시를 지운다 (안 지우면 strict 때문에 실패).

- 🆕 `tests/test_robustness.py` 6개
  1. 같은 worker가 자기 실행 중인 메시지를 `XAUTOCLAIM`으로 다시 가져와 중복 실행 (재현: 한 작업이 3번 시작됨)
  2. `job_finish` 기록 실패 후에도 ACK → DB는 running, Redis에선 사라져 작업이 버려짐
  3. lease를 잃은 이전 실행자(또는 이전 시도)의 permit으로 발행·체크포인트 저장이 성공함
  4. 슬롯이 꽉 찬 상태에서 종료 신호를 받으면 유예 시간이 시작되지 않고 작업 끝까지 기다림
- 이 네 가지가 고쳐지기 전에는 `notree/worker/`를 팀 저장소로 옮기지 않는다

### 저장소 정리: 코드를 notree/ 안으로 · `215a7b6`
루트에 있던 1차 PoC 파일을 정리했습니다. 정리 전 상태는 `archive/poc-v1` 브랜치에 그대로 있습니다 (`git checkout archive/poc-v1`).

- 🔧 `mcp_server.py` → `notree/mcp_server/server.py`
- 🔧 `graph.py`, `state.py`, `llm.py` → `notree/graphs/course_update/`
- 🔧 `run.py` → `notree/dev/graph_run.py`
- 🗑 `worker.py`, `scheduler.py`, `api.py`, `demo.py`, `settings.py`, `mcp_saver.py`, `tests/test_worker.py` (`notree/`의 같은 역할 코드로 대체)
- 🆕 `notree/dev/takeover.py`: 죽은 worker 인계 데모 (예전 `demo.py`가 하던 것)
- 🆕 `tests/test_takeover.py`: 죽음 인계 · 발행 직후 죽음 · 느린 worker 보호 (예전 `test_worker.py`를 옮김)
- 🆕 `SimulatedDeath`: 시험·데모용 worker 죽음. ACK·완료 표시·lease 반납 없이 worker가 끝남
- 🆕 `notree/dev/devenv.py`: 데모용 MCP 서버·Redis 준비 공통 코드
- 🔧 README를 새 구조에 맞게 다시 씀. `docs/worker-flow.md`, `worker-structure.md`, `team-decisions.md`의 경로·명령 갱신
- 🔧 `CLAUDE.md`: "코드는 notree/ 안에" 규칙 추가
- ⚠️ 실행 명령 변경

  | 이전 | 이후 |
  |---|---|
  | `uv run demo.py` | `uv run python -m notree.dev.takeover` |
  | `uv run run.py ...` | `uv run python -m notree.dev.graph_run ...` |
  | `uv run mcp_server.py --http` | `uv run python -m notree.mcp_server --http` |
  | `uv run scheduler.py` | `uv run python -m notree.scheduler.allocator` |
  | `uv run worker.py --name A` | `uv run python -m notree.worker --stream batch --name A` |
  | `uv run api.py ...` | `uv run python -m notree.dev.enqueue course_update sources=...` |
  | `uv run mcp dev mcp_server.py` | `uv run mcp dev notree/mcp_server/server.py` |
- ⚠️ import 경로 변경: `from graph import ...` → `from notree.graphs.course_update import ...`,
  `from mcp_saver import ...` → `from notree.worker.checkpointer import ...` / `from notree.worker.mcp_client import McpClient`
- 테스트 31개 (옮긴 3개 포함, 개수는 그대로)

### 업데이트 목록 추가 · `6d0de84`
- 🆕 `CHANGELOG.md`: 이 목록. 앞으로 커밋마다 갱신
- 🆕 `CLAUDE.md`: 변경 기록 규칙, uv·계약 문서·테스트 규칙

### 팀 결정 확정 반영 + MCP 도구 계약 · `cd20225`
자세히: [docs/changes/2026-10-15-team-decisions.md](docs/changes/2026-10-15-team-decisions.md)

- 🆕 `notree/contracts/`: 모든 MCP 도구의 공통 응답 형식과 도구별 입출력 계약 (구현 20개, 계획 12개)
- 🆕 `docs/tool-contracts.md`: 계약에서 자동 생성되는 도구 문서. 계약을 고치면 `uv run python -m notree.dev.gen_tool_docs`
- 🆕 마감 시각: payload에 `deadline_at`을 넣으면 그 시각까지만 실행하고, 지나면 재시도하지 않음
- 🆕 작업 종류별 시도 한도 (`live_summary`는 1회)
- 🆕 실패 시 "다시 시도 가능 여부"(`user_retryable`) 저장, `job_retry`로 되살리기 (체크포인트에서 이어감)
- 🆕 scheduler 주기 작업: 멈춘 의도 다시 보내기(10초마다), 체크포인트 정리(1시간마다)
- 🆕 개발용 명령: `notree.dev.jobs get/retry`, `notree.dev.enqueue --user --deadline-in`
- 🔧 permit에 사용자 ID를 담음. MCP 서버가 꺼내 쓰고, 도구 인자로는 받지 않음
- 🔧 `docs/team-decisions.md`: 제안 문서 → 확정 문서
- ⚠️ 모든 MCP 도구 응답이 `{ok, data}` 형식으로 바뀜. 응답을 직접 읽던 코드는 `McpClient.tool()`로 바꿔야 함
- ⚠️ `job_create`에 `user_id` 필수, `job_fail`에 `user_retryable` 필수
- ⚠️ `job_start` 결과가 `data.status`(`started`/`done`/`failed`/`leased`/`unknown`)로 바뀜
- ⚠️ permit 형식이 바뀌어 이전 permit은 무효
- 테스트 18 → 31개

### notree/ 패키지: 팀 저장소로 옮길 worker 뼈대 · `e198153`
- 🆕 `notree/worker/`: 핸들러 규약(`@handler`, `RetryableError`/`FatalError`), 동시 실행 루프, lease, 재시도 한도, 종료 신호 처리
- 🆕 핸들러: `echo`(시험용), `course_update`(LangGraph), `live_summary`·`qa`·`grading`·`final_note`(스텁)
- 🆕 `notree/scheduler/allocator.py`, `notree/dev/enqueue.py`, `notree/dev/demo.py`
- 🆕 MCP 서버: permit 토큰, `job_fail`, `job_release`
- 🆕 `docs/worker-structure.md`, `docs/team-decisions.md`(제안)
- 🔧 MCP 클라이언트·체크포인트 저장기를 `notree/worker/`로 옮김. `mcp_saver.py`는 호환용으로만 남김
- 실행: `uv run python -m notree.dev.demo`, `uv run python -m notree.worker --stream rt --name A`

### uv로 전환 · `b5e8a47`
- 🆕 `pyproject.toml`, `uv.lock`
- 🗑 `requirements.txt`
- ⚠️ 설치·실행 명령 변경: `pip install -r requirements.txt` → `uv sync`, `python x.py` → `uv run x.py`

## 2026-10-14

### worker 루프: Redis Streams + 죽은 worker 작업 이어받기 · `d5ce84a`
- 🆕 `api.py` → `scheduler.py`(배정기) → Redis Stream → `worker.py` 흐름
- 🆕 lease·heartbeat, 다른 worker가 `XAUTOCLAIM`으로 이어받아 체크포인트부터 계속
- 🆕 `demo.py`(Redis 없이 한 번에), `docker-compose.yml`(Redis), `docs/worker-flow.md`
- 🆕 MCP 서버를 HTTP로도 실행 (`--http`)

### A안: 체크포인트·발행을 내부 MCP 도구로만 · `c661c5f`
- 🆕 `mcp_server.py`(DB를 혼자 소유), `mcp_saver.py`(LangGraph 저장기를 MCP 호출로 구현)
- 🔧 `run.py`가 기본으로 MCP 저장 방식 사용 (`--saver sqlite`는 비교용)
- 🆕 worker 프로세스의 DB 접속을 막아도 동작하는지 확인하는 테스트

### 첫 PoC: 과목 노트 업데이트 모양의 LangGraph · `42474ce`, `9c8e05a`
- 🆕 `graph.py`: snapshot → 강의별 extract 병렬 → merge → validate 분기 → publish
- 🆕 `run.py`: 중단·이어하기(`--crash-at`, `--resume`), 분기 확인(`--sloppy`), Mermaid 출력(`--graph`)
- 🆕 가짜 LLM 기본, `--llm openai`로 실제 호출
