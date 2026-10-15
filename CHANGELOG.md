# 업데이트 목록

무엇이 바뀌었는지 최신 순으로 적습니다. 항목마다 **실행 방법이나 코드 사용법이 바뀌는지**를 같이 표시합니다.
큰 변경의 자세한 설명은 `docs/changes/`에 따로 둡니다.

표시
- 🆕 추가 · 🔧 변경 · 🗑 삭제 · 🐛 수정
- ⚠️ **깨지는 변경**: 기존 코드·명령을 고쳐야 함

---

## 2026-10-15

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
