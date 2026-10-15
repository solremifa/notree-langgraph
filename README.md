# notree-langgraph

Notree의 worker 구조를 미리 만들어 보는 PoC입니다.
**api → scheduler → Redis Streams → worker** 흐름, LangGraph로 만든 과목 노트 업데이트, 그리고
"worker는 DB에 직접 붙지 않고 내부 MCP 도구로만 접근한다"(A안)를 실제로 돌려 확인합니다.
과목 노트 그래프는 아직 hello world 모양입니다 (강의 내용 대신 단어를 씀).

## 설치

[uv](https://docs.astral.sh/uv/)를 씁니다. 의존성은 `pyproject.toml`, 고정 버전은 `uv.lock`.

```powershell
# uv가 없으면 (Windows PowerShell). 설치 후 터미널을 새로 열어야 uv 명령이 잡힘
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"

uv sync            # Python 3.11 이상
```

## 돌려 보기

Redis 없이 바로 됩니다 (fakeredis).

```powershell
uv run python -m notree.dev.demo         # 성공·실패·재시도·한도 초과·마감 초과·LangGraph 이어하기·누락 복구·되살리기·종료 처리
uv run python -m notree.dev.takeover     # worker-A가 작업 중 죽고 worker-B가 체크포인트에서 이어감
uv run python -m notree.dev.graph_run    # 그래프만 따로 (--crash-at, --resume, --sloppy, --graph, --help)
uv run pytest -q                         # 37개
```

진짜 Redis와 프로세스 분리로 (`docker compose up -d` 후 터미널 여러 개):

```powershell
uv run python -m notree.mcp_server --http
uv run python -m notree.scheduler.allocator
uv run python -m notree.worker --stream rt --name A
uv run python -m notree.worker --stream batch --name A
uv run python -m notree.dev.enqueue course_update sources=hello,big,world
uv run python -m notree.dev.jobs get <job_id>
```

실제 OpenAI로: `$env:OPENAI_API_KEY="sk-..."` 후 `enqueue ... llm=openai` 또는 `graph_run --llm openai`
(모델은 `NOTREE_MODEL`, 기본 gpt-4o-mini).

## 구조

```
notree/
├─ contracts/      MCP 도구 계약 (공통 응답 형식, 도구별 입출력) — 서버와 worker가 같이 씀
├─ mcp_server/     내부 MCP 서버 (DB를 소유하는 유일한 프로세스)
├─ scheduler/      배정기 + 주기 작업 (누락 복구, 체크포인트 정리)
├─ worker/         worker 루프, lease, 핸들러 규약, MCP 클라이언트, LangGraph 체크포인트 저장기
│   └─ handlers/   작업 종류별 핸들러 (course_update 동작, 나머지는 스텁)
├─ graphs/         LangGraph 그래프 (course_update)
├─ common/         설정, 로그
└─ dev/            데모·개발 도구
docs/              설계·결정·계약 문서
tests/
```

## 문서

| 문서 | 내용 |
|---|---|
| [CHANGELOG.md](CHANGELOG.md) | 업데이트 목록 (무엇이 바뀌었는지, 고쳐야 할 것) |
| [docs/worker-structure.md](docs/worker-structure.md) | 코드 구조, 핸들러 규약, 새 핸들러 추가 방법 |
| [docs/worker-flow.md](docs/worker-flow.md) | 작업 하나가 흘러가는 순서, 죽거나 느릴 때 |
| [docs/team-decisions.md](docs/team-decisions.md) | 팀 결정 사항 (2026-10-15 확정) |
| [docs/tool-contracts.md](docs/tool-contracts.md) | MCP 도구 계약 (자동 생성) |
| [docs/changes/](docs/changes/) | 큰 변경의 자세한 설명 |

정리 전의 1차 PoC(루트의 `worker.py`, `demo.py`, `run.py` 등)는 `archive/poc-v1` 브랜치에 남아 있습니다.
