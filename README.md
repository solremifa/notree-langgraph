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

**실행 상태 판단은 기본 `NOTREE_LIVENESS=redis`** (2026-10-19부터): Redis 소유 키 선점 → DB 조건부 승인 → Redis Lua heartbeat.
DB heartbeat는 보내지 않습니다. `NOTREE_LIVENESS=db`는 명시적으로 고르는 비교·호환 모드이고, Redis 장애 때 자동으로 바뀌지 않습니다.
절차: [docs/changes/2026-10-19-start-protocol.md](docs/changes/2026-10-19-start-protocol.md)

기본(redis) 모드는 Lua를 돌릴 수 있는 **실제 Redis**가 필요합니다 (fakeredis는 안 됨). Redis 없이 데모만 보려면 DB 모드를 명시하세요.

```powershell
$env:NOTREE_LIVENESS="db"; uv run python -m notree.dev.demo      # Redis 없이 (fakeredis, DB 모드)
uv run python -m notree.dev.demo --redis redis://localhost:6379/0  # 기본 redis 모드
uv run python -m notree.dev.takeover --redis redis://localhost:6379/0   # worker-A가 작업 중 죽고 worker-B가 체크포인트에서 이어감
uv run python -m notree.dev.graph_run    # 그래프만 따로 (--crash-at, --resume, --sloppy, --graph, --help)
uv run python -m notree.dev.qa_run       # Q&A 근거 탐색 그래프 (feat/qa-graph 실험)
```

시험: worker 시험은 기본(redis) 모드에서 **실제 Redis**로 돕니다. PATH에 `redis-server`가 있으면 시험이 띄우고, 없으면(Windows 등)
**시험 전용** Redis를 `NOTREE_TEST_REDIS_URL`로 주세요 — 시험이 FLUSHALL을 하므로 개발·운영 Redis를 주면 안 됩니다.
둘 다 없으면 Redis가 필요한 시험은 건너뜁니다 (`-rs`로 확인).

```powershell
docker run -d --name notree-test-redis -p 127.0.0.1:16399:6379 redis:7-alpine redis-server --save "" --appendonly no
$env:NOTREE_TEST_REDIS_URL="redis://127.0.0.1:16399/0"
uv run pytest -q -rs                              # 기본(redis) 모드
$env:NOTREE_LIVENESS="db"; uv run pytest -q -rs   # DB 모드 (비교·호환)
```

`redis-server` 프로세스를 직접 띄워야 하는 시험 3개(스냅숏 복원·스캔 중 재시작·compose 설정 확인)는 `NOTREE_TEST_REDIS_URL`로는 돌지 않습니다.

프록시 환경에서 시험이 HTTP 클라이언트 생성 단계에서 실패하면 (2026-10-18 리뷰 환경):
- SOCKS 프록시를 쓰는 환경: `uv pip install socksio==1.0.0` (모든 개발자에게 필수는 아님)
- 시험용 로컬 MCP 서버에는 직접 붙도록: `NO_PROXY=127.0.0.1,localhost`, `no_proxy=127.0.0.1,localhost`

진짜 Redis와 프로세스 분리로 (`docker compose up -d` 후 터미널 여러 개):

```powershell
uv run python -m notree.mcp_server --http
uv run python -m notree.scheduler.allocator
uv run python -m notree.worker --stream qa --name A      # Q&A·채점
uv run python -m notree.worker --stream rt --name A      # 실시간 요약
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
├─ graphs/         LangGraph 그래프 (course_update, qa)
├─ common/         설정, 로그
└─ dev/            데모·개발 도구
docs/              설계·결정·계약 문서
tests/
```

## 문서

| 문서 | 내용 |
|---|---|
| [CHANGELOG.md](CHANGELOG.md) | 업데이트 목록 (무엇이 바뀌었는지, 고쳐야 할 것) |
| [docs/status.md](docs/status.md) | 진행 상황: 구현·테스트된 것, 뼈대만 있는 것, 개발 순서 |
| [docs/worker-structure.md](docs/worker-structure.md) | 코드 구조, 핸들러 규약, 새 핸들러 추가 방법 |
| [docs/worker-flow.md](docs/worker-flow.md) | 작업 하나가 흘러가는 순서, 죽거나 느릴 때 |
| [docs/team-decisions.md](docs/team-decisions.md) | 팀 결정 사항 (2026-10-15 확정, 10-17 추가) |
| [docs/measurements/](docs/measurements/) | 측정·실행 기록 원본과 재현 방법 |
| [docs/tool-contracts.md](docs/tool-contracts.md) | MCP 도구 계약 (자동 생성) |
| [docs/changes/](docs/changes/) | 큰 변경의 자세한 설명 |

정리 전의 1차 PoC(루트의 `worker.py`, `demo.py`, `run.py` 등)는 `archive/poc-v1` 브랜치에 남아 있습니다.
