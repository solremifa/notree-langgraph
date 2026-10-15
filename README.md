# notree-langgraph

Notree **과목 노트 업데이트**와 같은 모양의 LangGraph를 hello world 입출력으로 돌려 보는 PoC.
실제 노트 대신 단어를 씁니다.

```
snapshot ─┬─ extract(hello) ─┐
          └─ extract(world) ─┴─ merge ─ validate ─┬─ publish ─ END
                               ▲                  ├─ 문제 + 1차 → merge 재시도
                               └──────────────────┘
                                                  └─ 문제 + 2차 → needs_review ─ publish
```

## A안: worker는 DB에 붙지 않는다

기본 실행(`--saver mcp`)은 설계서 원칙대로 동작합니다.

```
run.py (worker)                                 mcp_server.py (내부 MCP 서버)
 ├ LangGraph 그래프                               └ out/notree.db  ← 이 프로세스만 연다
 └ McpCheckpointSaver ── MCP(stdio) 도구 호출 ──▶   checkpoint_put / checkpoint_put_writes
   McpNoteStore       ──────────────────────────▶   checkpoint_get / checkpoint_list
                                                    note_publish (멱등키)
```

- `mcp_saver.py`의 `McpCheckpointSaver`가 LangGraph의 `BaseCheckpointSaver`를 상속해
  `aget_tuple / alist / aput / aput_writes`를 전부 MCP 도구 호출로 구현합니다.
- 체크포인트는 LangGraph가 바이트로 직렬화한 것을 base64로 넘기고, 서버는 내용을 모른 채 blob으로 저장합니다.
  그래서 MCP 서버에는 LangGraph 의존성이 없습니다.
- 테스트 `test_worker_never_opens_db_in_mcp_mode`는 worker 프로세스의 `sqlite3.connect`를 막아 둔 채로
  그래프를 끝까지 돌려, worker가 DB를 직접 열지 않는다는 걸 확인합니다.
- `--saver sqlite`는 비교용으로, worker가 파일 DB에 직접 붙는 방식(B안 모양)입니다.

| 실제 설계 | 여기서 |
|---|---|
| 출처 버전 고정 + 편집 잠금 | `snapshot`: 강의 목록 고정 |
| 강의별 핵심 추출 (LLM) | `extract`: `Send`로 강의 수만큼 병렬, 결과는 reducer로 합침 |
| merge · validate | 단어를 한 줄로 합치고, 빠진 단어가 있으면 재시도 → 그래도 있으면 '확인 필요' |
| MCP `checkpoint.put/get` | `checkpoint_put`, `checkpoint_put_writes`, `checkpoint_get`, `checkpoint_list` |
| MCP `note.publish` + 발행 게이트 | `note_publish` (멱등키 = thread_id, 1회만 저장) |
| PG checkpoint (sync) | `durability="sync"` |
| compose 네트워크의 내부 MCP | stdio 하위 프로세스 (배포 시 streamable HTTP로 바뀌는 부분) |
| operation_id | `--thread` |

## notree/ — 팀 저장소로 옮길 worker 구조

평면 파일들은 흐름을 확인하려고 만든 1차 PoC이고, 실제로 옮길 뼈대는 `notree/` 패키지입니다.
핸들러 규약, 동시 실행, 재시도 한도, lease 상실 시 취소, 종료 신호 처리, permit 토큰까지 들어 있습니다.

- 구조와 코드 흐름: **[docs/worker-structure.md](docs/worker-structure.md)**
- 팀 결정 사항 (2026-10-15 확정): **[docs/team-decisions.md](docs/team-decisions.md)**
- MCP 도구 계약 (자동 생성): **[docs/tool-contracts.md](docs/tool-contracts.md)**
- 변경 정리: **[docs/changes/](docs/changes/)**

```powershell
uv run python -m notree.dev.demo                       # 성공·실패·재시도·한도 초과·미구현·LangGraph 이어하기·종료 처리
uv run python -m notree.worker --stream rt --name A    # 실제 worker 프로세스 (Redis·MCP 서버 필요)
```

## worker까지 붙인 전체 흐름

api → scheduler(배정기) → Redis Streams → worker → (죽으면) 다른 worker가 이어받기까지 붙어 있습니다.
흐름 설명은 **[docs/worker-flow.md](docs/worker-flow.md)** 에 따로 정리했습니다.

```powershell
uv run demo.py                    # Redis 없이 한 번에: worker-A가 extract 도중 죽고 worker-B가 체크포인트에서 이어감
uv run demo.py --die-at publish   # 발행 직후 죽음 → 중복 발행 없이 마무리
```

## 설치 (uv)

[uv](https://docs.astral.sh/uv/)로 관리합니다. 의존성은 `pyproject.toml`, 고정 버전은 `uv.lock`에 있습니다.

```powershell
# uv가 없으면 (Windows PowerShell)
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"

uv sync            # .venv 생성 + uv.lock 그대로 설치 (Python 3.11 이상)
```

가상환경을 직접 켤 필요 없이 `uv run <스크립트>`로 실행합니다.
의존성을 추가할 때는 `uv add <패키지>`(개발용은 `uv add --dev <패키지>`)를 쓰고 `uv.lock`도 함께 커밋합니다.

## 돌려 보기

```powershell
uv run run.py                                   # 정상 경로 (A안, MCP)
uv run run.py --sloppy once                     # validate 실패 → merge 재시도 분기
uv run run.py --sloppy always                   # 재시도 한도 → 확인 필요 분기
uv run run.py --lectures a,b,c,d                # extract 4개 병렬

# worker가 죽었다가 다른 worker가 이어받는 상황
uv run run.py --thread op1 --lectures hello,big,world --crash-at extract:world
uv run run.py --thread op1 --resume             # 끝난 extract 2개는 다시 안 부름 (LLM 호출 수로 확인)

# 발행 커밋 직후 죽는 상황
uv run run.py --thread op2 --crash-at publish
uv run run.py --thread op2 --resume             # "중복 발행 막음"

uv run run.py --thread op1 --history            # 체크포인트 이력 (MCP checkpoint_list)
uv run run.py --saver sqlite                    # 비교: worker가 DB 파일에 직접
uv run run.py --graph                           # Mermaid 그래프 → out/graph.mmd (mermaid.live 에 붙여 넣기)
uv run pytest -q                                # 31개: 그래프 9 + worker 인계 3 + notree 뼈대 6 + 팀 결정 13
```

출력의 `💾 checkpoint step=N 다음=[...]`이 단계(super-step)마다 상태를 저장하는 지점이고,
마지막의 `MCP 도구 호출: {...}`이 그 저장이 어떤 도구 호출로 이뤄졌는지 보여 줍니다.

```
▶ 시작 thread=op1  saver=mcp  lectures=hello,big,world  crash-at=extract:world
  extract  'hello' → 'HELLO'
  extract  'big' → 'BIG'
  💥 extract:world 에서 강제 중단
  MCP 도구 호출: {'checkpoint_get': 2, 'checkpoint_put': 3, 'checkpoint_put_writes': 5}

▶ 이어하기 thread=op1  saver=mcp  남은 노드=['extract']
  extract  'world' → 'WORLD'            ← hello, big 결과는 MCP에서 읽어 와 재사용
  ...
  LLM 호출 2회: ['extract:world', 'merge#1']
```

MCP 서버 도구를 직접 눌러 보려면 `uv run mcp dev mcp_server.py` (Node.js 필요) (MCP Inspector).

## 실제 OpenAI로

```powershell
$env:OPENAI_API_KEY="sk-..."
$env:NOTREE_MODEL="gpt-4o-mini"                  # 생략 시 gpt-4o-mini
uv run run.py --llm openai
```

실제 모델은 문장을 자유롭게 바꾸기 때문에 validate에서 걸려 재시도·'확인 필요' 분기가 자연스럽게 나올 수 있습니다.
`--sloppy`는 가짜 LLM에만 적용됩니다.

## 파일

- `state.py` — 그래프 상태 (`extracts`는 `operator.add` reducer)
- `graph.py` — 노드, 분기, `build_graph()`, 파일 발행 저장소
- `mcp_server.py` — 내부 MCP 서버 (DB 소유자). 체크포인트·발행·작업(job)/lease 도구. `--http`로 HTTP 실행
- `mcp_saver.py` — `McpClient`(stdio/HTTP), `McpCheckpointSaver`(A안), `McpNoteStore`
- `api.py` · `scheduler.py` · `worker.py` — 요청 → 배정기 → worker 루프
- `demo.py` — 위 전부를 한 번에 돌리는 데모 · `settings.py` — 공통 설정
- `docs/worker-flow.md` — worker 실행 흐름 설명
- `llm.py` — FakeLLM / OpenAI 교체 지점, 호출 기록 `CALLS`
- `run.py` — CLI, 스트리밍 출력, 이어하기
- `tests/test_flow.py` · `tests/test_worker.py`

## A안 검증에서 알게 된 것

- **된다.** 저장기 메서드 4개(`aget_tuple / alist / aput / aput_writes`)를 MCP로 구현하는 것으로 충분했고,
  병렬 노드의 부분 결과(pending writes)까지 MCP를 거쳐 저장·복원된다.
- **MCP 클라이언트가 비동기라 그래프도 `ainvoke/astream`으로 돌려야 한다.** 동기 `invoke`는 막아 두었다.
  worker가 원래 asyncio 기반이면 문제없다.
- **호출 수**: 단계마다 `checkpoint_put` 1회, 노드마다 `checkpoint_put_writes` 1회가 나간다.
  재시도 없는 정상 경로에서 체크포인트 관련 호출은 강의 N개일 때 `N + 13`회(강의 1·2·5개에서 14·15·18회로 측정).
  실제 배포(HTTP)에서는 왕복 지연을 측정해 봐야 한다.
- **서버는 LangGraph를 몰라도 된다.** 불투명 blob 저장이라 LangGraph 버전이 바뀌어도 서버 스키마는 그대로다.

## 아직 안 한 것

- 서명된 permit 토큰 검사, 세대(generation) 비교로 삭제된 출처를 막는 발행 게이트
- PostgreSQL 저장 (지금은 MCP 서버 뒤의 SQLite) · streamable HTTP 전송
- 끝난 thread 체크포인트 정리 (`checkpoint_delete_thread`는 있음, 주기 실행은 없음)
