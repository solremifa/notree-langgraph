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

## 설치 (Windows PowerShell 기준)

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## 돌려 보기

```powershell
python run.py                                   # 정상 경로 (A안, MCP)
python run.py --sloppy once                     # validate 실패 → merge 재시도 분기
python run.py --sloppy always                   # 재시도 한도 → 확인 필요 분기
python run.py --lectures a,b,c,d                # extract 4개 병렬

# worker가 죽었다가 다른 worker가 이어받는 상황
python run.py --thread op1 --lectures hello,big,world --crash-at extract:world
python run.py --thread op1 --resume             # 끝난 extract 2개는 다시 안 부름 (LLM 호출 수로 확인)

# 발행 커밋 직후 죽는 상황
python run.py --thread op2 --crash-at publish
python run.py --thread op2 --resume             # "중복 발행 막음"

python run.py --thread op1 --history            # 체크포인트 이력 (MCP checkpoint_list)
python run.py --saver sqlite                    # 비교: worker가 DB 파일에 직접
python run.py --graph                           # Mermaid 그래프 → out/graph.mmd (mermaid.live 에 붙여 넣기)
pytest -q                                       # 9개: 시나리오 4개 × (memory, mcp) + DB 차단 테스트
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

MCP 서버 도구를 직접 눌러 보려면 `mcp dev mcp_server.py` (MCP Inspector).

## 실제 OpenAI로

```powershell
$env:OPENAI_API_KEY="sk-..."
$env:NOTREE_MODEL="gpt-4o-mini"                  # 생략 시 gpt-4o-mini
python run.py --llm openai
```

실제 모델은 문장을 자유롭게 바꾸기 때문에 validate에서 걸려 재시도·'확인 필요' 분기가 자연스럽게 나올 수 있습니다.
`--sloppy`는 가짜 LLM에만 적용됩니다.

## 파일

- `state.py` — 그래프 상태 (`extracts`는 `operator.add` reducer)
- `graph.py` — 노드, 분기, `build_graph()`, 파일 발행 저장소
- `mcp_server.py` — 내부 MCP 서버 (DB 소유자)
- `mcp_saver.py` — `McpClient`, `McpCheckpointSaver`(A안), `McpNoteStore`
- `llm.py` — FakeLLM / OpenAI 교체 지점, 호출 기록 `CALLS`
- `run.py` — CLI, 스트리밍 출력, 이어하기
- `tests/test_flow.py`

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
- 편집 lease 60초 · heartbeat 20초, Redis 재배송(XAUTOCLAIM)으로 다른 worker가 이어받기
- PostgreSQL 저장 (지금은 MCP 서버 뒤의 SQLite) · streamable HTTP 전송
- 끝난 thread 체크포인트 정리 (`checkpoint_delete_thread`는 있음, 주기 실행은 없음)
