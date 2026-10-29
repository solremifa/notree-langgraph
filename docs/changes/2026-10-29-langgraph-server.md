# 실험: Q&A 그래프를 LangGraph Agent Server로 띄우기 (1단계)

브랜치 `exp/langgraph-server`. **main에 넣지 않는 실험이다.** main의 worker · scheduler · MCP 경로는 바뀌지 않는다.

## 왜 하나

LangGraph에는 그래프를 서버로 띄우는 Agent Server(예전 이름 LangGraph Server)가 있다. API 서버와 queue worker로 나뉘고,
run 대기열 · lease · 체크포인트 저장 · SSE 스트리밍을 제공해서 우리가 직접 만든 worker 실행기와 겹치는 부분이 많다.
"worker를 LangGraph 서버로 구현할 수 있나"를 판단하기 전에, 먼저 지금 그래프가 서버 위에서 그대로 도는지 본다.

## 이번에 한 것

| 파일 | 내용 |
|---|---|
| `notree/lgserver/qa_app.py` | 서버가 run마다 부르는 팩토리 `make_qa_graph(config)`. 그래프(`notree/graphs/qa`)는 그대로 |
| `notree/lgserver/langgraph.json` | 서버 설정: 그래프 `qa` 등록 |
| `notree/dev/lgserver_run.py` | HTTP API로 thread · run을 만들고 노드별 결과를 받아 찍는 클라이언트 (`qa_run.py`와 같은 출력) |
| `tests/test_lgserver_app.py` | 서버 없이 팩토리만 시험 |
| `pyproject.toml` | 의존성 그룹 `lgserver`에 `langgraph-cli[inmem]` (기본 설치에는 안 들어감. langgraph는 1.2.15 그대로) |

### 팩토리 함수가 필요한 이유

worker handler는 LLM 객체와 검색기 객체(`McpRetriever`)를 `config["configurable"]`에 넣어 그래프에 넘긴다.
서버에서는 run 요청이 HTTP JSON이라 **객체를 넘길 수 없다.** 그래서

1. run 요청에는 JSON 값만 보낸다: `llm_kind`(fake | openai), `bad_citation`, `deadline_sec`
2. 서버가 run마다 `make_qa_graph(config)`를 부르면, 그 값을 보고 객체를 만들어 `graph.with_config(configurable=...)`로 붙인다
3. 그래프는 지금처럼 `llm` · `retriever` · `deadline_at`을 읽는다

키 이름을 그래프가 읽는 이름과 다르게 둔 것은, run 요청의 configurable이 그래프 기본 config 위에 덮이기 때문이다.
같은 이름(`llm: "fake"`)으로 보내면 문자열이 객체를 덮어써 그래프가 깨진다 (시험 `test_run_config가_객체를_덮어쓰지_않는다`).

검색은 아직 가짜 말뭉치(`sample_corpus`)다. 실제 MCP 검색은 2단계.

## 실행 방법

```powershell
uv sync --group lgserver

# 터미널 1: 서버 (메모리 안에서 도는 개발용. DB · Redis 불필요)
uv run --group lgserver langgraph dev --config notree/lgserver/langgraph.json --no-browser

# 터미널 2: 실행
uv run python -m notree.dev.lgserver_run
uv run python -m notree.dev.lgserver_run "스택은 어떤 구조야?" --bad-citation --history
uv run python -m notree.dev.lgserver_run "힙 정렬의 시간 복잡도는?" --deadline-sec 15
```

- **Windows**: 먼저 `$env:PYTHONUTF8 = "1"`. 안 하면 서버가 시작하다 `UnicodeDecodeError: 'cp949' codec can't decode ...`로 죽는다 (langgraph_api가 자기 파일을 인코딩 지정 없이 열어 Windows 기본 cp949로 읽음). 영구 적용은 `[Environment]::SetEnvironmentVariable("PYTHONUTF8", "1", "User")` 뒤 PowerShell 새로 열기.
- 서버가 뜨면 `API: http://127.0.0.1:2024`가 찍힌다. 2024가 이미 쓰이면 다른 포트로 뜨니 찍힌 주소를 `--url`로 준다.
- 프록시 환경에서는 `localhost` 요청이 프록시로 가서 끊길 수 있다. `127.0.0.1`을 쓰거나 `NO_PROXY=127.0.0.1,localhost`.
- Studio에서는 질문마다 새 thread(+ 버튼)로 실행할 것 (아래 '아직 안 맞는 것' 5).
- 서버 로그의 `Studio UI` 주소를 브라우저로 열면 그래프 그림과 노드별 State를 화면에서 볼 수 있다 (LangSmith 로그인이 필요할 수 있음).
- 서버 · 클라이언트 모두 OpenAI 없이 가짜 LLM이 기본이다. `--llm openai`는 서버 쪽에 `OPENAI_API_KEY`가 있어야 한다.

## 확인한 것 (2026-10-29, langgraph 1.2.15 · langgraph-api 0.16.9 · in-memory 서버)

`qa_run.py`와 같은 질문 · 같은 결과가 서버를 거쳐 나왔다.

| 질문 | 지난 노드 | 결과 |
|---|---|---|
| 스택은 어떤 구조야? | understand → search_note → answer → verify → save | 노트 n1 인용 |
| 스택 오버플로는 왜 생겨? | … → search_note → search_transcript → answer … | 전사 t1 인용 |
| 힙 정렬의 시간 복잡도는? | … → search_transcript → search_web → answer … | 웹 w1 인용 |
| 오늘 점심 뭐 먹지? | understand → answer → verify → save | 일반 지식 |
| 스택 · `--bad-citation` | … answer → verify(틀린 인용) → answer → verify → save | 두 번째 답 통과 |
| 힙 정렬 · `--deadline-sec 15` | … search_transcript → skip_web → answer … | 웹 건너뜀, 근거 부족 |

- `--history`: 서버가 thread에 슈퍼스텝마다 체크포인트를 남겼다 (step -1 ~ 7, `다음 노드` 포함). 체크포인터를 그래프에 따로 붙이지 않아도 서버가 붙인다.
- run을 만든 뒤 첫 노드 결과가 오기까지 약 0.9~1.0초 걸렸다. 개발용 in-memory 서버의 대기열 처리 방식 때문으로 보이며, 운영 구성(Postgres 대기열)에서 측정한 값은 아니다.

## 아직 안 맞는 것 (2단계 이후에 볼 것)

1. **DB 접근 규칙**: Agent Server worker는 run · 체크포인트를 자기 Postgres에 직접 쓴다. "worker에는 DB 접속 정보가 없다"는 지금 규칙과 다르다. 업무 데이터는 계속 MCP로 접근한다.
2. **outbox**: run 생성은 HTTP 요청이라 업무 DB 트랜잭션(질문 저장 + 작업 등록)과 묶이지 않는다. 작업 원장을 두고 scheduler가 XADD 대신 run을 만드는 식이 필요하다.
3. **실행 세대(permit)**: 서버의 lease와 우리 DB의 `exec_epoch`가 따로 있다. 늦게 끝난 이전 실행 결과를 막는 장치는 업무 서비스에 남아야 한다.
4. **레인 · 마감 · 재시도**: rt · qa · batch 분리, Q&A 90초 마감과 최대 3회 시도를 서버에서 어떻게 거는지 확인 필요.
5. **thread에 State가 이어 붙는다 (Studio에서 확인, 2026-10-29)**: 같은 thread에서 질문을 이어 하면 이전 State가 남는다.
   `llm_calls`가 질문마다 0으로 돌아가지 않아 상한(4)을 넘으면 시간이 남아도 "시간 초과"로 끝나고,
   누적 칸(`hits` · `searched`)에 이전 근거가 남아 전사 · 웹 검색을 건너뛴다 (같은 thread 4번째 질문에서 재현).
   Q&A 그래프는 "실행 한 번 = 질문 하나" 전제라서, 서버에서는 **질문마다 새 thread**로 돌리고 대화 기록은 지금처럼 업무 DB(`qa_context`)에서 읽는 쪽이 맞다.
   대화를 thread 하나로 이어가려면 첫 노드에서 질문 단위 칸을 비우도록 그래프를 고쳐야 한다. `lgserver_run.py`는 질문마다 새 thread를 만든다.
6. **라이선스 · 외부 통신**: 운영용 self-hosted 문서는 `LANGGRAPH_CLOUD_LICENSE_KEY`와 LangSmith 키, `beacon.langchain.com` 통신을 요구한다. 무료로 어디까지 되는지 확인 필요. 이번 실험은 개발용 `langgraph dev`만 썼다.

## 2단계: worker 자리를 서버 그래프로 (2026-10-29)

`qa`(1단계)는 그래프만 서버에 올린 것이다. `qa_job`은 **worker 실행기가 하던 일**까지 노드로 옮겼다.

```
START → start_job ─(started)→ run_handler → settle → END
                   └(done · failed · leased · unknown)→ END
```

| 노드 | 하는 일 | 지금 worker의 어디 |
|---|---|---|
| `start_job` | 업무 서비스 `job_start` → permit · 실행 세대 발급 | `loop.py` `_process` 앞부분 |
| `run_handler` | 지금 qa handler를 그대로 부름 (`qa_context` → Q&A 그래프 → `qa_complete`), 실행 내내 heartbeat, 마감 지킴 | `Lease` + `wait_for(handler)` |
| `settle` | 성공 `job_finish` / 영구 실패 `job_fail` / 일시 실패 `job_release` | `_confirm` · `_fail` · `_ctl` |

- 파일: `notree/lgserver/qa_job.py`(그래프), `notree/dev/lgserver_ask.py`(질문 등록 → run), `tests/test_lgserver_job.py`(시험 4개)
- run 입력은 `{"job_id"}` 하나. 질문 저장 + 작업 등록은 지금처럼 업무 서비스 `qa_ask`가 한 트랜잭션으로 한다 → outbox는 그대로. 이 그래프는 "scheduler가 XADD 대신 서버에 run을 만든다"면 들어갈 자리다.
- permit · 실행 세대 · 멱등 저장은 전부 업무 서비스가 그대로 판단한다. 서버 lease와 별개.

### 실행

```powershell
uv run python -m notree.mcp_server --http                                      # 터미널 1: 업무 서비스
$env:PYTHONUTF8 = "1"
uv run --group lgserver langgraph dev --config notree/lgserver/langgraph.json  # 터미널 2: Agent Server
uv run python -m notree.dev.lgserver_ask "스택 오버플로는 왜 생겨?"              # 터미널 3
uv run python -m notree.dev.lgserver_ask "힙 정렬의 시간 복잡도는?" --again
uv run python -m notree.dev.lgserver_ask "스택은 어떤 구조야?" --delete-first
```

Redis · 배정기 · worker는 띄우지 않는다.

### 확인한 것

| 시나리오 | 노드 | 작업 상태 |
|---|---|---|
| 정상 질문 | start_job(started) → run_handler(ok) → settle(finished) | done, 답 저장 (전사 t1 / 웹 w1) |
| 같은 작업으로 run 한 번 더 | start_job(**done**) → 끝 | done 그대로, 다시 실행 안 함 |
| 다른 실행이 lease를 쥠 (시험) | start_job(**leased**) → 끝 | running 그대로 |
| 질문 삭제 뒤 실행 | start_job → run_handler(fatal: not_found) → settle(failed) | failed, 다시 시도 불가 |

### 발견: config에 넣은 객체가 서버 체크포인트와 충돌한다

처음에는 정상 질문도 Q&A 그래프가 끝까지 돈 뒤 `TypeError: cannot pickle 'async_generator' object`로 실패했다 (작업은 released).

- `run_handler` 안에서 Q&A 그래프를 부르면 LangGraph가 그것을 **이 run의 하위 그래프**로 묶고, 서버 체크포인터를 물려준다.
- 서버 저장소가 handler가 `config["configurable"]`에 넣은 `FakeQALLM`(msgpack 불가) · `McpRetriever`(MCP 연결, pickle 불가)까지 저장하려다 실패.
- 로컬 `InMemorySaver`로는 재현되지 않았다. 서버 런타임(langgraph-api 0.16.9)에서만.

이번에는 안쪽 Q&A 그래프를 빈 `contextvars`로 따로 실행해 하위 그래프로 묶이지 않게 했다. 대신 Studio · 스트림에 안쪽 노드(understand 등)가 보이지 않는다.

**근본 해결 후보**: LangGraph 1.x의 **Runtime context**(`StateGraph(..., context_schema=...)`, `ainvoke(..., context=...)`, 노드에서 `runtime.context`). 실행 중 의존 객체를 넘기는 용도이고 체크포인트에 저장되지 않는다. Q&A 그래프가 LLM · 검색기를 config 대신 context에서 읽게 바꾸면, 하위 그래프로 묶어도 안쪽 노드를 볼 수 있다. main 그래프 코드를 고치는 일이라 이번 실험에서는 하지 않았다.

## 다음 단계

- ~~2단계~~: 위 '2단계'로 했다 (permit은 그래프의 첫 노드가 `job_start`로 받는다).
- Q&A 그래프의 의존 객체를 Runtime context로 옮기기 (위 '발견'). 하면 안쪽 노드를 Studio에서 볼 수 있다.
- 재시도: 일시 실패(`job_release`) 뒤 다시 run을 만드는 주체가 없다. scheduler가 run을 만들게 할지, 서버 쪽 재시도를 쓸지.
- 3단계: Docker로 API와 queue worker를 나눠 띄우고 (`N_JOBS_PER_WORKER=0` / `python -m langgraph_api.queue_entrypoint`), worker 강제 종료 · 중복 실행 때 동작을 지금 worker의 장애 시험과 같은 시나리오로 비교한다.
