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

- 서버가 뜨면 `API: http://127.0.0.1:2024`가 찍힌다. 2024가 이미 쓰이면 다른 포트로 뜨니 찍힌 주소를 `--url`로 준다.
- 프록시 환경에서는 `localhost` 요청이 프록시로 가서 끊길 수 있다. `127.0.0.1`을 쓰거나 `NO_PROXY=127.0.0.1,localhost`.
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
5. **라이선스 · 외부 통신**: 운영용 self-hosted 문서는 `LANGGRAPH_CLOUD_LICENSE_KEY`와 LangSmith 키, `beacon.langchain.com` 통신을 요구한다. 무료로 어디까지 되는지 확인 필요. 이번 실험은 개발용 `langgraph dev`만 썼다.

## 다음 단계

- 2단계: 그래프가 `question_id`만 받아 MCP(`qa_context` · `qa_search` · `qa_complete`)로 질문 · 근거 · 저장을 하도록 팩토리 안에서 MCP 클라이언트와 `McpRetriever`를 만든다. permit은 어디서 받을지 정해야 한다.
- 3단계: Docker로 API와 queue worker를 나눠 띄우고 (`N_JOBS_PER_WORKER=0` / `python -m langgraph_api.queue_entrypoint`), worker 강제 종료 · 중복 실행 때 동작을 지금 worker의 장애 시험과 같은 시나리오로 비교한다.
