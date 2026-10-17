# 2026-10-17 변경 정리: Q&A 검색을 PG로, 전사 우선 검색 단계 (`feat/qa-pg-search`)

측정: [measurements/2026-10-17-pg-search](../measurements/2026-10-17-pg-search/README.md). 실제 강의 7개·질문 33개(Claude 초안)에서 얻은 **잠정** 설정입니다.

## PG 검색 어댑터 (`notree/mcp_server/pg_search.py`)

- `NOTREE_QA_SEARCH=pg`(기본 `sqlite`)면 `qa_search`가 PostgreSQL `qa_search_docs`를 읽고, `qa_seed`가 노트·전사를 거기에도 넣습니다. 접속은 `NOTREE_PG_QA_DSN`.
- 점수 kw_mean = 검색어별 `word_similarity(검색어, 본문)` 평균. 범위 = 사용자 + 과목 + 출처 + (선택) 강의 목록, `deleted_at`이 있는 문서 제외.
- 사용자·과목 확인과 삭제된 질문은 기존처럼 질문 행(SQLite)으로 먼저 거릅니다. 출처 ID·강의 ID를 그대로 돌려줍니다.
- PG 연결 실패는 `unavailable`(다시 시도 가능) → 그래프가 1번 다시, 안 되면 다음 출처.
- DB는 "범위 안 문서를 점수와 함께 읽기"만 합니다. 어느 강의를 먼저, 언제 넓힐지는 그래프가 정합니다 (DB는 CRUD만).
- 범위 안 문서마다 점수를 계산합니다(1,100개에서 질문당 약 40~100ms). 큰 규모에서는 trgm 인덱스로 후보를 먼저 줄이고, 정답이 잘리지 않는지 지금 방식과 비교해야 합니다.

## 그래프: 전사 두 단계 (`notree/graphs/qa/graph.py`)

| 단계 | 하는 일 |
|---|---|
| `search_transcript` | 노트 결과의 강의별 최고 점수로 `PRIORITY_LECTURES`(2)개 강의를 골라 그 전사만. 노트 결과에 강의 정보가 없으면 과목 전체 (예전과 같음) |
| `search_transcript_all` (새로) | 과목 전체 전사 |

| 판단 | 처리 |
|---|---|
| 우선 강의 전사 최고 점수 < `EXPAND_MIN`(0.3) | 답하기 전에 바로 과목 전체 |
| 0.3 이상(충분 0.6 미만) | 웹으로 가지 않고 먼저 답함. LLM이 "근거 더 필요"면 남은 시간·LLM 호출 예산 안에서 과목 전체 → 웹 |
| 최종 답변 가능 여부 | 점수가 아니라 LLM이 판단 (정확성 보장은 아님) |

- 근거 선택(`evidence`)은 점수순 유지 + **같은 ID는 한 번만**(단계마다 다시 찾은 경우). 검색 단계의 순서는 근거 선택에서 유지되지 않으며, 이 조합을 `qa_pg_eval`로 함께 잽니다.
- 근거에 `lecture_id`가 붙습니다 (`Hit`, `QaHit`). `Retriever.search`에 선택 인자 `lecture_ids` — 그래프는 범위가 있을 때만 넘깁니다.

## 평가 도구

| 도구 | 보는 것 |
|---|---|
| `notree.dev.lecture_import` | 강의 노트 내보내기(.md) → 노트·전사 문서 |
| `notree.dev.pgsearch_eval` | 검색 결과 순위 (점수 방식 × 범위 × 확장 방식), 순위와 무관한 정답 점수 |
| `notree.dev.qa_keywords` | 질문의 LLM 검색어 (PG 없이) |
| `notree.dev.qa_pg_eval` | 그래프 + PG 검색에서 LLM에 실제 전달된 근거의 정답 포함률 |

## 깨지는 변경

| 무엇 | 이전 | 이후 |
|---|---|---|
| 강의 정보가 있는 근거로 전사를 찾을 때 | 과목 전체, 부족하면 웹 | 우선 강의 2개 → (0.3 미만) 과목 전체 / (0.3 이상) 먼저 답하고 LLM 요청 시 과목 전체 → 웹 |
| `searched` 값 | note / transcript / web | `transcript_all` 추가 |
| `Retriever.search` | `(source, keywords, limit)` | `lecture_ids` 선택 인자 추가 (기존 구현은 그대로 동작) |
| 의존성 | — | `psycopg[binary]` |

## 남은 것

- 팀원이 쓴 질문으로 기준값(0.3·0.6·강의 2개) 검증. 지금 질문은 본문 단어를 빌려 써서 쉬울 수 있음
- 노트에서 멈출 때 전달 근거가 3개뿐 — 노트 검색 상한·근거 자리 정책 검토
- 검색어 문제(ml13: "예측값" vs 강의의 "h", "로그 0") — 검색어 확장·동의어
- trgm 인덱스 후보 축소와 정답 손실 비교, 실제 규모 지연
- 질문·과목 행까지 PG로 (지금은 SQLite 작업 DB)
