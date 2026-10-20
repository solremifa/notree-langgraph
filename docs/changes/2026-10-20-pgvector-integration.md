# pgvector 통합 — 채택 확정 · 구현 계획 · 검증 항목 (2026-10-20)

근거: [측정 기록 2026-10-19](../measurements/2026-10-19-vector-compare/README.md) · 아래 pgvector 점검.
**이 문서는 결정과 구현 계획이며 코드는 아직 바꾸지 않았다.**

## 채택 결정 (2026-10-20)

- **PostgreSQL + pgvector를 Q&A 근거 검색의 중심으로 채택한다.** (같은 날 앞선 판의 "기본 검색 변경은 답변 품질 평가 후"를 대체)
- pg_trgm은 **미색인 · 최신 문서 보완**과 **장애 대체 경로**로 남긴다
- 처음에는 **정확 검색**. HNSW는 데이터 증가 · 부하 측정 뒤 고르는 최적화
- **RRF는 기본 적용하지 않는다** (동일 비중 RRF는 새 30에서 벡터 단독보다 낮았음)
- 남은 평가는 채택 여부가 아니라 **통합 정합성과 검색 · 답변 품질 검증**이 목적이다

## 바뀌는 범위

worker 구조(Redis Streams · 실행권 · 재시도 · ACK)와 대화 메모리는 그대로 쓴다. 바뀌는 곳은 Q&A 근거 검색, 문서 색인 경로, 그래프의 검색 분기다.
Redis는 벡터를 저장하지 않는다. 별도 벡터 DB 서버를 두지 않는다.

| 부분 | 변경 |
|---|---|
| PostgreSQL | 검색 문서에 문서 버전, 문서 버전별 임베딩 테이블 |
| 문서 저장 경로 | 문서 생성 · 수정과 같은 트랜잭션에 색인 의도 → outbox → batch |
| `qa_search` | pgvector 검색 추가, pg_trgm은 보완 · 대체 |
| Q&A 그래프 | 검색 방식별 설정 분리, 벡터 경로의 분기 조건 새로 정함 |
| 권한 · 삭제 | 검색 시 사용자 · 과목 · 삭제 상태 · 문서 버전을 서버가 확인 |

## 결정 (앞선 판에서 이어짐)

**D1. 최신 문서 공백** — 현재 버전 임베딩이 있는 문서는 벡터로, 없거나 본문보다 오래된 문서는 pg_trgm으로 찾는다. 두 결과에 **후보 자리를 각각 배정**하고 중복을 뺀다. 뒤에 붙인 뒤 상위 k개로 자르면 최신 문서가 다시 탈락하므로 모델에 전달하는 자리도 확보한다. 질문 표현이 최신 문서에 없으면 trgm도 못 찾으므로 **최근 전사 구간을 시간 기준으로 가져오는 후보**를 따로 평가한다.

**D2. 임베딩 작업과 batch** — 조각마다 작업을 만들지 않고 강의별로 묶는다. 묶음 하나의 문서 수 · 토큰 수 제한, 작은 묶음마다 슬롯 반환, 최종 노트 · 과목 노트와의 우선순위 명시, 색인 지연 측정. 우선 기존 batch를 쓰고 실제로 다른 작업을 지연시키면 실행 용량을 분리한다.

**D3. 질문 재작성과 지연** — `질문 재작성 → 질문 임베딩 → 검색`은 순차다. 911ms · 148ms는 서로 다른 측정이라 합산하지 않고 통합 경로에서 다시 잰다.

**D4. 0.3 · 0.6 기준** — trgm 점수 전용이다. 벡터 경로에는 쓰지 않는다. 범위 밖 질문과 정답 근거의 벡터 점수가 겹쳤으므로 점수 하나로 답변 가능 여부를 정하지 않는다. 인용 검증은 ID 유효성만 보고, 이전 주장 재사용 · 잘못된 근거 붙이기는 막지 못한다(대화 메모리 실제 시험에서 발생).

**D5. 장애 대체** — 질문 임베딩이 실패하거나 시간 안에 안 오면 그 질문은 지금의 pg_trgm 경로(기존 0.3 · 0.6 분기 포함)로 처리한다. 벡터 검색 SQL이 실패해도 같다.

**D6. 색인 방식** — 정확 검색부터.

## 구현 계획

### 0. 기준 브랜치

pgvector 작업은 오래된 `feat/qa-pg-search` 위에서 시작하지 않는다. 로컬 시험 병합 결과(main `a8995e4` 기준, 푸시 안 함):

| 순서 | 브랜치 | 충돌 |
|---|---|---|
| 1 | `feat/redis-liveness-default` 42b0338 (`fix/review-6` 포함) | CHANGELOG만 |
| 2 | `feat/qa-conversation-memory` bcd58bd | 없음 |
| 3 | `feat/qa-pg-search` f6a569a | CHANGELOG · README · `graphs/qa/graph.py` · `mcp_server/server.py` |

① liveness 머지 → ② 대화 메모리 머지 → ③ PG 검색을 최신 main에 맞춰 통합(충돌 해결 + 전체 시험 + PG 시험) → ④ 그 위에서 `feat/qa-pgvector`. 1 · 2는 팀원 브랜치라 머지는 팀 결정.

### 1. 스키마 (검색 저장소)

```sql
CREATE EXTENSION IF NOT EXISTS vector;
ALTER TABLE qa_search_docs ADD COLUMN doc_version bigint NOT NULL DEFAULT 1;   -- 본문이 바뀔 때마다 +1
CREATE TABLE qa_doc_embeddings(
  doc_id text REFERENCES qa_search_docs(doc_id) ON DELETE CASCADE,
  model text NOT NULL, doc_version bigint NOT NULL,
  embedding vector(1536) NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (doc_id, model));
```
- 차원은 모델에 따름(비교는 `text-embedding-3-small` 1536). 모델이 바뀌면 `model`로 구분해 다시 색인
- 처음엔 벡터 색인 없음(정확 검색). HNSW는 나중에 `CREATE INDEX … USING hnsw (embedding vector_cosine_ops)`
- 업무 데이터와 같은 PostgreSQL로 옮기는 것은 업무 저장 PG 전환과 함께

### 2. 색인 경로

1. 문서 저장 트랜잭션(전사 커밋 · 노트 발행)에서 `doc_version`을 올리고, 강의별 색인 의도를 남긴다. 강의당 대기 색인 작업은 1개(실시간 요약의 요청 합치기와 같은 DB 유일 제약)
2. scheduler가 batch로 전달 → worker-batch가 `embed_pending(lecture_id, max_docs, max_tokens)`로 "임베딩이 없거나 `doc_version`이 다른 문서"를 받음
3. 임베딩 API 호출(묶음)
4. `embedding_put(doc_id, doc_version, model, vector)` — 업무 서비스가 **현재 `doc_version`과 같고 삭제되지 않았을 때만** 저장, 아니면 `stale`. 같은 값 재저장은 멱등
5. 남은 문서가 있으면 작업을 이어서 다시 등록(작은 묶음마다 슬롯 반환)

계약(`notree/contracts/tools.py`)부터 고치고 `gen_tool_docs`로 문서 재생성: `embed_pending`, `embedding_put`, 아래 `qa_search` 확장.

### 3. 검색 경로

- worker-qa가 understand 뒤 재작성 질문(`search_query`)을 임베딩해 `qa_search(question_id, source, query_embedding, keywords, limit)`로 보냄
- 업무 서비스가 **질문의 사용자 · 과목(permit 기준)**으로 범위를 정한다. worker가 보낸 벡터나 인자로는 범위를 넓힐 수 없음
- 벡터 후보: `WHERE user_id = … AND course_id = … AND source = … AND d.deleted_at IS NULL AND e.doc_version = d.doc_version AND e.model = …  ORDER BY e.embedding <=> $q LIMIT k_vec`
- 보완 후보(D1): 같은 범위에서 **현재 버전 임베딩이 없는 문서**만 pg_trgm으로 `k_fresh`개
- 결과에 `method`(vector | trgm_fresh | trgm_fallback)와 거리 · 점수를 함께 돌려줌. 같은 ID에 다른 출처 · 원문이면 거절(리뷰 6차 규칙 유지)
- 임베딩이 없으면(D5) `query_embedding=None` → 기존 pg_trgm 경로

### 4. 최신 문서 처리

- 초기 자리: 출처별 `k_vec` + `k_fresh`(초기값 예: 3 + 2, 검증 항목). 보완 후보가 없으면 그 자리는 벡터로 채우지 않고 비움(최신 문서 자리를 벡터가 다시 차지하지 않게)
- 최근 전사 시간 기반 후보(예: 마지막 N분 전사)는 평가 뒤 추가 여부 결정

### 5. 버전

- 본문이 바뀌면 `doc_version` +1 → 이전 벡터는 검색 조건에서 자동 제외 → 색인 의도 등록
- 늦게 끝난 임베딩은 `embedding_put`에서 `stale`로 버림

### 6. 삭제

- 질문 · 강의 · 노트 삭제 시 검색 문서 `deleted_at` 설정(기존) → 벡터는 검색 조건에서 제외, 영구 삭제 때 CASCADE
- 삭제와 임베딩 저장이 겹쳐도 `embedding_put`이 삭제 여부를 같은 트랜잭션에서 확인

### 7. 권한

- 검색 범위는 서버가 permit의 사용자 · 과목으로만 정함(기존 `qa_search` 규칙 그대로)
- RLS는 업무 저장 PG 전환 때. 그 전까지는 모든 검색 SQL에 사용자 · 과목 조건을 넣고 시험으로 고정

### 8. 그래프 분기

- 검색 방식별 설정 분리(`QA_SEARCH=vector | trgm`). trgm 경로의 0.3 · 0.6은 그대로 두고 벡터 경로에는 쓰지 않음
- 벡터 경로 초기 규칙(검증 항목): 노트 · 전사를 한 번에 벡터로 찾음(검색 비용 작음) → 상위 k를 답변에 → LLM이 "근거 더 필요"면 시간이 남을 때 웹. 관련성(범위 밖)은 점수가 아니라 understand 판단
- 우선 강의 2개 → 과목 전체로 넓히는 trgm 브랜치의 단계는 벡터 경로에서 쓸지 평가로 정함

### 9. 측정

통합 경로에서 질문당: understand · 질문 임베딩 · 검색 · 답변 시간을 나눠 기록. 색인 지연(문서 저장 → 벡터 저장)과 batch 대기 시간도.

## 검증 항목

| # | 항목 | 통과 기준 | 방법 |
|---|---|---|---|
| V1 | 범위 · 권한 | 다른 사용자 · 과목 문서가 어떤 경로(vector · trgm_fresh · fallback)에서도 안 나옴 | 시험 (사용자 2 · 과목 2 섞은 데이터) |
| V2 | 삭제 | 삭제된 문서와 그 벡터가 안 나옴. 삭제와 `embedding_put`이 겹쳐도 저장 안 됨 | 시험 |
| V3 | 버전 | 임베딩 중 본문 수정 → `stale`, 이전 벡터로 검색 안 됨, 재색인 의도 등록 | 시험 |
| V4 | 최신 문서 | 방금 저장된 미색인 전사가 trgm 보완 자리로 근거에 들어감 | 시험 + 실제 수업 기록 재생 |
| V5 | 요청 합치기 | 전사 커밋 N번 → 강의당 대기 색인 작업 1개 | 시험 |
| V6 | batch 영향 | 색인 중에도 최종 노트 · 과목노트 대기 시간이 기준 안 | 측정 (기준값은 정할 것) |
| V7 | 장애 대체 | 임베딩 API 실패 · 시간 초과 · 벡터 SQL 실패 → pg_trgm 경로로 답함 | 시험 |
| V8 | 멱등 · 재배송 | 같은 색인 작업 재배송 · 저장 응답 유실에도 벡터 중복 없음 | 시험 |
| V9 | 검색 품질 | 기존 33 · 새 30에서 R@k · 항목 충족률이 오프라인 비교(벡터)와 맞음 | `vec_compare` 재실행 (PG 경로) |
| V10 | 답변 품질 | 그래프 전달 근거 포함률 · 기대 상태 · 인용 · 의미 채점, 항목별 "검색 vs 전달" 추적 | `qa_fresh_eval --llm openai` + `vec_trace` |
| V11 | 범위 밖 · 근거 오용 | 범위 밖 질문 거절 유지, 이전 주장 재사용 · 잘못된 근거 붙이기 건수 기록 | 답변 평가 · 채점표 |
| V12 | 지연 | understand · 임베딩 · 검색 · 답변 시간 분리 기록, 90초 마감 안 | 측정 |
| V13 | 대화 메모리와 함께 | 재작성 질문으로 검색, 대화 이력은 근거로 안 들어감 | 통합 시험 (②③ 통합 뒤) |

## pgvector 점검 (사용자 PC, 2026-10-20)

리뷰어가 제공한 `check_pgvector.py`(저장소에 없음)를 사용자 PC에서 실행한 결과. 기존 numpy 측정과 별개다.

| 항목 | 값 |
|---|---|
| 환경 | Windows의 점검 도구 → Docker PostgreSQL 16.15 · pgvector 0.8.7 |
| 데이터 | 문서 1,097개 · 질문 64개 · 1,536차원 |
| 정확 검색 | 중앙 1.699ms · p95 2.133ms · 계산 불일치 0건 |
| HNSW | 중앙 1.456ms · p95 1.931ms · 후보 부족 1건 |
| 측정 범위 | 연결을 유지한 반복 검색, DB 왕복 포함 / 임베딩 API · 최초 연결 · 답변 생성 제외 |

검색 엔진 동작 확인이며 답변 정확도 · 동시 부하 · 운영 권한 검증이 아니다.

## 아직 정하지 않은 것

- 임베딩 모델 (비교는 `text-embedding-3-small`)
- 강의 문장을 임베딩 API로 보내는 것의 동의 범위
- `k_vec` · `k_fresh` · 최근 전사 조회 범위, 색인 묶음 간격 · 크기, batch 우선순위, V6 기준값
- 전사 문맥 확장 방식 (현재 조각 · 인접 구간 묶음 · 검색 후 앞뒤 문맥)
