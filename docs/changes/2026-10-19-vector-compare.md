# 검색 방식 오프라인 비교 도구 (pg_trgm · 벡터 · 결합) — 2026-10-19

브랜치 `exp/vector-compare` (`feat/qa-pg-search` f6a569a에서 갈라짐). **worker와 그래프는 바꾸지 않았다.**
바뀐 것은 평가 스크립트(`notree/dev/`)와 테스트뿐이다.

## 무엇을 비교하나

| | 같게 맞춘 것 |
|---|---|
| 문서 | `data/qa_docs.jsonl` 그대로 (1,097개: 머신러닝 노트 357·전사 274, OOP 노트 252·전사 214 / 실시간 요약 488·전사 488·최종 노트 121). 분할·버전 같음 |
| 범위 | 질문의 과목 전체, 노트와 전사를 따로 찾음. 사용자 하나 |
| 후보 수 | 방식마다 출처별 상위 20개를 만들고 @3·@5로 자름 |
| 질문 | 기존 33개(+범위 밖 1) · 새 30개(범위 밖 성격 4). 같은 질문 문장을 세 방식에 모두 씀 |

- **trgm**: 지금 PG 검색과 같은 kw_mean. 기존 33은 `data/qa_keywords.json`의 LLM 검색어, 새 30은 실제 LLM 실행의 검색어(`--fresh-keywords`), 없으면 단어 추출 — 어느 쪽인지 결과에 표시
- **vec**: 코사인 정확 계산 (numpy). pgvector 정확 검색과 순위가 같다. 근사 색인(HNSW 등)의 손실은 이 비교에 없다
- **hybrid**: RRF(1/(60+순위)) — **결합 방법의 후보 하나**. 순위 결정과 "충분한가" 판단은 별개라 RRF 점수를 0.3·0.6 기준에 넣지 않는다. 그래프에 넣을지는 이번에 정하지 않는다

## 질문 세트 구분

- 기존 33: 구간 제목을 참고해 만든 질문. 검색 방식을 다듬을 때 봤던 세트
- 새 30: AI가 만든 질문. 기준값 조정에 쓰지 않음 (팀원이 쓴 질문이 아니다 — 이전 보고의 표현을 바로잡음)
- 실제 학생 질문: 따로 모아야 함. 기존 질문을 바꿔 쓴 변형은 독립 세트로 세지 않는다

## 지표를 나눠 본다

1. **검색 Recall@3·@5** (`vec_compare`): 출처별(노트 / 전사)과 "둘 중 하나" = 노트 상위 k ∪ 전사 상위 k (후보 최대 2k개). 통합 상위 k가 아니다
2. **질문 항목별 근거 충족률 @k** (`vec_compare`, 새 30): 정답표 항목마다 그 근거가 후보 안에 있는가
3. **최종 전달 근거 정답 포함률** (`qa_fresh_eval`): 그래프가 실제로 LLM에 넘긴 근거 (우선 강의 2개 → 과목 전체 확장, 0.3 기준, 근거 3·5개)
4. **답변 품질** (`qa_fresh_eval` 채점표): 기대 상태 일치, 인용, 의미 채점

범위 밖 질문은 방식별 최고 점수만 보여 준다. 점수만으로 거절을 정하지 않고 답변 평가에서 따로 본다.
요약 노트 `s{n}`과 전사 `t{n}`은 같은 구간이라 같은 근거로 센다 (`qa_fresh_eval.chunk`와 같음).

### 이전 "32/33" 정의 바로잡기
`docs/measurements/2026-10-17-pg-search`의 32/33은 **노트 상위 3 ∪ 전사 상위 3 (우선 강의 2개 + 0.3 미만이면 과목 전체 확장, kw_mean, LLM 검색어)** 에 정답이 하나라도 있는 질문 수다.
두 목록을 합친 검색 단계 지표이고, 그래프가 실제로 전달한 근거나 답변 품질이 아니다. 질문도 구간 제목을 보고 만든 세트다.

## 이 환경에서 확인한 것 (결과가 아니라 점검)

- 문서 수·분할·정답 ID: `--check`로 확인. 정답 ID 90개 모두 문서에 있음, 임베딩 문서 ID 일치
- trgm 기준선 (과목 전체, 노트·전사 각각, LLM 검색어): 기존 33 R@3 둘 중 97%(32/33) · R@5 100%
- **동점 문제**: kw_mean은 점수가 같은 문서가 많아, 기존 33 중 9개는 정답이 3번째 경계에서 동점이다. 동점 순서(정렬 기준 없음)만 바꿔도 전사 R@3가 79%↔88%로 움직였다. 비교 스크립트는 doc_id 순으로 고정하고 동점 경계 질문 수를 함께 출력한다. 지금 PG 검색(`pg_search.search`)도 동점 정렬 기준이 없다 — 이번 단계에서는 고치지 않고 기록만 한다
- 새 30 trgm은 이 환경에 실제 LLM 검색어가 없어 **단어 추출 검색어**로만 돌렸다 (R@3 둘 중 46%, R@5 65%, 항목@3 34%) — LLM 검색어 결과가 아니다
- 벡터·결합 경로는 `vec_embed --fake`(문자 3-gram 해시)로 끝까지 돌아가는 것만 확인. **가짜 벡터 수치는 비교 결과가 아니다**
- `data/fresh/out/run.jsonl`은 `llm=fake`(검색만) 실행이었다. 새 30의 실제 LLM 평가는 아직 없다

## 사용자 PC에서 할 일 (OpenAI 키 필요, 결과는 data/ 아래에만)

```
docker compose up -d postgres
uv run python -m notree.dev.qa_fresh_eval --load --llm fake                      # 그래프용 검색 문서 적재 (한 번)
uv run python -m notree.dev.qa_fresh_eval --llm openai --limits 3,5 --out data/fresh/out/run-openai.jsonl --sheet data/fresh/out/sheet-openai.md
uv run python -m notree.dev.pgsearch_eval --load --scores kw_mean --keywords data/qa_keywords.json   # 비교용 qa_docs 적재 + 기존 기준선
uv run python -m notree.dev.vec_embed --model text-embedding-3-small --out data/vec/oai-3-small
uv run python -m notree.dev.vec_compare --check --emb data/vec/oai-3-small
uv run python -m notree.dev.vec_compare --emb data/vec/oai-3-small --fresh-keywords data/fresh/out/run-openai.jsonl --out data/vec/compare.jsonl
```

시간·비용은 따로 집계된다.
- 검색만: 준비된 검색어·질문 벡터로 찾는 시간 (방식별)
- 질문 처리에 더해지는 준비: 질문 임베딩(`vec_embed` 질문별 시간·토큰), LLM 검색어 생성(`qa_fresh_eval`의 `understand_ms`, 이번에 추가)
- 초기 색인: 문서 임베딩 시간·토큰
- 결합의 검색 시간은 trgm + vec를 차례로 잰 합이다 (병렬로 돌리면 더 짧다)

강의 문장을 임베딩 API로 보내는 것은 외부 처리다. LLM 호출과 같은 동의 범위인지 팀에서 확인한 뒤 실행한다.
임베딩은 강의 원문에서 나온 데이터라 `data/` 밖에 두지 않는다 (스크립트가 막음).

## 측정 결과 (2026-10-19, 사용자 PC)

실제 임베딩·LLM 실행 결과와 해석은 [docs/measurements/2026-10-19-vector-compare](../measurements/2026-10-19-vector-compare/README.md)에 있다.
위 "이 환경에서 확인한 것"의 새 30 수치는 단어 추출 검색어 · 가짜 벡터로 경로만 점검한 값이라 결과로 쓰지 않는다.
