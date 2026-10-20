# 업데이트 목록

무엇이 바뀌었는지 최신 순으로 적습니다. 항목마다 **실행 방법이나 코드 사용법이 바뀌는지**를 같이 표시합니다.
큰 변경의 자세한 설명은 `docs/changes/`에 따로 둡니다.

표시
- 🆕 추가 · 🔧 변경 · 🗑 삭제 · 🐛 수정
- ⚠️ **깨지는 변경**: 기존 코드·명령을 고쳐야 함

---

## 2026-10-20

### pgvector 통합 결정 기록 (문서만) · (이번 커밋)
자세히: [docs/changes/2026-10-20-pgvector-integration.md](docs/changes/2026-10-20-pgvector-integration.md)

- 🆕 결정 D1~D6: 최신 문서 공백(색인 상태별 후보 자리 배정 · 최근 전사 조회 평가), 임베딩 작업 묶음과 batch 우선순위, 질문 재작성 → 임베딩 순차 지연, 0.3·0.6은 trgm 전용(방식별 설정 분리), RRF 상시 결합 안 함, 첫 통합은 정확 검색
- 🆕 pgvector 점검 결과(사용자 PC, 정확 검색 중앙 1.7ms) 기록과 통합 순서(liveness → 대화 메모리 → PG 검색 통합 → pgvector)
- 문서만 추가. 코드 · 실행 방법 변경 없음

## 2026-10-19

### 검색 방식 비교 측정 기록 (문서만) · `6ae423d`
자세히: [docs/measurements/2026-10-19-vector-compare](docs/measurements/2026-10-19-vector-compare/README.md)

- 🆕 pg_trgm · 벡터 · 동일 비중 RRF 오프라인 비교 기록: 새 30(AI 생성)에서 벡터가 가장 높음, RRF는 개선 못 함. 질문별 순위 · 항목별 "검색 vs 그래프 전달" 표 (질문 ID · 순위 · 개수만)
- 문서만 추가. 평가 도구와 PG 검색 구현은 `exp/vector-compare` · `feat/qa-pg-search`에 있고 main에는 들어오지 않음. 실행 방법·코드 변경 없음

## 2026-10-18

### main 머지 후 문서: 상태·시험 환경·요약 한계 · `974901f`
- 🔧 `docs/status.md`: 요약 합치기·배정 복구·전달 세대·Redis 모드(기본 db) 반영, 팀 결정 표에 Redis 모드 활성화와 요약 추가 복구를 별도 결정으로
- 🔧 README: 실제 Redis 시험은 `redis-server`가 없으면 건너뜀(`pytest -rs`로 확인), 프록시 환경 문제 해결(socksio, NO_PROXY)
- 🔧 요약 문서: 실시간 요약 총 1회 유지, 실패 구간은 새 전사가 없으면 다시 요약되지 않으며 최종 노트 자동 보완은 미보장

### 전달 세대 없는 메시지는 기본 거절 · `f7a957a`
- 🔧 운영 worker는 `gen` 없는 메시지로 시작하지 않고 ACK (재전송이 세대를 붙여 다시 보냄). 예전 메시지 허용은 `NOTREE_ALLOW_LEGACY_MESSAGES=1` 또는 `Worker(allow_legacy=True)`로만
- ⚠️ 세대 없이 XADD하는 도구는 허용 설정이 필요 (`lane_bench`는 `allow_legacy=True`로 바꿈)
- 테스트 → 129개

### 전달 세대를 Stream별로 기억 · `8a74f28`
- 🐛 worker가 메시지의 전달 세대를 메시지 ID만으로 기억 → Stream마다 ID가 따로 매겨져, 빌려 쓰는 worker는 두 Stream의 같은 ID를 함께 들 수 있음 → 한쪽 세대가 덮어써 정상 메시지를 이전 세대로 잘못 거절·ACK할 수 있었음 → (Stream, ID)로 기억. 실제 Redis에서 두 Stream에 같은 ID `5-0`으로 확인
- 테스트 → 127개

### 통합 갱신: 전달 세대 + DB 실행 세대 · `78dd2ad`
- 🔧 `integ/2026-10-18`에 전달 세대 반영. `job_start(dispatch_gen)` + 응답 `exec_epoch`를 함께, 소유 키 모드에서 이전 세대 메시지는 DB 시작 전에 걸러짐
- 🆕 결합 시험: 소유 키 모드에서 리뷰 4차 순서
- 테스트 → 143개 (통합 브랜치, 건너뜀 0)

### Redis 소유권 문서 정정 · `37381a8`
- 🔧 "Redis 소유자는 늘 DB 실행자와 같다" → "최종 쓰기는 DB의 현재 실행 세대만 허용" (세대 발급과 키 저장 사이 일시 불일치 명시), 동시 시작으로 시도가 소모되는 것은 시도 한도를 줄이는 비용으로 명시
### Redis 유실 복구 리뷰 4차: 전달 세대 · `9b31d5b`
- 🐛 초기화 전 메시지의 늦은 `job_start`가 복구 시작 뒤 도착하면 `updated_at`이 기준 뒤로 바뀌어 복구에서 빠지고 영영 running → 전달 세대(`dispatch_gen`)를 메시지·작업에 기록, 이전 세대의 시작은 `stale_dispatch`로 거절, 복구 대상은 "마지막 전달이 이전 세대"
- ⚠️ 계약: `ReservedJob.gen`, `job_start.dispatch_gen`, 상태 `stale_dispatch` (선택). XADD 메시지에 `gen` 필드
- 테스트 → 126개
### 통합 갱신: 리뷰 3차 반영 세 브랜치 · `d054287`
- 🔧 `integ/2026-10-18`에 job_retry 원자성, 복구 발견/완료 분리, DB 세대 기반 소유 키를 합침. 결합 장애 시험을 새 프로토콜에 맞춤
- 테스트 → 139개 (통합 브랜치, 건너뜀 0)

### Redis 소유권 리뷰 3차: 번호는 DB 실행 세대만 · `43c43b3`
자세히: [docs/changes/2026-10-18-redis-heartbeat.md](docs/changes/2026-10-18-redis-heartbeat.md#리뷰-23차-소유권-획득--db-시작-경쟁--번호는-db만-발급)

- 🐛 Redis가 발급했지만 DB가 아직 못 본 펜싱 번호가 Redis 초기화를 건너 늦게 도착하면 허용돼 실행자를 덮음 (재현) → Redis 펜싱 카운터를 없애고 DB의 `exec_epoch`만 번호로 사용
- 🔧 순서 변경: 소유 키 확인(held) → `job_start`가 세대 발급 → 세대를 담아 키 쓰기(더 큰 세대만 덮어씀)
- ⚠️ 계약: `job_start.fence`·`stale_fence`·`fence_max` 삭제 (2차에서 추가했던 것), `job_start` 응답에 `exec_epoch`
- 테스트 114 → 115개

### Redis 유실 복구 리뷰 3차: '발견'과 '완료' 분리 · `b712b2a`
- 🐛 표시 키를 복구 완료로 써서, 첫 복구 때 lease가 살아 있던 고립 작업이 영영 running에 남음 → 표시 키는 발견, 완료는 DB(`meta.recovery_since`)가 판단해 열려 있는 동안 매 주기 재확인
- 🐛 한 번에 500건만 처리 → job_id 순서로 쪽을 나눠 끝까지
- ⚠️ `job_recover_orphans`에 `start`·`after` 인자, 응답에 `held`·`open`·`next_after`
- 테스트 → 123개

### 실시간 요약 리뷰 3차: job_retry 원자성 · `1dc797d`
- 🐛 사용자 재시도의 "후속 요약 없음" 확인과 상태 변경이 한 트랜잭션이 아니어서, 그사이 커밋이 후속 요약을 넣으면 `UNIQUE constraint failed` → 한 쓰기 트랜잭션 + `WHERE state='failed'`, 제약 충돌도 "되살리지 않음"으로 응답. MCP 서버 2개로 동시 30회 재현 뒤 고침
- 테스트 115 → 116개
### 통합 시험: 결합 장애 (실제 Redis) · `91cec9e`
자세히: [docs/changes/2026-10-18-integration.md](docs/changes/2026-10-18-integration.md) (브랜치 `integ/2026-10-18` = summary-coalesce + dispatch-recovery + redis-heartbeat)

- 🆕 결합 장애 시험 2개: 소유 키 모드에서 요약 중 Redis 초기화 + 새 커밋 + 유실 복구, 다시 보낸 작업의 펜싱 복원
- 테스트 → 134개 (통합 브랜치)

### 배정·회수·Redis 유실 복구 · `6b98185`
자세히: [docs/changes/2026-10-18-dispatch-recovery.md](docs/changes/2026-10-18-dispatch-recovery.md) (브랜치 `fix/dispatch-recovery`)

- 🐛 배정기가 둘 이상이면 같은 작업을 두 번 허가하고 끝난 작업을 reserved로 되돌릴 수 있었음 → 한 트랜잭션 + `WHERE state='pending'`
- 🐛 `XAUTOCLAIM` 다음 탐색 위치를 버려 앞쪽 PEL 10개가 살아 있으면 뒤의 죽은 메시지를 못 찾음 → Stream별로 위치 보관
- 🐛 Redis 초기화 뒤 worker가 `NOGROUP`만 반복 → group 다시 만듦
- 🆕 `job_recover_orphans` + scheduler의 Redis 유실 감지(표시 키): reserved·sent·lease 끝난 running을 다시 보냄, 마감·시도 한도는 실패 확정
- 테스트 115 → 119개

### 실시간 요약 리뷰 2차: 사용자 재시도 충돌, 저장 응답 유실 · `501442d`
- 🐛 `job_retry`가 실패한 요약을 되살릴 때 후속 요약이 대기 중이면 `UNIQUE constraint failed`로 깨짐 → 후속 요약이 있거나 녹음이 끝났으면 되살리지 않고 이유를 돌려줌
- 🐛 `summary_publish` 응답만 유실되면 요약은 저장됐는데 작업은 failed(시도 한도 초과) → 같은 idem_key로 저장을 다시 보내 확인 (LLM은 다시 안 부름), 다시 실행돼도 `transcript_range.already_published`면 완료
- 테스트 112 → 115개

### 실시간 요약 리뷰 반영: 작업↔강의 연결 확인, 재전송 재등록 막기 · `a1face6`
자세히: [docs/changes/2026-10-18-summary-coalesce.md](docs/changes/2026-10-18-summary-coalesce.md#리뷰-반영-3b1c00e-이후)

- 🐛 P1 `summary_publish`가 permit의 작업이 그 강의를 요약하는 작업인지 확인하지 않아, 다른 작업의 유효한 permit으로 남의 강의 요약을 바꿀 수 있었음 → 종류·lecture_id·사용자·현재 실행권 확인 (duplicate 경로 포함)
- 🐛 P1 같은 전사 묶음 재전송이 실패한 요약을 다시 등록 → 새 전사가 저장됐을 때만 등록
- 🐛 P2 실행권을 잃은 worker도 `transcript_range`로 attempted_until을 올릴 수 있었음 → 현재 실행권 확인, 계약에 상태 변경 도구로 표시
- ⚠️ `transcript_commit` 응답에 `added`, `coalesced` 의미 좁힘
- 테스트 109 → 112개

### 실시간 요약 범위 합치기 (PoC) · `3b1c00e`
자세히: [docs/changes/2026-10-18-summary-coalesce.md](docs/changes/2026-10-18-summary-coalesce.md) (브랜치 `feat/summary-coalesce`)

- 🆕 실시간 요약 PoC 경로: `lecture_start`·`transcript_commit`·`lecture_end`·`lecture_get`·`summary_recover`, `transcript_range`·`summary_publish` 구현, `live_summary` 핸들러(가짜 LLM)
- 🆕 강의당 대기 요약 1개(DB 유일 부분 인덱스) + 동시 실행 1개(배정 조건) → 그사이 커밋은 다음 요약 하나로 합쳐짐
- 🆕 scheduler 주기 작업에 요약 누락 복구 (아무도 읽어 가지 않은 전사가 있을 때만)
- 🔧 `jobs.lecture_id`, `job_reserve_pending`에 실시간 요약 배정 조건, `.gitignore`에 `data/`
- 테스트 101 → 109개
### Redis 소유권 리뷰 2차: 펜싱 번호로 늦은 job_start 거절 · `5ccfa4a`
자세히: [docs/changes/2026-10-18-redis-heartbeat.md](docs/changes/2026-10-18-redis-heartbeat.md#리뷰-2차-소유권-획득--db-시작-경쟁-펜싱-번호)

- 🐛 소유 키를 얻은 뒤 DB 시작이 늦어지는 사이 키가 넘어가면, 늦게 온 시작이 더 높은 실행 세대를 받아 Redis 소유자와 DB 실행자가 달라졌음 → 키를 얻을 때 펜싱 번호(INCR), DB가 본 최대 번호 이하는 `stale_fence`
- 🆕 Redis 초기화로 카운터가 작아지면 DB가 알려 준 값까지 올려 복원
- ⚠️ 계약: `job_start.fence`, 응답 상태 `stale_fence`와 `fence_max` (선택 필드, redis 모드에서만)
- 테스트 111 → 114개

### heartbeat를 Redis 소유 키로 (시험 구현, 기본값은 db) · `ae42b20`
자세히: [docs/changes/2026-10-18-redis-heartbeat.md](docs/changes/2026-10-18-redis-heartbeat.md) (브랜치 `feat/redis-heartbeat`)

- 🆕 `NOTREE_LIVENESS=redis`: 소유 키 `SET NX` + Lua heartbeat 한 번(토큰 일치·PEL 소유자 나·원본 메시지 존재일 때만 갱신). DB heartbeat 없음, 늦은 쓰기는 `exec_epoch`
- 🆕 `notree/worker/ownership.py`, 실제 Redis 시험 10개 (`tests/test_redis_ownership.py`: Lua 규칙, 인계, 갱신 지연, 프로세스 SIGKILL·SIGSTOP)
- 🔧 계약: `job_start.lease_sec=0` = DB lease 없이 실행 세대만, `job_get`에 `exec_epoch`
- 🔧 `.gitignore`에 `data/` (main에 빠져 있어 강의 자료가 커밋될 수 있었음)
- 테스트 101 → 111개 (Redis 시험은 redis-server 없으면 건너뜀)

## 2026-10-17

### Q&A 평가 기준 정정: 모델 원본 검사 분리, 사례별 기대 상태 하나로 · `5a98cdb`
- 🐛 `qa_eval`의 인용 표시 검사가 정제 후 문장만 봐서 모델이 지시를 어겨도 통과하던 문제 → `model_no_markers`(LLM 원본)와 `delivered_no_markers`(정제 후)로 분리. 원본은 jsonl `raw_answers`
- 🔧 기대 상태를 사례마다 하나로: `not_found`는 근거 부족만, `partial`은 부분 답변만 (3차 결과는 새 기준으로도 통과)
- 🔧 측정 기록·`docs/status.md`에 "정해진 7개 사례의 자동 검사이며 일반 정확도가 아님" 명시
- 테스트 99 → 101개

### Q&A 3차 측정 기록: 42/42 통과 · `214fd9c`
- 🆕 `docs/measurements/2026-10-17-openai-qa/` 3차: gpt-4o-mini 3회 × 2번 모두 통과. partial은 n1·n2 인용으로 해결, tight_deadline은 시간 초과
- 🔧 관찰: partial은 "근거 더 필요" 때문에 출처를 더 찾으며 LLM을 상한(4번)까지 불러 입력 토큰 2.5배 — 새 근거가 없으면 다시 답하지 않는 최적화 후보

### Q&A 2차 측정 원인 정정: 검색어 띄어쓰기 분리, 검색 못 하면 시간 초과 · `f6eba70`
- 🐛 LLM이 `'데이터 구조'`처럼 띄어쓰기 있는 검색어를 내면 통째로 안 맞아 다른 근거 점수까지 깎이던 문제 → `split_keywords`로 단어로 나눔 (2차 측정에서 partial 실패의 진짜 원인. 1차의 "프롬프트 문제" 판단은 틀림)
- 🔧 시간이 없어 검색을 하나도 못 했으면 `근거 부족` 대신 `시간 초과`로 저장 (상태 `queried`: 실제로 검색 요청을 한 곳)
- 🆕 2차 측정 기록 (gpt-4o-mini 18/21, 질문당 중앙 1.65초, 21건 토큰 입력 7,641·출력 1,288)
- ⚠️ 결과 상태 의미 변경: 검색을 전혀 못 한 경우 `근거 부족` → `시간 초과`. 화면 문서(결과 형식)의 상태 설명도 맞춰야 함
- 테스트 97 → 99개

### Q&A 실제 OpenAI 1차 측정, 여러 부분 질문 프롬프트 보완 · `e0053c3`
- 🆕 `docs/measurements/2026-10-17-openai-qa/`: gpt-4o-mini 3회 반복, 21건 중 18건 통과, 질문당 1.5~3.5초. 형식·인용 오류 0
- 🐛 여러 부분으로 된 질문("A와 B의 차이, 각각 언제")에서 한 부분만 답하고 멈추던 문제 → 답변 프롬프트에 "부분마다 따져서 답할 수 있는 부분은 빠짐없이" 추가
- 🔧 `qa_eval`: 인용 목록 출력, 실패한 사례는 검색어·근거 점수·문장별 인용 출력(`--verbose`면 전부), jsonl에 `keywords`·`hits`·`sentences`

### Q&A 실제 Redis 재검증, 잠정 계약 표시, OpenAI 품질 측정 도구 · `46227ee`
- 🆕 실제 Redis + 별도 프로세스 재검증: 정상 Q&A, 답변 완료 후 삭제, 저장 커밋 후 응답 유실(재배송 20초 뒤 LLM 재호출 없이 `done`) — `docs/measurements/2026-10-17-real-redis/` 4절, 원본 로그
- 🆕 `notree.dev.qa_fault_worker`: 실제 프로세스에서 qa_complete 응답 유실을 일부러 내는 시험용 worker
- 🆕 `notree.dev.qa_ask --delete-after`
- 🆕 `notree.dev.qa_eval`: 가짜 검색 + 실제 LLM으로 사례 7개(노트·전사·웹·관련 없음·근거 없음·부분 답변·마감 4초) 판정, 지연·토큰·비용 추정. 실제 검색 품질은 측정 대상 아님
- 🔧 `OpenAIQALLM`이 호출마다 지연·토큰 기록 (`usage`)
- 🔧 Q&A 도구 계약에 "[잠정 · T5 확인 전]" 표시 (`docs/tool-contracts.md`), `docs/status.md`는 "가짜 검색·LLM을 사용한 Q&A 통합 PoC"로
- 테스트 95 → 97개

### Q&A worker 통합 main 머지, 진행 상황 갱신 · `b88aba0`
- 🔧 `feat/qa-worker`를 main에 머지 (`68a46e7`까지). Q&A 도구 계약은 T5 확인 전 반영 — 확인 결과에 따라 바뀔 수 있음
- 🔧 `docs/status.md`: 테스트 수 95개, Q&A 통합 1단계(계약, T5 확인 대기)·2단계(가짜 도구로 연결) 상태

### Q&A worker 리뷰 반영: 삭제 후 조회 차단, citations, 저장 응답 유실 후 재실행 · `df4d495`
자세히: [docs/changes/2026-10-17-qa-worker.md](docs/changes/2026-10-17-qa-worker.md) "리뷰 반영" (브랜치 `feat/qa-worker`)

- 🐛 삭제된 질문의 질문·답변·출처 원문이 `qa_get`으로 조회되던 문제 → `qa_delete`가 복사본까지 지우고, 조회는 `state=deleted`만
- 🐛 저장·조회 결과에 화면 계약의 전체 `citations`가 없던 문제 → 서버가 문장별 인용에서 순서대로 중복 없이 생성. 출처에 없는 인용은 저장 거절
- 🐛 저장 응답 유실 후 재시도 때 그래프(LLM)를 다시 돌리던 문제 → `qa_context.answered`가 참이면 건너뛰고 완료
- 테스트 92 → 95개

### Q&A worker 통합 1·2단계: 도구 계약 + 가짜 도구로 연결 · `769129e`
자세히: [docs/changes/2026-10-17-qa-worker.md](docs/changes/2026-10-17-qa-worker.md) (브랜치 `feat/qa-worker`)

- 🆕 Q&A 도구: `qa_ask`(api, 질문 + 작업 의도 한 트랜잭션), `qa_context`, `qa_search`, `web_search`, `qa_complete`(멱등·세대·실행권 확인), `qa_get`·`qa_delete`(api), `qa_seed`(dev). PoC는 SQLite + 키워드 일치 점수
- 🆕 `qa` 핸들러가 그래프를 실행: `McpRetriever`(검색 도구 → 그래프 Retriever), 오류 변환(일시 거절 → 다시/다음 출처, 영구 거절·세대 불일치 → FatalError), 마감 = 요청 + 90초
- 🆕 `notree.dev.qa_ask`: 질문 하나를 넣고 저장된 답을 출력
- 🔧 실제 Redis 실행 기록에 Q&A 경로(전사·웹·일반 지식 3건) 추가, `docs/status.md` 갱신
- 🔧 worker 시작 로그의 한도를 "기본 한도(종류별 설정 우선)"로 표기
- ⚠️ 계획 단계였던 `qa_context`·`qa_complete`·`web_search` 입출력을 그래프에 맞춰 바꿈 (T5 확인 필요). `web_search`는 `question_id`를 받음
- 테스트 84 → 92개

### 진행 상황 문서, 실제 Redis 실행 기록 · `bcf7d1d`
- 🆕 `docs/status.md`: "PoC 구현·테스트 완료"와 운영 검증을 구분, Q&A는 "정책 흐름 구현, 실제 검색·LLM·worker 통합 전", Q&A 안의 통합 순서, 과목노트 실제 병합을 개발 목록에
- 🆕 `docs/measurements/2026-10-17-real-redis/`: 실제 Redis + 프로세스 분리 실행 기록 (Windows 정상 경로, Linux 정상 경로 + worker 인계 90초 뒤 체크포인트에서 재개). 원본 로그 포함
- 🔧 README 문서 목록

### 실행 정책 확정, 결정 문서 정리, 측정 자료 보존 · `991b8c2`
- 🔧 작업 종류별 실행 정책 (팀 결정 2·3번): Q&A·채점 요청부터 전체 90초 안에서 최초 포함 3회, final_note 시도당 5분·총 3회, course_update 시도당 10분·총 3회 (`settings.KIND_MAX_ATTEMPTS`, `KIND_JOB_TIMEOUT_SEC`, `KIND_DEADLINE_SEC`)
- 🆕 Q&A·채점 작업에 `deadline_at`이 없으면 `job_create`가 요청 시각 + 90초로 넣음. `job_retry`는 새 마감을 안 주면 지금 + 90초
- 🐛 `job_heartbeat`가 호출 집계(`tool_stats`)에 빠져 있던 문제
- 🔧 `docs/team-decisions.md`: 2·3번 값, 11번(레인: 고정 기본·빌려 쓰기는 qa 총 슬롯과 함께), "결정 대기" 표 — heartbeat 위치 / 전달 누락 복구(outbox 유지) / 즉시 전달을 따로 정함, `pg_trgm`은 Read, 전사 검색은 "우선 2개 + 부족 시 확장"
- 🔧 `docs/changes/2026-10-16-lanes.md`: "커밋 직후 XADD면 outbox가 필요 없다"는 취지 삭제, 슬롯 표기
- 🆕 PG 부하 측정 재현 도구 `notree/dev/pgload/`와 원본 결과 `docs/measurements/` (Q&A 검색·전사 저장은 10/17 재측정. 전사 per_session p99는 0.12~4.2초로 흔들려 문서의 "2.5초"를 고침)
- 🆕 dev 의존성 `asyncpg`
- 🗑 `feat/taskiq` → `archive/taskiq`로 보관, 실험 기록 문서만 `docs/changes/2026-10-15-taskiq-trial.md`로 가져옴
- ⚠️ Q&A·채점·final_note·course_update의 시도 한도·시도당 제한이 바뀜 (rt 2회 → 3회, batch 15분 → 5·10분)
- 테스트 80 → 84개

## 2026-10-16

### 레인 리뷰 반영: 배정 단계 레인 분리, 벤치마크 집계 · `f94564d`
자세히: [docs/changes/2026-10-16-lanes.md](docs/changes/2026-10-16-lanes.md) "리뷰 반영" (브랜치 `feat/lanes`)

- 🐛 배정기가 모든 레인을 생성 순서로 골라 요약 2,000개 뒤 Q&A가 약 100초 늦게 나가던 문제 → 레인별 몫으로 배정 (`settings.ALLOC_LANES`)
- 🆕 `job_reserve_pending`에 선택 인자 `stream` (레인별 조회). `jobs(state, stream, created)` 인덱스
- 🐛 `lane_bench` 처리량에 측정 종료 뒤 끝난 작업이 들어가던 문제 → 구간 안 완료만, 남은 대기열·시작 못 한 작업 함께 보고
- 🆕 `lane_bench --alloc mixed,lanes --burst N`: 실제 배정기 코드까지 포함해 측정. `--seed`
- 🔧 측정 표 전부 다시 잼 (시드 3개). 처음 표는 폐기
- 테스트 77 → 80개

### 레인 분리: Q&A·채점 전용 qa 레인 + 빌려 쓰기 · `1f4a5cc`
자세히: [docs/changes/2026-10-16-lanes.md](docs/changes/2026-10-16-lanes.md) (브랜치 `feat/lanes`)

- 🆕 `qa` 레인(`notree:qa`): Q&A·채점이 실시간 요약 뒤에 줄 서지 않게 분리 (PG 실측에서 Q&A 56초의 원인)
- 🆕 빌려 쓰기: `--borrow-slots N` / `NOTREE_QA_BORROW_SLOTS` — qa가 비면 rt 작업을 N개까지. 기본 0(고정)
- 🆕 `notree/dev/lane_bench.py`: 한 대기열 / 고정 / 빌려 쓰기를 실제 worker·Redis로 비교
- 🔧 worker는 메시지가 온 레인의 설정으로 lease·재배송·ACK (`process`, `_ack`, `_touch`에 `cfg` 인자)
- ⚠️ `qa`, `grading` 작업이 `notree:rt` → `notree:qa`로 감 → **qa worker를 따로 띄워야 함** (`--stream qa`)
- 테스트 71 → 77개
- 측정 표는 집계 오류가 있어 위 항목에서 다시 잼

## 2026-10-15

### Q&A 결과 계약: 문장 정제, notice_code, sources · `7b8bae4`
- 🐛 `sentences[].text`에 LLM이 넣은 인용 표시가 남던 문제 → 문장을 먼저 정제하고 `answer`는 거기서 파생
- 🆕 결과에 `notice_code`(not_found / insufficient / timeout)와 `sources`(인용된 근거만) 추가
- 🔧 `docs/changes/2026-10-15-qa-hardening.md` "3차 검토 반영"
- 테스트 69 → 71개

### 2차 검토 반영: 인용 표시 훼손, 최종 상태, 웹 재시도, 실행 세대 · `9a91d80`
자세히: [docs/changes/2026-10-15-qa-hardening.md](docs/changes/2026-10-15-qa-hardening.md) "2차 검토 반영"

- 🐛 인용 표시를 지우는 정규식이 `a[0]`, `[1, 2, 3]` 같은 코드·수식을 지우던 문제 → 인용 표시는 `[^ID]` 문법, 지우는 대상을 좁힘
- 🐛 근거가 더 필요하다는 답이 `completed`로 저장되던 문제 → `부분 답변` 상태 추가
- 🐛 과목 질문에 인용 없는 답(근거 없이 지어냈을 수 있음)이 본문째 저장되던 문제 → 안내문으로 교체
- 🐛 웹 검색이 장애 시 2번 요청되던 문제 → 웹은 다시 요청하지 않음
- 🐛 사용자 재시도 뒤 이전 permit이 새 실행에서 통하던 문제 → 되돌리지 않는 실행 세대 `exec_epoch`
- 🔧 `docs/team-decisions.md` 4번(permit 형식), 10번(보완 목록 4·7번 수정, 16·17번 추가)
- ⚠️ 본문 인용 표시 `[n1]` → `[^n1]`, 상태에 `부분 답변` 추가
- ⚠️ permit에 `x`(실행 세대) 추가 → 이전 permit은 쓰기 도구에서 `permit_invalid`
- 테스트 63 → 69개

### Q&A 그래프 보완 5가지 · `a97c155`
자세히: [docs/changes/2026-10-15-qa-hardening.md](docs/changes/2026-10-15-qa-hardening.md)

- 🐛 인용 검증이 끝내 실패하면 틀린 본문까지 저장되던 문제 → 정해진 "근거 부족" 안내문으로 저장
- 🐛 본문의 `[ID]`와 인용 목록이 어긋날 수 있던 문제 → LLM은 문장별 인용만 내고 `[ID]` 표시는 코드가 생성
- 🆕 점수가 높아도 LLM이 "답할 수 없음"이면 다음 출처로 다시 검색 (LLM 호출 실행당 4번까지)
- 🆕 검색 장애 처리: 일시 장애·타임아웃은 1번 다시 후 다음 출처, 권한 없음·삭제는 `QAFatal`
- 🆕 시간 예산: 검색·LLM 호출별 제한, 답변·저장용 시간 확보, 모자라면 "시간 초과" 안내문
- 🔧 `docs/team-decisions.md` 10번 보완 목록 6~10번 ✅
- ⚠️ LLM `answer()` 반환이 `Answer(sentences, needs_more_evidence)`로 바뀜 (OpenAI 답변 JSON 형식도 변경)
- ⚠️ 상태에 `시간 초과` 추가, 남은 시간이 아주 적으면 검색 없이 답변
- 테스트 53 → 63개

### Q&A 그래프를 main에 합침 (결정 변경: Q&A에 LangGraph 사용) · `e7ab556`
- 🆕 `feat/qa-graph` 병합: `notree/graphs/qa/`, `notree/dev/qa_run.py`, `tests/test_qa_graph.py` (16개)
- 🔧 `docs/team-decisions.md` 10번: "LangGraph 안 씀" → **LangGraph로 구현**. 실험·외부 검토에서 나온 보완 목록은 그대로 두고, 그래프에 이미 반영된 것과 남은 것을 표시
- 기존 코드·명령 변경 없음. 새 명령 `uv run python -m notree.dev.qa_run`
- 테스트 37 → 53개

### 팀 결정 10번 보강: Q&A 외부 검토 반영 · `5fb69ab`
- 🔧 `docs/team-decisions.md` 10번에 Q&A 핸들러에 넣을 것 9가지 추가 (실패 시 본문 교체, 본문·인용 일치, 답변 불가 시 다음 출처, 점수 보정, 대화 맥락, 장애 구분, 시간 예산, `qa_context` 연결)
- 🐛 7번의 "임베딩 검색" 표현 수정: 계약상 `notes_search`는 pg_trgm. 검색 방식은 팀 확인 필요로 표시
- 코드 변경 없음

### Q&A는 LangGraph 없이 가기로 결정 · `470ca8c`
- 🔧 `docs/team-decisions.md` 10번 추가: Q&A는 설계서대로 tool calling. `feat/qa-graph` 브랜치는 합치지 않고 참고용으로 보관
- 🔧 같은 10번에 실험에서 확인한 것 중 구현 방식과 상관없이 Q&A 핸들러에 넣을 것 7가지 (근거 기준 분리, 코드로 인용 확인, 인용 기준 라벨 등)
- 코드 변경 없음
### [실험 브랜치 `feat/qa-graph`] 실제 LLM 실행 결과 반영: 근거 기준 분리 · `35ed1b7`
- 🐛 정답 근거를 찾고도 "근거 부족"이 되던 문제: 멈춤 기준(0.6)이 LLM에게 줄 근거까지 걸렀음.
  이제 `EVIDENCE_MIN`(0.3) 이상인 근거는 모두 LLM에게 주고, 라벨은 **LLM이 실제로 인용한 근거의 출처**로 정함
- 🔧 OpenAI 프롬프트: 검색어는 일반 단어 빼고 2~4개, 근거가 없으면 추측하지 말 것
- 🔧 `FakeQALLM(cite_min=0.6)`: 답이 되는 근거만 인용하는 흉내
- 🆕 테스트 2개 (총 53개)

### [실험 브랜치 `feat/qa-graph`] OpenAI JSON 모드 400 오류 수정 · `77c0325`
- 🐛 `--llm openai`에서 answer 단계가 400으로 실패: JSON 모드는 메시지에 'json'이라는 단어가 있어야 하는데 답변 프롬프트에 없었음.
  모든 요청에 "JSON으로만 답하라"는 system 메시지를 붙임
- 🆕 회귀 테스트 `test_openai_requests_always_mention_json` (가짜 클라이언트, 총 51개)

### [실험 브랜치 `feat/qa-graph`] Q&A 근거 탐색 그래프 · `5302e14`
자세히·merge 기준: [docs/changes/2026-10-15-qa-graph.md](docs/changes/2026-10-15-qa-graph.md). **main에는 아직 합치지 않음.**

- 🆕 `notree/graphs/qa/`: 노트 → 강의 전사 → 웹 순서로 근거를 찾고, 충분하면 멈추고, 마감이 가까우면 비싼 단계를 건너뛰고,
  근거에 없는 인용은 1번 다시 답하게 한 뒤 그래도 틀리면 "근거 부족"으로 저장하는 LangGraph. 체크포인트 없음
- 🆕 `notree/dev/qa_run.py`: 단계별 진행 출력 데모 (`--deadline-in`, `--bad-citation`, `--llm openai`, `--graph`)
- 🆕 `tests/test_qa_graph.py` 13개 (총 50개)
- 기존 코드·명령 변경 없음

### 실행 안정성 4건 수정 · `b6fe820`
자세히: [docs/changes/2026-10-15-execution-safety.md](docs/changes/2026-10-15-execution-safety.md)

- 🐛 같은 worker가 자기 실행 중인 메시지를 다시 가져와 중복 실행하던 문제: heartbeat마다 Redis 방치 시간 초기화(`XCLAIM … JUSTID`),
  `job_start`가 살아 있는 lease면 같은 worker도 거절, worker 내부 실행 중 목록
- 🐛 완료 기록 실패 후에도 ACK해 작업이 버려지던 문제: 완료·실패 표시를 최대 3번 시도하고 **확정 응답일 때만 ACK**
- 🐛 lease를 잃은 실행자의 permit으로 쓰기가 성공하던 문제: permit에 실행 세대(시도 번호), 쓰기 트랜잭션 안에서 소유자·세대를 DB와 대조
- 🐛 슬롯이 꽉 찬 상태에서 종료 신호를 못 보던 문제: 슬롯 대기와 종료 신호를 함께 기다림
- 🔧 `tests/test_robustness.py`의 xfail 표시 제거 → 6개 통과 (총 37개)
- 🔧 문서: `worker-flow.md`(한 바퀴·죽거나 느릴 때·알게 된 것), `worker-structure.md`, `team-decisions.md` 4번, 도구 계약 설명
- ⚠️ permit 형식에 `a`(실행 세대) 추가 → 이전 permit은 쓰기 도구에서 `permit_invalid`
- ⚠️ `job_start`가 같은 worker라도 살아 있는 lease면 `leased`
- ⚠️ `job_finish`·`job_fail`·`job_release`·`job_heartbeat`가 실행권(소유자·세대·running)이 없으면 `false`
- 이제 `notree/worker/`를 팀 저장소로 옮겨도 되는 조건(위 네 가지)이 충족됨

### 실행 안정성 회귀 테스트 추가 (아직 수정 전) · `2c85706`
외부 분석에서 지적된 네 가지 문제를 재현하는 테스트. 지금은 모두 실패하며, `xfail(strict=True)`로 표시해 둠.
고치면 그 테스트의 표시를 지운다 (안 지우면 strict 때문에 실패).

- 🆕 `tests/test_robustness.py` 6개
  1. 같은 worker가 자기 실행 중인 메시지를 `XAUTOCLAIM`으로 다시 가져와 중복 실행 (재현: 한 작업이 3번 시작됨)
  2. `job_finish` 기록 실패 후에도 ACK → DB는 running, Redis에선 사라져 작업이 버려짐
  3. lease를 잃은 이전 실행자(또는 이전 시도)의 permit으로 발행·체크포인트 저장이 성공함
  4. 슬롯이 꽉 찬 상태에서 종료 신호를 받으면 유예 시간이 시작되지 않고 작업 끝까지 기다림
- 이 네 가지가 고쳐지기 전에는 `notree/worker/`를 팀 저장소로 옮기지 않는다

### 저장소 정리: 코드를 notree/ 안으로 · `215a7b6`
루트에 있던 1차 PoC 파일을 정리했습니다. 정리 전 상태는 `archive/poc-v1` 브랜치에 그대로 있습니다 (`git checkout archive/poc-v1`).

- 🔧 `mcp_server.py` → `notree/mcp_server/server.py`
- 🔧 `graph.py`, `state.py`, `llm.py` → `notree/graphs/course_update/`
- 🔧 `run.py` → `notree/dev/graph_run.py`
- 🗑 `worker.py`, `scheduler.py`, `api.py`, `demo.py`, `settings.py`, `mcp_saver.py`, `tests/test_worker.py` (`notree/`의 같은 역할 코드로 대체)
- 🆕 `notree/dev/takeover.py`: 죽은 worker 인계 데모 (예전 `demo.py`가 하던 것)
- 🆕 `tests/test_takeover.py`: 죽음 인계 · 발행 직후 죽음 · 느린 worker 보호 (예전 `test_worker.py`를 옮김)
- 🆕 `SimulatedDeath`: 시험·데모용 worker 죽음. ACK·완료 표시·lease 반납 없이 worker가 끝남
- 🆕 `notree/dev/devenv.py`: 데모용 MCP 서버·Redis 준비 공통 코드
- 🔧 README를 새 구조에 맞게 다시 씀. `docs/worker-flow.md`, `worker-structure.md`, `team-decisions.md`의 경로·명령 갱신
- 🔧 `CLAUDE.md`: "코드는 notree/ 안에" 규칙 추가
- ⚠️ 실행 명령 변경

  | 이전 | 이후 |
  |---|---|
  | `uv run demo.py` | `uv run python -m notree.dev.takeover` |
  | `uv run run.py ...` | `uv run python -m notree.dev.graph_run ...` |
  | `uv run mcp_server.py --http` | `uv run python -m notree.mcp_server --http` |
  | `uv run scheduler.py` | `uv run python -m notree.scheduler.allocator` |
  | `uv run worker.py --name A` | `uv run python -m notree.worker --stream batch --name A` |
  | `uv run api.py ...` | `uv run python -m notree.dev.enqueue course_update sources=...` |
  | `uv run mcp dev mcp_server.py` | `uv run mcp dev notree/mcp_server/server.py` |
- ⚠️ import 경로 변경: `from graph import ...` → `from notree.graphs.course_update import ...`,
  `from mcp_saver import ...` → `from notree.worker.checkpointer import ...` / `from notree.worker.mcp_client import McpClient`
- 테스트 31개 (옮긴 3개 포함, 개수는 그대로)

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
