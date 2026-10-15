# 2026-10-15 변경 정리: Q&A 그래프 보완 5가지

외부 검토에서 나온 문제 중 그래프 코드만으로 고칠 수 있는 다섯 가지를 고쳤습니다. 팀 결정 10번 보완 목록 6~10번입니다.
코드: `notree/graphs/qa/graph.py`, `llm.py`, `retriever.py`, `state.py`. 테스트: `tests/test_qa_graph.py` (17 → 26개).

## 한눈에

| # | 문제 (검토에서 재현) | 고친 방법 | 테스트 |
|---|---|---|---|
| 6 | 인용 검증이 끝내 실패하면 라벨만 "근거 부족"으로 바뀌고 **틀린 본문이 저장됨** | 본문·인용을 버리고 정해진 안내문 `INSUFFICIENT_TEXT`로 저장 | `test_bad_citation_twice_ends_as_insufficient` |
| 7 | 본문에는 `[x9]`, 인용 목록에는 `n1` → 둘이 어긋나도 통과 | LLM은 문장별 `{text, citations}`만 냄. 본문의 `[ID]`는 코드(`render`)가 붙이고, LLM이 넣은 표시는 지움 | `test_render_builds_markers_from_citations`, `test_openai_requests_always_mention_json` |
| 8 | 노트 점수 0.95("스택은 LIFO")인데 질문은 "오버플로 원인" → 노트에서 멈추고 근거 부족 | LLM이 `needs_more_evidence`를 내면 남은 출처로 다시 검색. LLM 호출은 실행당 4번까지 | `test_high_score_but_unanswerable_goes_to_next_source`, `test_llm_call_cap_stops_the_chain` |
| 9 | 노트 검색 타임아웃 하나로 그래프 전체가 예외로 끝남 | 일시 장애·타임아웃 → 1번 다시, 안 되면 다음 출처. 권한 없음·삭제 → `QAFatal` | `test_transient_search_error_is_retried_once`, `test_repeated_search_error_moves_to_next_source`, `test_forbidden_search_stops_the_graph`, `test_slow_search_times_out_and_moves_on` |
| 10 | 마감이 지나도 답변 생성·저장까지 진행, 개별 호출에 시간 제한 없음 | 단계별 시간 예산 (아래) | `test_slow_llm_answer_saves_timeout_message`, `test_expired_deadline_does_not_call_llm`, `test_no_search_when_only_answer_time_is_left` |

## 바뀐 흐름

```
understand → search_note ─(충분)→ answer → verify ─(통과)──────────────→ save
                  └(부족)→ search_transcript → …          ├(인용 문제·재시도 남음)→ answer
                                                         └(답할 수 없음·남은 출처·시간)→ 다음 search_*   ← 새 경로
```

- 다음 출처로 갈 때 "다시 답하기" 횟수는 새로 셉니다 (출처 묶음마다 인용 재시도 1번).
- 웹은 여전히 최대 1번입니다. 다음 출처를 고르는 함수(`next_source`)가 이미 찾은 출처를 건너뜁니다.

## 시간 예산 (`deadline_at` 기준)

| 값 | 기본 | 뜻 |
|---|---|---|
| `SEARCH_TIMEOUT_SEC` | 8초 | 검색 한 번의 최대 시간 |
| `ANSWER_RESERVE_SEC` | 8초 | 검색할 때 답변용으로 남겨 두는 시간. 이만큼 남지 않으면 검색을 하지 않음 |
| `LLM_TIMEOUT_SEC` | 25초 | LLM 호출 한 번의 최대 시간 |
| `SAVE_RESERVE_SEC` | 2초 | LLM 호출 때 저장용으로 남겨 두는 시간 |
| `MIN_ANSWER_SEC` | 3초 | 이보다 적게 남으면 LLM을 부르지 않고 "시간 초과" 안내문 |
| `MAX_LLM_CALLS` | 4 | 실행 하나의 LLM 호출 상한 (understand 1 + answer 3) |

- 질문 이해(understand)가 시간이 모자라거나 시간 초과가 나면, LLM 대신 단어 추출로 검색어를 만들고 "관련 있음"으로 보고 찾습니다.
- 답변이 시간 안에 안 나오면 상태 `시간 초과`, 본문은 `TIMEOUT_TEXT`.
- 전사 10초·웹 25초 기준은 그대로이고, 위 예산과 함께 봅니다.

## 깨지는 변경

| 무엇 | 이전 | 이후 |
|---|---|---|
| LLM `answer()` 반환 | `Answer(answer, citations)` | `Answer(sentences=[Sentence(text, citations)], needs_more_evidence)` |
| OpenAI 답변 JSON | `{"answer", "citations"}` | `{"sentences": [{"text", "citations"}], "needs_more_evidence"}` |
| 상태 `status` | `completed` / `근거 부족` | 여기에 `시간 초과` 추가 |
| 남은 시간 5초로 시작 | 노트는 검색 | 검색 없이 답변만 (답변용 시간을 남김) |
| 검색 오류 | 그대로 예외 | `SearchUnavailable`·`TimeoutError`는 흡수, `SearchForbidden` → `QAFatal` |

## 확인한 것 / 못 한 것

- 가짜 LLM·가짜 검색으로 분기별 테스트 26개, 전체 63개 통과. `notree.dev.qa_run` 기본 질문 4개 결과는 그대로.
- **바뀐 OpenAI 프롬프트(문장별 JSON, `needs_more_evidence`)는 실제 모델로 아직 돌려 보지 않았습니다.**
  `uv run python -m notree.dev.qa_run --llm openai`로 확인이 필요합니다.
- 시간 예산의 숫자는 실측 전 값입니다. 실제 검색·LLM 지연을 재 보고 조정해야 합니다.

## 2차 검토 반영 (같은 날)

`9d470e2` 기준 외부 검토에서 재현된 네 가지를 고쳤습니다. 먼저 재현 테스트를 쓰고 실패를 확인한 뒤 고쳤습니다.

| # | 문제 | 고친 방법 | 테스트 |
|---|---|---|---|
| 1 | 인용 표시를 지우는 정규식이 **코드·수식을 훼손** (`a = [1, 2, 3]` → `a =`) | 인용 표시를 `[^ID]` 문법으로 정함. 지우는 것은 `[^..]`와 이번 근거 ID와 정확히 같은 `[ID]`뿐 | `test_render_keeps_code_and_math_brackets`, `test_render_strips_only_citation_markers` |
| 2 | LLM이 "근거가 더 필요"라고 했는데 더 찾을 곳이 없어도 `completed` / 인용 없는 과목 답변이 본문째 저장 | 상태를 라벨과 분리: `부분 답변` 추가. 과목 질문에 인용이 없으면 본문을 버리고 안내문 | `test_partial_answer_is_not_saved_as_completed`, `test_uncited_course_answer_is_replaced_by_notice` |
| 3 | "웹 최대 1번"인데 장애 시 실제 요청은 2번 | 웹은 다시 요청하지 않음 (`SEARCH_RETRIES`: 노트 1, 전사 1, 웹 0) | `test_web_search_is_requested_at_most_once_even_on_error` |
| 4 | `job_retry`가 시도 번호를 0으로 되돌려, **재시도 전 permit이 새 실행에서 통함** (공통 worker 문제) | 되돌리지 않는 실행 세대 `exec_epoch`를 따로 두고 permit(`x`)과 실행권 확인에 사용 | `test_permit_from_before_user_retry_is_rejected` |

- LLM이 지어낸 모르는 ID(`[x9]`)가 본문에 남으면 지우지 않습니다. 코드·수식과 구분할 수 없어서입니다. 프롬프트에서 인용 표시를 쓰지 말라고 하고, 화면은 `citations`로 출처를 보여 주는 것을 전제로 합니다.

### 깨지는 변경

| 무엇 | 이전 | 이후 |
|---|---|---|
| 본문의 인용 표시 | `[n1]` | `[^n1]` |
| 상태 | completed / 근거 부족 / 시간 초과 | 여기에 `부분 답변` 추가 |
| 인용 없는 과목 답변 | LLM 본문 그대로 + 근거 부족 | 본문을 안내문으로 교체 |
| 웹 검색 장애 | 1번 다시 요청 | 다시 요청하지 않고 건너뜀 |
| permit | `{j, w, a, u, e}` | `{j, w, a, x, u, e}`. 실행권 확인은 `x`(실행 세대). 이전 permit은 쓰기 도구에서 `permit_invalid` |

## 3차 검토 반영: 결과 계약 (같은 날)

화면·API 공유 문서(Q&A 답변 결과 형식과 화면 표시)를 검토받고 코드를 맞췄습니다.

- 🐛 인용 표시 정제가 `answer`에만 적용되고 `sentences[].text`에는 LLM이 넣은 `[n1]`, `[^x9]`가 남던 문제 → 문장을 먼저 정제(`clean_sentences`)하고 `answer`는 정제된 문장에서 파생
- 🆕 `notice_code`: 안내문을 보일 때 `not_found` / `insufficient` / `timeout`, 아니면 `None`. 화면은 이 코드로 안내문을 고른다
- 🆕 `sources`: 인용된 근거만 `{id, source, text, ref}`. 화면·저장용. 전체 `hits`는 내부 처리용
- 결과 상태(`status`)는 작업 실행 상태(done/failed)와 별개. 시간 초과·근거 부족도 그래프는 정상 종료하므로 작업은 `done`이 된다 → "다시 묻기"는 `job_retry`가 아니라 새 Q&A 작업

테스트 `test_sentences_text_is_cleaned_like_answer`, `test_sources_hold_only_cited_hits_and_notice_code_is_set`.
