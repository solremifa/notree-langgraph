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
