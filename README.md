# notree-langgraph

Notree **과목 노트 업데이트**와 같은 모양의 LangGraph를 hello world 입출력으로 돌려 보는 PoC.
실제 노트 대신 단어를 쓰고, 실제 DB/MCP 대신 `out/` 폴더의 파일을 씁니다.

```
snapshot ─┬─ extract(hello) ─┐
          └─ extract(world) ─┴─ merge ─ validate ─┬─ publish ─ END
                               ▲                  ├─ 문제 + 1차 → merge 재시도
                               └──────────────────┘
                                                  └─ 문제 + 2차 → needs_review ─ publish
```

| 실제 설계 | 여기서 |
|---|---|
| 출처 버전 고정 + 편집 잠금 | `snapshot`: 강의 목록 고정 |
| 강의별 핵심 추출 (LLM) | `extract`: `Send`로 강의 수만큼 병렬, 결과는 reducer로 합침 |
| merge · validate | 단어를 한 줄로 합치고, 빠진 단어가 있으면 재시도 → 그래도 있으면 '확인 필요' |
| MCP `note.publish` + 발행 게이트 | `out/published.json`에 멱등키(= thread_id)로 1회만 저장 |
| PG checkpoint (sync) | `out/checkpoints.sqlite` (`SqliteSaver`, `durability="sync"`) |
| operation_id | `--thread` |

## 설치 (Windows PowerShell 기준)

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## 돌려 보기

```powershell
python run.py                                   # 정상 경로
python run.py --sloppy once                     # validate 실패 → merge 재시도 분기
python run.py --sloppy always                   # 재시도 한도 → 확인 필요 분기
python run.py --lectures a,b,c,d                # extract 4개 병렬

# worker가 죽었다가 다른 worker가 이어받는 상황
python run.py --thread op1 --lectures hello,big,world --crash-at extract:world
python run.py --thread op1 --resume             # 끝난 extract 2개는 다시 안 부름 (LLM 호출 수로 확인)

# 발행 커밋 직후 죽는 상황
python run.py --thread op2 --crash-at publish
python run.py --thread op2 --resume             # "중복 발행 막음"

python run.py --thread op1 --history            # 체크포인트 이력
python run.py --graph                           # Mermaid 그래프 → out/graph.mmd (mermaid.live 에 붙여 넣기)
pytest -q                                       # 시나리오 테스트 5개
```

출력에서 `💾 checkpoint step=N 다음=[...]` 줄이 LangGraph가 단계(super-step)마다 상태를 저장하는 지점입니다.
`다음=['extract', 'extract']`처럼 같은 노드가 여러 번 보이면 병렬 실행입니다.

## 실제 OpenAI로

```powershell
$env:OPENAI_API_KEY="sk-..."
$env:NOTREE_MODEL="gpt-4o-mini"                  # 생략 시 gpt-4o-mini
python run.py --llm openai
```

실제 모델은 문장을 자유롭게 바꾸기 때문에 validate에서 걸려 재시도·'확인 필요' 분기가 자연스럽게 나올 수 있습니다.
`--sloppy`는 가짜 LLM에만 적용됩니다. 키를 쓰는 동안 `--crash-at`도 그대로 쓸 수 있습니다.

## 파일

- `state.py` — 그래프 상태 (`extracts`는 `operator.add` reducer)
- `graph.py` — 노드, 분기, `build_graph()`
- `llm.py` — FakeLLM / OpenAI 교체 지점, 호출 기록 `CALLS`
- `run.py` — CLI, 스트리밍 출력, 이어하기
- `tests/test_flow.py` — 정상 / 재시도 / 확인 필요 / 중단 후 이어하기 / 중복 발행 방지

## 아직 안 한 것 (실제 설계와의 차이)

- 체크포인트를 MCP `checkpoint.put/get`으로 보내는 저장기 (계획서의 A안)
- 편집 lease 60초 · heartbeat 20초, Redis 재배송
- 세대(generation) 비교로 삭제된 출처를 막는 발행 게이트
