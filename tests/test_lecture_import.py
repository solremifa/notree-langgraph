"""강의 노트 내보내기(.md) → 검색 문서 변환. 실제 강의 자료는 저장소에 없으므로 같은 구조의 작은 예시로 확인한다."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from notree.dev.lecture_import import parse_export, to_docs  # noqa: E402

EXPORT = """# 강의 노트

2026. 10. 1. 오전 11:02:09

다운로드 시점에 확정된 원문과 생성이 완료된 노트만 포함합니다.

## 최종 노트

강의 개요
- 표준화를 다뤘다.
핵심 개념·정의
- 평균과 표준편차는 train에서만 계산한다.

## 실시간 요약 노트

### 표준화 통계값은 Train에서 계산

청크 1

새 내용
- train 평균을 test에도 쓴다.

반복된 내용
- 없음

보완·정정
- 없음

전사 원문:
트레인에서 구한 평균을 테스트에도 그대로 씁니다.

### 짧은 발화

청크 2

새 내용
- 없음

반복된 내용
- 없음

전사 원문:
자.

## 원문 (한국어·영어)

트레인에서 구한 평균을 테스트에도 그대로 씁니다.
 자.
"""


def test_parse_export_maps_sections_to_notes_and_chunks(tmp_path):
    f = tmp_path / "ml-1001-am.md"
    f.write_text(EXPORT, encoding="utf-8")
    lec = parse_export(f)
    assert lec["course"] == "ml" and lec["lecture"] == "ml-1001-am"
    finals = [n for n in lec["notes"] if n["kind"] == "final"]
    assert [n["id"] for n in finals] == ["ml-1001-am:f1", "ml-1001-am:f2"]
    assert finals[1]["section"] == "핵심 개념·정의"
    live = [n for n in lec["notes"] if n["kind"] == "live"]
    assert [n["id"] for n in live] == ["ml-1001-am:s1", "ml-1001-am:s2"]
    assert live[0]["text"] == "표준화 통계값은 Train에서 계산. train 평균을 test에도 쓴다."
    assert live[1]["text"] == "짧은 발화."                              # '없음'은 빼고 제목만
    assert [(t["id"], t["chunk"]) for t in lec["transcripts"]] == [("ml-1001-am:t1", 1), ("ml-1001-am:t2", 2)]
    docs = to_docs(lec)
    assert {d["source"] for d in docs} == {"note", "transcript"} and len(docs) == 6
    assert all(d["course_id"] == "ml" and d["lecture_id"] == "ml-1001-am" for d in docs)
