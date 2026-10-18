"""새 질문 평가 도구의 입력 해석 (실제 질문·정답 파일은 저장소 밖 data/에 있음)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from notree.dev.qa_fresh_eval import chunk, parse_key, parse_questions, score  # noqa: E402

Q = """# 질문

## 머신러닝

fresh01. 과적합은 어떻게 알아요?

## OOP

fresh21. abstract 클래스는 왜 써요?
"""
KEY = """# 정답

## fresh01 · completed

과적합은 어떻게 알아요?

1. validation loss가 다시 오른다.
   근거: fresh-v1/ml-a:f5, fresh-v1/ml-a:t3

2. 학습을 멈춘다.
   근거: fresh-v1/ml-a:t4

- 채점 주의: 수치는 자료에 없다.

## fresh21 · 근거 부족

abstract 클래스는 왜 써요?

- 자료로 확인 불가: 없음.
"""


def test_parse_and_score(tmp_path):
    (tmp_path / "q.md").write_text(Q, encoding="utf-8")
    (tmp_path / "k.md").write_text(KEY, encoding="utf-8")
    qs = parse_questions(str(tmp_path / "q.md"))
    assert [(q["id"], q["course"]) for q in qs] == [("fresh01", "ml"), ("fresh21", "oop")]
    k = parse_key(str(tmp_path / "k.md"))
    assert k["fresh01"]["status"] == "completed" and k["fresh01"]["items"][0]["gold"] == ["ml-a:f5", "ml-a:t3"]
    assert k["fresh01"]["notes"] == ["수치는 자료에 없다."] and k["fresh21"]["status"] == "근거 부족"
    assert chunk("ml-a:s3") == "ml-a:t3" and chunk("ml-a:f5") == "ml-a:f5"
    row = {"attempts": [{"evidence": ["ml-a:s3", "x:t1"]}, {"evidence": ["ml-a:t4"]}],
           "citations": ["ml-a:s3"], "status": "부분 답변"}
    s = score(row, k["fresh01"])                     # 요약 노트 s3 = 전사 t3과 같은 근거
    assert s["items"] == [{"first": True, "passed": True, "cited": True}, {"first": False, "passed": True, "cited": False}]
    assert not s["status_ok"]
