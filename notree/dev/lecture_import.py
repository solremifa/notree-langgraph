"""Notree 강의 노트 내보내기(.md)를 검색용 문서로 바꾼다.

    uv run python -m notree.dev.lecture_import data/lectures/*.md --out data/qa_docs.jsonl
    uv run python -m notree.dev.lecture_import data/lectures/ml-1001-am.md --show      # 구간 제목만 훑기

파일 이름이 `<과목>-<강의>.md`면 과목 id = `<과목>`, 강의 id = 파일 이름 (예: ml-1001-am.md → ml, ml-1001-am).

내보내기 구조 → 문서
  ## 최종 노트          의 "- " 항목 하나    → note  (id <강의>:f<번호>, kind=final)
  ## 실시간 요약 노트   의 ### 구간 하나     → note  (id <강의>:s<청크>, kind=live, 제목 + '새 내용' 항목)
                         그 구간의 '전사 원문' → transcript (id <강의>:t<청크>)
  실시간 요약 구간 N과 전사 N은 같은 발화 → 질문의 정답을 '청크 번호'로 정하면 노트·전사 양쪽에 쓸 수 있다.
  ## 원문 은 전사 원문을 이어 붙인 것이라 따로 넣지 않는다.

원문 파일은 교수님 강의 전사·수업 중 대화를 담고 있어 저장소에 넣지 않는다 (data/는 .gitignore).
"""
import argparse
import json
import re
from pathlib import Path


def _section(text: str, start: str, end: str | None = None) -> str:
    if start not in text:
        return ""
    body = text.split(start, 1)[1]
    return body.split(end, 1)[0] if end and end in body else body


def parse_export(path: str | Path) -> dict:
    p = Path(path)
    text = p.read_text(encoding="utf-8")
    lecture = p.stem
    course = lecture.split("-", 1)[0]
    lines = text.splitlines()
    title = lines[0].lstrip("# ").strip() if lines else lecture
    date = lines[2].strip() if len(lines) > 2 else ""

    final = _section(text, "## 최종 노트", "## 실시간 요약 노트")
    notes = []
    head = ""
    for ln in final.splitlines():
        s = ln.strip()
        if s.startswith("- "):
            notes.append({"id": f"{lecture}:f{len(notes) + 1}", "kind": "final", "section": head, "text": s[2:].strip()})
        elif s and not s.startswith("완료된 최종 노트가 없습니다"):
            head = s                                                   # "강의 개요", "핵심 개념·정의" 같은 소제목

    live = _section(text, "## 실시간 요약 노트", "## 원문")
    transcripts = []
    for block in re.split(r"^### ", live, flags=re.M)[1:]:
        sec_title, _, rest = block.partition("\n")
        m = re.search(r"청크\s*(\d+)", rest)
        if not m:
            continue
        chunk = int(m.group(1))
        new = _section(rest, "새 내용", "반복된 내용")
        bullets = [b.strip()[2:].strip() for b in new.splitlines() if b.strip().startswith("- ")]
        bullets = [b for b in bullets if b and b != "없음"]
        notes.append({"id": f"{lecture}:s{chunk}", "kind": "live", "chunk": chunk, "section": sec_title.strip(),
                      "text": " ".join([sec_title.strip() + "."] + bullets)})
        raw = _section(rest, "전사 원문:").strip()
        if raw:
            transcripts.append({"id": f"{lecture}:t{chunk}", "chunk": chunk, "text": raw})
    return {"course": course, "lecture": lecture, "title": title, "date": date, "notes": notes, "transcripts": transcripts}


def to_docs(lec: dict) -> list[dict]:
    """qa_seed·PG 적재용 한 줄짜리 문서들."""
    base = {"course_id": lec["course"], "lecture_id": lec["lecture"]}
    return ([{**base, "source": "note", "id": n["id"], "kind": n["kind"], "text": n["text"], "ref": n["id"]} for n in lec["notes"]]
            + [{**base, "source": "transcript", "id": t["id"], "kind": "transcript", "text": t["text"], "ref": t["id"]}
               for t in lec["transcripts"]])


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("files", nargs="+")
    ap.add_argument("--out", default="")
    ap.add_argument("--show", action="store_true", help="구간 번호와 제목만 출력")
    a = ap.parse_args()
    docs = []
    for f in a.files:
        lec = parse_export(f)
        finals = sum(n["kind"] == "final" for n in lec["notes"])
        print(f"{lec['lecture']:<14} {lec['title'][:20]:<20} {lec['date']:<26} 최종 노트 {finals:3} · 요약 구간 {len(lec['notes']) - finals:3} · "
              f"전사 {len(lec['transcripts']):3} ({sum(len(t['text']) for t in lec['transcripts']):,}자)")
        if a.show:
            for n in lec["notes"]:
                print(f"   {n['id']:<18} {n['text'][:110]}")
        docs += to_docs(lec)
    if a.out:
        Path(a.out).parent.mkdir(parents=True, exist_ok=True)
        with open(a.out, "w", encoding="utf-8") as f:
            for d in docs:
                f.write(json.dumps(d, ensure_ascii=False) + "\n")
        print(f"문서 {len(docs)}개 → {a.out}")


if __name__ == "__main__":
    main()
