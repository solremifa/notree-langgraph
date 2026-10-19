"""벡터 검색 비교용 임베딩을 만든다 (오프라인 실험, worker·그래프와 무관).

    uv run python -m notree.dev.vec_embed --model text-embedding-3-small --out data/vec/oai-3-small   # OpenAI 키 필요
    uv run python -m notree.dev.vec_embed --fake --out data/vec/fake                                  # 키 없이 경로 점검용

**결과는 data/ 아래에만 둔다.** 임베딩은 강의 원문에서 나온 데이터라 원문과 같이 저장소에 올리지 않는다 (data/는 git 제외).
강의 문장을 임베딩 API로 보내는 것도 외부 처리다 — LLM 호출과 같은 동의 범위인지 팀에서 확인한 뒤 실행한다.

입력
  --docs data/qa_docs.jsonl            문서 (노트·전사, notree.dev.lecture_import 결과). 지금 분할 그대로 쓴다
  --old  data/qa_questions.json        기존 33(+범위 밖 1)개 질문
  --fresh data/fresh/questions.md      새 30개 질문 (질문만. 정답표는 읽지 않는다)

출력 (--out 폴더)
  meta.json     모델·차원·문서 수·문서 임베딩 시간과 토큰(초기 색인 비용)·질문별 임베딩 시간(질문 처리 시간의 일부)
  docs.npy      문서 벡터 (L2 정규화, float32) · doc_ids.json  같은 순서의 문서 ID
  questions.npy 질문 벡터 · question_ids.json  "old:ml01", "fresh:fresh01" 형식

질문은 실제 질문 처리처럼 한 번에 하나씩 임베딩해 시간을 잰다. 문서는 묶음으로 보낸다.
--fake는 문자 3-gram 해시 벡터다. 의미를 담지 않고 pg_trgm과 비슷하게 움직이므로 **비교 결과로 쓰면 안 된다** (경로 점검 전용).
"""
import argparse
import hashlib
import json
import os
import time
from pathlib import Path

import numpy as np

from notree.dev.qa_fresh_eval import parse_questions


def load_docs(path: str) -> list[dict]:
    return [json.loads(line) for line in open(path, encoding="utf-8")]


def load_questions(old: str, fresh: str) -> list[tuple[str, str]]:
    qs = [(f"old:{q['id']}", q["q"]) for q in json.load(open(old, encoding="utf-8"))["questions"]] if old else []
    if fresh:
        qs += [(f"fresh:{q['id']}", q["q"]) for q in parse_questions(fresh)]
    return qs


def normalize(m: np.ndarray) -> np.ndarray:
    n = np.linalg.norm(m, axis=1, keepdims=True)
    n[n == 0] = 1
    return (m / n).astype(np.float32)


class FakeEmbedder:
    """문자 3-gram 해시 (512차원). 경로 점검 전용 — 의미 검색이 아니다."""
    name, dim = "fake-char3-hash", 512

    def embed(self, texts: list[str]) -> tuple[np.ndarray, int]:
        m = np.zeros((len(texts), self.dim), dtype=np.float32)
        for i, t in enumerate(texts):
            s = f"  {t}  "
            for j in range(len(s) - 2):
                h = int.from_bytes(hashlib.blake2b(s[j:j + 3].encode(), digest_size=4).digest(), "little")
                m[i, h % self.dim] += 1.0
        return m, 0


class OpenAIEmbedder:
    def __init__(self, model: str) -> None:
        from openai import OpenAI
        self.client, self.name, self.dim = OpenAI(), model, None

    def embed(self, texts: list[str]) -> tuple[np.ndarray, int]:
        r = self.client.embeddings.create(model=self.name, input=texts)
        m = np.array([d.embedding for d in r.data], dtype=np.float32)
        self.dim = m.shape[1]
        return m, getattr(r.usage, "total_tokens", 0) or 0


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--docs", default="data/qa_docs.jsonl")
    ap.add_argument("--old", default="data/qa_questions.json")
    ap.add_argument("--fresh", default="data/fresh/questions.md")
    ap.add_argument("--model", default="text-embedding-3-small")
    ap.add_argument("--fake", action="store_true")
    ap.add_argument("--batch", type=int, default=100)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    out = Path(a.out)
    if not str(out.resolve()).startswith(str(Path("data").resolve())):
        raise SystemExit("--out은 data/ 아래여야 한다 (임베딩을 저장소에 올리지 않기 위해)")
    if not a.fake and not os.getenv("OPENAI_API_KEY"):
        raise SystemExit("OPENAI_API_KEY가 없다. 키 없이 경로만 볼 때는 --fake")
    emb = FakeEmbedder() if a.fake else OpenAIEmbedder(a.model)
    docs, qs = load_docs(a.docs), load_questions(a.old, a.fresh)
    out.mkdir(parents=True, exist_ok=True)

    t0, tokens, parts = time.perf_counter(), 0, []
    for i in range(0, len(docs), a.batch):
        m, tk = emb.embed([d["text"] for d in docs[i:i + a.batch]])
        parts.append(m)
        tokens += tk
        print(f"문서 {min(i + a.batch, len(docs))}/{len(docs)}")
    doc_sec = time.perf_counter() - t0
    dv = normalize(np.vstack(parts))

    qv, q_ms, q_tokens = [], [], 0
    for qid, text in qs:
        t1 = time.perf_counter()
        m, tk = emb.embed([text])
        q_ms.append(round((time.perf_counter() - t1) * 1000, 1))
        qv.append(m[0])
        q_tokens += tk
    qv = normalize(np.vstack(qv))

    np.save(out / "docs.npy", dv)
    np.save(out / "questions.npy", qv)
    json.dump([d["id"] for d in docs], open(out / "doc_ids.json", "w"), ensure_ascii=False)
    json.dump([q for q, _ in qs], open(out / "question_ids.json", "w"), ensure_ascii=False)
    meta = {"model": emb.name, "fake": a.fake, "dim": int(dv.shape[1]), "docs": len(docs), "questions": len(qs),
            "doc_seconds": round(doc_sec, 2), "doc_tokens": tokens, "question_tokens": q_tokens,
            "question_ms": dict(zip([q for q, _ in qs], q_ms)), "created": time.strftime("%Y-%m-%d %H:%M:%S")}
    json.dump(meta, open(out / "meta.json", "w"), ensure_ascii=False, indent=1)
    print(f"완료: {meta['model']} · {meta['dim']}차원 · 문서 {len(docs)}개 {doc_sec:.1f}초 · 토큰 {tokens} · "
          f"질문 {len(qs)}개 (질문당 중앙 {sorted(q_ms)[len(q_ms) // 2]}ms) → {out}")


if __name__ == "__main__":
    main()
