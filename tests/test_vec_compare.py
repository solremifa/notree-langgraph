"""벡터 비교 스크립트의 순수 함수 (DB·API 없이). 실제 강의 데이터는 쓰지 않는다."""
import numpy as np
import pytest

pytest.importorskip("asyncpg")
from notree.dev.vec_compare import first_rank, is_gold, items_covered, rrf, tie_at  # noqa: E402
from notree.dev.vec_embed import FakeEmbedder, normalize  # noqa: E402


def fresh_q(items):
    its = [set(i) for i in items]
    return {"gold_docs": set().union(*its), "gold_chunks": None, "items": its}


def test_rrf_rewards_agreement_and_ignores_raw_scores():
    a = [("x", 0.9), ("y", 0.8), ("z", 0.1)]
    b = [("y", 100.0), ("w", 50.0), ("x", 1.0)]
    order = [d for d, _ in rrf(a, b)]
    assert order[0] == "y"                     # 두 목록에서 모두 상위
    assert set(order) == {"x", "y", "z", "w"}
    assert all(s < 1 / 30 for _, s in rrf(a, b))   # 점수는 1/(60+순위) 합 — 0.3·0.6 기준과 비교할 값이 아니다


def test_fresh_gold_counts_summary_note_as_same_segment():
    q = fresh_q([["ml-1:t3"]])
    assert is_gold(q, "ml-1:s3") and is_gold(q, "ml-1:t3") and not is_gold(q, "ml-1:t4")


def test_old_gold_by_chunk_and_negative_never_gold():
    q = {"gold_docs": None, "gold_chunks": {"ml-1": [2]}}
    assert is_gold(q, "ml-1:s2") and is_gold(q, "ml-1:t2") and not is_gold(q, "ml-1:f1")
    assert not is_gold({"gold_docs": None, "gold_chunks": {}}, "ml-1:t2")


def test_item_coverage_uses_union_of_sources_at_k():
    q = fresh_q([["a:t1"], ["a:t9"]])
    lists = {"note": [("a:s1", 0.9), ("a:s2", 0.8)], "transcript": [("a:t5", 0.7), ("a:t9", 0.6)]}
    assert items_covered(q, lists, 1) == (1, 2)
    assert items_covered(q, lists, 2) == (2, 2)


def test_tie_at_boundary_is_flagged():
    q = fresh_q([["a:t2"]])
    tied = [("a:t1", 0.5), ("a:t2", 0.5), ("a:t3", 0.5)]
    assert first_rank(q, tied) == 2
    assert tie_at(q, tied, 2) and tie_at(q, tied, 1)
    assert not tie_at(q, [("a:t1", 0.9), ("a:t2", 0.5), ("a:t3", 0.4)], 2)


def test_fake_embedder_is_normalized_and_deterministic():
    e = FakeEmbedder()
    m1, _ = e.embed(["선형 회귀", "객체"])
    m2, _ = e.embed(["선형 회귀", "객체"])
    v = normalize(m1)
    assert np.allclose(m1, m2) and np.allclose(np.linalg.norm(v, axis=1), 1)
