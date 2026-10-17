# 레인 비교 원본 (2026-10-16)

`lane_bench.jsonl`: `notree/dev/lane_bench.py` 결과 (리뷰 반영 후 집계, 시드 1·2·3).
요약과 해석은 [docs/changes/2026-10-16-lanes.md](../../changes/2026-10-16-lanes.md) "측정".

재현 (Redis 7 필요):

```bash
redis-server --port 6390 --save '' &
for s in 1 2 3; do
  for lr in 9 12; do uv run python -m notree.dev.lane_bench --live-rate $lr --qa-rate 2 --seed $s --json; done
  for b in 4 12; do uv run python -m notree.dev.lane_bench --live-rate 9 --designs borrow --borrow $b --seed $s --json; done
  uv run python -m notree.dev.lane_bench --live-rate 9 --alloc mixed,lanes --burst 2000 --designs borrow --seed $s --json
done > lane_bench.jsonl
```

DB·MCP는 가짜(큐·슬롯·배정 정책만), 시간은 10배 빠르게 돌린 뒤 실제 시간으로 환산합니다.
