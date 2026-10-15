"""[실험 브랜치 feat/taskiq] worker 루프를 taskiq로 바꾼 버전.

notree/worker/loop.py의 '메시지 받기·동시 실행·종료 대기' 부분만 taskiq(RedisStreamBroker + Receiver)로 바꾸고,
작업 하나를 처리하는 규칙(job_start → 핸들러 → 완료 확정 뒤 ACK, lease, permit, 시도 한도)은 그대로 쓴다.
비교 결과는 docs/changes/2026-10-15-taskiq-trial.md.
"""
from notree.worker_tq.worker import TqWorker, make_broker, run_allocator_tq  # noqa: F401
