"""프로세스들이 공유하는 설정. 환경변수로 덮어쓸 수 있다."""
import os

REDIS_URL = os.getenv("NOTREE_REDIS_URL", "redis://localhost:6379/0")
MCP_URL = os.getenv("NOTREE_MCP_URL", "http://127.0.0.1:8765/mcp")
GROUP = "workers"                                       # Redis consumer group
BATCH_STREAM = "notree:batch"                           # worker-batch가 읽는 Stream
LEASE_SEC = float(os.getenv("NOTREE_LEASE_SEC", "4"))   # 설계서 60초 (데모용으로 짧게)
HEARTBEAT_SEC = LEASE_SEC / 3                           # 설계서 20초
CLAIM_IDLE_MS = int(os.getenv("NOTREE_CLAIM_IDLE_MS", "6000"))  # 이보다 오래 ACK 없는 메시지는 재배송 (lease보다 길게)
