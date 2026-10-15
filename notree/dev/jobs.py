"""작업 조회·되살리기 (api 역할 흉내).

    uv run python -m notree.dev.jobs get op-1234abcd
    uv run python -m notree.dev.jobs retry op-1234abcd       # 사용자의 '다시 시도' (팀 결정 7번)
    uv run python -m notree.dev.jobs retry op-1234abcd --deadline-in 90   # 마감 있던 작업은 새 마감과 함께
"""
import argparse
import asyncio
import json
import time

from notree.common import settings as S
from notree.worker.mcp_client import McpClient


async def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("action", choices=["get", "retry"])
    p.add_argument("job_id")
    p.add_argument("--deadline-in", type=float, default=None, help="retry 때 새 마감 (지금부터 N초 뒤)")
    a = p.parse_args()
    async with McpClient(url=S.MCP_URL) as mcp:
        if a.action == "get":
            res = await mcp.tool("job_get", job_id=a.job_id)
        else:
            deadline = time.time() + a.deadline_in if a.deadline_in is not None else None
            res = await mcp.tool("job_retry", job_id=a.job_id, deadline_at=deadline)
        print(json.dumps(res, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
