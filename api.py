"""api 역할: 과목 노트 업데이트 요청을 받아 의도(intent)만 남긴다.

    uv run api.py --lectures hello,big,world
"""
import argparse
import asyncio

import settings as S
from graph import ACTOR, log
from mcp_saver import McpClient


async def request_course_update(mcp: McpClient, lectures: list[str], llm: str = "fake") -> str:
    ACTOR.set("api")
    job = await mcp.tool("job_create", kind="course_update", user_id="demo-user",
                         payload={"course": "자료구조", "sources": lectures, "llm": llm})
    log("request", f"과목 노트 업데이트 요청 → 의도 기록 job={job['job_id']}  sources={lectures}")
    return job["job_id"]


async def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--lectures", default="hello,big,world")
    p.add_argument("--llm", choices=["fake", "openai"], default="fake")
    a = p.parse_args()
    async with McpClient(url=S.MCP_URL) as mcp:
        print(await request_course_update(mcp, a.lectures.split(","), a.llm))


if __name__ == "__main__":
    asyncio.run(main())
